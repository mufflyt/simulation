suppressMessages({library(dplyr); library(tidyr)})
SCR  <- "/private/tmp/claude-501/-Users-tylermuffly-simulation/1f034148-4385-41e2-9828-bd0c2abe4fc4/scratchpad"
ROOT <- path.expand("~/simulation"); OUT <- file.path(ROOT, "inst/extdata/provider_year")
source(file.path(ROOT, "data-raw/provider_year_activity/hcpcs_urogyn_taxonomy.R"))
panel <- readRDS(file.path(SCR, "panel.rds")); YEARS <- sort(unique(panel$year))
DEFS <- c("d0_certified","d1_any_partb","d2_meaningful_partb","d3_multisource","d4_high_confidence")

# ---- A. definition comparison ----------------------------------------------
cmp <- lapply(DEFS, function(d) {
  panel |> group_by(year) |> summarise(
    definition = d,
    estimable = !all(is.na(.data[[d]])),
    active_headcount = if (all(is.na(.data[[d]]))) NA_integer_ else sum(.data[[d]], na.rm = TRUE),
    roster_n = n(),
    n_with_any_observed_evidence = sum(ev_partb_any | ev_openpay_any),
    pct_with_any_observed_evidence = round(100 * mean(ev_partb_any | ev_openpay_any), 1),
    n_with_no_observed_evidence = sum(!(ev_partb_any | ev_openpay_any)),
    n_differs_from_d0 = if (all(is.na(.data[[d]]))) NA_integer_ else sum(.data[[d]] != d0_certified, na.rm = TRUE),
    pct_of_d0 = if (all(is.na(.data[[d]]))) NA_real_ else round(100 * sum(.data[[d]], na.rm = TRUE) / pmax(sum(d0_certified), 1), 1),
    svc_median_among_active = median(ev_partb_services[which(.data[[d]])]),
    svc_q25_among_active = quantile(ev_partb_services[which(.data[[d]])], .25),
    svc_q75_among_active = quantile(ev_partb_services[which(.data[[d]])], .75),
    pct_female_among_active = round(100 * mean(gender[which(.data[[d]])] == "F", na.rm = TRUE), 1),
    pct_male_among_active = round(100 * mean(gender[which(.data[[d]])] == "M", na.rm = TRUE), 1),
    median_cert_year_among_active = median(cert_year[which(.data[[d]])], na.rm = TRUE),
    median_yrs_since_cert_among_active = median((year - cert_year)[which(.data[[d]])], na.rm = TRUE),
    pct_abog_among_active = round(100 * mean(board_pathway[which(.data[[d]])] == "ABOG"), 1),
    .groups = "drop")
}) |> bind_rows() |> arrange(definition, year) |>
  select(definition, year, everything())
write.csv(cmp, file.path(OUT, "provider_year_activity_definition_comparison.csv"), row.names = FALSE)

# ---- C. case mix ------------------------------------------------------------
lines <- arrow::read_parquet(file.path(SCR, "roster_partb_lines.parquet")) |>
  mutate(npi = as.character(npi)) |> filter(year %in% YEARS)
drugs <- lines |> filter(drug_ind == "Y") |> group_by(year, hcpcs, hcpcs_desc) |>
  summarise(n_providers = n_distinct(npi), total_units = sum(tot_srvcs, na.rm = TRUE),
            .groups = "drop") |> arrange(year, desc(total_units))
write.csv(drugs, file.path(OUT, "provider_year_drug_units.csv"), row.names = FALSE)
lines <- lines |> filter(drug_ind != "Y") |>
  mutate(service_category = classify_hcpcs(hcpcs))
d1 <- panel |> filter(d1_any_partb %in% TRUE) |> select(npi, year)
mix <- lines |> semi_join(d1, by = c("npi","year")) |>
  group_by(year, service_category) |>
  summarise(n_providers_billing = n_distinct(npi), n_hcpcs_codes = n_distinct(hcpcs),
            total_services = sum(tot_srvcs, na.rm = TRUE),
            total_allowed_usd = round(sum(tot_srvcs * avg_allowed, na.rm = TRUE)),
            median_services_per_billing_provider = median(tapply(tot_srvcs, npi, sum)),
            .groups = "drop") |>
  group_by(year) |> mutate(
    pct_of_year_services = round(100 * total_services / sum(total_services), 2),
    pct_of_d1_active_providers = round(100 * n_providers_billing /
                                       n_distinct(d1$npi[d1$year == first(year)]), 1)) |>
  ungroup() |> arrange(year, desc(total_services))
write.csv(mix, file.path(OUT, "provider_year_case_mix.csv"), row.names = FALSE)

cat("=== A: active headcount by definition and year ===\n")
print(cmp |> select(year, definition, active_headcount) |>
      pivot_wider(names_from = definition, values_from = active_headcount) |> as.data.frame(),
      row.names = FALSE)
cat("\n=== A: median [IQR] Part B services among active ===\n")
print(cmp |> filter(year %in% c(2013, 2018, 2023)) |>
      transmute(year, definition, active = active_headcount,
                svc = sprintf("%.0f [%.0f-%.0f]", svc_median_among_active,
                              svc_q25_among_active, svc_q75_among_active),
                pct_F = pct_female_among_active, pct_ABOG = pct_abog_among_active,
                med_yrs_since_cert = median_yrs_since_cert_among_active) |>
      as.data.frame(), row.names = FALSE)
cat("\n=== C: case mix 2023 ===\n")
print(mix |> filter(year == 2023) |>
      select(service_category, n_providers_billing, total_services,
             pct_of_year_services, pct_of_d1_active_providers,
             median_services_per_billing_provider) |> as.data.frame(), row.names = FALSE)
