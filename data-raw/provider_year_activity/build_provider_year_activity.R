# Provider-year activity measurement development for the URPS roster.
#
# Builds three artifacts under inst/extdata/provider_year/:
#   A  provider_year_activity_definition_comparison.csv
#   B  provider_year_activity_long.csv
#   C  provider_year_case_mix.csv
#
# Design rule: RAW evidence flags (ev_*) are stored separately from DERIVED
# definitions (d0_*..d4_*). No definition is treated as truth; D0-D4 are
# competing estimands reported side by side.

suppressMessages({library(dplyr); library(tidyr)})
SCR  <- "/private/tmp/claude-501/-Users-tylermuffly-simulation/1f034148-4385-41e2-9828-bd0c2abe4fc4/scratchpad"
ROOT <- path.expand("~/simulation")
OUT  <- file.path(ROOT, "inst/extdata/provider_year")
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)
source(file.path(ROOT, "data-raw/provider_year_activity/hcpcs_urogyn_taxonomy.R"))

YEARS <- 2013:2023
# --- prespecified before any backtest was run (see ledger) -------------------
D2_MIN_SERVICES <- 100    # ~2 services/week
D2_MIN_BENES    <- 11     # CMS cell-suppression floor

roster <- read.csv(file.path(ROOT, "data-raw/urps_roster/urps_roster_2026-07-22.csv"),
                   colClasses = "character") |>
  transmute(npi, board_pathway, gender,
            state_roster = state,
            cert_year = as.integer(cert_year),
            last_confirmed_active_year = suppressWarnings(as.integer(last_confirmed_active_year))) |>
  distinct(npi, .keep_all = TRUE)

lines <- arrow::read_parquet(file.path(SCR, "roster_partb_lines.parquet")) |>
  mutate(npi = as.character(npi))

# ---- provider-year Part B evidence -----------------------------------------
# NOTE: Tot_Benes is per HCPCS line; summing it across lines double-counts
# patients. max() over lines is a true LOWER BOUND on distinct beneficiaries and
# is what the D2 beneficiary criterion uses. The line sum is retained but named
# so it can never be mistaken for a patient count.
pb <- lines |>
  filter(year %in% YEARS, drug_ind != "Y") |>
  group_by(npi, year) |>
  summarise(ev_partb_lines = n(),
            ev_partb_services = sum(tot_srvcs, na.rm = TRUE),
            ev_partb_benes_line_sum = sum(tot_benes, na.rm = TRUE),
            ev_partb_max_line_benes = max(tot_benes, na.rm = TRUE),
            ev_partb_hcpcs_n = n_distinct(hcpcs),
            ev_partb_allowed = sum(tot_srvcs * avg_allowed, na.rm = TRUE),
            ev_partb_state = names(sort(table(state), decreasing = TRUE))[1],
            ev_partb_type = names(sort(table(prvdr_type), decreasing = TRUE))[1],
            .groups = "drop")

# ---- Open Payments (independent, per-year; NPI published only from 2015) ----
op_path <- file.path(SCR, "roster_openpayments_year.rds")
op <- if (file.exists(op_path)) {
  readRDS(op_path) |> transmute(npi = as.character(npi), year = as.integer(year),
                                ev_openpay_n = as.numeric(n_payments))
} else data.frame(npi = character(), year = integer(), ev_openpay_n = numeric())
OP_YEARS <- if (nrow(op)) sort(unique(op$year)) else integer()

# ---- assemble the long panel (artifact B) ----------------------------------
panel <- tidyr::expand_grid(npi = roster$npi, year = YEARS) |>
  left_join(roster, by = "npi") |>
  left_join(pb, by = c("npi", "year")) |>
  left_join(op, by = c("npi", "year")) |>
  mutate(
    across(c(ev_partb_lines, ev_partb_services, ev_partb_benes_line_sum,
             ev_partb_max_line_benes, ev_partb_hcpcs_n, ev_partb_allowed,
             ev_openpay_n), ~ tidyr::replace_na(.x, 0)),
    # raw evidence flags
    ev_partb_any        = ev_partb_lines > 0,
    ev_openpay_any      = ev_openpay_n > 0,
    ev_openpay_observable = year %in% OP_YEARS,   # FALSE => absence is uninformative
    ev_certified_by     = !is.na(cert_year) & cert_year <= year,
    # derived competing definitions
    d0_certified        = ev_certified_by,
    d1_any_partb        = ev_certified_by & ev_partb_any,
    d2_meaningful_partb = ev_certified_by & ev_partb_services >= D2_MIN_SERVICES &
                          ev_partb_max_line_benes >= D2_MIN_BENES,
    # 2013-2014: CMS published no NPI in Open Payments, so the second source is
    # UNOBSERVED, not negative. D3 in those years is a lower bound (the OR is
    # missing a term) and D4 is not estimable at all -- recording it as FALSE
    # would turn a gap in the data into a claim of inactivity.
    d3_multisource      = ev_certified_by & (ev_partb_any | ev_openpay_any),
    d3_is_lower_bound   = !ev_openpay_observable,
    d4_high_confidence  = ifelse(ev_openpay_observable,
                                 ev_certified_by & ev_partb_any & ev_openpay_any, NA),
    n_independent_sources = as.integer(ev_partb_any) + as.integer(ev_openpay_any)
  ) |>
  arrange(npi, year)

write.csv(panel, file.path(OUT, "provider_year_activity_long.csv"), row.names = FALSE)
saveRDS(panel, file.path(SCR, "panel.rds"))
cat("B provider_year_activity_long:", nrow(panel), "rows,",
    n_distinct(panel$npi), "npis,", length(YEARS), "years\n")
cat("  Open Payments years observable:", paste(range(OP_YEARS), collapse="-"), "\n")
