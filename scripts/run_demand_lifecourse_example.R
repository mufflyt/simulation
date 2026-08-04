#!/usr/bin/env Rscript
# Example: the reproductive life-course demand pathway, end to end.
#
# Runs WITHOUT external data. Shows the childbirth-driven demand generator
# (R/25) handing service volumes to the work-RVU FTE conversion (R/17), writing
# the versioned demand contract (tiers 5-6), and taking part in the demand
# concordance framework (R/13) as an independent fourth estimand.
#
# The coefficient tables are PLACEHOLDERS (calibration_status =
# "placeholder_uncalibrated"); this is a wiring demonstration, never a result.
#
#   Rscript scripts/run_demand_lifecourse_example.R

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(purrr)
})

if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# --- a synthetic female population by single year of age, over the demo years
bands <- example_female_population_by_band()
years <- sort(unique(bands$year))
pop_by_age_year <- tidyr::expand_grid(year = years, age = 40:85) %>%
  dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))

# --- Step 1: life-course demand -> service volumes -> required FTE ----------
# wrvu_per_fte is ILLUSTRATIVE here (median benchmark). In a real run derive it
# from a base-year anchor with calibrate_wrvu_per_fte().
out <- lifecourse_required_fte(
  pop_by_age_year,
  wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]],
  scenario = "baseline", n = 20000, seed = 1
)

cat("\n===== LIFE-COURSE REQUIRED FTE (illustrative wRVU/FTE) =====\n")
out$required_fte %>%
  dplyr::filter(year %in% c(min(years), stats::median(years), max(years))) %>%
  dplyr::mutate(dplyr::across(where(is.numeric), ~round(.x, 1))) %>%
  print()

# --- Step 2: write the versioned demand contract (tiers 5-6) ----------------
contract <- export_hdmm_demand_contract(
  out$demand_summary, output_directory = file.path("outputs", "demand_contracts"),
  scenario = "baseline",
  # The life-course transition equations are not fitted yet, so the contract is
  # written as an explicitly exploratory artifact rather than refused.
  allow_uncalibrated = TRUE
)
cat("Wrote demand contract:", contract$csv_path, "\n")

# --- Step 3: concordance vs the aging-population denominators D1/D2/D3 -------
# The life-course pathway enters as an independent 4th estimand. Because its
# generator differs, it is NOT a proportional rescaling of D1/D2/D3 - which is
# the point of a concordance check (Fraher & Knapton 2017).
d123 <- compute_demand_denominators(bands)
d4   <- lifecourse_demand_estimand(out$demand_summary, measure = "service_units")
demand_long <- dplyr::bind_rows(d123, d4)

# illustrative supply series (FTE) just to exercise growth adequacy
supply <- tibble::tibble(year = years,
                         effective_fte_median = seq(1300, 1500, length.out = length(years)))
adequacy <- compute_growth_adequacy(supply, demand_long)
conc <- assess_demand_concordance(adequacy, demand_long)

cat("\n===== DEMAND CONCORDANCE (D1/D2/D3 + life-course D4) =====\n")
cat("Estimands distinct (not proportional):", conc$distinct_estimands, "\n")
cat("Final-year adequacy spread:", round(conc$final_year_spread, 4), "\n")
cat("All estimands agree on the conclusion:", conc$conclusion_agrees, "\n")
print(conc$final_year_adequacy)

# --- Step 4: parameter-uncertainty intervals on demand ----------------------
ci <- lifecourse_demand_trajectory_ci(pop_by_age_year, n = 20000, n_draws = 100, seed = 1)
cat("\n===== DEMAND WITH PARAMETER-UNCERTAINTY INTERVALS (final year) =====\n")
ci %>%
  dplyr::filter(year == max(year)) %>%
  dplyr::transmute(year,
                   service_units = round(service_units_national),
                   lo = round(service_units_national_lo),
                   hi = round(service_units_national_hi)) %>%
  print()

# --- Step 5: calibrate base-year volumes to national anchors -----------------
# Illustrative "observed" anchors (replace with the sources in
# config/calibration_targets.yml: HCUP SASD / Medicare CPT 57288, NAMCS/MEPS).
predicted <- lifecourse_anchor_predictions(out$service_volumes, base_year = min(years))
observed <- predicted %>% dplyr::mutate(observed = predicted * 0.9) %>%
  dplyr::select(category, observed)                    # pretend the model overshoots 11%
cal <- calibrate_lifecourse_demand(out$service_volumes, observed, base_year = min(years))
cat("\n===== CALIBRATION SCALARS (observed / predicted) =====\n")
cal$scalars %>% dplyr::select(category, predicted, observed, scalar, flagged) %>%
  dplyr::mutate(dplyr::across(where(is.numeric), ~round(.x, 3))) %>% print()

cat("\nNote: placeholder coefficients + illustrative anchors - wiring demonstration, not a result.\n")
