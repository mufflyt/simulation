#!/usr/bin/env Rscript
# =============================================================================
# End-to-end: empirical entrant regime + ABOG departure hazard -> forecast
# calibration -> train/oracle decomposition of the supply back-test miss
# =============================================================================
# The supply back-test failed one-sidedly: every arm UNDER-predicted and the
# 95% intervals were 6.5-8.2x too narrow, so widening intervals alone could not
# fix it. This runner exercises the forecast-calibration layer that addresses
# both at once (R/calibration-supply_dynamics):
#   1. calibrate_urps_supply_dynamics() fits an entrant regime that may change
#      LEVEL AND SLOPE after an empirical break, and an age-spline departure
#      hazard whose coefficient covariance is resampled per draw. The retirement
#      hazard CV is READ OFF the fit -- hazard_cv = 0 is retired.
#   2. backtest_urps_supply_calibration() hindcasts and scores bias, coverage,
#      and interval width TOGETHER.
#   3. decompose_urps_forecast_miss() runs the leakage-free vs oracle experiment
#      so the miss splits into an unforeseeable regime break versus a deficient
#      entrant model. advance_urps_supply_one_year() is the per-year kernel both
#      the back-test and the decomposition step through.
#
#   Rscript scripts/supply_forecast_calibration_end_to_end.R
#
# FAIL-CLOSED. This runner needs REAL inputs: an ABOG entrant series and an
# ABOG provider-year departure panel. It does NOT fabricate them. When the
# canonical artifacts are absent it prints exactly what is missing and exits 0
# without inventing a calibrated number -- the same ordering-trap discipline the
# geography layer uses. The unit contract of the four functions is pinned in
# tests/testthat/test-supply-dynamics-calibration.R against fixtures.
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr); library(purrr)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# --- Locate real inputs (no fabrication) -------------------------------------
# Expected artifacts, produced upstream from ABOG certification + billing data:
#   data-raw/supply/urps_entrant_years.csv    (year, entrants)
#   data-raw/supply/abog_provider_years.csv   (provider_id, year, age, departed)
#   data-raw/supply/urps_observed_supply.csv  (year, observed_supply)
#   data-raw/supply/urps_roster_start.csv     (provider_id, age) at start_year
root        <- "data-raw/supply"
entrant_fp  <- file.path(root, "urps_entrant_years.csv")
depart_fp   <- file.path(root, "abog_provider_years.csv")
observed_fp <- file.path(root, "urps_observed_supply.csv")
roster_fp   <- file.path(root, "urps_roster_start.csv")

needed <- c(entrant_fp, depart_fp, observed_fp, roster_fp)
missing <- needed[!file.exists(needed)]
if (length(missing) > 0) {
  message("Supply-forecast calibration: required real inputs are absent, ",
          "so nothing is computed (fail-closed).")
  message("Missing:\n  ", paste(missing, collapse = "\n  "))
  message("Provide the ABOG-derived artifacts above, then re-run. The unit ",
          "contract is exercised in tests/testthat/test-supply-dynamics-calibration.R.")
  quit(save = "no", status = 0L)
}

entrant_tbl  <- readr::read_csv(entrant_fp,  show_col_types = FALSE)
departure_tbl <- readr::read_csv(depart_fp,  show_col_types = FALSE)
observed_tbl <- readr::read_csv(observed_fp, show_col_types = FALSE)
roster_tbl   <- readr::read_csv(roster_fp,   show_col_types = FALSE)

start_year <- min(observed_tbl$year)
end_year   <- max(observed_tbl$year)

# --- Calibrate + back-test on the full series --------------------------------
supply_calibration <- calibrate_urps_supply_dynamics(
  entrant_tbl    = entrant_tbl,
  departure_tbl  = departure_tbl,
  forecast_years = seq.int(start_year, end_year),
  n_draws        = 5000L,
  recent_years   = 5L,
  min_retirement_age = 50,
  seed           = 42L
)
message("Empirical retirement hazard CV: ",
        format(supply_calibration$retirement_hazard_cv, digits = 3))

backtest <- backtest_urps_supply_calibration(
  base_provider_tbl   = roster_tbl,
  observed_supply_tbl = observed_tbl,
  calibration         = supply_calibration,
  start_year          = start_year,
  end_year            = end_year,
  n_draws             = 5000L,
  seed                = 42L
)
print(backtest$metrics)

# --- Decompose the miss: leakage-free vs oracle ------------------------------
decomposition <- decompose_urps_forecast_miss(
  entrant_tbl         = entrant_tbl,
  departure_tbl       = departure_tbl,
  base_provider_tbl   = roster_tbl,
  observed_supply_tbl = observed_tbl,
  start_year          = start_year,
  end_year            = end_year,
  n_draws             = 2000L,
  seed                = 42L
)
print(decomposition$metrics)
print(decomposition$decomposition)
message(decomposition$summary_sentence)
