#!/usr/bin/env Rscript
# =============================================================================
# End-to-end: fitted transitions -> DMDM trajectory -> demand contract -> cliff
# =============================================================================
# Proves the whole demand hand-off works with per-condition provenance intact:
#   1. take a DMDM transition object -- the SWAN-fitted one from
#      run_swan_dmdm_fit.R (artifacts/swan_dmdm_transitions.rds) if present, else
#      the literature-POP object (dmdm_transitions_with_pop_literature());
#   2. run the open-population trajectory (R/30);
#   3. export the versioned demand contract with per-tier provenance (R/export_);
#   4. round-trip it through cliff's ingestion (dpmm_tier_status), if a cliff
#      checkout is found.
#
#   Rscript scripts/demand_contract_end_to_end.R
#
# The point is the PROVENANCE surviving each hop: dmdm_ui reads "fitted" once
# SWAN is fitted, dmdm_pop "derived_by_analogy", dmdm_ai "placeholder", and
# tier3 (any-PFD) the weakest of the three -- so a downstream consumer gates on
# the tier it actually reads. Uncalibrated tiers are allowed here only because
# this is an exploratory demonstration (allow_uncalibrated = TRUE).
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# ---- 1. transitions: prefer the SWAN-fitted object --------------------------
fitted_path <- Sys.getenv("SWAN_DMDM_TRANSITIONS", "artifacts/swan_dmdm_transitions.rds")
transitions <- if (file.exists(fitted_path)) {
  message("Using SWAN-fitted transitions: ", fitted_path)
  readRDS(fitted_path)
} else {
  message("No SWAN-fitted transitions at ", fitted_path,
          "; using the literature-POP object (UI/AI placeholder).")
  dmdm_transitions_with_pop_literature()
}
cat("Transition provenance: ",
    paste(names(unlist(transitions$provenance)), unlist(transitions$provenance),
          sep = "=", collapse = " | "), "\n", sep = "")

# ---- 2. open-population trajectory -------------------------------------------
pop_by_age_year <- tidyr::expand_grid(year = 2025:2035, age = 40:85) %>%
  dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))
traj <- dmdm_open_prevalence_trajectory(
  pop_by_age_year, 2025, 2035, transitions = transitions, seed = 1,
  reweight_to_projection = TRUE, allow_uncalibrated = TRUE)

# ---- 3. export the versioned demand contract (per-tier provenance) ----------
dir.create("artifacts", showWarnings = FALSE)
out <- export_dmdm_demand_contract(
  traj, output_directory = "artifacts/demand_contract", transitions = transitions,
  allow_uncalibrated = TRUE, verbose = FALSE)
tier_status <- unique(out$data[, c("denominator_tier", "tier_calibration_status")])
cat("\n== Contract tiers + per-tier provenance ==\n")
print(tier_status[order(tier_status$denominator_tier), ], row.names = FALSE)
cat("CSV: ", out$csv_path, "\n", sep = "")

# ---- 4. cliff round-trip (optional) -----------------------------------------
cliff_fn <- Find(file.exists,
                 c("../cliff/R/dpmm_contract.R", "/home/user/cliff/R/dpmm_contract.R"))
if (!is.null(cliff_fn)) {
  source(cliff_fn)
  ct <- read_dpmm_demand_contract(out$csv_path)
  cat("\n== cliff ingestion ==\n")
  for (tier in c("tier3_prevalent_pfd", "dmdm_ui", "dmdm_pop", "dmdm_ai")) {
    idx <- dpmm_alt_d1_index(ct$data, 2025:2035, base_year = 2025L, tier = tier)
    cat(sprintf("  %-20s status=%-24s usable=%s  index(2035)=%.0f\n",
                tier, dpmm_tier_status(ct, tier), dpmm_series_usable(idx),
                idx[length(idx)]))
  }
  cat("cliff reads each tier's provenance and rebases the series to 2025 = 100.\n")
} else {
  cat("\n(cliff checkout not found; skipped the downstream round-trip.)\n")
}

cat("\nEnd-to-end demand hand-off complete.\n")
