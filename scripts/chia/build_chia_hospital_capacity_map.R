#!/usr/bin/env Rscript
# =============================================================================
# Build CHIA Hospital Surgical-Capacity & Volume Map
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(readr)
})

pkgload::load_all(".", quiet = TRUE)

cat("=================================================================\n")
cat("BUILDING CHIA HOSPITAL SURGICAL CAPACITY MAP\n")
cat("=================================================================\n\n")

cap_res <- build_chia_hospital_capacity_map(
  con = NULL,
  min_year = 2004L,
  max_year = 2018L,
  save_dir = "artifacts/chia_capacity"
)

cat("Hospital Capacity Map Built Successfully!\n\n")
cat("--- Hospital Market Concentration Summary ---\n")
print(as.data.frame(cap_res$market_summary |> dplyr::select(year, total_state_cases, n_active_facilities, gini_concentration, pct_high_volume_facs)))

cat("\n=================================================================\n")
cat("CHIA HOSPITAL CAPACITY MAPPING COMPLETE\n")
cat("=================================================================\n")
