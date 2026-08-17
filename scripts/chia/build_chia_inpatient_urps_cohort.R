#!/usr/bin/env Rscript
# =============================================================================
# Build CHIA Inpatient URPS Surgical Cohort & Estimand D6 Series
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(readr)
})

pkgload::load_all(".", quiet = TRUE)

cat("=================================================================\n")
cat("BUILDING CHIA INPATIENT URPS SURGICAL COHORT (ESTIMAND D6)\n")
cat("=================================================================\n\n")

d6_series <- build_chia_inpatient_urps_series(
  con = NULL,
  min_year = 2004L,
  max_year = 2018L,
  save_dir = "artifacts/chia_inpatient"
)

cat("D6 Inpatient Series Built Successfully!\n")
cat("Total Observations:", nrow(d6_series), "rows across 2004-2018\n")
cat("Procedure Families Included:", paste(unique(d6_series$procedure_family), collapse=", "), "\n\n")
cat("--- Sample D6 Annual Inpatient Rates per 100k ---\n")
print(head(d6_series |> dplyr::select(year, age_band, procedure_family, inpatient_cases, rate_per_100k), 10))

cat("\n=================================================================\n")
cat("CHIA ESTIMAND D6 COHORT BUILD COMPLETE\n")
cat("=================================================================\n")
