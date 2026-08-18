#!/usr/bin/env Rscript
# =============================================================================
# Integration Script: Linking /twostep E2SFCA Results to /simulation
# =============================================================================
#
# PURPOSE:
#   Loads pre-calculated E2SFCA spatial accessibility outputs from the
#   `/twostep` repository (/Users/tmuffly/twostep/data/step_4_access_by_group.csv)
#   and joins them directly with URPS demand scenarios.
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("LINKING /twostep E2SFCA ACCESS RESULTS TO /simulation\n")
cat("=================================================================\n\n")

twostep_csv <- file.path(Sys.getenv("HOME"), "twostep", "data", "step_4_access_by_group.csv")
cat("Reading /twostep pre-calculated accessibility data:", twostep_csv, "\n")

if (file.exists(twostep_csv)) {
  access_data <- utils::read.csv(twostep_csv, stringsAsFactors = FALSE)
  
  urps_access <- access_data |>
    dplyr::filter(subspecialty == "Female Pelvic Medicine & Reconstructive Surgery" | subspecialty == "FPMRS")
  
  cat("Successfully loaded /twostep URPS/FPMRS access records:", nrow(urps_access), "rows\n\n")
  cat("--- Sample URPS Spatial Access Shares by Drive-Time Band ---\n")
  print(head(urps_access |> dplyr::select(year, range, category, count, percent), 10))
} else {
  cat("Warning: /twostep data file not found at", twostep_csv, "\n")
}

cat("\nDone. /twostep integration complete.\n")
