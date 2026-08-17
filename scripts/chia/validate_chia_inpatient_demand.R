#!/usr/bin/env Rscript
# =============================================================================
# Validate Inpatient Demand Model via Rolling-Origin Backtest
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(readr)
})

pkgload::load_all(".", quiet = TRUE)

cat("=================================================================\n")
cat("VALIDATING INPATIENT DEMAND MODEL (ROLLING-ORIGIN BACKTEST)\n")
cat("=================================================================\n\n")

d6_series <- build_chia_inpatient_urps_series(save_dir = "artifacts/chia_inpatient")
val_res <- validate_chia_inpatient_demand(d6_series, start_cutoff = 2010L, save_dir = "artifacts/chia_validation")

cat("Backtest Validation Completed Successfully!\n\n")
cat("--- Validation Performance Scores ---\n")
print(as.data.frame(val_res$summary))

cat("\n=================================================================\n")
cat("ROLLING-ORIGIN BACKTEST VALIDATION COMPLETE\n")
cat("=================================================================\n")
