#!/usr/bin/env Rscript
# =============================================================================
# Fit Poisson Population-Offset Inpatient Surgery Rate Model
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(readr)
})

pkgload::load_all(".", quiet = TRUE)

cat("=================================================================\n")
cat("FITTING INPATIENT SURGERY POISSON RATE MODEL\n")
cat("=================================================================\n\n")

d6_series <- build_chia_inpatient_urps_series(save_dir = "artifacts/chia_inpatient")
fit_res <- fit_inpatient_surgery_rate_model(d6_series, family = "quasipoisson", include_interaction = TRUE)

cat("Model Fit Completed Successfully!\n")
cat("Quasi-Poisson Dispersion:", round(fit_res$dispersion, 3), "\n\n")
cat("--- Model Coefficients Summary ---\n")
print(head(fit_res$coefficients, 10))

cat("\n=================================================================\n")
cat("INPATIENT RATE MODEL FITTING COMPLETE\n")
cat("=================================================================\n")
