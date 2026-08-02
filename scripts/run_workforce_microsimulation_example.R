#!/usr/bin/env Rscript
# Example: run the integrated workforce microsimulation end-to-end.
#
# Runs WITHOUT any external data (uses example_population_65plus()); swap in a
# canonical Census NPP series via resolve_canonical() for a production run.
#
#   Rscript scripts/run_workforce_microsimulation_example.R
#
# Strict, reproducible run:
#   REPRODUCIBILITY_MODE=strict Rscript scripts/run_workforce_microsimulation_example.R

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(purrr)
})

# Load the numbered modules (10-15). load_workforce_microsimulation lives in 15.
source("R/15-run_workforce_microsimulation.R")
load_workforce_microsimulation("R")

logger::log_info("Reproducibility mode: {resolve_reproducibility_mode()}")

result <- run_workforce_microsimulation(
  initial_workforce = 1295,     # cliff frozen URPS baseline (SSOT)
  years = 2025:2050,
  subspecialty = "FPMRS",
  n_iterations = 1000,          # publication-grade CIs
  baseline_entrants = 55,
  output_dir = "outputs",
  verbose = TRUE
)

cat("\n===== SUPPLY (effective FTE, Status Quo) =====\n")
result$supply %>%
  filter(scenario == "Status Quo", year %in% c(2025, 2035, 2050)) %>%
  select(year, headcount_median, effective_fte_median,
         effective_fte_lo, effective_fte_hi, mean_age_median) %>%
  print()

cat("\n===== REPLACEMENT-RATIO OUTLOOK BY SCENARIO =====\n")
print(result$outlook)

cat("\n===== DEMAND CONCORDANCE (three estimands) =====\n")
cat("Robust (adequacy >= 1 under ALL of D1/D2/D3 in final year):",
    result$concordance$robust, "\n")
cat("Adequacy trough year:", result$concordance$trough_year, "\n")
cat("Spearman rho across coverage curves:\n")
print(round(result$concordance$spearman, 3))

cat("\nRun id:", result$run_id, "\n")
