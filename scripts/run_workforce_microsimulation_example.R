#!/usr/bin/env Rscript
# Example: run the integrated workforce microsimulation end-to-end.
#
# Runs WITHOUT external data. The synthetic provider cohort and the illustrative
# service-volume basket are for demonstration only; the run reports its own
# calibration status so an example can never be mistaken for a result.
#
#   Rscript scripts/run_workforce_microsimulation_example.R
#
# Strict mode fails closed on every uncalibrated input, which is the point:
#   REPRODUCIBILITY_MODE=strict Rscript scripts/run_workforce_microsimulation_example.R

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(purrr)
})

# From an installed package: library(urpssim). From a source checkout the
# pkgload fallback keeps the script runnable without installing first.
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

message("Reproducibility mode: ", resolve_reproducibility_mode())

# --- Step 1: estimate the BASE-YEAR GAP before projecting anything ----------
# Without this the model can only report growth relative to current levels.
supply <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
adequacy <- capacity_survey_adequacy(example_capacity_survey())

gap <- baseline_gap(
  base_supply_fte = supply$national,
  adequacy = adequacy$adequacy,
  method = "capacity_survey",
  evidence = c(
    "Stand-in: Zarek 2025 physical-therapy capacity distribution",
    "Replace with a fielded URPS practice-capacity survey"
  )
)
cat("\n===== BASE-YEAR SUPPLY ADEQUACY =====\n")
print(gap)

# --- Step 2: run supply x demand under the scenario registry ---------------
result <- run_workforce_microsimulation(
  baseline_supply = NULL,          # resolved from the mufflyaccess contract
  supply_geography = "national",
  years = 2025:2050,
  subspecialty = "FPMRS",
  baseline_gap_estimate = gap,
  n_iterations = 200,
  # baseline_entrants defaults to the NRMP-matched value (70/yr) rather than the
  # old round 55, which matched no source.
  # Demand is anchored to the NAMCS national URPS visit total. Drop this
  # argument to see the uncalibrated comparator (and the guard that refuses it
  # in strict mode).
  calibration = "namcs",
  output_dir = "outputs",
  verbose = TRUE
)

cat("\n===== SUPPLY VS REQUIRED FTE (status quo, FTE on both sides) =====\n")
result$fte_gap %>%
  filter(year %in% c(2025, 2035, 2050)) %>%
  mutate(across(where(is.numeric), ~round(.x, 1))) %>%
  print()

cat("\n===== SUPPLY BY SCENARIO (effective FTE, final year) =====\n")
result$supply %>%
  filter(year == max(year)) %>%
  select(scenario_label, effective_fte_median, effective_fte_lo, effective_fte_hi) %>%
  mutate(across(where(is.numeric), round)) %>%
  print()

cat("\n===== REPLACEMENT-RATIO OUTLOOK =====\n")
result$outlook %>%
  mutate(across(where(is.numeric), ~round(.x, 3))) %>%
  select(scenario_label, implied_departure_rate, replacement_ratio, outlook) %>%
  print()

cat("\n===== DEMAND CONCORDANCE ACROSS D1/D2/D3 =====\n")
cat("Estimands are distinct (not proportional rescalings):",
    result$concordance$distinct_estimands, "\n")
cat("Final-year adequacy spread:", round(result$concordance$final_year_spread, 4), "\n")
cat("All estimands agree on the conclusion:", result$concordance$conclusion_agrees, "\n")
if (isTRUE(result$concordance$rho_uninformative)) {
  cat("Note: Spearman rho across years is 1 for any monotone series; read the\n",
      "     spread and the conclusion agreement instead.\n", sep = "")
}

cat("\n===== VALIDATION =====\n")
print(as.data.frame(result$validation))

cat("\n===== CALIBRATION STATUS (gates on publication) =====\n")
cat("workload basket:", result$scenario_meta$workload_status, "\n")
cat("hours schedule: ", result$scenario_meta$hours_status, "\n")
cat("example only:   ", result$scenario_meta$example_only, "\n")
cat("FTE definition: ", result$scenario_meta$fte_definition$label, "\n")
cat("supply contract:", result$scenario_meta$supply_contract$contract_version, "\n")
cat("\nRun id:", result$run_id, "\n")
