#!/usr/bin/env Rscript
# Run the microsimulation on the PRODUCTION roster ----
#
#   Rscript scripts/run_with_production_roster.R
#
# The difference between this and run_workforce_microsimulation_example.R is the
# cohort. The example builds agents from aggregate certification counts, which
# `cohort_composition()` refuses to call a production cohort: the contract ships
# no age, sex or state, so the base cohort is partly assumed. Every output of
# that run carries example_only = TRUE.
#
# This script supplies the real roster -- board-certified URPS subspecialists
# matched to NPI, with Medicare CY2024 billing as the activity attestation --
# and the run reports example_only = FALSE.
#
# THE ROSTER IS NOT IN THIS REPOSITORY BY DEFAULT. data-raw/urps_roster is not
# whitelisted in .gitignore, deliberately: the extract carries NPIs. Derive it
# from mufflyt/cliff (data/abog_all_urps_ENRICHED_2026-07-22.csv and the ABU
# equivalent), keeping the columns load_urps_roster() expects and dropping
# physician names, which nothing in the pipeline needs.
#
# WHAT THIS RUN DOES NOT FIX. The base-year capacity anchor is still the
# physical-therapy stand-in -- see capacity_status(). Observed workload cannot
# replace it without assuming the answer, so a production cohort improves the
# SUPPLY side and leaves the demand anchor exactly as unresolved as before.

suppressPackageStartupMessages({
  library(dplyr)
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})

ROSTER <- Sys.getenv("URPS_ROSTER_CSV",
                     unset = "data-raw/urps_roster/urps_roster_2026-07-22.csv")
N_ITER <- as.integer(Sys.getenv("N_ITERATIONS", "200"))
YEARS <- 2025:2050

if (!file.exists(ROSTER)) {
  stop("Roster not found at '", ROSTER, "'. Set URPS_ROSTER_CSV, or derive it ",
       "from mufflyt/cliff as described in the header.", call. = FALSE)
}

message("Reproducibility mode: ", resolve_reproducibility_mode())
roster <- urps_provider_roster(load_urps_roster(ROSTER))
message(sprintf("Roster: %d providers, %d states, %d confirmed active in 2024",
                nrow(roster), length(unique(roster$state)),
                sum(!is.na(roster$last_confirmed_active_year))))

# Workload concentration is reported BEFORE the projection, because it bears on
# how the supply number should be read: board certification is not the same as
# delivering urogynaecologic care.
conc <- roster_workload_concentration(load_urps_roster(ROSTER))
cat("\n===== ROSTER WORKLOAD CONCENTRATION (Medicare CY2024) =====\n")
cat(sprintf("zero URPS Medicare volume : %d of %d (%.1f%%)\n",
            conc$n_zero, conc$n_providers, 100 * conc$share_zero))
cat(sprintf("median annual services    : %.0f\n", conc$median_volume))
cat(sprintf("top quartile delivers     : %.1f%% of volume\n",
            100 * conc$share_from_top_quartile))
cat(sprintf("providers for 90%% of work : %d (%.0f%% of roster)\n",
            conc$n_for_90pct, 100 * conc$share_of_roster_for_90pct))
for (c in attr(conc, "caveats")) cat(strwrap(paste("NOTE:", c), 78, exdent = 6), sep = "\n")

supply <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
gap <- baseline_gap(
  base_supply_fte = supply$national,
  adequacy = capacity_survey_adequacy(example_capacity_survey())$adequacy,
  method = "capacity_survey",
  evidence = c("STAND-IN: Zarek 2025 physical-therapy capacity distribution",
               "Replace with a fielded URPS practice-capacity survey",
               "See capacity_status() and urps_capacity_survey_requirements()")
)

result <- run_workforce_microsimulation(
  roster = roster,
  years = YEARS,
  subspecialty = "FPMRS",
  baseline_gap_estimate = gap,
  n_iterations = N_ITER,
  calibration = "namcs",
  allow_analogy = TRUE,
  output_dir = "outputs",
  verbose = TRUE
)

cat("\n===== COHORT STATUS =====\n")
m <- result$scenario_meta
cat("example_only    :", m$example_only, "\n")
cat("cohort source   :", m$cohort_provenance$source, "\n")
cat("is_production   :", m$cohort_provenance$is_production, "\n")
cat("entrants source :", m$entrants_source, " (", m$baseline_entrants, "/yr )\n")
cat("demand calibrated:", m$demand_calibrated, "\n")

cat("\n===== SUPPLY VS REQUIRED FTE =====\n")
result$fte_gap %>%
  filter(year %in% c(min(YEARS), 2035, max(YEARS))) %>%
  mutate(across(where(is.numeric), ~round(.x, 1))) %>%
  print()

cat("\n===== STILL UNRESOLVED =====\n")
cs <- capacity_status()
cat("capacity anchor resolved:", cs$resolved, "\n")
cat(strwrap(cs$leverage, 78, prefix = "  "), sep = "\n")
cat("\nbacktest:", format(backtest_status()$coverage_95 * 100), "% coverage of",
    backtest_status()$n_arms, "arms -- intervals are a Monte Carlo range.\n")
cat("Run id:", result$run_id, "\n")
