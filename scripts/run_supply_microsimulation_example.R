#!/usr/bin/env Rscript

# Run the SUPPLY microsimulation on its own and report the trajectory.
#
# scripts/run_workforce_microsimulation_example.R runs supply x demand through
# the orchestrator. This is the supply half by itself: useful when the question
# is "how many providers will there be", with no demand side and no FTE gap.
#
#   Rscript scripts/run_supply_microsimulation_example.R
#
# Runs WITHOUT external data -- the cohort is synthesised by
# initialize_provider_agents(). Override the size with SUPPLY_N_ITERATIONS for a
# faster smoke run:
#
#   SUPPLY_N_ITERATIONS=20 Rscript scripts/run_supply_microsimulation_example.R
#
# ---------------------------------------------------------------------------
# THE FOUR ARGUMENTS THAT DECIDE WHETHER THE OUTPUT MEANS ANYTHING
# ---------------------------------------------------------------------------
#
# param_spec      WITHOUT it every coefficient is held fixed and the reported
#                 effective_fte_lo/hi describe Monte Carlo sampling noise, not
#                 forecast uncertainty -- the 2020->2023 back-test found
#                 intervals of that kind 6.5-8.2x too narrow (see
#                 docs/BACKTEST_2020_TO_2023.md). The guard is unconditional;
#                 omitting the argument is the case it exists to catch, and the
#                 only way past it is allow_fixed_parameters = TRUE, which
#                 always warns.
#
# hours_intercept The package default (HWSM_HOURS_INTERCEPT) is NOT consistent
#                 with the 37.2 hr/wk FTE definition: leaving it produces a mean
#                 clinical FTE near 1.10, so FTE supply exceeds headcount from
#                 the base year onward. calibrate_hours_intercept() rescales it
#                 to the cohort actually being simulated.
#
# entrants        entrants_per_year is IGNORED when param_spec carries an
#                 entrant_mean -- the spec wins, and a contradicted explicit
#                 argument warns. This script therefore does not pass
#                 entrants_per_year at all; entrant_spec_from_series() supplies
#                 the rate and scenario$entrants_source records that it did.
#
# retirement      urps_empirical_retirement_schedule() is measured on THIS
#                 subspecialty. The alternative (RETIREMENT_HAZARD_BY_AGE, the
#                 HWSM/FutureDocs curve) is fitted on a different physician
#                 population and is a modelling choice worth declaring.
#
# ---------------------------------------------------------------------------
# READING THE OUTPUT
# ---------------------------------------------------------------------------
#
# * Effective FTE can EXCEED headcount in later years. That is not a bug: the
#   hours intercept is calibrated to the base-year age structure, and the cohort
#   rejuvenates as entrants accumulate, so mean clinical FTE per provider drifts
#   above 1. It means FTE growth is partly an artifact of a fixed base-year
#   calibration, and it is why the printout shows mean age alongside.
# * The band still UNDERSTATES uncertainty. Only the entrant rate is drawn;
#   the retirement hazard and the hours schedule are published without standard
#   errors and are held fixed, which the spec printout states explicitly.
# * This is supply only. It says nothing about adequacy -- that needs the demand
#   side and a base-year gap, which is what the orchestrator script does.
# * Growth is SENSITIVE TO THE BASE COHORT, not just to the entrant rate. The
#   run is seeded end to end so repeated invocations agree exactly, but that
#   makes it one draw of the starting age structure, not the central case. Vary
#   SUPPLY_SEED before reporting any growth figure: at n = 15 the unseeded
#   spread across starting cohorts spanned +0.2% to -7.0%, which is wider than
#   most effects anyone would want to claim from this script.

suppressPackageStartupMessages(library(pkgload))

root <- normalizePath(".")
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(root, quiet = TRUE)
} else {
  library(urpssim)
}

n_iter <- as.integer(Sys.getenv("SUPPLY_N_ITERATIONS", "500"))
years  <- 2025:2050
# One seed covers BOTH stochastic stages: the base-cohort draw below and the
# Monte Carlo loop inside the engine. Override with SUPPLY_SEED to confirm a
# result is not an artifact of one particular starting cohort.
SUPPLY_SEED <- as.integer(Sys.getenv("SUPPLY_SEED", "20260801"))

message("Reproducibility mode: ", resolve_reproducibility_mode())

# SEED BEFORE BUILDING THE COHORT. run_supply_microsimulation() calls
# seed_microsimulation() itself, but only once it has been entered -- by which
# point initialize_provider_agents() has already drawn the base-year age
# distribution from whatever RNG state the session started with. Two identical
# invocations of this script then reported 2025->2050 growth of +0.2% and -7.0%,
# all of it base-cohort noise rather than anything the model did. Seeding here
# puts the cohort draw inside the reproducible stream too.
seed_microsimulation(SUPPLY_SEED)

# The synthetic cohort stands in for the mufflyaccess roster. It carries no
# `sex` column, so calibrate_hours_intercept() is called on age alone and takes
# its own "female" default -- matching what the engine assumes internally.
agents <- initialize_provider_agents(1306, "FPMRS", min(years))
spec   <- entrant_spec_from_series(agents)

cat("\n===== PARAMETER UNCERTAINTY =====\n")
print(spec)

sim <- run_supply_microsimulation(
  initial_workforce   = agents,
  years               = years,
  n_iterations        = n_iter,
  retirement_schedule = urps_empirical_retirement_schedule(),
  param_spec          = spec,
  fte_method          = "hours",
  hours_intercept     = calibrate_hours_intercept(agents$age),
  seed                = SUPPLY_SEED
)

cat("\n===== RESOLVED INPUTS (what the engine used, not what was passed) =====\n")
cat("entrants              :", round(sim$scenario$entrants_per_year, 1), "\n")
cat("entrant source        :", sim$scenario$entrants_source, "\n")
cat("quantified parameters :", sim$scenario$parameter_uncertainty, "\n")
cat("initial cohort        :", sim$scenario$initial_workforce, "\n")
cat("implied departure rate:", round(sim$scenario$implied_departure_rate, 4), "\n")

cat("\n===== SUPPLY TRAJECTORY (median [95% band]) =====\n")
s <- as.data.frame(sim$summary)
show <- s[s$year %in% seq(min(years), max(years), by = 5), ]
print(data.frame(
  year          = show$year,
  headcount     = sprintf("%6.0f [%5.0f, %5.0f]", show$headcount_median,
                          show$headcount_lo, show$headcount_hi),
  effective_fte = sprintf("%6.0f [%5.0f, %5.0f]", show$effective_fte_median,
                          show$effective_fte_lo, show$effective_fte_hi),
  mean_age      = sprintf("%.1f", show$mean_age_median)
), row.names = FALSE)

growth <- function(field) {
  (s[[field]][s$year == max(years)] / s[[field]][s$year == min(years)] - 1) * 100
}
band <- s$effective_fte_hi[s$year == max(years)] - s$effective_fte_lo[s$year == max(years)]

cat(sprintf("\n%d->%d growth: headcount %+.1f%% | effective FTE %+.1f%%\n",
            min(years), max(years), growth("headcount_median"),
            growth("effective_fte_median")))
cat(sprintf("Final-year band: %.0f FTE (%.0f%% of median)\n", band,
            100 * band / s$effective_fte_median[s$year == max(years)]))
cat(sprintf("Iteration panel: %d rows (%d replicates x %d years)\n",
            nrow(sim$iterations), n_iter, length(years)))
cat("\n", sim$scenario$interval_label, "\n", sep = "")
