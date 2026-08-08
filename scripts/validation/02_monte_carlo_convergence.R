#!/usr/bin/env Rscript
# Monte Carlo convergence and parameter-uncertainty sensitivity ----
#
#   Rscript scripts/validation/02_monte_carlo_convergence.R
#
# TWO NUMERICAL QUESTIONS, kept apart from the scientific ones.
#
# 1. CONVERGENCE. How many iterations does the 2050 endpoint need? The criterion
#    is DECLARED HERE, before the run, so it cannot be fitted to the result:
#    across independent seeds the 2050 median must vary by < 0.5%, and the 2.5th
#    percentile, 97.5th percentile and interval width by <= 5%.
#
#    MULTIPLE SEEDS ARE THE POINT. A single-seed sweep of this design produced a
#    monotonically falling width (249 -> 242 -> 232 -> 229) that reads as
#    convergence and is nothing of the kind: across three seeds the MEAN width is
#    flat (227.4, 228.9, 226.6, 227.8) and that sequence was one seed sitting at
#    the high end at every count. What actually improves with n is the
#    REPRODUCIBILITY of the endpoints, not their expected value. Monte Carlo
#    error moves an estimated quantile in either direction; there is no general
#    property by which small n widens an interval.
#
# 2. PARAMETER UNCERTAINTY. The engine draws the entrant rate but holds the
#    retirement hazard fixed, because it is published without standard errors.
#    Fixing it is not the same as knowing it. These runs propagate a declared
#    coefficient of variation and report INTERVAL-WIDTH INFLATION,
#    100 x (W_uncertain / W_fixed - 1).
#
#    The CVs are SENSITIVITY ASSUMPTIONS, labelled moderate and high. They are
#    not estimated uncertainty distributions and must not be reported as
#    confidence bounds on retirement rates.
#
# Baseline scenario only: the interval question is a supply-side one, and running
# all 14 demand scenarios to answer it costs an order of magnitude for nothing.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})

ITERATIONS <- c(250L, 500L, 1000L, 2000L)
SEEDS      <- c(20260801L, 11L, 202L)
HAZARD_CV  <- c(fixed = 0, moderate = 0.15, high = 0.30)
CRIT_MEDIAN_PCT <- 0.5
CRIT_ENDPOINT_PCT <- 5

roster <- urps_provider_roster(load_urps_roster())
gap <- baseline_gap(
  base_supply_fte = 1306,
  adequacy = capacity_survey_adequacy(example_capacity_survey())$adequacy,
  method = "capacity_survey", calibration_status = "derived_by_analogy",
  source = "Zarek 2025 PTJ", evidence = "STAND-IN: physical-therapy distribution")
hist <- nrmp_entrant_series()$positions_filled
baseline_only <- supply_scenario_registry(70)[1]

one <- function(n, seed, cv) {
  spec <- supply_parameter_spec(entrant_series = hist, entrant_mean = 70, hazard_cv = cv)
  r <- suppressMessages(run_workforce_microsimulation(
    roster = roster, years = 2025:2050, subspecialty = "FPMRS",
    baseline_gap_estimate = gap, n_iterations = n, calibration = "namcs",
    supply_scenarios = baseline_only, parameter_spec = spec, seed = seed,
    allow_analogy = TRUE, verbose = FALSE))
  s <- r$supply[r$supply$year == 2050, ]
  data.frame(n = n, seed = seed, hazard_cv = cv, median = s$effective_fte_median[1],
             lo = s$effective_fte_lo[1], hi = s$effective_fte_hi[1],
             width = s$effective_fte_hi[1] - s$effective_fte_lo[1])
}

grid <- expand.grid(n = ITERATIONS, seed = SEEDS)
conv <- do.call(rbind, Map(function(n, s) one(n, s, 0), grid$n, grid$seed))
agg <- do.call(rbind, lapply(split(conv, conv$n), function(d) data.frame(
  n = d$n[1], median_mean = mean(d$median),
  median_range_pct = 100 * diff(range(d$median)) / mean(d$median),
  lo_range_pct = 100 * diff(range(d$lo)) / mean(d$lo),
  hi_range_pct = 100 * diff(range(d$hi)) / mean(d$hi),
  width_mean = mean(d$width),
  width_range_pct = 100 * diff(range(d$width)) / mean(d$width))))
agg$verdict <- ifelse(
  agg$median_range_pct < CRIT_MEDIAN_PCT &
    pmax(agg$lo_range_pct, agg$hi_range_pct, agg$width_range_pct) <= CRIT_ENDPOINT_PCT,
  "PASS", "FAIL")
cat("== convergence across", length(SEEDS), "seeds (range as % of mean) ==\n")
print(agg, row.names = FALSE, digits = 4)
cat(sprintf("\ncriterion: median range < %.1f%%, endpoints and width <= %.0f%%\n",
            CRIT_MEDIAN_PCT, CRIT_ENDPOINT_PCT))
cat("smallest passing iteration count:", min(agg$n[agg$verdict == "PASS"]), "\n")

n_ref <- max(ITERATIONS)
haz <- do.call(rbind, lapply(HAZARD_CV, function(cv) one(n_ref, SEEDS[1], cv)))
haz$label <- names(HAZARD_CV)
haz$width_inflation_pct <- 100 * (haz$width / haz$width[1] - 1)
haz$median_shift_pct <- 100 * (haz$median - haz$median[1]) / haz$median[1]
cat(sprintf("\n== retirement-hazard sensitivity at n = %d ==\n", n_ref))
print(haz[, c("label", "hazard_cv", "median", "lo", "hi", "width",
              "width_inflation_pct", "median_shift_pct")], row.names = FALSE, digits = 4)
cat("\nCVs are declared sensitivity assumptions, NOT estimated uncertainty.\n")
