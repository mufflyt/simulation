#!/usr/bin/env Rscript
# Does propagating fellowship-entrant uncertainty restore forecast coverage? ----
#
#   Rscript scripts/validation/07_entrant_regime_coverage.R
#
# THE DEFECT THIS MEASURES. The supply forecast's entrant inflow was treated as
# a fixed constant (or varied only by entrant_se = sd/sqrt(n), the standard error
# of a multi-year mean, which is ~0.6/yr). With no real parameter variance the
# 2020->2023 back-test intervals covered the observation in 0 of 8 arms: they
# were individual-stochasticity bands, not forecast intervals.
#
# The fix is not a wider hand-tuned SD. The package already carries a data-fit
# regime model (fit_entrant_regime_model / draw_entrant_paths) that propagates
# entrant uncertainty from its sources -- a trend coefficient drawn from the
# Poisson-GLM vcov, negative-binomial over-dispersion, and a Jeffreys-smoothed
# regime-break rate -- as one latent path per replicate, counts nested within.
# It was implemented and left dormant: no forecast or exported entry point ran it
# end to end. This script runs it, so the question "does entrant uncertainty
# restore coverage?" is answered by measurement rather than assertion.
#
# THREE READINGS, and the distinction between them is the point:
#   1. BASELINE      run_backtest() as shipped (entrant_se only) -- the ~0/8.
#   2. REGIME REFIT  run_entrant_regime_backtest() on 2020->2023. Coverage under
#                    the regime model, but IN-SAMPLE: the estimator was written
#                    after the 2023 miss was seen, so good coverage here is
#                    necessary, not sufficient. The function says so itself
#                    (out_of_sample = FALSE).
#   3. OUT-OF-SAMPLE entrant_regime_rolling_validation(): refit at each earlier
#                    cutoff and score the never-seen horizon. THIS is the honest
#                    signal, and it is reported against a naive baseline.
#
# COVERAGE IS REPORTED WITH BIAS, never alone. The arms under-predict the 2023
# stock by 3-17.6% (a level miss, not a width miss); widening intervals raises
# coverage while hiding that. percent_error is printed beside every coverage
# number so a coverage "win" bought by swallowing a biased point estimate is
# visible as such.
#
# DATA. Needs mufflyaccess (urps_entry_counts / urps_counts_long / urps_count).
# It therefore runs on a machine that has the contract data, NOT in CI, where
# these are absent. Each section fails soft with a stated reason if the data or
# a dependency is missing, so a partial environment still reports what it can.

suppressWarnings(suppressMessages(pkgload::load_all(".", quiet = TRUE)))

pct <- function(x) paste0(format(round(x, 1), nsmall = 1), "%")
rule <- function(title) cat("\n", title, "\n", strrep("-", nchar(title)), "\n", sep = "")

cut_year <- 2020L
tgt_year <- 2023L

## --- 1. baseline: entrants as shipped (entrant_se only) ----------------------
rule("1. BASELINE  run_backtest() as shipped  (fixed inflow / entrant_se only)")
baseline <- tryCatch(
  run_backtest(cutoff_year = cut_year, target_year = tgt_year),
  error = function(e) {
    cat("  skipped: ", conditionMessage(e), "\n", sep = ""); NULL
  })
if (!is.null(baseline)) {
  s <- baseline$summary
  cat("  arms:", nrow(s),
      " | within_95 covered:", sum(s$within_95), "/", nrow(s),
      " | median |percent_error|:", pct(stats::median(abs(s$percent_error))), "\n")
}

## --- 2. regime model, 2020->2023 refit (IN-SAMPLE) ---------------------------
rule("2. REGIME REFIT  run_entrant_regime_backtest()  (in-sample; see header)")
# Internal by design: the codebase deliberately keeps the regime out of the
# frozen run_backtest() record and exposes it as this separate estimator.
regime <- tryCatch(
  urpssim:::run_entrant_regime_backtest(cutoff_year = cut_year, target_year = tgt_year,
                                        verbose = FALSE),
  error = function(e) {
    cat("  skipped: ", conditionMessage(e), "\n", sep = ""); NULL
  })
if (!is.null(regime)) {
  s <- regime$summary
  cat("  arms:", nrow(s),
      " | within_95 covered:", sum(s$within_95), "/", nrow(s),
      " | median |percent_error|:", pct(stats::median(abs(s$percent_error))), "\n")
  cat("  (in-sample refit -- read section 3 for the out-of-sample signal.)\n")
}

## --- 3. honest out-of-sample: rolling refit at each earlier cutoff ------------
rule("3. OUT-OF-SAMPLE  entrant_regime_rolling_validation()  (the trustworthy signal)")
rolling <- tryCatch({
  series <- urpssim:::urps_entrant_series()
  stock <- mufflyaccess::urps_counts_long()
  cumulative_series <- data.frame(year = as.integer(stock$year),
                                  n_active = as.numeric(stock$n_active))
  entrant_regime_rolling_validation(series, cumulative_series, verbose = FALSE)
}, error = function(e) {
  cat("  skipped: ", conditionMessage(e), "\n", sep = ""); NULL
})
if (!is.null(rolling) && is.data.frame(rolling) && nrow(rolling) > 0) {
  cat("  folds:", nrow(rolling),
      " | out-of-sample coverage:", sum(rolling$covered), "/", nrow(rolling),
      " (", pct(100 * mean(rolling$covered)), ")\n", sep = "")
  cat("  median |percent_error|: regime ", pct(stats::median(abs(rolling$percent_error))),
      "  vs naive ", pct(stats::median(abs(rolling$naive_percent_error))), "\n", sep = "")
  cat("\n  per fold:\n")
  print(rolling[, c("cutoff_year", "target_year", "observed", "predicted_median",
                    "pi95_lower", "pi95_upper", "covered", "percent_error",
                    "naive_percent_error")])
}

## --- verdict -----------------------------------------------------------------
rule("VERDICT")
cat("Compare section 1 (baseline coverage) against sections 2-3. The regime model\n")
cat("is worth adopting as the default entrant-uncertainty path only if section 3\n")
cat("(out-of-sample) restores coverage toward its nominal 95% WITHOUT the median\n")
cat("|percent_error| growing -- i.e. it widens honestly rather than papering over\n")
cat("the level miss. Phase 2 (making it the propagated default) is gated on this.\n")

invisible(list(baseline = baseline, regime = regime, rolling = rolling))
