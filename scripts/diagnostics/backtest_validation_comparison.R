#!/usr/bin/env Rscript
# LOO versus Rolling-Origin Validation ----
#
#   Rscript scripts/diagnostics/backtest_validation_comparison.R
#
# THE COMPARISON THIS EXISTS TO MAKE. Leave-one-out estimates the error
# distribution for a window from every OTHER window, including later ones. But a
# window with cutoff c' has target c' + horizon, and its error is not observable
# until that target year. Using it to bound a forecast made at origin c requires
# an outcome that had not happened yet -- future leakage, even though every
# individual prediction respected its own cutoff.
#
# Rolling origin admits a training window only when target_year <= origin.
#
# If LOO improves dramatically and rolling origin does not, the improvement was
# leakage rather than predictive skill. That is exactly what happens here.
#
# Writes (versioned separately from the frozen 10-arm result, never over it):
#   artifacts/diagnostics/validation_loo.csv
#   artifacts/diagnostics/validation_rolling_origin.csv
#   artifacts/diagnostics/validation_comparison_summary.csv

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})
if (!requireNamespace("mufflyaccess", quietly = TRUE)) {
  stop("mufflyaccess is required: observed endpoints come from the contract.", call. = FALSE)
}
dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)

CUTOFFS <- 2013:2020
MIN_TRAIN <- 2L

stats_block <- function(d, label) {
  data.frame(
    method = label,
    eligible_origins = nrow(d),
    coverage = mean(d$covered),
    mean_signed_error = mean(d$signed_error),
    sd_signed_error = stats::sd(d$signed_error),
    median_abs_pct_error = stats::median(d$abs_pct_error),
    p25_abs_pct_error = unname(stats::quantile(d$abs_pct_error, 0.25)),
    p75_abs_pct_error = unname(stats::quantile(d$abs_pct_error, 0.75)),
    mean_width = mean(d$width),
    stringsAsFactors = FALSE)
}

cat("=== WINDOW-LEVEL ERRORS (no interval; the raw predictor) ===\n")
w <- backtest_multi_window(cutoffs = CUTOFFS, predictor = "nrmp")
print(as.data.frame(w[, c("cutoff_year", "target_year", "baseline_stock",
                          "entrant_rate", "predicted", "observed",
                          "absolute_error", "percent_error")]),
      row.names = FALSE, digits = 5)

cat("\n=== A. LEAVE-ONE-OUT (leaky comparator) ===\n")
loo <- backtest_loo_validation(cutoffs = CUTOFFS, predictor = "nrmp", min_train = MIN_TRAIN)
print(as.data.frame(loo[, c("origin", "target_year", "n_train", "n_train_future",
                            "observed", "median_prediction", "lower", "upper",
                            "width", "signed_error", "abs_pct_error", "covered")]),
      row.names = FALSE, digits = 5)
cat(sprintf("\ntraining sets containing outcomes AFTER their origin: %d of %d\n",
            sum(loo$n_train_future > 0), nrow(loo)))

cat("\n=== B. ROLLING ORIGIN (temporally honest) ===\n")
cat(sprintf("minimum training windows: %d. Origins with fewer are EXCLUDED, not scored\n",
            MIN_TRAIN))
cat("on a spread that is not estimable.\n\n")
ro <- backtest_rolling_origin(cutoffs = CUTOFFS, predictor = "nrmp", min_train = MIN_TRAIN)
print(as.data.frame(ro[, c("origin", "target_year", "n_train", "train_targets",
                           "observed", "median_prediction", "lower", "upper",
                           "width", "signed_error", "abs_pct_error", "covered")]),
      row.names = FALSE, digits = 5)

cmp <- rbind(stats_block(loo, "leave-one-out (leaky)"),
             stats_block(ro, "rolling origin (honest)"))
cat("\n=== SUMMARY ===\n")
print(cmp, row.names = FALSE, digits = 4)

cat("\n=== READING ===\n")
cat("LOO looks better on accuracy and sharpness. It is not better; it is leaking.\n")
cat("Every LOO training set contains outcomes that had not occurred at its origin.\n\n")
cat("Rolling origin reaches 100% coverage, and that is NOT calibration either:\n")
cat("the intervals are enormous, and at least one lower bound is NEGATIVE, which\n")
cat("is impossible for a cumulative certification stock. Coverage bought with a\n")
cat("degenerate interval is the same failure as the certification-derived arms.\n\n")

cat("=== PERIOD CLUSTERING ===\n")
early <- w[w$cutoff_year <= 2014, ]; late <- w[w$cutoff_year >= 2015, ]
cat(sprintf("establishment-era origins (<=2014): errors %s\n",
            paste(sprintf("%+.1f%%", early$percent_error), collapse = ", ")))
cat(sprintf("plateau-era origins (>=2015):       errors %s\n",
            paste(sprintf("%+.1f%%", late$percent_error), collapse = ", ")))
cat("\nThe misses cluster by calendar period. The 2013 origin misses by -20.3%\n")
cat("because the certification backlog was still clearing; excluding it, every\n")
cat("remaining window sits within +3.8% / -4.8%. Rolling origin is contaminated\n")
cat("by that era for EVERY eligible origin -- its training sets all begin at\n")
cat("target 2016, which is the 2013 window. There is not yet enough post-\n")
cat("establishment history for an honest interval to be sharp.\n")

utils::write.csv(loo, "artifacts/diagnostics/validation_loo.csv", row.names = FALSE)
utils::write.csv(ro, "artifacts/diagnostics/validation_rolling_origin.csv", row.names = FALSE)
utils::write.csv(cmp, "artifacts/diagnostics/validation_comparison_summary.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/validation_{loo,rolling_origin,comparison_summary}.csv\n")
