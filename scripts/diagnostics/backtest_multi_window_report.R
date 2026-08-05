#!/usr/bin/env Rscript
# Multi-Window Back-Test Report ----
#
#   Rscript scripts/diagnostics/backtest_multi_window_report.R
#
# WHY THIS EXISTS. The headline back-test scores ONE endpoint (2023) with ten
# arms, and every arm under-predicts. That reads as a structural downward bias.
# It is not: ten arms scoring one observation are ten views of the same number,
# and across cutoff years the sign of the error flips.
#
# Writes:
#   artifacts/diagnostics/backtest_multi_window.csv
#   artifacts/diagnostics/backtest_oos_interval.csv
#
# Everything here is leakage-clean by construction: `backtest_multi_window()`
# filters NRMP on PUBLICATION year, and `backtest_oos_interval()` drops the
# scored window from its own error estimate.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})
dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)

if (!requireNamespace("mufflyaccess", quietly = TRUE)) {
  stop("mufflyaccess is required: the observed endpoints come from the contract.",
       call. = FALSE)
}

WINDOWS <- 2016:2020

cat("=== 1. SIGNED ERROR BY WINDOW, BOTH PREDICTORS ===\n")
cert <- backtest_multi_window(cutoffs = WINDOWS, predictor = "certification")
nrmp <- backtest_multi_window(cutoffs = WINDOWS, predictor = "nrmp")
both <- rbind(cert, nrmp)
both$direction <- ifelse(both$absolute_error > 0, "ABOVE", "BELOW")
print(as.data.frame(both[, c("cutoff_year", "target_year", "predictor",
                             "baseline_stock", "entrant_rate", "predicted",
                             "observed", "absolute_error", "percent_error",
                             "direction")]),
      row.names = FALSE, digits = 5)

cat("\n=== 2. PREDICTOR COMPARISON ON SHARED WINDOWS ===\n")
shared <- intersect(cert$cutoff_year, nrmp$cutoff_year)
c2 <- cert[cert$cutoff_year %in% shared, ]; n2 <- nrmp[nrmp$cutoff_year %in% shared, ]
cmp <- data.frame(
  predictor = c("certification flow", "NRMP filled positions"),
  n_windows = c(nrow(c2), nrow(n2)),
  mean_abs_error_pct = c(mean(abs(c2$percent_error)), mean(abs(n2$percent_error))),
  mean_signed_error_pct = c(mean(c2$percent_error), mean(n2$percent_error)),
  sd_error_pct = c(stats::sd(c2$percent_error), stats::sd(n2$percent_error))
)
print(cmp, row.names = FALSE, digits = 4)
cat("\nLower error SPREAD is the criterion that matters for a predictor. It is not\n",
    "the same criterion as covering one endpoint, and the two disagree here.\n", sep = "")

cat("\n=== 3. OUT-OF-SAMPLE PREDICTIVE INTERVAL (scored window excluded) ===\n")
oos <- backtest_oos_interval(target_cutoff = 2020L, cutoffs = shared, predictor = "nrmp")
cat(sprintf("training windows (n = %d): errors %s\n", oos$n_train,
            paste(sprintf("%+.2f%%", oos$train_errors_pct), collapse = ", ")))
cat(sprintf("out-of-sample error: mean %+.2f%%, sd %.2f%%\n",
            100 * oos$mean_error, 100 * oos$sd_error))
cat(sprintf("raw prediction      %.0f\n", oos$raw_prediction))
cat(sprintf("bias-corrected      %.0f\n", oos$bias_corrected))
cat(sprintf("95%% interval        [%.0f, %.0f]  width %.0f  (t = %.2f on %d df)\n",
            oos$lower, oos$upper, oos$upper - oos$lower, oos$t_quantile, oos$n_train - 1))
cat(sprintf("observed %.0f covered: %s\n", oos$observed, oos$covered))

cat("\nREAD THIS CAREFULLY. Coverage here is NOT evidence of calibration. The\n",
    "interval rests on three windows, so the t(2) critical value is 4.30, and\n",
    "the bias correction overshoots the observation. A wide interval covering\n",
    "one endpoint is the same failure mode as the certification-derived arms --\n",
    "coverage bought with width rather than earned with accuracy.\n", sep = "")

utils::write.csv(both, "artifacts/diagnostics/backtest_multi_window.csv", row.names = FALSE)
utils::write.csv(
  data.frame(target_cutoff = oos$target_cutoff, raw_prediction = oos$raw_prediction,
             bias_corrected = oos$bias_corrected, lower = oos$lower, upper = oos$upper,
             observed = oos$observed, covered = oos$covered, n_train = oos$n_train,
             mean_error = oos$mean_error, sd_error = oos$sd_error),
  "artifacts/diagnostics/backtest_oos_interval.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/backtest_multi_window.csv and _oos_interval.csv\n")
