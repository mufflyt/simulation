#!/usr/bin/env Rscript
# Coverage is not enough: a proper scoring rule on this project's real OOS runs -
#
#   Rscript scripts/diagnostics/interval_honesty_scorecard.R
#
# WHAT THIS EXISTS TO SHOW. This project already has real out-of-sample forecasts
# of the certification stock, and coverage alone ranks them exactly backwards.
# Three evaluations, all on committed artifacts (no mufflyaccess, no digest):
#
#   * rolling-origin (wide)          -- frozen rolling-origin, 100% coverage, but
#     intervals so wide one lower bound is NEGATIVE (impossible for a cumulative
#     stock);
#   * sharp, attrition ON (MISMATCH) -- headline backtest arm 1 with career
#     attrition applied to a CUMULATIVE certification count. Nobody exits a
#     cumulative stock, so attrition is a definition error, not a model: it drags
#     every forecast low and covers ~0%;
#   * sharp, no-attrition (MATCHED)  -- the same model, definition-matched to the
#     cumulative target. Sharp AND covers ~67%.
#
# COVERAGE would crown the wide model (100% > 67% > 0%) -- the uninformative one.
# The INTERVAL SCORE (Gneiting & Raftery 2007: width + (2/alpha) x shortfall)
# ranks them correctly: the definition-matched model scores an order of magnitude
# better than the wide one, because it cannot be gamed by widening (you pay the
# width) or by narrowing without fixing the point forecast (you pay the miss).
# The residual low bias of the matched model is the genuine entrant-regime
# question -- see scripts/diagnostics/entrant_regime_bias_decomposition.R.
#
# Writes artifacts/diagnostics/interval_honesty_scorecard.csv.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    if (requireNamespace("pkgload", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE)
    else source("R/forecast_scorecard.R")   # digest-free base-R cores
  } else {
    library(urpssim)
  }
})

ro_path <- Find(file.exists, c(
  "artifacts/frozen_2026-08-04_validation_v2/validation_rolling_origin.csv",
  "artifacts/diagnostics/validation_rolling_origin.csv"))
bt_path <- Find(file.exists, c(
  "artifacts/backtest_2020_to_2023_trajectory.csv",
  "artifacts/frozen_2026-08-04_backtest10/backtest_2020_to_2023_trajectory.csv"))
if (is.null(ro_path) || is.null(bt_path))
  stop("Need the frozen rolling-origin and backtest trajectory artifacts.", call. = FALSE)

ro <- utils::read.csv(ro_path)
bt <- utils::read.csv(bt_path)
BASE_YEAR <- min(bt$year)          # anchor row: prediction == observed by construction
ARM_MM <- "1. Derived cohort, entrants = 55 (shipped assumption)"
ARM_DM <- "1. Derived cohort, entrants = 55 (shipped assumption) [no-attrition, definition-matched]"
mm <- bt[bt$arm == ARM_MM & bt$year > BASE_YEAR, ]
dm <- bt[bt$arm == ARM_DM & bt$year > BASE_YEAR, ]

# 95% level: the backtest columns are labelled pi95, so alpha=0.05 is exact for
# the models whose misses the penalty bites. The wide model covers 100%, so its
# interval score equals its width regardless of the assumed level.
sc <- function(obs, pred, lo, hi, lab) {
  s <- forecast_scorecard(data.frame(observed = obs, predicted = pred, lower = lo, upper = hi),
                          interval_level = 0.95, label = lab)
  s[, c("label", "n", "coverage", "mean_width", "mean_interval_score", "mape", "signed_bias")]
}

out <- rbind(
  sc(ro$observed, ro$median_prediction, ro$lower, ro$upper, "rolling-origin (wide)"),
  sc(mm$observed, mm$predicted_median, mm$pi95_lower, mm$pi95_upper, "sharp, attrition ON (definition MISMATCH)"),
  sc(dm$observed, dm$predicted_median, dm$pi95_lower, dm$pi95_upper, "sharp, no-attrition (definition-MATCHED)")
)

cat("== Forecast scorecard on three real out-of-sample evaluations (2021-2023, PI95) ==\n")
cat("wide :", ro_path, "\n")
cat("sharp:", bt_path, " [arm 1]\n\n")
print(out, row.names = FALSE, digits = 5)

rank_by <- function(col) out$label[order(out[[col]], decreasing = (col == "coverage"))]
cat("\n== Reading ==\n")
cat("COVERAGE ranks:", paste(sprintf("%s (%.0f%%)", rank_by("coverage"),
    100 * out$coverage[order(out$coverage, decreasing = TRUE)]), collapse = " > "), "\n")
cat("  -> coverage crowns the WIDE model, the least informative of the three.\n")
cat("INTERVAL SCORE ranks (lower=better):",
    paste(sprintf("%s (%.0f)", out$label[order(out$mean_interval_score)],
    out$mean_interval_score[order(out$mean_interval_score)]), collapse = " < "), "\n")
cat("  -> the proper score crowns the definition-MATCHED sharp model, which\n")
cat("     coverage ranks SECOND. Same data, opposite verdict; the score is right.\n")
cat("DEFINITION, NOT CALIBRATION: attrition on a cumulative stock turns a 67%-\n")
cat("  covering, interval-score-", round(out$mean_interval_score[3]), " model into a 0%-covering, ",
    round(out$mean_interval_score[2]), " one.\n", sep = "")
cat("RESIDUAL: the matched model still runs low (bias ", round(out$signed_bias[3]),
    "); that is the entrant-regime\n  question, not an interval-width problem.\n", sep = "")

dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, "artifacts/diagnostics/interval_honesty_scorecard.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/interval_honesty_scorecard.csv\n")
