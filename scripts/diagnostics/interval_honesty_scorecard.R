#!/usr/bin/env Rscript
# Coverage is not enough, and neither is sharpness: a scorecard on real OOS runs -
#
#   Rscript scripts/diagnostics/interval_honesty_scorecard.R
#
# WHAT THIS EXISTS TO SHOW. This project already has two real out-of-sample
# evaluations on the certification stock, and they reach OPPOSITE coverage:
#
#   * the frozen rolling-origin (artifacts/.../validation_rolling_origin.csv)
#     reaches 100% coverage -- but only because its intervals are enormous (one
#     lower bound is NEGATIVE, impossible for a cumulative stock);
#   * the headline 2020->2023 backtest (arm 1, the shipped assumption) uses sharp
#     Monte Carlo intervals and covers ~0-25% -- every forecast year misses HIGH,
#     because the point forecast is biased low on a growing series.
#
# Judged by COVERAGE alone, the wide model is perfect and the sharp one fails.
# That verdict is exactly the trap forecast_scorecard() / .interval_score() exist
# to defeat. The interval score (Gneiting & Raftery 2007) adds the interval WIDTH
# to a miss penalty of (2/alpha) x shortfall, so it CANNOT be gamed in either
# direction: widen to force coverage and you pay the width; narrow without fixing
# the bias and you pay the miss. Scored properly, BOTH models are bad -- for
# opposite reasons -- and that is the honest finding.
#
# Runs on committed artifacts only: no mufflyaccess, no digest, base R + the
# scorecard cores. Writes artifacts/diagnostics/interval_honesty_scorecard.csv.

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
arm1 <- bt[bt$arm == "1. Derived cohort, entrants = 55 (shipped assumption)", ]
BASE_YEAR <- min(arm1$year)   # the anchor row: prediction == observed by construction

# One scorecard row per (evaluation x scoring-set), at the nominal 95% level. The
# rolling-origin covers everything, so its interval score equals its width no
# matter the assumed level; the backtest columns are labelled pi95, so 95% is
# exact for the model whose misses the penalty actually bites.
sc <- function(obs, pred, lo, hi, lab) {
  forecast_scorecard(data.frame(observed = obs, predicted = pred, lower = lo, upper = hi),
                     interval_level = 0.95, label = lab)
}
keep <- c("label", "n", "coverage", "mean_width", "mean_interval_score",
          "mape", "signed_bias", "calibration_slope")

all_rows <- rbind(
  sc(ro$observed, ro$median_prediction, ro$lower, ro$upper, "rolling-origin (wide)"),
  sc(arm1$observed, arm1$predicted_median, arm1$pi95_lower, arm1$pi95_upper, "backtest arm1 (sharp)")
)[keep]
all_rows$scoring_set <- "all rows (incl. base-year anchor)"

fc <- arm1[arm1$year > BASE_YEAR, ]   # genuine forecasts: drop the base-year anchor
fore_rows <- rbind(
  sc(ro$observed, ro$median_prediction, ro$lower, ro$upper, "rolling-origin (wide)"),
  sc(fc$observed, fc$predicted_median, fc$pi95_lower, fc$pi95_upper, "backtest arm1 (sharp)")
)[keep]
fore_rows$scoring_set <- sprintf("genuine forecasts only (targets > %d)", BASE_YEAR)

out <- rbind(all_rows, fore_rows)

cat("== Forecast scorecard on two real out-of-sample evaluations ==\n")
cat("source (wide) :", ro_path, "\n")
cat("source (sharp):", bt_path, " [arm 1, shipped assumption]\n\n")
for (s in unique(out$scoring_set)) {
  cat("--", s, "--\n")
  print(out[out$scoring_set == s, setdiff(keep, "calibration_slope")], row.names = FALSE, digits = 5)
  cat("\n")
}

cat("== Reading ==\n")
cat("COVERAGE alone: wide 100% >> sharp 0-25%. On coverage the uninformative model\n")
cat("  wins outright -- the exact failure the interval score exists to prevent.\n")
cat("INTERVAL SCORE: on the genuine forecast horizon the WIDE model scores better\n")
cat("  (~1466 = pure width) than the SHARP model (~1732 = pure miss penalty). The\n")
cat("  ranking is not the point; the point is that BOTH are bad and the score says\n")
cat("  so -- widening to cover pays the width, narrowing without fixing the bias\n")
cat("  pays the (2/alpha) penalty. It cannot be gamed either way.\n")
cat("WHAT IS ACTUALLY WRONG: signed bias is about -87 in both -- the point forecast\n")
cat("  under-predicts a growing series. No interval width fixes a biased centre;\n")
cat("  that is the defect to report, not a coverage number.\n")

dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, "artifacts/diagnostics/interval_honesty_scorecard.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/interval_honesty_scorecard.csv\n")
