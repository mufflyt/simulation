#!/usr/bin/env Rscript
# Where the low bias comes from: definition error vs entrant-regime acceleration -
#
#   Rscript scripts/diagnostics/entrant_regime_bias_decomposition.R
#
# THE QUESTION. The 2020->2023 backtest under-predicts the certification stock:
# the shipped arm reaches 1179 against an observed 1306 (a -127, -9.7% miss), and
# the interval-honesty scorecard shows no interval width repairs a biased centre.
# So WHY is the centre biased low? This decomposes the miss into two parts a
# reader can act on, from the committed per-arm summary (no mufflyaccess):
#
#   1. DEFINITION ERROR (correctable now). The target is a CUMULATIVE certification
#      count -- a stock nobody exits. The shipped arm nevertheless applies career
#      attrition, removing providers the cumulative count never loses. Turning
#      attrition off (definition-matched) is the same model measured correctly.
#
#   2. ENTRANT-REGIME residual (a hypothesis, NOT yet validated). Even
#      definition-matched with the shipped entrant rate (55/yr), the stock grew
#      faster than predicted: realized net entry ran ~+69/yr. The gap is the
#      post-2015 entrant acceleration. It was identified AFTER seeing this miss,
#      so it is model selection on the test set -- it CANNOT be claimed from this
#      backtest and must be settled prospectively (the preregistered
#      rolling-origin, scripts/run_preregistered_rolling_origin.R).
#
# Writes artifacts/diagnostics/entrant_regime_bias_decomposition.csv.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    if (requireNamespace("pkgload", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE)
  } else {
    library(urpssim)
  }
})

sum_path <- Find(file.exists, c(
  "artifacts/backtest_2020_to_2023_summary.csv",
  "artifacts/frozen_2026-08-04_backtest10/backtest_2020_to_2023_summary.csv"))
if (is.null(sum_path)) stop("Need the backtest summary artifact.", call. = FALSE)
s <- utils::read.csv(sum_path, stringsAsFactors = FALSE)

pick <- function(arm) {
  r <- s[s$arm == arm, ]
  if (nrow(r) != 1) stop("expected exactly one row for: ", arm, call. = FALSE)
  r
}
ARM_MM <- "1. Derived cohort, entrants = 55 (shipped assumption)"
ARM_DM <- "1. Derived cohort, entrants = 55 (shipped assumption) [no-attrition, definition-matched]"
mm <- pick(ARM_MM); dm <- pick(ARM_DM)

base   <- mm$baseline_count          # 1099 (2020)
obs    <- mm$observed                # 1306 (2023)
shipped<- mm$predicted_median        # 1179  (attrition ON)
matched<- dm$predicted_median        # 1265  (no-attrition)
horizon<- mm$target_year - mm$baseline_year

total_miss   <- obs - shipped                 # -127
attrition_fix<- matched - shipped             # +86  : share removed by fixing the definition
regime_resid <- obs - matched                 # +41  : residual entrant-regime acceleration

wf <- data.frame(
  step = c("shipped forecast (attrition ON, entrants=55)",
           "+ fix definition error (attrition OFF on a cumulative stock)",
           "+ close entrant-regime residual (realized net entry > 55/yr)"),
  level_2023 = c(shipped, matched, obs),
  delta = c(NA_real_, attrition_fix, regime_resid),
  pct_of_total_miss = c(NA_real_, 100 * attrition_fix / (obs - shipped),
                        100 * regime_resid / (obs - shipped)),
  annualized_per_year = c(NA_real_, attrition_fix / horizon, regime_resid / horizon),
  stringsAsFactors = FALSE)

cat(sprintf("== Decomposing the %+d (%.1f%%) certification-stock miss, %d->%d ==\n",
            total_miss, 100 * total_miss / shipped, mm$baseline_year, mm$target_year))
cat(sprintf("base %d (%d) -> observed %d (%d); horizon %d yr; observed net change %+.1f/yr\n\n",
            base, mm$baseline_year, obs, mm$target_year, horizon, (obs - base) / horizon))
print(wf, row.names = FALSE, digits = 4)

cat("\n== Reading ==\n")
cat(sprintf("DEFINITION ERROR accounts for %.0f%% of the miss (%+d of %+d; %+.1f/yr).\n",
            100 * attrition_fix / (obs - shipped), attrition_fix, obs - shipped,
            attrition_fix / horizon))
cat("  Attrition applied to a cumulative certification stock removes providers the\n")
cat("  count never loses. This is correctable NOW and is not a forecasting failure.\n")
cat(sprintf("ENTRANT-REGIME residual is the other %.0f%% (%+d; %+.1f/yr): realized net\n",
            100 * regime_resid / (obs - shipped), regime_resid, regime_resid / horizon))
cat("  entry outran the shipped 55/yr. It was seen only AFTER the miss, so it is a\n")
cat("  hypothesis to test PROSPECTIVELY (preregistered rolling-origin), never a\n")
cat("  parameter to tune on this backtest.\n")

# Cross-check: the entrant-assumption gradient across arms (definition-matched only).
cat("\n== Entrant-assumption gradient (no-attrition arms; observed net +",
    round((obs - base) / horizon), "/yr) ==\n", sep = "")
dmm <- s[grepl("no-attrition", s$arm), c("arm", "entrants_per_year", "predicted_annual_change",
                                         "predicted_median", "percent_error", "within_95")]
dmm$arm <- substr(sub(" \\[.*", "", dmm$arm), 1, 46)
print(dmm, row.names = FALSE, digits = 4)

dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)
utils::write.csv(wf, "artifacts/diagnostics/entrant_regime_bias_decomposition.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/entrant_regime_bias_decomposition.csv\n")
