#!/usr/bin/env Rscript
# Temporal validation: rolling-origin, matched-origin leakage experiment, sharpness ----
#
#   Rscript scripts/validation/01_temporal_validation.R
#
# THE DESIGN, in the order the manuscript should report it.
#
# 1. PRIMARY. Rolling-origin validation on the PRESPECIFIED contemporary origins
#    2017-2020 -- these are backtest_multi_window()'s own defaults, not a set
#    chosen after seeing performance. A training window is admitted only when
#    its outcome was observable at the origin, so no future information enters.
#
# 2. HISTORICAL STRESS TEST. The same procedure extended back to 2013. Those
#    origins cross a documented structural break: classify_certification_regimes()
#    labels 2013-2015 BACKLOG (655/175/102 certifications against a 36-72 steady
#    state) and 2020 DISRUPTED (cancelled examination), from the series' own
#    structure and without reference to forecast error. Degraded performance
#    there is a finding about transportability, not a defect to hide.
#
# 3. LEAKAGE EXPERIMENT. Leave-one-out on the SAME origins. LOO is not a
#    competing validation method; it exists here to quantify what temporal
#    leakage buys. Comparing 4 rolling-origin rows against 8 LOO rows -- which an
#    earlier draft of this analysis did -- cannot support any such claim, because
#    the denominators differ.
#
# 4. SHARPNESS. Coverage alone is incomplete: an interval can cover by being
#    uselessly wide. The Winkler interval score combines coverage and width
#    (width, plus 2/alpha x the miss distance). Where everything covers it
#    reduces to width, which is exactly the comparison of interest.
#
# A NOTE ON THE 2017 INTERVAL. It is enormous and its lower bound is negative.
# That is not a bug and is deliberately NOT truncated at zero. Two limitations
# compound: only two prior errors are available (df = 1, t(0.975) = 12.71), and
# both come from the backlog regime (mean relative error 139%). The interval is
# reporting that the empirical error model is essentially unidentified at that
# origin. Truncating would conceal the most informative thing it says. A
# log-scale construction has positive support and is the principled improvement;
# it is reported here as a SECONDARY construction rather than swapped in after
# the fact, which would be outcome-driven.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})

CUTOFFS_PRIMARY <- 2017:2020          # backtest_multi_window() default
CUTOFFS_STRESS  <- 2013:2020
HORIZON <- 3L
MIN_TRAIN <- 2L

# Winkler interval score at level alpha. Lower is better, and it cannot be
# improved by widening -- the point of reporting it beside coverage.
winkler <- function(obs, lo, hi, alpha = 0.05) {
  (hi - lo) + ifelse(obs < lo, 2 / alpha * (lo - obs),
              ifelse(obs > hi, 2 / alpha * (obs - hi), 0))
}

cat("== certification regimes (classified from the series, not from error) ==\n")
s <- urps_certification_cohorts()
print(as.data.frame(classify_certification_regimes(
  data.frame(year = s$cert_year, count = s$n_certified))), row.names = FALSE)

cat("\n== multi-window point accuracy, stress range ==\n")
w <- backtest_multi_window(cutoffs = CUTOFFS_STRESS, horizon = HORIZON)
print(as.data.frame(w[, c("cutoff_year", "target_year", "predicted", "observed",
                          "percent_error")]), row.names = FALSE, digits = 4)

ro  <- backtest_rolling_origin(cutoffs = CUTOFFS_STRESS, horizon = HORIZON, min_train = MIN_TRAIN)
loo <- backtest_loo_validation(cutoffs = CUTOFFS_STRESS, horizon = HORIZON, min_train = MIN_TRAIN)
common <- sort(intersect(ro$origin, loo$origin))
r <- ro[match(common, ro$origin), ]
l <- loo[match(common, loo$origin), ]

cat("\n== MATCHED-ORIGIN leakage experiment ==\n")
tab <- data.frame(
  origin = common, observed = r$observed,
  ro_err = round(r$abs_pct_error, 2), ro_width = round(r$upper - r$lower),
  ro_cov = r$covered, ro_winkler = round(winkler(r$observed, r$lower, r$upper)),
  loo_err = round(l$abs_pct_error, 2), loo_width = round(l$upper - l$lower),
  loo_cov = l$covered, loo_winkler = round(winkler(l$observed, l$lower, l$upper)),
  loo_future_windows = l$n_train_future)
print(tab, row.names = FALSE)

summarise <- function(keep, label) {
  cat(sprintf("%-28s RO: |err| %.2f%%  width %.0f  Winkler %.0f  cov %d/%d   |   LOO: |err| %.2f%%  width %.0f  Winkler %.0f  cov %d/%d  (future windows %d)\n",
      label,
      median(tab$ro_err[keep]), median(tab$ro_width[keep]), median(tab$ro_winkler[keep]),
      sum(tab$ro_cov[keep]), sum(keep),
      median(tab$loo_err[keep]), median(tab$loo_width[keep]), median(tab$loo_winkler[keep]),
      sum(tab$loo_cov[keep]), sum(keep), sum(tab$loo_future_windows[keep])))
}
cat("\n")
summarise(rep(TRUE, nrow(tab)), "all matched origins")
summarise(tab$origin != 2017, "excluding 2017 (unstable)")

cat("\n== 2017 diagnosis: why the lower bound is negative ==\n")
tr <- w[w$target_year <= 2017, ]
e <- tr$percent_error / 100
tq <- stats::qt(0.975, nrow(tr) - 1L)
cat(sprintf("n_train %d, df %d, t(0.975) %.2f, mean rel err %.3f, sd %.3f\n",
            nrow(tr), nrow(tr) - 1L, tq, mean(e), stats::sd(e)))
cat(sprintf("additive lower factor  1 + mu - t*s      = %+.3f  (negative)\n",
            1 + mean(e) - tq * stats::sd(e)))
cat(sprintf("log-scale (SECONDARY)  exp(mu_l - t*s_l) = %+.3f  (positive support)\n",
            exp(mean(log(1 + e)) - tq * stats::sd(log(1 + e)))))
