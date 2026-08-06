#!/usr/bin/env Rscript
# =============================================================================
# Preregistered rolling-origin evaluation runner
# =============================================================================
# Demonstrates the governance loop that removes the "designed after the miss"
# contamination for future evaluations (R/validation-preregistration.R):
#   1. FREEZE the model specification and record it (hash + freeze date);
#   2. run a leakage-free rolling-origin evaluation GATED on that record, so a
#      spec silently re-tuned after seeing the targets is refused.
#
#   Rscript scripts/run_preregistered_rolling_origin.R
#
# INPUT: an observed annual series with a time column and a count/target column.
#   Point OBSERVED_SERIES_CSV at it (columns `year`, `count`), e.g. the ABOG/ABU
#   board-certified active URPS count by year. A small synthetic series is used
#   as a documented fallback so the runner executes before the real series is
#   supplied -- clearly flagged, never passed off as a result.
#
# OUTPUT:
#   inst/extdata/preregistration/entrant_regime_v1.txt   (immutable prereg record)
#   artifacts/preregistered_rolling_origin.csv           (per-origin OOS scores)
# =============================================================================

suppressPackageStartupMessages({ library(stats) })
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# ---- 1. The FROZEN specification --------------------------------------------
# Field order is irrelevant to the hash; values are not. Edit this ONLY before
# the next data vintage is observed -- any change after that is model selection
# on the test set and the guard below will reject it.
spec <- list(
  model            = "entrant_regime_loglinear",
  form             = "log(count) ~ year",         # the frozen functional form
  predictor        = "year",
  refit            = "parameters_only",           # never re-select the form per origin
  horizon          = 1L,
  origins          = 2015:2022,
  metric           = "mape",
  frozen_rationale = "form fixed before the post-2023 vintage; only slope/intercept refit"
)
FROZEN_AT <- Sys.getenv("PREREG_FROZEN_AT", "2026-08-05")   # state the freeze date honestly
prereg_path <- "inst/extdata/preregistration/entrant_regime_v1.txt"

rec <- preregister_spec(spec, prereg_path, frozen_at = FROZEN_AT,
                        notes = "Preregistered entrant-regime rolling-origin protocol")
cat(sprintf("Preregistered spec %s (frozen_at %s)\n  record: %s\n",
            substr(rec$spec_hash, 1, 12), FROZEN_AT, prereg_path))

# ---- 2. Observed series -----------------------------------------------------
# Priority: (1) an explicit CSV; (2) the real ABOG/ABU board-certified active
# count from mufflyaccess, wherever that data package is installed -- so this
# produces a REAL preregistered evaluation with no extra steps in an equipped
# environment; (3) a clearly-flagged synthetic fallback, never passed off as a
# result. `n_active` is the cumulative certification STOCK (its 2020 value is
# 1099 and 2023 is 1306), which is exactly the `year`/`count` series the frozen
# log-linear form is specified on.
series_csv <- Sys.getenv("OBSERVED_SERIES_CSV", "")
real_data <- FALSE
if (nzchar(series_csv) && file.exists(series_csv)) {
  series <- utils::read.csv(series_csv)
  src <- series_csv
  real_data <- TRUE
} else if (requireNamespace("mufflyaccess", quietly = TRUE)) {
  x <- mufflyaccess::urps_counts_long()
  a <- x[x$measure == "board_certified_active" & x$geography == "national" &
           x$board_pathway == "ABOG_PLUS_ABU", c("year", "n_active")]
  a <- a[order(a$year), ]
  series <- data.frame(year = a$year, count = a$n_active)
  src <- sprintf("mufflyaccess::urps_counts_long() [national/ABOG_PLUS_ABU/board_certified_active, %d-%d]",
                 min(a$year), max(a$year))
  real_data <- TRUE
} else {
  # Documented fallback: a smooth synthetic count series. NOT a result.
  series <- data.frame(year = 2008:2023,
                       count = round(1000 * exp(0.02 * (0:15))))
  src <- "SYNTHETIC fallback (install mufflyaccess or set OBSERVED_SERIES_CSV for the real ABOG/ABU series)"
}
cat("Observed series: ", src, " (", nrow(series), " years)\n", sep = "")

# ---- 3. The frozen fit_predict (parameters refit per origin, form fixed) ----
# Refits ONLY the coefficients of the frozen log-linear form on data up to the
# origin, then predicts the target year. It never inspects the target.
fit_predict <- function(train, target_time) {
  fit <- stats::lm(log(count) ~ year, data = train)
  exp(unname(stats::predict(fit, newdata = data.frame(year = target_time))))
}

# ---- 4. Guarded rolling-origin evaluation -----------------------------------
res <- rolling_origin_evaluation(series, "year", "count", origins = spec$origins,
                                 horizon = spec$horizon, fit_predict = fit_predict,
                                 prereg = prereg_path, spec = spec)
dir.create("artifacts", showWarnings = FALSE)
utils::write.csv(res$by_origin, "artifacts/preregistered_rolling_origin.csv", row.names = FALSE)

cat("\n== Preregistered rolling-origin (leakage-free, spec-gated) ==\n")
print(res$by_origin, row.names = FALSE)
cat(sprintf("\nMAPE %.1f%% over %d origins | horizon %d | all targets future: %s | prereg %s\n",
            res$summary$mape, res$summary$n, res$summary$horizon,
            res$summary$all_targets_future, substr(res$summary$spec_hash, 1, 12)))
if (!real_data)
  cat("NOTE: synthetic fallback series -- the MAPE is structure, not a result.",
      "Install mufflyaccess or set OBSERVED_SERIES_CSV for a real preregistered evaluation.\n")
cat("\nCommit inst/extdata/preregistration/entrant_regime_v1.txt so the freeze is on record.\n")
