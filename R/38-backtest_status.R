# Back-Test Status Stamping ----
#
# The 2020->2023 back-test is the only external validation this engine has, and
# it FAILED coverage: the observed 2023 count fell outside the 95% interval in
# every one of the eight arms, and outside the 80% interval in every arm too.
# That result is written up honestly in docs/BACKTEST_2020_TO_2023.md and scored
# in artifacts/backtest_2020_to_2023_summary.csv.
#
# Neither of those travels with a projection object. A reader handed a table of
# medians and 95% bands has no way to know the bands are unvalidated, and the
# natural reading of "95% interval" is a forecast interval -- which is precisely
# the claim the back-test refuted. This module attaches the validation status to
# the outputs themselves, so the caveat moves with the number instead of living
# in a document beside it.
#
# The status is DERIVED from scored arms, never asserted: `backtest_status()`
# runs the same computation over the frozen record that
# `backtest_status_from_summary()` runs over a live `run_backtest()` result, so a
# future run that achieves coverage flips the verdict automatically.

# Frozen record of the prespecified back-test, transcribed from
# artifacts/backtest_2020_to_2023_summary.csv (manifest: cutoff 2020, target
# 2023, target value 1306, 1000 iterations per arm, seed 20260802). `artifacts/`
# is in .Rbuildignore and does not ship, so the scored numbers are carried here
# rather than read at run time. Re-derive from a live run with
# `backtest_status_from_summary(run_backtest()$summary)`.
BACKTEST_RECORD_2020_2023 <- tibble::tribble(
  ~arm,                                        ~percent_error, ~within_80, ~within_95,
  "1. Derived cohort, assumed entrants",              -9.800919,      FALSE,      FALSE,
  "1. Derived cohort [no-attrition]",                 -3.215926,      FALSE,      FALSE,
  "2. Derived cohort, pre-cutoff entrants",           -8.499234,      FALSE,      FALSE,
  "2. Derived cohort [no-attrition]",                 -1.914242,      FALSE,      FALSE,
  "3. Synthetic cohort, assumed entrants",           -12.557427,      FALSE,      FALSE,
  "3. Synthetic cohort [no-attrition]",               -3.215926,      FALSE,      FALSE,
  "4. Synthetic cohort, pre-cutoff entrants",        -11.255743,      FALSE,      FALSE,
  "4. Synthetic cohort [no-attrition]",               -1.914242,      FALSE,      FALSE
)

BACKTEST_RECORD_SOURCE <- paste(
  "artifacts/backtest_2020_to_2023_summary.csv; cutoff 2020, target 2023",
  "(observed 1306, national/ABOG_PLUS_ABU/board_certified_active, contract",
  "v3.0.0), 1000 iterations per arm, seed 20260802"
)

# Share of arms whose 95% interval must contain the observed value before the
# engine's intervals may be described as validated forecast intervals. Not all
# eight: the arms deliberately differ in cohort construction and attrition
# definition, so a single discordant arm should not veto the claim. A majority
# threshold is the weakest defensible bar, and the engine currently scores zero.
BACKTEST_COVERAGE_REQUIRED <- 0.8

#' Derive validation status from scored back-test arms
#'
#' @param summary Tibble of scored arms with `within_80`, `within_95` and
#'   `percent_error` (the shape [score_backtest_arm()] returns).
#' @param source Provenance string recorded in the status.
#' @param required Share of arms whose 95% interval must cover the observation.
#' @return An object of class `urps_backtest_status`.
#' @export
backtest_status_from_summary <- function(summary,
                                         source = "live run_backtest() result",
                                         required = BACKTEST_COVERAGE_REQUIRED) {
  need <- c("within_95", "percent_error")
  missing <- setdiff(need, names(summary))
  if (length(missing) > 0) {
    stop(sprintf("backtest_status_from_summary: missing column(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  n <- nrow(summary)
  if (n == 0) stop("backtest_status_from_summary: no scored arms", call. = FALSE)

  cov95 <- mean(as.logical(summary$within_95), na.rm = TRUE)
  cov80 <- if ("within_80" %in% names(summary)) {
    mean(as.logical(summary$within_80), na.rm = TRUE)
  } else NA_real_
  pe <- summary$percent_error[is.finite(summary$percent_error)]

  structure(
    list(
      validated = isTRUE(cov95 >= required),
      n_arms = n,
      coverage_95 = cov95,
      coverage_80 = cov80,
      coverage_required = required,
      worst_percent_error = if (length(pe)) pe[which.max(abs(pe))] else NA_real_,
      median_percent_error = if (length(pe)) stats::median(pe) else NA_real_,
      # All eight arms under-predicted. A one-sided miss is a different problem
      # from scatter around the truth: it points at the entrant rate, not noise.
      all_same_direction = length(pe) > 0 && length(unique(sign(pe))) == 1L,
      source = source
    ),
    class = "urps_backtest_status"
  )
}

#' Validation status of this engine's projected intervals
#'
#' The single place any output may ask "has this engine been validated?". Built
#' from the prespecified 2020->2023 back-test.
#'
#' @param record Scored arms; defaults to the frozen published record.
#' @param source Provenance string.
#' @return An object of class `urps_backtest_status`.
#' @export
backtest_status <- function(record = BACKTEST_RECORD_2020_2023,
                            source = BACKTEST_RECORD_SOURCE) {
  backtest_status_from_summary(record, source = source)
}

#' How an interval from this engine may be described
#'
#' Returns the phrase that belongs in a table caption or figure legend. While
#' coverage fails, "forecast interval" is not available: the back-test refuted
#' exactly that claim, and the honest description is a Monte Carlo range.
#'
#' @param status A [backtest_status()] object.
#' @param ci Nominal interval width.
#' @return A character label.
#' @export
interval_label <- function(status = backtest_status(), ci = 0.95) {
  pct <- format(round(100 * ci))
  if (isTRUE(status$validated)) {
    return(sprintf("%s%% forecast interval (back-test coverage %.0f%% of %d arms)",
                   pct, 100 * status$coverage_95, status$n_arms))
  }
  sprintf(paste("%s%% Monte Carlo range -- NOT a validated forecast interval:",
                "the observed value fell outside it in %d of %d back-test arms"),
          pct, round((1 - status$coverage_95) * status$n_arms), status$n_arms)
}

#' Refuse forecast-interval language while coverage fails
#'
#' Call before publishing anything that describes this engine's bands as
#' forecast or prediction intervals.
#'
#' @param status A [backtest_status()] object.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when the language is permitted.
#' @export
assert_forecast_intervals_validated <- function(status = backtest_status(),
                                                mode = resolve_reproducibility_mode()) {
  if (isTRUE(status$validated)) return(invisible(TRUE))
  msg <- sprintf(paste(
    "This engine's intervals are not validated: the observed value fell outside",
    "the 95%% interval in %d of %d back-test arms (coverage %.0f%%, required",
    "%.0f%%), and every arm under-predicted by up to %.1f%%. Report the band as",
    "a Monte Carlo range -- see interval_label() -- not as a forecast or",
    "prediction interval. Source: %s."),
    round((1 - status$coverage_95) * status$n_arms), status$n_arms,
    100 * status$coverage_95, 100 * status$coverage_required,
    abs(status$worst_percent_error), status$source)
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' Attach the back-test status to an exported object
#'
#' The URPS projection contract is a fixed 13-column schema, so the status rides
#' as an attribute rather than a column: adding one would fail
#' `mufflyaccess::validate_urps_projection()`.
#'
#' @param x Object to stamp (typically a projection data frame).
#' @param status A [backtest_status()] object.
#' @return `x`, with a `backtest_status` attribute.
#' @export
stamp_backtest_status <- function(x, status = backtest_status()) {
  attr(x, "backtest_status") <- status
  x
}

#' Read the back-test status stamped on an object
#'
#' @param x A stamped object.
#' @return The [backtest_status()] object, or NULL if the object is unstamped.
#' @export
stamped_backtest_status <- function(x) attr(x, "backtest_status", exact = TRUE)

#' @export
print.urps_backtest_status <- function(x, ...) {
  cat(sprintf("External validation: %s\n",
              if (isTRUE(x$validated)) "PASSED" else "FAILED (coverage)"))
  cat(sprintf("  arms                %d\n", x$n_arms))
  cat(sprintf("  95%% coverage        %.0f%% (required %.0f%%)\n",
              100 * x$coverage_95, 100 * x$coverage_required))
  if (is.finite(x$coverage_80)) {
    cat(sprintf("  80%% coverage        %.0f%%\n", 100 * x$coverage_80))
  }
  cat(sprintf("  worst error         %.1f%%\n", x$worst_percent_error))
  cat(sprintf("  median error        %.1f%%\n", x$median_percent_error))
  if (isTRUE(x$all_same_direction)) {
    cat("  every arm missed in the SAME direction: a level problem, not noise\n")
  }
  cat(sprintf("  intervals           %s\n", interval_label(x)))
  cat(sprintf("  source              %s\n", x$source))
  invisible(x)
}
