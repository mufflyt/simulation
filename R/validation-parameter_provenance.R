# Parameter Provenance for the Historical Back-Test ----
#
# WHY THIS FILE EXISTS. `assert_no_leakage()` guards DATA reads: it fails if the
# back-test reads a contract series past the cutoff. It says nothing about
# PARAMETERS, and that gap is not hypothetical. The 1.42%/yr under-50 career
# change hazard, published in 2025, sat inside a back-test that advertised a
# 2020 forecast origin for as long as the audit was data-only. The data guard
# passed the whole time, because the parameter did not arrive through a series
# read.
#
# A forecast origin binds every input, not just the ones with a year column.
# This table is the parameter-side audit, and `BACKTEST_PARAMETER_PROVENANCE` is
# deliberately hand-maintained: adding a parameter to the back-test without
# recording where it came from should be a visible omission, not a silent one.
#
# `basis` distinguishes two things that cannot be audited the same way:
#
#   * "published" -- a value taken from a dated source. It leaks if the source
#     postdates the cutoff, and `available_by` is the publication year.
#   * "assumption" -- a structural choice the analyst made, carrying no
#     publication date. It cannot leak from the future because it was never
#     read from anywhere. It can still be WRONG, which is a different audit.

#' Provenance of every parameter in the historical back-test path
#'
#' One row per parameter that reaches the 2020 back-test, recording where the
#' value came from, the year that source became available, and whether the
#' primary analysis uses it. [assert_backtest_parameters_precede_cutoff()]
#' reads this table.
#'
#' @format Tibble with `parameter`, `value`, `basis`, `source`, `available_by`,
#'   `in_primary_backtest`.
#' @family backtest
#' @concept validation
#' @export
BACKTEST_PARAMETER_PROVENANCE <- tibble::tribble(
  ~parameter,                          ~basis,       ~source,                                              ~available_by, ~in_primary_backtest,
  "RETIREMENT_HAZARD_BY_AGE",          "published",  "HWSM Exhibit 17, doc v5.19.20 (May 2020); FutureDocs 2017", 2020L,  TRUE,
  "RETIREMENT_SEX_HAZARD_MULTIPLIER",  "published",  "HWSM Exhibit 17, doc v5.19.20 (May 2020)",                  2020L,  TRUE,
  "MICROSIM_TERMINAL_AGE",             "published",  "HWSM: age 90 for physicians and dentists",                  2020L,  TRUE,
  "CAREER_CHANGE_HAZARD_UNDER_50",     "published",  "Zarek et al, Phys Ther 2025;105:pzaf014 (CPS ASEC)",        2025L,  FALSE,
  "MICROSIM_ENTRY_AGE",                "assumption", "cliff WC_ENTRY_AGE: age at entry to practice",              NA_integer_, TRUE,
  "BACKLOG_COHORT_AGE_MEAN_AT_CERT",   "assumption", "practice-pathway cohort certified <= 2013",                 NA_integer_, TRUE,
  "BACKLOG_COHORT_AGE_SD_AT_CERT",     "assumption", "practice-pathway cohort certified <= 2013",                 NA_integer_, TRUE,
  "backtest_cohort_at(female_share)",  "assumption", "0.55 female share at certification",                        NA_integer_, TRUE
)

#' Fail if any published parameter in the back-test postdates the cutoff
#'
#' The parameter-side companion to [assert_no_leakage()]. Checks only rows
#' flagged `in_primary_backtest`, because a parameter scored in a supplementary
#' sensitivity analysis is allowed to postdate the origin: that is what makes it
#' a sensitivity analysis rather than the primary result.
#'
#' @details
#' A SENSITIVITY ANALYSIS DOES NOT CURE LEAKAGE. If a post-cutoff parameter is
#' in the primary path, the primary result is not an out-of-time forecast, and
#' showing that the effect is small does not restore the claim. The remedy is to
#' remove the parameter from the primary analysis and score it separately, which
#' is why `in_primary_backtest` gates this check and the report names the
#' omitted rows rather than hiding them.
#'
#' @param cutoff_year The declared forecast origin.
#' @param provenance Provenance table; defaults to
#'   [BACKTEST_PARAMETER_PROVENANCE].
#' @return Invisibly, the rows that were checked. Errors on any violation.
#' @family backtest
#' @concept validation
#' @export
#' @examples
#' assert_backtest_parameters_precede_cutoff(2020L)
assert_backtest_parameters_precede_cutoff <- function(
    cutoff_year = BACKTEST_CUTOFF_YEAR,
    provenance = BACKTEST_PARAMETER_PROVENANCE) {

  # `isTRUE()` is not vectorised and this is a column. Subsetting with a bare
  # logical column would also admit NA rows, silently skipping an unaudited
  # parameter, which is the exact failure this file exists to prevent.
  # `isTRUE_vec()` (R/data-swan_dmdm_panel.R) already coerces NA to FALSE.
  used <- provenance[isTRUE_vec(provenance$in_primary_backtest), , drop = FALSE]
  pub <- used[used$basis == "published", , drop = FALSE]

  if (any(is.na(pub$available_by))) {
    stop("assert_backtest_parameters_precede_cutoff: a published parameter has ",
         "no availability year, so it cannot be audited: ",
         paste(pub$parameter[is.na(pub$available_by)], collapse = ", "),
         call. = FALSE)
  }

  bad <- pub[pub$available_by > cutoff_year, , drop = FALSE]
  if (nrow(bad)) {
    stop(sprintf(paste(
      "PARAMETER LEAKAGE: %d parameter(s) in the primary back-test postdate the",
      "%d cutoff:\n%s\nA sensitivity analysis does not repair this. Remove the",
      "parameter from the primary path and score it separately."),
      nrow(bad), cutoff_year,
      paste(sprintf("  - %s (%s, available %d)", bad$parameter, bad$source,
                    bad$available_by), collapse = "\n")),
      call. = FALSE)
  }
  invisible(used)
}
