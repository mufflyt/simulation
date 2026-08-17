################################################################################
# R/calibration-backtest_estimand.R
# The back-test estimand contract: what the prediction and the observation each
# measure, and the refusal that stops them being compared when they differ.
#
# THE DEFECT THIS EXISTS TO PREVENT
#
# The 2020->2023 back-test under-predicted in all ten arms with 95% interval
# coverage of 0.20. The largest single component was not a parameter error and
# not data leakage. It was a CATEGORY ERROR: arms with `apply_attrition = TRUE`
# were scored against an observation that has no attrition in it.
#
#   apply_attrition FALSE (definition-matched)  n=5  mean error  -5.44%  cov 0.40
#   apply_attrition TRUE                        n=5  mean error -11.29%  cov 0.00
#
# 5.84 percentage points of bias, and all of the coverage loss in those arms,
# came from comparing a stock NET OF ATTRITION against a CUMULATIVE COUNT.
#
# WHY THE OBSERVATION CANNOT SIMPLY BE FIXED INSTEAD
#
# It is not that retirement removal was forgotten. It is unavailable:
# `mufflyaccess::urps_counts_long()` reports `n_retired` as 0 in every row and
# `n_active == n_ever_certified` (see R/supply-roster.R). The shipped artifact is
# a CUMULATIVE CERTIFICATION SERIES. The canonical 2023 value is 1,306 = 1,339
# roster headcount minus the 33 providers whose URPS certification postdates
# 2023 -- the ONLY exclusion applied.
#
# The contract's own wording, "board_certified_active: urps_subspecialty_cert_year
# <= Y and not retired by Y", therefore promises a retirement clause that the
# data cannot deliver. The number is not net of attrition, whatever the label says.
#
# THE RECONCILIATION
#
# For BACK-TESTING, the prediction must match the observation: no attrition.
# That is the only like-for-like comparison available.
#
# For FORECASTING, attrition is the scientifically meaningful quantity and
# should stay in the model. The attrited arms are not wrong -- they measure a
# different estimand, `active_net_of_attrition`, which currently has NO
# OBSERVABLE COUNTERPART. They must be reported as unvalidatable rather than as
# failing, because "failing" implies a comparison that was never legitimate.
#
# NOTHING HERE IMPROVES THE HEADLINE BY DISCARDING ARMS. The all-arms ratchet in
# .github/backtest-baseline.txt is untouched; matched coverage is recorded
# ALONGSIDE it. Deleting a failing arm is not an improvement.
################################################################################

#' Back-test estimand contract
#'
#' Declares what the observed series measures, what each prediction arm
#' measures, and which pairings are legitimate comparisons.
#'
#' @return A list describing the observed and predicted estimands.
#' @family validation
#' @concept calibration
#' @export
backtest_estimand_contract <- function() {
  list(
    observed = list(
      id = "cumulative_board_certified",
      applies_attrition = FALSE,
      definition = paste(
        "URPS subspecialty certification year <= Y, minus providers whose",
        "certification postdates Y. NOT net of attrition."),
      why_no_attrition = paste(
        "mufflyaccess::urps_counts_long() reports n_retired = 0 in every row",
        "and n_active == n_ever_certified. Retirement removal is unavailable,",
        "not merely unapplied."),
      canonical_2023 = 1306,
      arithmetic = "1339 roster headcount - 33 post-2023 certifications = 1306"),
    predicted = list(
      matched = list(
        id = "cumulative_board_certified",
        applies_attrition = FALSE,
        comparable_to_observed = TRUE),
      attrited = list(
        id = "active_net_of_attrition",
        applies_attrition = TRUE,
        comparable_to_observed = FALSE,
        status = "unvalidatable",
        why = paste(
          "Scientifically the more meaningful quantity for a workforce",
          "forecast, but no observed series measures it, so it cannot be",
          "back-tested. Report as unvalidatable, never as failing."))),
    rule = paste(
      "A prediction may be scored against the observed series ONLY when its",
      "attrition setting matches the observation's."))
}

#' Assert that a back-test summary compares like with like
#'
#' Refuses when an arm whose attrition setting differs from the observed series
#' is presented as a validated comparison.
#'
#' @param summary_df Back-test summary, one row per arm. Must carry
#'   `apply_attrition` and `observed_applies_attrition`; `arm` is used to
#'   identify arms labelled as definition-matched.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return Invisibly, a tibble with an added `estimand` and `comparable` column.
#' @family validation
#' @concept calibration
#' @export
assert_backtest_estimand_match <- function(summary_df,
                                           mode = resolve_reproducibility_mode()) {
  need <- c("apply_attrition", "observed_applies_attrition")
  missing <- base::setdiff(need, base::names(summary_df))
  if (base::length(missing) > 0L) {
    base::stop("Back-test summary is missing: ", base::paste(missing, collapse = ", "),
               ". Without them the estimand of each arm is undeclared and no ",
               "comparison can be justified.", call. = FALSE)
  }

  obs <- base::unique(base::as.logical(summary_df$observed_applies_attrition))
  if (base::length(obs) != 1L) {
    base::stop("The observed series declares more than one attrition definition; ",
               "it must have exactly one.", call. = FALSE)
  }

  pred <- base::as.logical(summary_df$apply_attrition)
  comparable <- pred == obs
  out <- tibble::tibble(
    arm = if ("arm" %in% base::names(summary_df)) summary_df$arm else NA_character_,
    apply_attrition = pred,
    estimand = base::ifelse(pred, "active_net_of_attrition", "cumulative_board_certified"),
    comparable = comparable,
    status = base::ifelse(comparable, "validated_comparison", "unvalidatable"))

  n_bad <- base::sum(!comparable)
  if (n_bad > 0L) {
    msg <- base::sprintf(
      paste0("%d of %d back-test arms compare a prediction with attrition = %s ",
             "against an observation with attrition = %s. That is a category ",
             "error, not a model error: the observed series is a cumulative ",
             "certification count and cannot be made net of attrition (n_retired ",
             "is 0 in every row). Those arms measure 'active_net_of_attrition', ",
             "which has no observable counterpart, and must be reported as ",
             "unvalidatable rather than as failing."),
      n_bad, base::nrow(out), !obs, obs)
    if (base::identical(mode, "strict")) base::stop(msg, call. = FALSE)
    .msg_warn(msg)
  }
  base::invisible(out)
}

#' Headline back-test metrics, computed on comparable arms only
#'
#' Reports matched-arm performance ALONGSIDE the all-arms figures, never
#' instead of them. Dropping an arm is not an improvement, so both are returned.
#'
#' @param summary_df Back-test summary, one row per arm.
#' @return A one-row tibble of all-arms and matched-arm metrics.
#' @family validation
#' @concept calibration
#' @export
backtest_headline_metrics <- function(summary_df) {
  obs <- base::unique(base::as.logical(summary_df$observed_applies_attrition))[1]
  m <- base::as.logical(summary_df$apply_attrition) == obs
  cov <- function(i) base::mean(base::as.logical(summary_df$within_95)[i])
  err <- function(i) base::mean(summary_df$percent_error[i])
  tibble::tibble(
    n_arms_all = base::nrow(summary_df),
    n_arms_matched = base::sum(m),
    coverage95_all = cov(base::rep(TRUE, base::nrow(summary_df))),
    coverage95_matched = cov(m),
    mean_error_all = err(base::rep(TRUE, base::nrow(summary_df))),
    mean_error_matched = err(m),
    mean_error_unvalidatable = if (base::any(!m)) err(!m) else NA_real_,
    bias_from_estimand_mismatch_pp = if (base::any(!m)) err(m) - err(!m) else NA_real_)
}
