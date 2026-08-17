# Recurrence as a treatment-cohort convolution ----
#
# THE DEFECT THIS REPLACES. The engine computed
#
#     recurrences_t = procedures_t x annual_hazard
#
# i.e. THIS YEAR's operations as the entire risk set: 350 x 0.12 = 42 per 1,000
# treated. Recurrences actually arise from the ACCUMULATED stock of everyone
# previously operated, so the model exposed a single cohort-year and a year with
# no new procedures produced no recurrences at all -- which is false for any
# condition whose failures accrue over years.
#
# THE REPLACEMENT is a convolution of historical treatment volume with the
# recurrence-time distribution:
#
#     R_t = SUM_k  C_{t-k} * g_k
#
# where C_{t-k} is the treatment cohort from k years ago and g_k is the
# probability that a member of that cohort generates the modelled recurrence
# event during year k after treatment.
#
# This needs NO person-level microsimulation. Today's recurrence burden is a
# property of past treatment volume and one distribution.
#
# RISK, HAZARD, CUMULATIVE INCIDENCE AND ANNUAL EVENTS ARE DIFFERENT THINGS and
# were previously conflated -- a multi-year cumulative observation (E-CARE) was
# used to license an annual rate. The converters below make the intended
# reading explicit at the call site instead of leaving it to a comment.

#' Recurrence-event probabilities from a cumulative incidence curve
#'
#' @param cumulative Numeric vector `F(1), F(2), ...`, non-decreasing, in [0,1].
#' @return `g_k = F(k) - F(k-1)`, the probability of first recurrence occurring
#'   during year k.
#' @family recurrence
#' @concept demand
#' @export
recurrence_g_from_cumulative <- function(cumulative) {
  if (!length(cumulative) || anyNA(cumulative)) {
    stop("cumulative must be a non-empty numeric vector without NA.", call. = FALSE)
  }
  if (any(cumulative < 0 | cumulative > 1)) {
    stop("cumulative incidence must lie in [0, 1].", call. = FALSE)
  }
  if (any(diff(cumulative) < -1e-12)) {
    stop("cumulative incidence must be non-decreasing. A decreasing curve means ",
         "the input is not a cumulative function -- most often an annual series ",
         "supplied by mistake.", call. = FALSE)
  }
  diff(c(0, cumulative))
}

#' Recurrence-event probabilities from conditional hazards
#'
#' @details
#' `S_k = prod_{j<k}(1 - h_j)`, `g_k = S_k * h_k`. Supplying hazards where
#' cumulative incidence is meant (or the reverse) changes the answer
#' substantially, which is why the two entry points are separate and named.
#'
#' @param hazards Numeric vector `h_1, h_2, ...` in [0,1).
#' @return `g_k`, the probability of first recurrence during year k.
#' @family recurrence
#' @concept demand
#' @export
recurrence_g_from_hazards <- function(hazards) {
  if (!length(hazards) || anyNA(hazards)) {
    stop("hazards must be a non-empty numeric vector without NA.", call. = FALSE)
  }
  if (any(hazards < 0 | hazards >= 1)) {
    stop("each conditional hazard must lie in [0, 1).", call. = FALSE)
  }
  surv <- cumprod(c(1, 1 - hazards[-length(hazards)]))
  surv * hazards
}

#' Recurrence events from treatment-cohort convolution
#'
#' @details
#' `cohorts` must carry EVERY treatment year that can still contribute,
#' including years BEFORE the forecast window. Initialising prior cohorts to
#' zero systematically suppresses early-forecast recurrence, which is the very
#' error this function exists to remove -- so an absent history is refused
#' rather than assumed empty.
#'
#' @param cohorts Data frame with `year` and `n` -- unique women newly treated.
#' @param g Recurrence-event probabilities by year since treatment, from
#'   [recurrence_g_from_cumulative()] or [recurrence_g_from_hazards()].
#' @param years Years to report. Defaults to the cohort years.
#' @param require_history If `TRUE` (default), refuse when a reported year has
#'   fewer than `length(g)` preceding cohort years available.
#' @return Tibble of `year`, `recurrences`, and `cohorts_contributing`.
#' @family recurrence
#' @concept demand
#' @export
recurrence_from_cohorts <- function(cohorts, g, years = NULL,
                                    require_history = TRUE) {
  if (!is.data.frame(cohorts) || !all(c("year", "n") %in% names(cohorts))) {
    stop("cohorts must be a data frame with columns `year` and `n`.", call. = FALSE)
  }
  if (anyNA(cohorts$n) || any(cohorts$n < 0)) {
    stop("cohort sizes must be non-negative and non-missing.", call. = FALSE)
  }
  if (!length(g) || anyNA(g) || any(g < 0)) {
    stop("g must be non-negative and non-missing.", call. = FALSE)
  }
  if (sum(g) > 1 + 1e-9) {
    stop(sprintf(
      paste0("g sums to %.4f. Cumulative FIRST recurrence cannot exceed the ",
             "treated cohort -- a sum above 1 means annual hazards were passed ",
             "where event probabilities were expected, or a cumulative curve ",
             "was differenced twice."), sum(g)), call. = FALSE)
  }

  cohorts <- cohorts[order(cohorts$year), , drop = FALSE]
  K <- length(g)
  if (is.null(years)) years <- cohorts$year

  out <- vapply(years, function(y) {
    # k = 1 .. K years since treatment; cohort treated in y - k contributes g[k]
    contrib <- vapply(seq_len(K), function(k) {
      n <- cohorts$n[cohorts$year == (y - k)]
      if (!length(n)) 0 else n[[1]] * g[[k]]
    }, numeric(1))
    sum(contrib)
  }, numeric(1))

  n_avail <- vapply(years, function(y) sum(cohorts$year < y & cohorts$year >= y - K),
                    numeric(1))
  if (isTRUE(require_history) && any(n_avail < K)) {
    short <- years[n_avail < K][1]
    stop(sprintf(
      paste0("Year %s has only %d of the %d preceding treatment cohorts the ",
             "recurrence window requires. Supply the pre-baseline cohorts, or ",
             "set require_history = FALSE and treat the early years as burn-in. ",
             "Initialising them to zero would suppress early recurrence, which ",
             "is the defect this function replaces."),
      short, n_avail[years == short][1], K), call. = FALSE)
  }

  tibble::tibble(year = years, recurrences = out,
                 cohorts_contributing = n_avail)
}

#' Status of the recurrence parameters
#'
#' @return A length-one character calibration tier.
#' @family recurrence
#' @concept demand
#' @export
recurrence_parameter_status <- function() {
  # 0.12 is documented as an ANNUAL hazard but justified by SUPeR/E-CARE
  # retreatment CURVES -- a multi-year cumulative observation licensing an
  # annual rate. Until the source estimand is established the parameters are
  # unresolved, and the convolution above must not be fed with them.
  # See docs/POP_RECURRENCE_ESTIMAND_AUDIT.md.
  "unresolved_requires_source"
}
