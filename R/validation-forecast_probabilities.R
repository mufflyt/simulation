# Decision-relevant probability statements from Monte Carlo draws ---------------
#
# WHY THIS EXISTS
# A microsimulation's output is a distribution, so its headline should be a
# distribution -- median and a prediction interval -- plus the probability
# statements a reader actually decides on: "how likely is the shortage to exceed
# X%?", "how likely is access to improve?". Reporting a single number (e.g.
# "2050 workforce = 1,780") throws away exactly the uncertainty the engine spent
# its draws computing. These helpers read those quantities off draws the model
# already produces; they add no modelling and invent no uncertainty.

#' Probability summary of a Monte Carlo output
#'
#' Turns a vector of draws into the report a decision-maker wants: the median, a
#' central prediction interval, and -- optionally -- the probability the quantity
#' exceeds (or falls below) each of a set of thresholds. Works on any Monte Carlo
#' output: a workforce gap, an access metric, a shortfall count.
#'
#' @param x Numeric draws (e.g. one column of a [run_psa()] result's `draws`).
#' @param prob_level Central interval mass (e.g. 0.95 gives the 2.5/97.5
#'   quantiles).
#' @param exceed Optional numeric threshold(s) for probability statements; `NULL`
#'   skips them.
#' @param direction `"above"` reports `P(x > threshold)` (the default), `"below"`
#'   reports `P(x < threshold)`.
#' @param label Optional label carried into the summary.
#' @param na.rm Drop `NA` draws (failed PSA evaluations) before summarising;
#'   their count is reported as `n_na`.
#' @return A list: `summary` (one-row data frame: `label`, `n`, `n_na`, `mean`,
#'   `median`, `pi_lo`, `pi_hi`, `prob_level`) and `exceedance` (a data frame of
#'   `threshold`, `direction`, `probability`, `statement`, or `NULL`).
#' @family forecast probabilities
#' @concept validation
#' @export
forecast_probabilities <- function(x, prob_level = 0.95, exceed = NULL,
                                   direction = c("above", "below"),
                                   label = NA_character_, na.rm = TRUE) {
  direction <- match.arg(direction)
  stopifnot(is.numeric(x), length(x) >= 1L,
            is.numeric(prob_level), prob_level > 0, prob_level < 1)
  n_na <- sum(is.na(x))
  if (na.rm) x <- x[!is.na(x)]
  if (!length(x)) stop("forecast_probabilities(): no non-NA draws.", call. = FALSE)
  a <- (1 - prob_level) / 2
  summary <- data.frame(
    label = label, n = length(x), n_na = n_na,
    mean = mean(x), median = stats::median(x),
    pi_lo = unname(stats::quantile(x, a)),
    pi_hi = unname(stats::quantile(x, 1 - a)),
    prob_level = prob_level, stringsAsFactors = FALSE)

  exceedance <- NULL
  if (!is.null(exceed) && length(exceed)) {
    stopifnot(is.numeric(exceed))
    prob <- vapply(exceed, function(t) if (direction == "above") mean(x > t) else mean(x < t),
                   numeric(1))
    exceedance <- data.frame(
      threshold = exceed, direction = direction, probability = prob,
      statement = sprintf("P(value %s %g) = %.1f%%",
                          if (direction == "above") ">" else "<", exceed, 100 * prob),
      stringsAsFactors = FALSE)
  }
  list(summary = summary, exceedance = exceedance)
}

#' Probability report for the workforce FTE gap
#'
#' Applies [forecast_probabilities()] to a [psa_workforce_gap()] result (or a raw
#' vector of gap draws) and phrases the exceedance probabilities as shortage
#' statements: the median gap, its prediction interval, and P(shortage exceeds
#' X%) at each threshold. A threshold of `0` reads as "P(any shortage)".
#'
#' @param psa A [psa_workforce_gap()] result (a list with `$draws`), or a numeric
#'   vector of gap draws.
#' @param metric Column of `$draws` to summarise when `psa` is a PSA result;
#'   `"gap_pct"` (percent of demand, default) or `"gap_fte"`.
#' @param shortage_thresholds Numeric thresholds for the shortage probabilities,
#'   in the units of `metric` (percent for `"gap_pct"`). `0` gives P(any shortage).
#' @param prob_level Central prediction-interval mass.
#' @return A list: `summary` (one-row data frame) and `probabilities` (a data
#'   frame of `threshold`, `probability`, `statement`).
#' @seealso [psa_workforce_gap()], [forecast_probabilities()]
#' @family forecast probabilities
#' @concept validation
#' @examples
#' # Decision-relevant probabilities from a vector of gap draws (percent):
#' # P(any shortage) and P(shortage exceeds 10%).
#' set.seed(1)
#' workforce_gap_probabilities(rnorm(500, mean = 8, sd = 4),
#'                             metric = "gap_pct", shortage_thresholds = c(0, 10))
#' @export
workforce_gap_probabilities <- function(psa, metric = c("gap_pct", "gap_fte"),
                                        shortage_thresholds = c(0, 5, 10, 15),
                                        prob_level = 0.95) {
  metric <- match.arg(metric)
  if (is.numeric(psa)) {
    draws <- psa
  } else if (is.list(psa) && !is.null(psa$draws) && metric %in% names(psa$draws)) {
    draws <- psa$draws[[metric]]
  } else {
    stop("workforce_gap_probabilities(): `psa` must be a psa_workforce_gap() result ",
         "with a '", metric, "' column, or a numeric vector of gap draws.", call. = FALSE)
  }
  unit <- if (metric == "gap_pct") "%" else " FTE"
  fp <- forecast_probabilities(draws, prob_level = prob_level, exceed = shortage_thresholds,
                               direction = "above", label = metric)
  probs <- fp$exceedance[, c("threshold", "probability")]
  probs$statement <- ifelse(
    fp$exceedance$threshold == 0,
    sprintf("P(any shortage) = %.1f%%", 100 * fp$exceedance$probability),
    sprintf("P(shortage exceeds %g%s) = %.1f%%",
            fp$exceedance$threshold, unit, 100 * fp$exceedance$probability))
  list(summary = fp$summary, probabilities = probs)
}
