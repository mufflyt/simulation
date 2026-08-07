# validation-interval_coverage.R ---------------------------------------------------
# Rolling-origin interval coverage for the certification series. Replaces a
# single 2020 -> 2023 split with leave-future-out replication, solves the
# interval inflation factor from the observed series, and gates on coverage.
# Logging via base::message() to match the package convention.

#' Fit an expected-count baseline to the certification series
#'
#' Fits a smooth count trend to certifications observed strictly before
#' `fit_through_year`, in the observed-versus-expected spirit of the excess
#' mortality literature: the baseline states what the series would have done
#' absent disruption, and the disrupted window is scored against it rather
#' than absorbed into it.
#'
#' Dispersion is estimated from Pearson residuals rather than assumed to be
#' one. The certification series is strongly overdispersed relative to
#' Poisson, and treating it as Poisson is one of the two reasons the shipped
#' intervals are too narrow.
#'
#' @param certification_series A data frame with integer `year` and integer
#'   non-negative `certification_count`, one row per year, no gaps.
#' @param fit_through_year Integer scalar. Only years at or before this are
#'   used. Every later year is held out and never read, so this argument is
#'   the leakage boundary.
#' @param trend_family Character scalar, either `"loglinear"` for a
#'   log-linear trend in year or `"intercept"` for a flat mean. Use
#'   `"intercept"` when fewer than six training years are available.
#' @param verbose Logical flag controlling console logging via
#'   [base::message()]. Defaults to `TRUE`.
#'
#' @return A list with elements `fitted_model`, `dispersion`,
#'   `training_years`, `trend_family`, and `fit_through_year`.
#'
#' @importFrom assertthat assert_that is.string is.flag is.count
#' @importFrom dplyr filter arrange
#' @importFrom stats glm poisson predict residuals qnbinom qpois quantile
#' @importFrom rlang .data
#' @family interval coverage
#' @concept validation
#' @export
#'
#' @examples
#' illustrative_certification_series <- data.frame(
#'   year = 2012:2023,
#'   certification_count = c(
#'     33L, 36L, 41L, 39L, 45L, 44L, 40L, 48L, 10L, 81L, 54L, 72L
#'   )
#' )
#' baseline <- fit_certification_baseline(
#'   illustrative_certification_series, 2020L, "loglinear", verbose = FALSE
#' )
#' round(baseline$dispersion, 1)
fit_certification_baseline <- function(certification_series,
                                       fit_through_year,
                                       trend_family = "loglinear",
                                       verbose = TRUE) {
  assertthat::assert_that(is.data.frame(certification_series))
  assertthat::assert_that(assertthat::is.count(fit_through_year))
  assertthat::assert_that(assertthat::is.string(trend_family))
  assertthat::assert_that(trend_family %in% c("loglinear", "intercept"))
  assertthat::assert_that(assertthat::is.flag(verbose))

  validate_certification_series(certification_series)

  training_series <- dplyr::filter(certification_series,
                                   .data$year <= fit_through_year)
  training_series <- dplyr::arrange(training_series, .data$year)

  if (nrow(training_series) < 3L) {
    stop(sprintf(
      "only %d training years at or before %d; need at least 3",
      nrow(training_series), fit_through_year
    ), call. = FALSE)
  }

  model_formula <- if (identical(trend_family, "loglinear")) {
    stats::as.formula("certification_count ~ year")
  } else {
    stats::as.formula("certification_count ~ 1")
  }

  fitted_model <- stats::glm(model_formula, family = stats::poisson(),
                             data = training_series)

  pearson_residuals       <- stats::residuals(fitted_model, type = "pearson")
  residual_degrees_freedom <- max(fitted_model$df.residual, 1L)
  estimated_dispersion    <- sum(pearson_residuals^2) / residual_degrees_freedom

  log_coverage_step(verbose, "fit_certification_baseline(): %d training years, dispersion=%.3f",
                    nrow(training_series), estimated_dispersion)

  list(
    fitted_model    = fitted_model,
    dispersion      = estimated_dispersion,
    training_years  = training_series$year,
    trend_family    = trend_family,
    fit_through_year = fit_through_year
  )
}

#' Forecast certification counts with an overdispersed predictive interval
#'
#' Produces expected counts and a predictive interval for held-out years. The
#' interval is negative-binomial when the fitted dispersion exceeds one and
#' Poisson otherwise, then widened by `interval_inflation`. Pass an inflation
#' factor from [solve_interval_inflation()] rather than a hand-picked one.
#'
#' @param baseline_fit A list returned by [fit_certification_baseline()].
#' @param forecast_years Integer vector of years to forecast. Must all be
#'   strictly later than `baseline_fit$fit_through_year`.
#' @param interval_inflation Numeric scalar >= 1 multiplying the half-width.
#' @param nominal_coverage Numeric scalar in `(0, 1)`. Defaults to 0.95.
#' @param verbose Logical. Defaults to `TRUE`.
#'
#' @return A tibble with columns `year`, `expected_count`, `lower_bound`,
#'   and `upper_bound`.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom tibble tibble
#' @importFrom stats predict qnbinom qpois
#' @family interval coverage
#' @concept validation
#' @export
forecast_certification <- function(baseline_fit,
                                   forecast_years,
                                   interval_inflation = 1.0,
                                   nominal_coverage   = 0.95,
                                   verbose            = TRUE) {
  assertthat::assert_that(is.list(baseline_fit))
  assertthat::assert_that(is.numeric(forecast_years), length(forecast_years) >= 1L)
  assertthat::assert_that(is.numeric(interval_inflation), length(interval_inflation) == 1L)
  assertthat::assert_that(interval_inflation >= 1)
  assertthat::assert_that(is.numeric(nominal_coverage))
  assertthat::assert_that(nominal_coverage > 0, nominal_coverage < 1)
  assertthat::assert_that(assertthat::is.flag(verbose))

  leaking_years <- forecast_years[forecast_years <= baseline_fit$fit_through_year]
  if (length(leaking_years) > 0) {
    stop(sprintf(
      "forecast years %s are at or before the fit cutoff %d; this is leakage",
      paste(leaking_years, collapse = ", "), baseline_fit$fit_through_year
    ), call. = FALSE)
  }

  prediction_frame <- data.frame(year = as.numeric(forecast_years))
  expected_count   <- as.numeric(stats::predict(
    baseline_fit$fitted_model, newdata = prediction_frame, type = "response"
  ))

  tail_probability <- (1 - nominal_coverage) / 2
  raw_bounds <- overdispersed_count_interval(
    expected_count, baseline_fit$dispersion, tail_probability
  )

  inflated_lower <- pmax(
    expected_count - interval_inflation * (expected_count - raw_bounds$lower_bound), 0
  )
  inflated_upper <- expected_count +
    interval_inflation * (raw_bounds$upper_bound - expected_count)

  log_coverage_step(verbose,
    "forecast_certification(): %d years, inflation=%.2f, mean_width=%.1f",
    length(forecast_years), interval_inflation,
    mean(inflated_upper - inflated_lower)
  )

  tibble::tibble(
    year           = as.integer(forecast_years),
    expected_count = expected_count,
    lower_bound    = inflated_lower,
    upper_bound    = inflated_upper
  )
}

#' Score interval coverage by rolling-origin replication
#'
#' Refits the baseline at every admissible cutoff, forecasts the next
#' `forecast_horizon` years, and records whether each held-out observation
#' fell inside its interval. A single 2020 cutoff gives one realization of
#' the forecast problem no matter how many model arms run against it;
#' this gives one per fold.
#'
#' @param certification_series Data frame with `year` and `certification_count`.
#' @param minimum_train_years Integer. Smallest training window per fold. Default 6.
#' @param forecast_horizon Integer. Years forecast per fold. Default 3.
#' @param interval_inflation Numeric >= 1.
#' @param nominal_coverage Numeric in `(0, 1)`. Default 0.95.
#' @param verbose Logical. Default `TRUE`.
#'
#' @return List with `fold_results`, `empirical_coverage`, `nominal_coverage`,
#'   `n_folds`, and `coverage_ratio`.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr bind_rows
#' @importFrom tibble tibble
#' @importFrom rlang .data
#' @family interval coverage
#' @concept validation
#' @export
rolling_origin_coverage <- function(certification_series,
                                    minimum_train_years = 6L,
                                    forecast_horizon    = 3L,
                                    interval_inflation  = 1.0,
                                    nominal_coverage    = 0.95,
                                    verbose             = TRUE) {
  assertthat::assert_that(is.data.frame(certification_series))
  assertthat::assert_that(assertthat::is.count(minimum_train_years))
  assertthat::assert_that(assertthat::is.count(forecast_horizon))
  assertthat::assert_that(is.numeric(interval_inflation), interval_inflation >= 1)
  assertthat::assert_that(nominal_coverage > 0, nominal_coverage < 1)
  assertthat::assert_that(assertthat::is.flag(verbose))

  validate_certification_series(certification_series)

  ordered_series   <- certification_series[order(certification_series$year), ]
  available_years  <- ordered_series$year
  candidate_cutoffs <- available_years[
    seq_along(available_years) >= minimum_train_years &
    seq_along(available_years) < length(available_years)
  ]

  if (length(candidate_cutoffs) < 1L) {
    stop(sprintf(
      "no admissible folds: %d years available, minimum_train_years = %d",
      length(available_years), minimum_train_years
    ), call. = FALSE)
  }

  log_coverage_step(verbose,
    "rolling_origin_coverage(): %d candidate cutoffs, horizon=%d, inflation=%.2f",
    length(candidate_cutoffs), forecast_horizon, interval_inflation
  )

  fold_result_list <- lapply(candidate_cutoffs, function(cutoff_year) {
    holdout_years <- available_years[
      available_years > cutoff_year &
      available_years <= cutoff_year + forecast_horizon
    ]
    if (length(holdout_years) < 1L) return(NULL)

    fold_baseline <- fit_certification_baseline(
      certification_series = ordered_series,
      fit_through_year     = cutoff_year,
      trend_family         = "loglinear",
      verbose              = FALSE
    )
    fold_forecast <- forecast_certification(
      baseline_fit       = fold_baseline,
      forecast_years     = holdout_years,
      interval_inflation = interval_inflation,
      nominal_coverage   = nominal_coverage,
      verbose            = FALSE
    )

    observed_counts <- ordered_series$certification_count[
      match(holdout_years, ordered_series$year)
    ]

    tibble::tibble(
      cutoff_year    = as.integer(cutoff_year),
      year           = as.integer(holdout_years),
      horizon_step   = as.integer(holdout_years - cutoff_year),
      observed_count = as.integer(observed_counts),
      expected_count = fold_forecast$expected_count,
      lower_bound    = fold_forecast$lower_bound,
      upper_bound    = fold_forecast$upper_bound,
      is_covered     = observed_counts >= fold_forecast$lower_bound &
                       observed_counts <= fold_forecast$upper_bound
    )
  })

  fold_results       <- dplyr::bind_rows(fold_result_list)
  empirical_coverage <- mean(fold_results$is_covered)
  coverage_ratio     <- if (empirical_coverage > 0) {
    nominal_coverage / empirical_coverage
  } else Inf

  log_coverage_step(verbose,
    "rolling_origin_coverage(): %d folds, %d points, empirical=%.3f, ratio=%.2f",
    length(unique(fold_results$cutoff_year)), nrow(fold_results),
    empirical_coverage, coverage_ratio
  )

  list(
    fold_results       = fold_results,
    empirical_coverage = empirical_coverage,
    nominal_coverage   = nominal_coverage,
    n_folds            = length(unique(fold_results$cutoff_year)),
    coverage_ratio     = coverage_ratio
  )
}

#' Solve the interval inflation factor the historical series implies
#'
#' Searches a grid for the smallest inflation factor whose rolling-origin
#' empirical coverage reaches `nominal_coverage`. When no factor in the grid
#' reaches it, returns the best attainable and flags `reached_nominal = FALSE`.
#'
#' @param certification_series Data frame with `year` and `certification_count`.
#' @param minimum_train_years Integer. Default 6.
#' @param forecast_horizon Integer. Default 3.
#' @param nominal_coverage Numeric in `(0, 1)`. Default 0.95.
#' @param maximum_inflation Numeric upper bound of search grid. Default 10.
#' @param verbose Logical. Default `TRUE`.
#'
#' @return List with `solved_inflation`, `attained_coverage`, `reached_nominal`,
#'   `tier`, and `search_grid`.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @family interval coverage
#' @concept validation
#' @export
solve_interval_inflation <- function(certification_series,
                                     minimum_train_years = 6L,
                                     forecast_horizon    = 3L,
                                     nominal_coverage    = 0.95,
                                     maximum_inflation   = 10,
                                     verbose             = TRUE) {
  assertthat::assert_that(is.data.frame(certification_series))
  assertthat::assert_that(assertthat::is.count(minimum_train_years))
  assertthat::assert_that(assertthat::is.count(forecast_horizon))
  assertthat::assert_that(nominal_coverage > 0, nominal_coverage < 1)
  assertthat::assert_that(is.numeric(maximum_inflation), maximum_inflation >= 1)
  assertthat::assert_that(assertthat::is.flag(verbose))

  search_grid <- seq(from = 1, to = maximum_inflation, by = 0.05)

  attained_coverage_by_factor <- vapply(search_grid, function(f) {
    rolling_origin_coverage(
      certification_series, minimum_train_years, forecast_horizon,
      f, nominal_coverage, verbose = FALSE
    )$empirical_coverage
  }, numeric(1))

  reaching_index  <- which(attained_coverage_by_factor >= nominal_coverage)
  reached_nominal <- length(reaching_index) > 0L
  chosen_index    <- if (reached_nominal) min(reaching_index) else
                     which.max(attained_coverage_by_factor)

  log_coverage_step(verbose,
    "solve_interval_inflation(): solved=%.2f, attained=%.3f, reached_nominal=%s",
    search_grid[chosen_index], attained_coverage_by_factor[chosen_index],
    reached_nominal
  )

  if (!reached_nominal) {
    message("  nominal coverage unreachable within the grid; ",
            "the residual is structural break, not sampling error")
  }

  list(
    solved_inflation = search_grid[chosen_index],
    attained_coverage = attained_coverage_by_factor[chosen_index],
    reached_nominal   = reached_nominal,
    tier              = "solved",
    search_grid       = data.frame(
      inflation_factor  = search_grid,
      attained_coverage = attained_coverage_by_factor
    )
  )
}

#' Refuse to publish intervals whose coverage is not defensible
#'
#' Rule 5 refuses fixed-parameter runs (the input to an interval); this gates
#' the measured output. Fails if `n_folds < minimum_folds` or
#' `coverage_ratio > maximum_coverage_ratio`.
#'
#' @param coverage_report List from [rolling_origin_coverage()].
#' @param maximum_coverage_ratio Numeric >= 1. Default 1.25.
#' @param minimum_folds Integer. Default 3.
#' @param verbose Logical. Default `TRUE`.
#'
#' @return Invisibly `TRUE` on pass; otherwise an error.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @family interval coverage
#' @concept validation
#' @export
assert_interval_coverage_publishable <- function(coverage_report,
                                                 maximum_coverage_ratio = 1.25,
                                                 minimum_folds          = 3L,
                                                 verbose                = TRUE) {
  assertthat::assert_that(is.list(coverage_report))
  assertthat::assert_that(is.numeric(maximum_coverage_ratio), maximum_coverage_ratio >= 1)
  assertthat::assert_that(assertthat::is.count(minimum_folds))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (coverage_report$n_folds < minimum_folds) {
    stop(sprintf(
      "%d rolling-origin folds against a minimum of %d; a single split is one realization of the forecast problem no matter how many model arms saw it",
      coverage_report$n_folds, minimum_folds
    ), call. = FALSE)
  }

  if (coverage_report$coverage_ratio > maximum_coverage_ratio) {
    stop(sprintf(
      "empirical coverage %.3f against nominal %.3f (ratio %.2f) exceeds the %.2f ceiling; widen via solve_interval_inflation() or report the intervals as not covering",
      coverage_report$empirical_coverage, coverage_report$nominal_coverage,
      coverage_report$coverage_ratio, maximum_coverage_ratio
    ), call. = FALSE)
  }

  log_coverage_step(verbose, "assert_interval_coverage_publishable(): passes")
  invisible(TRUE)
}

# Helpers ------------------------------------------------------------------

#' @noRd
log_coverage_step <- function(verbose, template, ...) {
  if (isTRUE(verbose)) base::message(sprintf(template, ...))
  invisible(NULL)
}

#' @noRd
overdispersed_count_interval <- function(expected_count, dispersion,
                                         tail_probability) {
  if (dispersion > 1 + 1e-8) {
    nb_size    <- expected_count / (dispersion - 1)
    lower_bound <- stats::qnbinom(tail_probability,
                                  size = nb_size, mu = expected_count)
    upper_bound <- stats::qnbinom(1 - tail_probability,
                                  size = nb_size, mu = expected_count)
  } else {
    lower_bound <- stats::qpois(tail_probability,    lambda = expected_count)
    upper_bound <- stats::qpois(1 - tail_probability, lambda = expected_count)
  }
  list(lower_bound = as.numeric(lower_bound),
       upper_bound = as.numeric(upper_bound))
}

#' @noRd
validate_certification_series <- function(certification_series) {
  required <- c("year", "certification_count")
  missing  <- setdiff(required, names(certification_series))
  if (length(missing) > 0) {
    stop(sprintf("certification series is missing columns: %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  assertthat::assert_that(is.numeric(certification_series$year))
  assertthat::assert_that(is.numeric(certification_series$certification_count))
  assertthat::assert_that(all(certification_series$certification_count >= 0))
  assertthat::assert_that(!any(duplicated(certification_series$year)),
                          msg = "certification series has duplicated years")

  ordered_years <- sort(certification_series$year)
  if (any(diff(ordered_years) != 1)) {
    stop("certification series has gaps; every year must be present", call. = FALSE)
  }
  invisible(TRUE)
}
