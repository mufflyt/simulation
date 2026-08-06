# Entrant Regime Model: Disruption, Deferral, and Regime-Break Uncertainty ----
#
# docs/BACKTEST_2020_TO_2023.md records two distinct failures in the 2020->2023
# back-test, and this module addresses both. They are separate problems and the
# fix for one does not help the other.
#
#   POINT ESTIMATE. `backtest_entrant_estimate()` takes a flat mean of the
#   pre-cutoff certification counts: 40, 48, 10 -> 32.7/yr. The 10 is the 2020
#   COVID collapse. `mufflyaccess::urps_entry_counts()` shows where it came from:
#   ABOG entrants fell 35 -> 3 while ABU entrants held at 13 -> 7, which is an
#   exam-administration failure, not a change in the size of the fellowship
#   pipeline. Averaging it in treats a cancelled examination as evidence about
#   the steady-state entrant rate, which it is not.
#
#   INTERVAL. The shipped intervals describe Monte Carlo sampling noise. The
#   addendum in that document adds sampling variation in the mean of three
#   observations and correctly reports that it is still not enough: variation
#   WITHIN a window cannot represent a change in regime BETWEEN windows.
#
# THE MECHANISM THIS MODULE ENCODES
#
# A board examination that is cancelled or curtailed does not destroy the
# candidates. They sit the next administration. The deficit is DEFERRED, not
# lost, and it returns as a bulge in the following years. That single structural
# claim does the work here:
#
#   - it justifies excluding a disrupted year from the steady-state fit rather
#     than averaging it in (the year is uninformative about the rate);
#   - it justifies adding the deficit back as a scheduled release rather than
#     discarding it (the candidates are still in the pipeline);
#   - it gives regime-break uncertainty its shape. A break SUPPRESSES a year by
#     a share in [0, 1] and carries the remainder forward. There is no mechanism
#     that multiplies a certification cohort upward except the release of an
#     earlier deferral, which the carry term already produces. An earlier draft
#     of this module drew the break magnitude from a symmetric lognormal, which
#     let a single year certify five times its cohort and produced a 95% band of
#     [1232, 3084]. That band is wide enough to "cover" anything and means
#     nothing.
#
# HONEST STATUS OF THE 2020->2023 NUMBERS
#
# This estimator was written AFTER the 2023 miss was examined. Scoring it on the
# 2020 cutoff is therefore a REFIT, not an out-of-sample test, and no claim in
# this package treats it as one. `entrant_regime_rolling_validation()` exists
# because the defensible evidence is coverage across cutoffs the estimator was
# not designed against -- and it reports how few of those the series can supply.
#
# Every quantity below is estimated from years at or before the cutoff. The
# module never reads `mufflyaccess` directly; callers pass a series obtained
# through the audited `.series_through()` / `urps_entrant_series()` path so the
# leakage assertion still covers it.

#' Annual entrant counts, audited against the back-test leakage log
#'
#' [.series_through()] reads cumulative `n_active`, from which entrants are a
#' first difference. This reads `mufflyaccess::urps_entry_counts()` instead,
#' which reports the ABOG and ABU components separately, and records the read in
#' the same audit log so [assert_no_leakage()] still covers it.
#'
#' The two agree by construction on the combined total; the split matters
#' because it is what identifies 2020 as an examination failure rather than a
#' pipeline contraction.
#'
#' @param through_year Latest year the caller may see.
#' @param geography Contract geography.
#' @return Tibble of `year`, `count`, `abog`, `abu`.
#' @keywords internal
urps_entrant_series <- function(through_year = BACKTEST_CUTOFF_YEAR,
                                geography = "national") {
  .require_mufflyaccess("The URPS entrant series")
  x <- mufflyaccess::urps_entry_counts()
  x <- x[x$geography == geography, , drop = FALSE]
  x <- x[order(x$year), , drop = FALSE]
  x <- x[x$year <= through_year, , drop = FALSE]
  if (!nrow(x)) {
    stop("urps_entrant_series(): no rows at or before ", through_year,
         call. = FALSE)
  }

  if (is.null(.backtest_audit$max_year)) reset_leakage_audit()
  .backtest_audit$max_year <- max(.backtest_audit$max_year, max(x$year))
  .backtest_audit$reads <- c(.backtest_audit$reads,
                             sprintf("entrants: <= %d", max(x$year)))

  tibble::tibble(
    year  = as.integer(x$year),
    count = as.numeric(x$combined_entrants),
    abog  = as.numeric(x$abog_entrants),
    abu   = as.numeric(x$abu_entrants)
  )
}

#' Classify each certification year as backlog, disrupted, or steady
#'
#' Three regimes, each identified by a rule that reads only the years supplied:
#'
#' \describe{
#'   \item{`backlog`}{The initial certification of an already-practising pool.
#'     A leading year is backlog when its count exceeds `backlog_multiple` times
#'     the median of every later year. Backlog is a PREFIX regime -- the run ends
#'     at the first year that fails the test and never resumes -- because the
#'     initial pool is certified once and cannot recur.}
#'   \item{`disrupted`}{A year whose count falls below the lower tail of what the
#'     other steady years predict for it. Screened leave-one-out, so the year
#'     under test contributes nothing to the baseline it is compared against, and
#'     against an overdispersed (negative binomial) tail rather than a Poisson
#'     one, so ordinary year-to-year variation does not trip it.}
#'   \item{`release`}{A year AFTER a disruption whose count exceeds the upper
#'     tail: the deferred candidates arriving. Excluded from the trend fit, since
#'     a release bulge is not the steady rate, and its excess is credited against
#'     the outstanding deficit so the same candidates are not counted twice --
#'     once in the inflated trend and again as a future release.}
#'   \item{`steady`}{Everything else. Only these years fit the trend.}
#' }
#'
#' A high year is only ever a `release`, never a break. It is the consequence of
#' an earlier disruption rather than an independent event, so counting it would
#' inflate the break frequency with the echo of a break already counted.
#'
#' @param series Data frame with integer `year` and numeric `count`, one row per
#'   year, no gaps.
#' @param screen_alpha Lower-tail probability for the disruption screen. Small by
#'   design: at 0.01 the 2016-2020 window flags only 2020.
#' @param backlog_multiple Multiple of the later-year median above which a
#'   leading year counts as backlog.
#' @param trend_family `"loglinear"` or `"intercept"`.
#' @param verbose Emit progress via [base::message()].
#' @return Tibble of `year`, `count`, `regime`, `expected`, `lower_bound`.
#'
#' @importFrom stats glm poisson predict residuals qnbinom qpois median
#' @export
#'
#' @examples
#' s <- data.frame(year = 2013:2020,
#'                 count = c(655, 175, 102, 36, 33, 40, 48, 10))
#' classify_certification_regimes(s, verbose = FALSE)[, c("year", "regime")]
classify_certification_regimes <- function(series,
                                           screen_alpha = 0.01,
                                           backlog_multiple = 2,
                                           trend_family = "loglinear",
                                           verbose = TRUE) {
  series <- .validate_entrant_series(series)
  n <- nrow(series)
  regime <- rep("steady", n)

  # ---- Backlog: a prefix run of years far above the later median -------------
  for (i in seq_len(n)) {
    later <- series$count[seq_len(n) > i]
    if (length(later) < 2L) break
    if (!(series$count[i] > backlog_multiple * stats::median(later))) break
    regime[i] <- "backlog"
  }

  expected <- rep(NA_real_, n)
  lower <- rep(NA_real_, n)
  upper <- rep(NA_real_, n)

  # ---- Disruption: leave-one-out lower-tail screen on non-backlog years ------
  idx <- which(regime != "backlog")
  if (length(idx) >= 4L) {
    for (i in idx) {
      others <- setdiff(idx, i)
      fit <- .entrant_trend_fit(series[others, , drop = FALSE], trend_family)
      mu <- .entrant_trend_predict(fit$model, series$year[i], trend_family)
      expected[i] <- mu
      lower[i] <- .count_lower_bound(mu, fit$dispersion, screen_alpha)
      upper[i] <- .count_upper_bound(mu, fit$dispersion, screen_alpha)
      if (series$count[i] < lower[i]) regime[i] <- "disrupted"
    }
    # Release years are identified only AFTER the disruptions are known: a bulge
    # can only be a release if there is a deferral for it to be releasing.
    first_disruption <- which(regime == "disrupted")[1]
    if (!is.na(first_disruption)) {
      for (i in idx) {
        if (i <= first_disruption || regime[i] != "steady") next
        if (is.finite(upper[i]) && series$count[i] > upper[i]) {
          regime[i] <- "release"
        }
      }
    }
  } else if (isTRUE(verbose)) {
    message(sprintf(paste(
      "classify_certification_regimes(): %d non-backlog years is too few for a",
      "leave-one-out screen (need 4); no year classified as disrupted."),
      length(idx)))
  }

  if (isTRUE(verbose)) {
    message(sprintf(
      "classify_certification_regimes(): %d backlog, %d disrupted, %d release, %d steady",
      sum(regime == "backlog"), sum(regime == "disrupted"),
      sum(regime == "release"), sum(regime == "steady")))
  }

  tibble::tibble(
    year = series$year, count = series$count, regime = regime,
    expected = expected, lower_bound = lower, upper_bound = upper
  )
}

#' Fit the entrant rate with explicit disruption and deferral handling
#'
#' Fits the steady-state trend on steady years only, measures the deficit left
#' by disrupted years, and estimates how often a break occurs and how deep it is
#' -- all from years at or before `fit_through_year`.
#'
#' The deferred deficit is `sum(expected - observed)` over disrupted years and is
#' released over the first `release_years` projected years. It is added back
#' because a cancelled examination defers candidates rather than removing them.
#'
#' Break frequency uses the Jeffreys-style `(k + 0.5) / (n + 1)` rather than
#' `k / n`: with one break in five screened years, `k / n` would state 0.2 with
#' false precision and, had no break been observed, would state that a break is
#' impossible.
#'
#' @param series Data frame of `year` and `count` (see [urps_entrant_series()]).
#' @param fit_through_year Cutoff. Later years are dropped and never read.
#' @param trend_family `"loglinear"` or `"intercept"`. Use `"intercept"` when
#'   fewer than four steady years survive the screen.
#' @param screen_alpha,backlog_multiple Passed to
#'   [classify_certification_regimes()].
#' @param release_years Years over which a deferred deficit clears.
#' @param break_concentration Beta concentration for the surviving-share prior.
#'   The default 2 is weak on purpose: one observed disruption does not identify
#'   the shape of the distribution of future ones.
#' @param break_surviving_share_prior Surviving share to assume when the fitting
#'   window contains no disruption at all. `NA` (the default) simulates no break
#'   and says so, matching this package's rule that an unquantified parameter is
#'   reported rather than invented. Supplying a value is an explicit modelling
#'   choice by the caller.
#' @param verbose Emit progress via [base::message()].
#' @return An object of class `urps_entrant_regime`.
#'
#' @importFrom stats glm poisson predict residuals
#' @export
#'
#' @examples
#' s <- data.frame(year = 2013:2020,
#'                 count = c(655, 175, 102, 36, 33, 40, 48, 10))
#' fit_entrant_regime_model(s, 2020L, verbose = FALSE)$deferred_backlog > 0
fit_entrant_regime_model <- function(series,
                                     fit_through_year,
                                     trend_family = "loglinear",
                                     screen_alpha = 0.01,
                                     backlog_multiple = 2,
                                     release_years = 2L,
                                     break_concentration = 2,
                                     break_surviving_share_prior = NA_real_,
                                     verbose = TRUE) {
  series <- .validate_entrant_series(series)
  series <- series[series$year <= fit_through_year, , drop = FALSE]
  if (!nrow(series)) {
    stop("fit_entrant_regime_model(): no years at or before ", fit_through_year,
         call. = FALSE)
  }
  if (!trend_family %in% c("loglinear", "intercept")) {
    stop("fit_entrant_regime_model(): trend_family must be 'loglinear' or ",
         "'intercept'.", call. = FALSE)
  }
  if (!(release_years >= 1)) {
    stop("fit_entrant_regime_model(): release_years must be at least 1.",
         call. = FALSE)
  }

  regimes <- classify_certification_regimes(
    series, screen_alpha = screen_alpha, backlog_multiple = backlog_multiple,
    trend_family = trend_family, verbose = verbose)

  steady <- regimes[regimes$regime == "steady", , drop = FALSE]
  if (nrow(steady) < 2L) {
    stop(sprintf(paste(
      "fit_entrant_regime_model(): %d steady years survive the screen, which is",
      "too few to fit a rate. Widen the window, or lower screen_alpha so fewer",
      "years are called disrupted."), nrow(steady)), call. = FALSE)
  }
  family_used <- trend_family
  if (identical(trend_family, "loglinear") && nrow(steady) < 4L) {
    family_used <- "intercept"
    if (isTRUE(verbose)) {
      message(sprintf(paste(
        "fit_entrant_regime_model(): %d steady years is too few to identify a",
        "log-linear slope; falling back to an intercept-only rate."),
        nrow(steady)))
    }
  }

  fit <- .entrant_trend_fit(steady[, c("year", "count")], family_used)

  disrupted <- regimes[regimes$regime == "disrupted", , drop = FALSE]
  gross_deficit <- if (nrow(disrupted)) {
    sum(pmax(disrupted$expected - disrupted$count, 0), na.rm = TRUE)
  } else 0

  # Candidates deferred by a disruption arrive as a bulge in a later year. Any
  # such bulge ALREADY OBSERVED has been delivered, and scheduling it again as a
  # future release would count the same candidates twice -- once inside the
  # fitted trend and once more on top of it.
  release <- regimes[regimes$regime == "release", , drop = FALSE]
  already_released <- if (nrow(release)) {
    sum(pmax(release$count - release$expected, 0), na.rm = TRUE)
  } else 0
  deferred <- max(gross_deficit - already_released, 0)

  # Break frequency and depth are estimated only over the years the screen could
  # actually have flagged. Backlog years were never eligible, so including them
  # in the denominator would understate the rate.
  screened <- regimes[regimes$regime != "backlog", , drop = FALSE]
  n_screened <- nrow(screened)
  n_breaks <- nrow(disrupted)
  p_break <- if (n_screened > 0) (n_breaks + 0.5) / (n_screened + 1) else NA_real_
  surviving_share <- if (n_breaks > 0) {
    mean(pmin(pmax(disrupted$count / disrupted$expected, 0), 1))
  } else break_surviving_share_prior

  # A window containing no disruption yields no depth estimate, so no break is
  # simulated and the interval silently excludes regime-break risk. That is the
  # exact failure mode this module exists to remove, so it is announced rather
  # than left to be inferred. Rolling-origin validation makes it concrete: the
  # fold fitted through 2019 has seen no break, simulates none, and misses the
  # 2020 collapse. Supplying `break_surviving_share_prior` opts in to a depth
  # the data does not supply; leaving it NA keeps the package's standing rule
  # that an unquantified parameter is reported, not invented.
  if (!is.finite(surviving_share) && isTRUE(verbose)) {
    message(paste(
      "fit_entrant_regime_model(): no disruption in the fitting window, so no",
      "break depth is estimable and breaks are NOT simulated. The resulting",
      "interval covers trend and count uncertainty only. Set",
      "`break_surviving_share_prior` to include a break of assumed depth."))
  }

  structure(
    list(
      trend_model = fit$model,
      dispersion = fit$dispersion,
      trend_family = family_used,
      trend_family_requested = trend_family,
      steady_years = steady$year,
      disrupted_years = disrupted$year,
      backlog_years = regimes$year[regimes$regime == "backlog"],
      release_observed_years = release$year,
      gross_deficit = gross_deficit,
      already_released = already_released,
      deferred_backlog = deferred,
      release_years = as.integer(release_years),
      break_probability = p_break,
      break_surviving_share = surviving_share,
      break_concentration = break_concentration,
      regimes = regimes,
      fit_through_year = as.integer(fit_through_year),
      n_screened = n_screened
    ),
    class = "urps_entrant_regime"
  )
}

#' @export
print.urps_entrant_regime <- function(x, ...) {
  cat("Entrant regime model\n")
  cat(sprintf("  fitted through      %d\n", x$fit_through_year))
  cat(sprintf("  trend               %s%s\n", x$trend_family,
              if (!identical(x$trend_family, x$trend_family_requested))
                sprintf(" (requested %s; too few steady years)",
                        x$trend_family_requested) else ""))
  cat(sprintf("  steady years        %s\n",
              paste(x$steady_years, collapse = ", ")))
  cat(sprintf("  backlog years       %s\n",
              if (length(x$backlog_years)) paste(x$backlog_years, collapse = ", ")
              else "none"))
  cat(sprintf("  disrupted years     %s\n",
              if (length(x$disrupted_years)) paste(x$disrupted_years, collapse = ", ")
              else "none"))
  cat(sprintf("  release years       %s\n",
              if (length(x$release_observed_years))
                paste(x$release_observed_years, collapse = ", ") else "none"))
  cat(sprintf("  deferred backlog    %.1f outstanding (%.1f deferred - %.1f already released), over %d year(s)\n",
              x$deferred_backlog, x$gross_deficit, x$already_released,
              x$release_years))
  cat(sprintf("  dispersion          %.2f\n", x$dispersion))
  cat(sprintf("  break probability   %s\n",
              if (is.finite(x$break_probability))
                sprintf("%.3f/yr (%d in %d screened years, Jeffreys)",
                        x$break_probability, length(x$disrupted_years), x$n_screened)
              else "not estimable"))
  cat(sprintf("  break depth         %s\n",
              if (is.finite(x$break_surviving_share))
                sprintf("surviving share %.2f of the cohort, Beta(concentration %.1f); deficit deferred",
                        x$break_surviving_share, x$break_concentration)
              else "no break observed -- breaks NOT simulated"))
  invisible(x)
}

#' Expected entrant path implied by a regime model
#'
#' The deterministic counterpart of [draw_entrant_paths()]: the steady trend plus
#' the scheduled release of any deferred backlog. Carries no uncertainty and must
#' not be reported with an interval.
#'
#' @param model A [fit_entrant_regime_model()] object.
#' @param years Integer vector of projection years, all after the cutoff.
#' @return Tibble of `year`, `steady`, `backlog_release`, `expected`.
#' @export
project_entrant_path <- function(model, years) {
  stopifnot(inherits(model, "urps_entrant_regime"))
  years <- .validate_projection_years(model, years)
  steady <- .entrant_trend_predict(model$trend_model, years, model$trend_family)
  release <- .backlog_release_schedule(model$deferred_backlog,
                                       model$release_years, length(years))
  tibble::tibble(year = as.integer(years), steady = steady,
                 backlog_release = release, expected = steady + release)
}

#' Draw entrant paths carrying regime-break uncertainty
#'
#' Each draw layers four sources, and each can be switched off to show what it
#' contributes:
#'
#' \describe{
#'   \item{`trend`}{Coefficients of the steady trend, drawn from `coef`/`vcov`
#'     through the same internal `.param_draw()` the demand side uses. This is
#'     the dominant term at a three-year horizon and the one the shipped engine
#'     omitted entirely.}
#'   \item{`dispersion`}{Negative-binomial count noise at the fitted dispersion,
#'     rather than the Poisson noise that assumes variance equals mean.}
#'   \item{`break`}{Per year, a Bernoulli draw at `break_probability`. A break
#'     multiplies that year's entrants by a share drawn from the Beta prior and
#'     DEFERS the remainder into the following year. A break late in the horizon
#'     therefore pushes entrants past the end of the window, which is why this
#'     term lowers the median as well as widening the band.}
#'   \item{`release_timing`}{The deferred backlog clears over 1 to
#'     `2 * release_years` years rather than exactly `release_years`. Nothing in
#'     the pre-cutoff data pins the clearance schedule.}
#' }
#'
#' @param model A [fit_entrant_regime_model()] object.
#' @param years Integer vector of projection years, all after the cutoff.
#' @param n_draws Number of paths.
#' @param include Character vector of components to switch on.
#' @return Numeric matrix, `n_draws` rows by `length(years)` columns.
#'
#' @importFrom stats rnbinom rpois rbeta runif
#' @export
draw_entrant_paths <- function(model, years, n_draws = 1000L,
                               include = c("trend", "dispersion", "break",
                                           "release_timing")) {
  stopifnot(inherits(model, "urps_entrant_regime"))
  years <- .validate_projection_years(model, years)
  include <- match.arg(include, several.ok = TRUE)
  n_years <- length(years)

  coefs <- if ("trend" %in% include) {
    .param_draw(model$trend_model, n_draws)
  } else NULL

  breaks_on <- "break" %in% include &&
    is.finite(model$break_probability) && is.finite(model$break_surviving_share)

  out <- matrix(0, nrow = n_draws, ncol = n_years)
  for (i in seq_len(n_draws)) {
    mu <- if (is.null(coefs)) {
      .entrant_trend_predict(model$trend_model, years, model$trend_family)
    } else {
      .entrant_trend_predict_coef(coefs[i, , drop = TRUE], years,
                                  model$trend_family)
    }

    rel_years <- if ("release_timing" %in% include) {
      sample.int(2L * model$release_years, 1L)
    } else model$release_years
    mu <- mu + .backlog_release_schedule(model$deferred_backlog, rel_years,
                                         n_years)

    if (breaks_on) {
      carry <- 0
      for (j in seq_len(n_years)) {
        mu[j] <- mu[j] + carry
        carry <- 0
        if (stats::runif(1) < model$break_probability) {
          a <- model$break_concentration * model$break_surviving_share
          b <- model$break_concentration * (1 - model$break_surviving_share)
          share <- if (a > 0 && b > 0) stats::rbeta(1, a, b) else
            model$break_surviving_share
          carry <- mu[j] * (1 - share)
          mu[j] <- mu[j] * share
        }
      }
      # Whatever is still deferred at the horizon never lands inside the window.
      # That is the point: a late break removes entrants from the projection.
    }

    mu <- pmax(mu, 0)
    out[i, ] <- if ("dispersion" %in% include && model$dispersion > 1 + 1e-8) {
      stats::rnbinom(n_years, size = pmax(mu, 1e-8) / (model$dispersion - 1),
                     mu = mu)
    } else {
      stats::rpois(n_years, mu)
    }
  }
  colnames(out) <- as.character(years)
  out
}

#' Rolling-origin validation of the regime estimator
#'
#' The 2020 cutoff cannot validate an estimator built after the 2020 miss was
#' examined. This refits at every admissible cutoff and scores the cumulative
#' count at each horizon against the naive flat-mean estimator the engine ships.
#'
#' Read the `n_folds` it reports before reading anything else. A short series
#' admits few folds, and few folds cannot establish coverage however favourable
#' they look; [assert_interval_coverage_publishable()] enforces that separately.
#'
#' @param series Data frame of `year` and `count`.
#' @param cumulative_series Data frame of `year` and `n_active`: the cumulative
#'   counts the horizon is scored against.
#' @param horizon Years projected per fold.
#' @param minimum_steady_years Smallest number of steady years a fold may fit on.
#' @param n_draws Draws per fold.
#' @param naive_window Years the naive comparator averages over.
#' @param verbose Emit progress via [base::message()].
#' @return List with `folds`, `coverage_95`, `n_folds`, and `naive_coverage_95`.
#'
#' @importFrom stats quantile
#' @export
entrant_regime_rolling_validation <- function(series,
                                              cumulative_series,
                                              horizon = 3L,
                                              minimum_steady_years = 4L,
                                              n_draws = 2000L,
                                              naive_window = 3L,
                                              verbose = TRUE) {
  series <- .validate_entrant_series(series)
  if (!all(c("year", "n_active") %in% names(cumulative_series))) {
    stop("entrant_regime_rolling_validation(): cumulative_series needs `year` ",
         "and `n_active`.", call. = FALSE)
  }

  cutoffs <- series$year[series$year + horizon <= max(series$year)]
  rows <- list()
  for (cy in cutoffs) {
    fit <- tryCatch(
      fit_entrant_regime_model(series, cy, verbose = FALSE),
      error = function(e) NULL)
    if (is.null(fit) || length(fit$steady_years) < minimum_steady_years) next

    yrs <- (cy + 1L):(cy + horizon)
    if (!all(yrs %in% cumulative_series$year)) next

    base <- cumulative_series$n_active[cumulative_series$year == cy]
    obs <- cumulative_series$n_active[cumulative_series$year == max(yrs)]
    if (!length(base) || !length(obs)) next

    paths <- draw_entrant_paths(fit, yrs, n_draws)
    cum <- base + rowSums(paths)
    q <- stats::quantile(cum, c(0.025, 0.5, 0.975), names = FALSE)

    naive_years <- series$count[series$year > cy - naive_window & series$year <= cy]
    naive_pred <- base + horizon * mean(naive_years)

    rows[[length(rows) + 1L]] <- tibble::tibble(
      cutoff_year = as.integer(cy),
      target_year = as.integer(max(yrs)),
      observed = as.numeric(obs),
      predicted_median = q[2],
      pi95_lower = q[1],
      pi95_upper = q[3],
      covered = obs >= q[1] && obs <= q[3],
      percent_error = 100 * (q[2] - obs) / obs,
      naive_prediction = naive_pred,
      naive_percent_error = 100 * (naive_pred - obs) / obs,
      steady_years = length(fit$steady_years),
      disrupted_years = length(fit$disrupted_years)
    )
  }

  folds <- dplyr::bind_rows(rows)
  if (!nrow(folds)) {
    stop("entrant_regime_rolling_validation(): no admissible folds. The series ",
         "is too short for horizon ", horizon, " at ", minimum_steady_years,
         " steady years.", call. = FALSE)
  }
  if (isTRUE(verbose)) {
    message(sprintf(
      "entrant_regime_rolling_validation(): %d folds, 95%% coverage %.2f",
      nrow(folds), mean(folds$covered)))
  }

  empirical <- mean(folds$covered)
  # Shaped so the result drops straight into
  # assert_interval_coverage_publishable(), which already encodes the rule that
  # a handful of folds cannot establish coverage however favourable they look.
  list(
    folds = folds,
    n_folds = nrow(folds),
    coverage_95 = empirical,
    empirical_coverage = empirical,
    nominal_coverage = 0.95,
    coverage_ratio = if (empirical > 0) 0.95 / empirical else Inf,
    median_absolute_percent_error = stats::median(abs(folds$percent_error)),
    naive_median_absolute_percent_error =
      stats::median(abs(folds$naive_percent_error))
  )
}

#' Score the regime estimator against the same cutoff and target as the back-test
#'
#' Deliberately separate from [run_backtest()]. That function runs the four
#' PRESPECIFIED arms whose result is frozen in `BACKTEST_RECORD_2020_2023`, and
#' folding a later estimator into it would rewrite a record whose whole value is
#' that nothing was re-tuned after the error was seen.
#'
#' The result of this function is NOT an out-of-sample test at the 2020 cutoff:
#' the estimator was built after that miss was examined. It is a refit, reported
#' as one. [entrant_regime_rolling_validation()] carries the out-of-sample claim.
#'
#' @param cutoff_year,target_year Cutoff and scoring year.
#' @param n_iterations Monte Carlo replicates per arm.
#' @param seed RNG seed.
#' @param trend_family Passed to [fit_entrant_regime_model()].
#' @param verbose Emit progress via [base::message()].
#' @return List with `summary`, `model`, and `entrant_path`.
#' @keywords internal
run_entrant_regime_backtest <- function(cutoff_year = BACKTEST_CUTOFF_YEAR,
                                        target_year = BACKTEST_TARGET_YEAR,
                                        n_iterations = 1000L,
                                        seed = 20260802L,
                                        trend_family = "loglinear",
                                        verbose = TRUE) {
  reset_leakage_audit()

  series <- urps_entrant_series(cutoff_year)
  model <- fit_entrant_regime_model(
    as.data.frame(series[, c("year", "count")]), cutoff_year,
    trend_family = trend_family, verbose = verbose)

  # Same discipline as run_backtest(): assert before the observed series is read.
  assert_no_leakage(cutoff_year)

  obs_years <- cutoff_year:target_year
  observed <- stats::setNames(
    vapply(obs_years, function(y) {
      mufflyaccess::urps_count(y, geography = "national", include_urology = TRUE)
    }, numeric(1)),
    as.character(obs_years))

  spec <- supply_parameter_spec(entrant_regime = model)
  rows <- list()
  for (att in c(TRUE, FALSE)) {
    lab <- sprintf("Regime entrant model%s",
                   if (att) ", attrition applied" else " [no-attrition, definition-matched]")
    arm <- run_backtest_arm(
      cohort = "derived", entrants_per_year = NA_real_,
      cutoff_year = cutoff_year, target_year = target_year,
      n_iterations = n_iterations, apply_attrition = att,
      param_spec = spec, seed = seed)
    rows[[length(rows) + 1L]] <- score_backtest_arm(arm, observed, lab)
  }

  summary_tbl <- dplyr::bind_rows(rows)
  summary_tbl$estimator <- "entrant regime model"
  summary_tbl$out_of_sample <- FALSE
  summary_tbl$out_of_sample_note <- paste(
    "REFIT, not a held-out test: this estimator was written after the 2023 miss",
    "was examined. See entrant_regime_rolling_validation() for folds it was not",
    "designed against.")

  list(summary = summary_tbl, model = model,
       entrant_path = project_entrant_path(model, (cutoff_year + 1L):target_year),
       observed = observed, leakage_audit = .backtest_audit$reads)
}

# ---- Internals -------------------------------------------------------------

.validate_entrant_series <- function(series) {
  if (!is.data.frame(series)) {
    stop("entrant series must be a data frame", call. = FALSE)
  }
  missing <- setdiff(c("year", "count"), names(series))
  if (length(missing)) {
    stop(sprintf("entrant series is missing column(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  if (!is.numeric(series$year) || !is.numeric(series$count)) {
    stop("entrant series `year` and `count` must be numeric", call. = FALSE)
  }
  if (any(series$count < 0, na.rm = TRUE)) {
    stop("entrant series has negative counts", call. = FALSE)
  }
  if (anyDuplicated(series$year)) {
    stop("entrant series has duplicated years", call. = FALSE)
  }
  series <- series[order(series$year), , drop = FALSE]
  if (nrow(series) > 1L && any(diff(series$year) != 1)) {
    stop("entrant series has gaps; every year must be present", call. = FALSE)
  }
  series
}

.validate_projection_years <- function(model, years) {
  years <- sort(unique(as.integer(years)))
  leaking <- years[years <= model$fit_through_year]
  if (length(leaking)) {
    stop(sprintf(paste(
      "projection years %s are at or before the fit cutoff %d; the regime model",
      "may only project years it did not fit."),
      paste(leaking, collapse = ", "), model$fit_through_year), call. = FALSE)
  }
  years
}

# Poisson GLM plus a Pearson dispersion estimate. Dispersion is floored at 1: a
# below-Poisson estimate from three or four points is a small-sample artefact,
# and letting it through would make the intervals NARROWER than Poisson, which
# is the defect this module exists to remove.
.entrant_trend_fit <- function(d, trend_family) {
  d <- data.frame(year = as.numeric(d$year), count = as.numeric(d$count))
  f <- if (identical(trend_family, "loglinear")) count ~ year else count ~ 1
  m <- stats::glm(f, family = stats::poisson(), data = d)
  pr <- stats::residuals(m, type = "pearson")
  disp <- sum(pr^2) / max(m$df.residual, 1L)
  list(model = m, dispersion = max(disp, 1))
}

.entrant_trend_predict <- function(model, years, trend_family) {
  as.numeric(stats::predict(model, newdata = data.frame(year = as.numeric(years)),
                            type = "response"))
}

# Predict from a DRAWN coefficient vector without rebuilding the model object.
# Both supported families have a closed-form linear predictor, so this is exact
# rather than an approximation of predict().
.entrant_trend_predict_coef <- function(coefs, years, trend_family) {
  intercept <- unname(coefs[["(Intercept)"]])
  if (identical(trend_family, "loglinear")) {
    exp(intercept + unname(coefs[["year"]]) * as.numeric(years))
  } else {
    rep(exp(intercept), length(years))
  }
}

.backlog_release_schedule <- function(deferred, release_years, n_years) {
  out <- numeric(n_years)
  if (!is.finite(deferred) || deferred <= 0 || n_years < 1L) return(out)
  k <- min(as.integer(release_years), n_years)
  if (k < 1L) return(out)
  # The full deficit is released over `release_years`, so a horizon shorter than
  # the clearance window sees only the part that lands inside it.
  out[seq_len(k)] <- deferred / as.integer(release_years)
  out
}

.count_lower_bound <- function(mu, dispersion, alpha) {
  if (dispersion > 1 + 1e-8) {
    stats::qnbinom(alpha, size = mu / (dispersion - 1), mu = mu)
  } else {
    stats::qpois(alpha, lambda = mu)
  }
}

.count_upper_bound <- function(mu, dispersion, alpha) {
  if (dispersion > 1 + 1e-8) {
    stats::qnbinom(1 - alpha, size = mu / (dispersion - 1), mu = mu)
  } else {
    stats::qpois(1 - alpha, lambda = mu)
  }
}
