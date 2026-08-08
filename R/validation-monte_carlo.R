# Monte Carlo Adequacy of the Reported Bands ----
#
# THE DEFECT THIS EXISTS FOR. `run_supply_microsimulation()` reports
# `effective_fte_lo/hi` as the 2.5% and 97.5% quantiles of the iteration draws,
# and reports them identically whether the run used 1,000 replicates or 3. Those
# are not the same object. To estimate the 2.5% quantile you need at least one
# draw below it, so a 95% band needs n >= 40; below that the "quantiles" are the
# sample minimum and maximum wearing a quantile's name, and they get NARROWER
# with fewer draws rather than wider. Iteration counts across this repository run
# 2, 3, 5, 25, 40 -- most of them under the floor.
#
# TWO DIFFERENT UNCERTAINTIES, AND ONLY ONE OF THEM SHRINKS. Monte Carlo error is
# how imprecisely the simulation has estimated its own summary; it falls as
# 1/sqrt(n) and is a property of how long you ran the model. Forecast uncertainty
# is how wrong the model may be about the world; running longer does not touch
# it. A band that is mostly Monte Carlo error is reporting the simulator's
# indecision, not the workforce's.
#
# The existing guards address neither. `backtest_status()` says the bands are not
# validated as FORECAST intervals -- a statement about the world -- and the
# engine warns only when n_iterations == 1. Nothing said whether the number in
# front of you is stable at the n that produced it.
#
# What is added here is deliberately narrow: report the simulation error
# alongside every summary, and refuse to dress up an interval that the iteration
# count cannot support. It makes no claim about forecast accuracy, which remains
# backtest_status()'s job.

#' Smallest iteration count at which a central interval's bounds are quantiles
#'
#' A two-sided `ci` interval puts `(1 - ci) / 2` in each tail, so the outer
#' bounds are order statistics only once at least one draw falls beyond each.
#' Below this the bounds are the sample extremes and understate the spread.
#'
#' @param ci Interval width (e.g. 0.95).
#' @return Integer minimum iteration count.
#' @family monte carlo
#' @export
mc_min_iterations <- function(ci = 0.95) {
  assertthat::assert_that(is.numeric(ci), length(ci) == 1L, ci > 0, ci < 1)
  # Rounded before the ceiling: 1 - 0.8 is 0.19999999999999996 in binary, so the
  # exact answer 10 arrives as 10.000000000000002 and ceiling() returns 11. That
  # would demand an extra iteration for no statistical reason, at the one ci a
  # caller is most likely to pick as a cheaper alternative to 95%.
  as.integer(ceiling(round(2 / (1 - ci), 9)))
}

#' Monte Carlo standard error of a summary statistic
#'
#' `mean` uses `sd / sqrt(n)`. `median` uses the asymptotic
#' `1.2533 * sd / sqrt(n)`, which assumes approximate normality -- stated here
#' because the supply panel is a sum over many providers and is close enough,
#' not because it is exact.
#'
#' @param x Numeric draws.
#' @return List with `n`, `mcse_mean`, `mcse_median`.
#' @family monte carlo
#' @export
monte_carlo_se <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 2L) {
    return(list(n = n, mcse_mean = NA_real_, mcse_median = NA_real_))
  }
  s <- stats::sd(x)
  list(n = n,
       mcse_mean = s / sqrt(n),
       mcse_median = sqrt(pi / 2) * s / sqrt(n))
}

#' How much of a reported band is simulation noise
#'
#' `noise_share` is the median's Monte Carlo standard error divided by the
#' band's half-width. It answers the question a reader actually has: is this
#' interval describing the workforce, or the simulator? A share near or above 1
#' means the band would move materially on a re-run with a different seed.
#'
#' @param x Numeric draws.
#' @param ci Interval width.
#' @return One-row tibble of diagnostics.
#' @family monte carlo
#' @export
monte_carlo_diagnostics <- function(x, ci = 0.95) {
  se <- monte_carlo_se(x)
  lo_p <- (1 - ci) / 2
  x_ok <- x[is.finite(x)]
  lo <- unname(stats::quantile(x_ok, lo_p, na.rm = TRUE))
  hi <- unname(stats::quantile(x_ok, 1 - lo_p, na.rm = TRUE))
  half <- (hi - lo) / 2
  n_min <- mc_min_iterations(ci)

  tibble::tibble(
    n_iterations = se$n,
    ci = ci,
    mcse_mean = se$mcse_mean,
    mcse_median = se$mcse_median,
    half_width = half,
    # Guarded: a degenerate band (every draw identical) would divide by zero and
    # report Inf noise, which reads as a catastrophe rather than as "no spread".
    noise_share = if (is.finite(half) && half > 0) se$mcse_median / half else NA_real_,
    bounds_are_quantiles = se$n >= n_min,
    min_iterations_for_ci = n_min
  )
}

#' Refuse to report an interval the iteration count cannot support
#'
#' Fails closed when `n_iterations` is below [mc_min_iterations()]. The bounds
#' still exist -- they are the sample extremes -- but they must not be published
#' as a `ci` interval, because they are narrower than the truth and get narrower
#' still as `n` falls.
#'
#' @param n_iterations Replicates actually run.
#' @param ci Interval width the caller intends to report.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @param what Label used in the message.
#' @return (Invisibly) TRUE when the count supports the interval.
#' @family monte carlo
#' @export
assert_monte_carlo_adequate <- function(n_iterations, ci = 0.95,
                                        mode = resolve_reproducibility_mode(),
                                        what = "supply") {
  n_min <- mc_min_iterations(ci)
  if (n_iterations >= n_min) return(invisible(TRUE))

  msg <- sprintf(paste(
    "%s ran %d Monte Carlo iterations, but a %.0f%% interval needs at least %d",
    "for its bounds to be quantiles rather than the sample minimum and maximum.",
    "Reported this way the band is NARROWER than the truth and narrows further",
    "as iterations fall. Raise n_iterations to >= %d, or report a point estimate",
    "with monte_carlo_se() instead of an interval."),
    what, n_iterations, 100 * ci, n_min, n_min)

  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}
