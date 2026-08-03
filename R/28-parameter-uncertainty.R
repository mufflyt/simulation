# Parameter Uncertainty for the Supply Engine ----
#
# The back-test (docs/BACKTEST_2020_TO_2023.md) found the supply engine's
# prediction intervals 6.5-8.2x too narrow: a 95% half-width of 17-20 providers
# against misses of 111-164, with a Monte Carlo standard error of 0.28-0.33. The
# simulation had converged precisely on the wrong answer.
#
# The cause is structural. The engine draws INDIVIDUAL stochasticity -- Bernoulli
# retirement per agent, a fractional entrant draw -- but holds every COEFFICIENT
# fixed across replicates. The intervals therefore describe Monte Carlo sampling
# noise, not forecast uncertainty. This is the gap HDMM_IMPROVEMENT_PLAN.md sec 7
# names, and the one Dall's own models close with
# `MASS::mvrnorm(1, coef(m), vcov(m))` per iteration.
#
# PR #8 built exactly that for the demand side (`.param_draw()`, a base-R
# Cholesky draw around coef/vcov). This module reuses it where a fitted model
# exists and supplies the equivalent where the supply side has only a point
# estimate and an observed series.
#
# WHAT CAN AND CANNOT BE QUANTIFIED
#
#   entrant rate      QUANTIFIED. The annual net-growth series is a sample; its
#                     mean has a standard error. Drawn per iteration.
#   fitted models     QUANTIFIED. Any model with coef/vcov (an hours model from
#                     fit_clinical_hours_model(), a demand model from PR #8)
#                     goes through .param_draw().
#                     WHERE THE HOURS DRAW LANDS: run_supply_microsimulation()
#                     substitutes the drawn coefficients back into the fitted
#                     model (.hours_model_with_coef) and passes it to the engine,
#                     which consults it only inside fte_of(). It therefore widens
#                     `effective_fte` and NOTHING else -- headcount is the size of
#                     the active set and cannot move with hours. This wiring was
#                     missing until the draw was traced end-to-end: the
#                     coefficients were drawn, discarded, and still reported as a
#                     quantified parameter, which is the too-narrow-interval
#                     defect this module exists to prevent, in its own code.
#   retirement hazard NOT QUANTIFIED from the published sources. HWSM Exhibit 17
#                     and the FutureDocs curve are printed without sample sizes
#                     or standard errors, so there is no defensible sampling
#                     distribution. Exposed as an explicit coefficient of
#                     variation that DEFAULTS TO ZERO and warns when left there,
#                     rather than an invented spread.
#   hours schedule    NOT QUANTIFIED for the same reason (HWSM Exhibit 14 is
#                     published as point estimates).
#
# Inventing a spread for the last two would manufacture confidence intervals
# that look rigorous and mean nothing. They are named, defaulted off, and
# reported as unquantified.

#' Standard error of a mean estimated from an observed annual series
#'
#' @param x Observed annual values.
#' @return Numeric standard error of the mean.
#' @export
series_mean_se <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 2) return(NA_real_)
  stats::sd(x) / sqrt(length(x))
}

#' Specify which supply parameters vary across Monte Carlo iterations
#'
#' @param entrant_series Observed annual net-growth values behind the entrant
#'   rate. Supplying it turns the entrant rate into a drawn parameter.
#' @param entrant_mean Point estimate of gross annual entrants.
#' @param departures Expected annual departures added to net growth to give
#'   gross entrants; held fixed unless `hazard_cv` is set.
#' @param hazard_cv Coefficient of variation on a multiplicative factor applied
#'   to the whole retirement schedule. 0 means the hazard is treated as known,
#'   which it is not -- see the module note.
#' @param hours_model Optional fitted hours model; its coefficients are drawn
#'   through the shared internal `.param_draw()` helper.
#' @return An object of class `urps_param_spec`.
#' @export
supply_parameter_spec <- function(entrant_series = NULL,
                                  entrant_mean = NULL,
                                  departures = 0,
                                  hazard_cv = 0,
                                  hours_model = NULL) {
  se <- if (!is.null(entrant_series)) series_mean_se(entrant_series) else NA_real_
  structure(
    list(
      entrant_series = entrant_series,
      entrant_mean = entrant_mean,
      entrant_se = se,
      departures = departures,
      hazard_cv = hazard_cv,
      hours_model = hours_model,
      quantified = c(
        entrant_rate = is.finite(se),
        retirement_hazard = hazard_cv > 0,
        hours = !is.null(hours_model)
      )
    ),
    class = "urps_param_spec"
  )
}

#' @export
print.urps_param_spec <- function(x, ...) {
  cat("Supply parameter uncertainty\n")
  cat(sprintf("  entrant rate      %s\n",
              if (isTRUE(x$quantified[["entrant_rate"]]))
                sprintf("drawn: mean %.1f, SE %.1f (n = %d observed years)",
                        x$entrant_mean, x$entrant_se, length(x$entrant_series))
              else "FIXED (no observed series supplied)"))
  cat(sprintf("  retirement hazard %s\n",
              if (isTRUE(x$quantified[["retirement_hazard"]]))
                sprintf("drawn: CV %.2f on a multiplicative factor", x$hazard_cv)
              else "FIXED -- published without standard errors, UNQUANTIFIED"))
  cat(sprintf("  hours schedule    %s\n",
              if (isTRUE(x$quantified[["hours"]]))
                "drawn from the fitted model's coef/vcov"
              else "FIXED -- published as point estimates, UNQUANTIFIED"))
  invisible(x)
}

#' Draw one parameter set for a Monte Carlo iteration
#'
#' `hours_coef` is the COEFFICIENT VECTOR of the fitted weekly-clinical-hours
#' model (`clinical_hours ~ ns(age, df) + sex`), drawn from its own `coef`/`vcov`
#' -- not a scalar multiplier. The no-uncertainty case is therefore
#' `hours_coef == coef(spec$hours_model)`, which reproduces the point-estimate
#' fit exactly. Substitute it into the model with `.hours_model_with_coef()`
#' rather than applying it to anything by hand.
#'
#' @param spec A [supply_parameter_spec()].
#' @param schedule Base retirement hazard schedule.
#' @return List with `entrants`, `retirement_schedule`, `hours_coef`.
#' @export
draw_supply_parameters <- function(spec, schedule = RETIREMENT_HAZARD_BY_AGE) {
  stopifnot(inherits(spec, "urps_param_spec"))

  entrants <- spec$entrant_mean
  if (isTRUE(spec$quantified[["entrant_rate"]])) {
    # The net-growth mean is drawn from its sampling distribution; departures are
    # added back to recover gross entrants.
    net_draw <- stats::rnorm(1, mean(spec$entrant_series), spec$entrant_se)
    entrants <- max(net_draw + spec$departures, 0)
  }

  sched <- schedule
  if (isTRUE(spec$quantified[["retirement_hazard"]])) {
    # Lognormal so the multiplier is positive and centred on 1.
    sdlog <- sqrt(log(1 + spec$hazard_cv^2))
    mult <- stats::rlnorm(1, meanlog = -sdlog^2 / 2, sdlog = sdlog)
    sched <- pmin(schedule * mult, 1)
  }

  hours_coef <- NULL
  if (isTRUE(spec$quantified[["hours"]])) {
    hours_coef <- .param_draw(spec$hours_model, 1L)[1, , drop = TRUE]
    # A non-finite draw means the covariance was singular or ill-conditioned.
    # Returning it would produce NA weekly hours, hence NA clinical FTE, hence a
    # silently missing replicate in the effective-FTE quantiles. Fail closed.
    if (!all(is.finite(hours_coef))) {
      stop("draw_supply_parameters(): the hours-coefficient draw is non-finite, ",
           "which means the fitted model's covariance is singular or ill-",
           "conditioned. Refit the hours model on data that identifies every ",
           "term rather than propagating a draw that yields NA clinical FTE.",
           call. = FALSE)
    }
  }

  list(entrants = entrants, retirement_schedule = sched, hours_coef = hours_coef)
}

# Substitute a drawn coefficient vector into a fitted hours model, so the ordinary
# predict() path -- terms, spline basis, contrasts, xlevels -- is reused unchanged
# and the drawn coefficients enter the estimand through exactly the same code the
# point-estimate model uses. `predict.lm()` reads `object$coefficients`, so
# replacing that slot is sufficient and leaves the rest of the fit intact.
# Internal.
.hours_model_with_coef <- function(model, coefs) {
  if (is.null(model) || is.null(coefs)) return(model)
  b <- stats::coef(model)
  common <- intersect(names(b)[!is.na(b)], names(coefs))
  if (!length(common)) {
    stop(".hours_model_with_coef(): the drawn coefficients share no names with ",
         "the fitted model, so they cannot be substituted.", call. = FALSE)
  }
  b[common] <- coefs[common]
  model$coefficients <- b
  model
}

#' Warn when the interval will describe only sampling noise
#'
#' @param spec A [supply_parameter_spec()], or NULL.
#' @param mode Reproducibility mode; strict errors.
#' @return (Invisibly) TRUE when at least one parameter varies.
#' @export
assert_parameter_uncertainty <- function(spec, mode = resolve_reproducibility_mode()) {
  any_drawn <- inherits(spec, "urps_param_spec") && any(spec$quantified)
  if (!any_drawn) {
    msg <- paste(
      "No parameter varies across Monte Carlo iterations, so the reported",
      "intervals describe individual stochasticity ONLY, not forecast",
      "uncertainty. The 2020->2023 back-test found intervals of this kind",
      "6.5-8.2x too narrow and outside coverage in every arm. Supply a",
      "supply_parameter_spec() or do not report intervals."
    )
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    return(invisible(FALSE))
  }
  unquantified <- names(spec$quantified)[!spec$quantified]
  if (length(unquantified)) {
    .msg_info(sprintf(
      "Parameter uncertainty: %s drawn; %s held fixed (published without standard errors).",
      paste(names(spec$quantified)[spec$quantified], collapse = ", "),
      paste(unquantified, collapse = ", ")))
  }
  invisible(TRUE)
}

#' Entrant-rate spec built from the observed certification series
#'
#' Convenience wrapper: reads the net-growth series through the contract, adds
#' modelled departures, and returns a spec whose entrant rate is drawn from the
#' series' own sampling distribution.
#'
#' @param agents Base-year cohort (supplies the age structure for departures).
#' @param from_year First year of the steady-state window.
#' @param through_year Last year that may be used.
#' @param hazard_cv Coefficient of variation on the retirement hazard.
#' @param hours_model Optional fitted hours model.
#' @return A [supply_parameter_spec()].
#' @export
entrant_spec_from_series <- function(agents, from_year = 2018L,
                                     through_year = NULL,
                                     hazard_cv = 0,
                                     hours_model = NULL) {
  coh <- if (is.null(through_year)) {
    urps_certification_cohorts()
  } else {
    backtest_cohorts_through(through_year)
  }
  hi <- if (is.null(through_year)) max(coh$cert_year) else through_year
  series <- coh$n_certified[coh$cert_year >= from_year & coh$cert_year <= hi]

  rate <- implied_annual_departure_rate(
    agents$age, if ("sex" %in% names(agents)) agents$sex else "female")
  departures <- nrow(agents) * rate

  supply_parameter_spec(
    entrant_series = series,
    entrant_mean = mean(series) + departures,
    departures = departures,
    hazard_cv = hazard_cv,
    hours_model = hours_model
  )
}
