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
#
# STATUS. The entrant draw is now wired through run_backtest() as well as the
# main projections, so the back-test scores real intervals: PI95 widths went
# from 0-40 to 129-148 providers, and coverage from 0/8 to 2/8. Two arms is
# still far short of the required 80%, and every arm continues to under-predict
# by 3.1-17.6%. Widening intervals cannot fix a one-sided level error, and this
# module deliberately does not try to -- see R/validation-backtest_status.R.

#' Standard error of a mean estimated from an observed annual series
#'
#' @param x Observed annual values.
#' @return Numeric standard error of the mean.
#' @family parameter uncertainty
#' @concept calibration
#' @export
series_mean_se <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 2) return(NA_real_)
  stats::sd(x) / sqrt(length(x))
}

#' Specify which supply parameters vary across Monte Carlo iterations
#'
#' @details
#' Declares which parameters are redrawn each Monte Carlo iteration. This is the
#' difference between an interval that describes forecast uncertainty and one
#' that describes only the luck of who retires when. In the 2020->2023
#' back-test, intervals built without a spec were 0-40 providers wide on a count
#' near 1,300 -- two arms had zero width -- and covered the observation in 0 of
#' 8 arms; drawing the entrant rate widens them to 129-148.
#'
#' A parameter is drawn only where an uncertainty estimate genuinely exists.
#' `quantified` records which ones those are, so a fixed parameter is visible as
#' a limitation rather than being given an invented spread to make the interval
#' look complete.
#' @param entrant_series Observed annual entrant counts behind the entrant rate.
#'   Supplying it turns the entrant rate into a drawn parameter.
#' @param entrant_mean Point estimate of gross annual entrants.
#' @param departures Expected annual departures, recorded for reporting only.
#'   NOT added to `entrant_mean`: the certification series is a gross flow that
#'   never nets departures out, so adding them double-counts (see
#'   [observed_entrant_rate()]).
#' @param hazard_cv Coefficient of variation on a multiplicative factor applied
#'   to the whole retirement schedule. 0 means the hazard is treated as known,
#'   which it is not -- see the module note.
#' @param hours_model Optional fitted hours model; its coefficients are drawn
#'   through the shared internal `.param_draw()` helper.
#' @param entrant_regime Optional [fit_entrant_regime_model()] object. Supplying
#'   it REPLACES the scalar entrant draw with a whole per-year entrant path, so
#'   the iteration carries trend-coefficient, overdispersion, and regime-break
#'   uncertainty rather than sampling variation in a three-year mean. This is the
#'   component the back-test addendum found missing: variation WITHIN the fitting
#'   window cannot represent a change of regime BETWEEN windows.
#' @param entrant_regime_include Components of the regime draw to switch on; see
#'   [draw_entrant_paths()].
#' @return An object of class `urps_param_spec`.
#' @examples
#' # Draws the entrant rate per iteration from an observed series. Without a
#' # spec, every iteration shares one entrant rate and the resulting interval
#' # describes individual stochasticity only -- in the 2020->2023 back-test
#' # those intervals covered the observation in 0 of 8 arms.
#' spec <- supply_parameter_spec(entrant_series = c(50, 57, 53, 59, 59),
#'                               entrant_mean = 55)
#' spec$quantified
#' @family parameter uncertainty
#' @concept calibration
#' @export
supply_parameter_spec <- function(entrant_series = NULL,
                                  entrant_mean = NULL,
                                  departures = 0,
                                  hazard_cv = 0,
                                  hours_model = NULL,
                                  entrant_regime = NULL,
                                  entrant_regime_include = c("trend", "dispersion",
                                                             "break", "release_timing")) {
  se <- if (!is.null(entrant_series)) series_mean_se(entrant_series) else NA_real_
  if (!is.null(entrant_regime) &&
      !inherits(entrant_regime, "urps_entrant_regime")) {
    stop("supply_parameter_spec(): entrant_regime must come from ",
         "fit_entrant_regime_model().", call. = FALSE)
  }
  structure(
    list(
      entrant_series = entrant_series,
      entrant_mean = entrant_mean,
      entrant_se = se,
      departures = departures,
      hazard_cv = hazard_cv,
      hours_model = hours_model,
      entrant_regime = entrant_regime,
      entrant_regime_include = entrant_regime_include,
      quantified = c(
        # SE > 0, not merely finite. A constant series (c(50,50,50,50)) has
        # sd 0, so se is 0 and `is.finite(0)` is TRUE -- the spec then claimed
        # the entrant rate was DRAWN while every replicate used the same value.
        # That is the too-narrow-interval defect this module exists to prevent,
        # passing this module's own gate.
        entrant_rate = (is.finite(se) && se > 0) || !is.null(entrant_regime),
        entrant_regime_break = !is.null(entrant_regime) &&
          "break" %in% entrant_regime_include &&
          is.finite(entrant_regime$break_surviving_share),
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
              if (!is.null(x$entrant_regime))
                sprintf("drawn as a PATH from the regime model (fitted through %d, %s trend)",
                        x$entrant_regime$fit_through_year,
                        x$entrant_regime$trend_family)
              else if (isTRUE(x$quantified[["entrant_rate"]]))
                sprintf("drawn: mean %.1f, SE %.1f (n = %d observed years)",
                        x$entrant_mean, x$entrant_se, length(x$entrant_series))
              else "FIXED (no observed series supplied)"))
  cat(sprintf("  regime break      %s\n",
              if (isTRUE(x$quantified[["entrant_regime_break"]]))
                sprintf("drawn: %.3f/yr, surviving share %.2f, deficit deferred",
                        x$entrant_regime$break_probability,
                        x$entrant_regime$break_surviving_share)
              else if (!is.null(x$entrant_regime))
                "NOT simulated -- no disruption in the fitting window"
              else "FIXED -- no regime model supplied"))
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
#' @param years Simulated years. Required when `spec` carries an entrant regime
#'   model, because that draw is a per-year path rather than a scalar rate. The
#'   returned `entrants` is then one value per TRANSITION -- `length(years) - 1`,
#'   element i being the cohort entering between `years[i]` and `years[i + 1]` --
#'   which is one of the lengths [simulate_provider_career_once()] accepts.
#' @return List with `entrants`, `retirement_schedule`, `hours_coef`.
#' @family parameter uncertainty
#' @concept calibration
#' @examples
#' # One draw per Monte Carlo iteration. `quantified` records WHICH parameters
#' # actually vary -- anything published without a standard error is held fixed
#' # and says so, rather than being given an invented spread.
#' spec <- supply_parameter_spec(entrant_series = c(50, 57, 53, 59, 59),
#'                               entrant_mean = 55)
#' draw <- draw_supply_parameters(spec)
#' draw$entrants_per_year
#' @export
draw_supply_parameters <- function(spec, schedule = RETIREMENT_HAZARD_BY_AGE,
                                   years = NULL) {
  stopifnot(inherits(spec, "urps_param_spec"))

  entrants <- spec$entrant_mean
  if (!is.null(spec$entrant_regime)) {
    if (is.null(years)) {
      stop("draw_supply_parameters(): the spec carries an entrant regime model, ",
           "whose draw is a per-year path, so `years` must be supplied. Passing ",
           "a scalar mean instead would discard the regime structure the model ",
           "exists to represent.", call. = FALSE)
    }
    years <- sort(unique(as.integer(years)))
    # ONE VALUE PER TRANSITION, which is the length
    # simulate_provider_career_once() documents alongside one-per-projected-year:
    # element i is the cohort entering between years[i] and years[i + 1]. The
    # base year needs no entrant count, because its cohort arrives in `agents`.
    projected <- years[-1]
    entrants <- if (length(projected)) {
      as.numeric(draw_entrant_paths(spec$entrant_regime, projected, 1L,
                                    include = spec$entrant_regime_include))
    } else numeric(0)
  } else if (isTRUE(spec$quantified[["entrant_rate"]])) {
    # Draw around `entrant_mean` itself, NOT around mean(entrant_series) with
    # departures added back. The two differ whenever the series is already a
    # gross flow, and centring the draw anywhere other than the point estimate
    # silently biases every interval away from the median the model reports.
    # Tying them together makes that class of mismatch unrepresentable: whatever
    # balance the constructor chose, the draw inherits it.
    entrants <- max(stats::rnorm(1, spec$entrant_mean, spec$entrant_se), 0)
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
#' @family parameter uncertainty
#' @concept calibration
#' @export
assert_parameter_uncertainty <- function(spec, mode = resolve_reproducibility_mode()) {
  any_drawn <- inherits(spec, "urps_param_spec") && any(spec$quantified)
  if (!any_drawn) {
    msg <- paste(
      "No parameter varies across Monte Carlo iterations, so the reported",
      "intervals describe individual stochasticity ONLY, not forecast",
      "uncertainty. In the 2020->2023 back-test, intervals of this kind were",
      "0-40 providers wide on a count near 1,300 -- two arms had zero width --",
      "and covered the observation in 0 of 8 arms; drawing the entrant rate",
      "widens them to 129-148. Supply a",
      "supply_parameter_spec() with an entrant_series and entrant_mean (or a fitted",
      "hours_model), or do not report intervals. An empty supply_parameter_spec()",
      "does not quantify any parameter."
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
#' Convenience wrapper: reads the annual certification flow through the contract
#' and returns a spec whose entrant rate is drawn from the series' own sampling
#' distribution.
#'
#' The flow is used as-is. It is a GROSS entrant count -- the source series never
#' removes departures -- so modelled departures are recorded but not added; see
#' [observed_entrant_rate()].
#'
#' @param agents Base-year cohort (supplies the age structure for departures).
#' @param from_year First year of the steady-state window.
#' @param through_year Last year that may be used.
#' @param entrant_mean Point estimate to centre the draw on. `NULL` (default)
#'   uses the series mean. Supply it to keep a caller's or a scenario's entrant
#'   level while still taking the SPREAD from the observed series -- see
#'   [recentre_entrant_spec()].
#' @param hazard_cv Coefficient of variation on the retirement hazard.
#' @param hours_model Optional fitted hours model.
#' @return A [supply_parameter_spec()].
#' @family parameter uncertainty
#' @concept calibration
#' @export
entrant_spec_from_series <- function(agents, from_year = 2018L,
                                     through_year = NULL,
                                     entrant_mean = NULL,
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
    entrant_mean = entrant_mean %||% {
      m <- if (length(series)) mean(series, na.rm = TRUE) else NA_real_
      if (!is.finite(m))
        stop("entrant_spec_from_series(): the entrant series is empty/all-NA over the ",
             "selected window; cannot derive entrant_mean. Supply `entrant_mean` or ",
             "widen the year window.", call. = FALSE)
      m
    },
    departures = departures,
    hazard_cv = hazard_cv,
    hours_model = hours_model
  )
}

#' Re-centre a parameter spec on a new entrant level
#'
#' Keeps the observed series (and therefore the SPREAD) while moving the point
#' estimate the draw is centred on.
#'
#' WHY THIS EXISTS. `run_supply_microsimulation()` lets `entrant_mean` take
#' precedence over `entrants_per_year`, so a single spec shared across scenarios
#' silently overrode every scenario's entrant level. The "Fellowship output
#' +10%" and "-10%" scenarios returned results identical to Baseline to the last
#' digit -- the entrant-policy lever, which is the most policy-relevant knob in
#' the model, did nothing at all. Re-centring per scenario keeps the scenario
#' definition authoritative for the LEVEL and the observed series authoritative
#' for the UNCERTAINTY.
#'
#' @param spec A [supply_parameter_spec()], or NULL.
#' @param entrant_mean New point estimate.
#' @return The spec with `entrant_mean` replaced; NULL passes through.
#' @keywords internal
recentre_entrant_spec <- function(spec, entrant_mean) {
  if (!inherits(spec, "urps_param_spec")) return(spec)
  if (!is.numeric(entrant_mean) || length(entrant_mean) != 1L ||
      !is.finite(entrant_mean)) {
    return(spec)
  }
  spec$entrant_mean <- entrant_mean
  spec
}
