# Anchor the life-course disease model to published symptomatic prevalence ----
#
# THE DEFECT. The life-course risk model produced POP prevalence 5.6x above
# published symptomatic prevalence (population-weighted 24.3% against 4.4%),
# and 59.7% among women 75+ -- a figure closer to EXAM-detected POP-Q stage >=2,
# most of which is asymptomatic, than to a bulge a woman notices.
#
# Because `treated` is prevalence x a care cascade, and prolapse_procedure
# volume is `treated` x p_advance, that error passes straight through: it
# accounts for most of the 8.51x procedure overstatement, leaving a residual
# near 1.5x. Diagnosing the overstatement as a p_advance problem was therefore
# looking in the wrong place.
#
# WHAT IS AND IS NOT RECALIBRATED. The linear predictor is
#
#   b0 + bvag*deliveries + bage*age + bysl*... + bbmi*bmi + bhyst + bmeno + bcomorb
#
# Only `b0` and `bage` are adjusted. Both are registered as
# "placeholder (expert judgement; not evidence-anchored)". The coefficients
# that CARRY CITATIONS are left exactly as they are -- for POP that is `bvag`
# (Hendrix WHI / Mant Oxford-FPA, OR ~1.35 per birth) and `bbmi` (Giri 2017).
#
# This matters for more than provenance: the scenario levers (delivery mode,
# BMI reduction) act through those coefficients. Replacing the risk model with
# an age-band lookup would match the marginal prevalence and silently destroy
# every scenario the model exists to run.

#' Published symptomatic prevalence targets by age band
#'
#' @details
#' SYMPTOMATIC prevalence, deliberately. For POP this is the bulge symptom a
#' woman reports, not exam-detected POP-Q stage, which is far more common and
#' largely asymptomatic. Confusing the two is the specific error this function
#' exists to correct.
#'
#' @param condition One of `"ui"`, `"pop"`, `"ai"`.
#' @return Named numeric vector over age bands.
#' @family prevalence calibration
#' @concept demand
#' @export
lifecourse_prevalence_targets <- function(condition = c("ui", "pop", "ai")) {
  condition <- match.arg(condition)
  switch(
    condition,
    # Nygaard 2008 JAMA / Wu 2014 -- symptomatic UI, women
    ui  = c("18-34" = 0.064, "35-44" = 0.148, "45-64" = 0.263,
            "65-74" = 0.318, "75+" = 0.356),
    # Nygaard 2008 JAMA -- SYMPTOMATIC prolapse
    pop = c("18-34" = 0.007, "35-44" = 0.025, "45-64" = 0.050,
            "65-74" = 0.067, "75+" = 0.068),
    # Whitehead 2009 / Bharucha 2005 -- faecal incontinence
    ai  = c("18-34" = 0.018, "35-44" = 0.032, "45-64" = 0.058,
            "65-74" = 0.085, "75+" = 0.104)
  )
}

.lc_band <- function(age) {
  cut(age, breaks = c(-Inf, 34, 44, 64, 74, Inf),
      labels = c("18-34", "35-44", "45-64", "65-74", "75+"))
}

#' Recalibrate the placeholder intercept and age slope to published prevalence
#'
#' @details
#' Solves for `b0` and `bage` so the cohort's marginal prevalence by age band
#' matches [lifecourse_prevalence_targets()]. Two free parameters against five
#' targets, so the fit is over-determined and cannot absorb an arbitrary
#' shape -- a poor match means the covariate structure, not the intercept, is
#' wrong, and the residuals are returned so that is visible rather than hidden.
#'
#' @param cohort Person-level tibble from the life-course generator, carrying
#'   `age` and the covariates the linear predictor uses.
#' @param risk_params Risk parameters to adjust, e.g. `lifecourse_risk_params()`.
#' @param conditions Which limbs to recalibrate.
#' @return A list with the adjusted `risk_params` and a `fit` tibble of
#'   achieved-versus-target prevalence by band.
#' @family prevalence calibration
#' @concept demand
#' @export
calibrate_lifecourse_prevalence <- function(cohort,
                                            risk_params = lifecourse_risk_params(),
                                            conditions = c("ui", "pop", "ai")) {
  needed <- c("age", "cumulative_vaginal_deliveries",
              "years_since_last_vaginal_birth", "bmi", "hysterectomy",
              "menopause_status", "comorbidity")
  missing <- setdiff(needed, names(cohort))
  if (length(missing) > 0L) {
    stop("cohort is missing column(s): ", paste(missing, collapse = ", "),
         call. = FALSE)
  }

  band <- .lc_band(cohort$age)
  fits <- list()

  for (cond in conditions) {
    p <- risk_params[[cond]]
    target <- lifecourse_prevalence_targets(cond)

    # Everything EXCEPT b0 and the age term. These are the parts that stay.
    fixed <- p$bvag    * cohort$cumulative_vaginal_deliveries +
             p$bysl    * (cohort$years_since_last_vaginal_birth / 10) +
             p$bbmi    * ((cohort$bmi - 27) / 5) +
             p$bhyst   * cohort$hysterectomy +
             p$bmeno   * cohort$menopause_status +
             p$bcomorb * cohort$comorbidity
    age_term <- (cohort$age - 50) / 10

    obj <- function(par) {
      pr <- stats::plogis(par[1] + par[2] * age_term + fixed)
      got <- tapply(pr, band, mean)
      got <- got[names(target)]
      # Weight by target magnitude so the small POP bands are not swamped by
      # the large ones; an unweighted fit would match 75+ and ignore 18-34,
      # which is where the model was worst (11.5x).
      sum(((got - target) / pmax(target, 1e-4))^2, na.rm = TRUE)
    }

    opt <- stats::optim(c(p$b0, p$bage), obj, method = "Nelder-Mead",
                        control = list(reltol = 1e-10, maxit = 2000))
    p$b0 <- opt$par[1]
    p$bage <- opt$par[2]
    risk_params[[cond]] <- p

    pr <- stats::plogis(p$b0 + p$bage * age_term + fixed)
    got <- tapply(pr, band, mean)[names(target)]
    fits[[cond]] <- tibble::tibble(
      condition = cond, age_band = names(target),
      target = unname(target), achieved = unname(got),
      ratio = unname(got) / unname(target)
    )
  }

  list(risk_params = risk_params, fit = dplyr::bind_rows(fits))
}

#' Life-course risk parameters anchored to published symptomatic prevalence
#'
#' @details
#' A drop-in alternative to `lifecourse_risk_params()` whose intercept and age
#' slope are recalibrated to published prevalence, with cited covariate effects
#' unchanged. Deterministic: it builds its own cohort under a fixed seed, so
#' repeated calls return identical coefficients.
#'
#' @param n Cohort size used for the calibration.
#' @param seed RNG seed.
#' @param pop_by_age Age distribution the calibration cohort is drawn from.
#'   Defaults to a flat 18-90 span so the fit is not dominated by whichever
#'   ages happen to be numerous in a particular projection year.
#' @return A risk-params list in the shape of `lifecourse_risk_params()`.
#' @family prevalence calibration
#' @concept demand
#' @export
lifecourse_risk_params_prevalence_anchored <- function(
    n = 40000L,
    seed = 20260817L,
    pop_by_age = data.frame(age = 18:90, population = 1e5)) {
  .preserve_rng_scope()
  set.seed(seed)
  cohort <- .lifecourse_population(pop_by_age, year = 2025L, n = n)
  calibrate_lifecourse_prevalence(cohort)$risk_params
}
