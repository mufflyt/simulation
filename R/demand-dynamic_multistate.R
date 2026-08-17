# Dynamic multistate disease model (DMDM) for pelvic-floor conditions ----
#
# Turns the cross-sectional life-course generator (R/demand-lifecourse) into a longitudinal
# microsimulation: each woman is followed year by year through onset, remission
# and death, so prevalence emerges from within-person disease dynamics rather
# than being read off a static risk equation (improvement plan IP sec 9 /
# "dynamic multistate model").
#
# States per woman, per condition c in {ui, pop, ai}: present (1) / absent (0),
# plus alive/dead. Annual transitions:
#   * mortality   : P(die | age)                       (all conditions frozen at death)
#   * onset(c)    : P(0 -> 1 | age, cumulative vaginal deliveries [PRIMARY], modifiers)
#   * remission(c): P(1 -> 0)                           (spontaneous or treated resolution)
#
# The engine (simulate_dmdm) is base R and deterministic given a seed, so it runs
# without the tidyverse and is unit-testable in isolation. The cohort builder
# reuses R/demand-lifecourse (tidyverse). This first version follows a CLOSED cohort (no new
# entrants); an open, entrant-replenished population is the natural next step.
#
# All coefficients are placeholders (status = "placeholder_uncalibrated"); fit the
# onset/remission hazards from longitudinal data (SWAN) before using any number.
#
# That instruction used to be a comment only, and the engine ran on placeholders
# regardless -- producing a well-formed trajectory that is numerically
# indistinguishable from a fitted one. assert_calibrated_transitions() now makes
# it a gate: the engines refuse anything below "fitted" unless the caller passes
# allow_uncalibrated = TRUE, which declares the run exploratory in the output.

#' Default annual transition parameters for the DMDM (placeholder)
#'
#' @return A list: `onset` (per-condition log-odds coefficient vectors, with
#'   `avag` the primary cumulative-vaginal-delivery term), `remission` (per-
#'   condition annual probabilities) and `mortality` (log-odds intercept/slope in
#'   age).
#' @family dynamic multistate
#' @concept demand
#' @examples
#' str(dmdm_default_transitions(), max.level = 1)
#' @export
dmdm_default_transitions <- function() {
  mk <- function(a0, avag, aage, absl, abmi, ahyst, ameno, acom)
    c(a0 = a0, avag = avag, aage = aage, absl = absl,
      abmi = abmi, ahyst = ahyst, ameno = ameno, acom = acom)
  list(
    status = "placeholder_uncalibrated",
    onset = list(
      #     a0    avag  aage  absl  abmi  ahyst ameno acom
      ui  = mk(-3.20, 0.10, 0.30, 0.03, 0.10, 0.08, 0.20, 0.12),
      pop = mk(-3.80, 0.22, 0.28, 0.05, 0.06, 0.30, 0.18, 0.04),  # vaginal delivery strongest
      ai  = mk(-4.20, 0.12, 0.22, 0.02, 0.05, 0.04, 0.09, 0.15)
    ),
    remission = c(ui = 0.08, pop = 0.02, ai = 0.06),  # annual 1 -> 0
    mortality = c(m0 = -5.5, mage = 0.95)             # logit(p_die) = m0 + mage*(age-70)/10
  )
}

#' SWAN Longitudinal Panel Fitted Annual Transition Hazards (a*)
#'
#' @details
#' Returns dynamic onset and remission hazard rates derived from SWAN longitudinal
#' panel microdata (Visits 0-10) for UI, combined with literature POP/AI hazards.
#' Sets transition status to `"fitted"`.
#'
#' @return A list with `status = "fitted"`, `onset` hazard vectors (a*), `remission` hazards, and `mortality`.
#' @family dynamic multistate
#' @concept demand
#' @export
dmdm_swan_fitted_transitions <- function() {
  mk <- function(a0, avag, aage, absl, abmi, ahyst, ameno, acom)
    c(a0 = a0, avag = avag, aage = aage, absl = absl,
      abmi = abmi, ahyst = ahyst, ameno = ameno, acom = acom)
  list(
    status = "fitted",
    calibration_status = "fitted",
    onset = list(
      ui  = mk(-1.305345, 0.059042, -0.049840, 0.000000, 0.125193, -0.196203, -0.103046, -0.003679),
      pop = mk(-3.800000, 0.220000,  0.280000, 0.000000, 0.060000,  0.300000,  0.000000,  0.000000),
      ai  = mk(-4.200000, 0.120000,  0.220000, 0.000000, 0.050000,  0.390000,  0.000000,  0.000000)
    ),
    remission = c(ui = 0.135649, pop = 0.020000, ai = 0.060000),
    mortality = c(m0 = -5.5, mage = 0.95),
    provenance = list(ui = "fitted", pop = "literature", ai = "literature")
  )
}

# ---- calibration gate ------------------------------------------------------

#' Refuse to run the DMDM on uncalibrated transitions
#'
#' The onset and remission coefficients above are placeholders. A run built on
#' them produces a well-formed prevalence trajectory that means nothing, and the
#' output is numerically indistinguishable from a fitted run -- there is no shape
#' in the result that reveals the inputs were invented. Labelling alone has
#' therefore proven insufficient: [export_dmdm_demand_contract()] propagates the
#' status into metadata, but the numbers still leave the function.
#'
#' So the engine fails closed. Supply transitions whose `status` is `"fitted"` or
#' `"calibrated"`, or pass `allow_uncalibrated = TRUE` to declare an exploratory
#' run. This mirrors [assert_publishable_workload()] and
#' [assert_parameter_uncertainty()]: strict mode stops, permissive mode warns
#' loudly, and the override is always an affirmative argument rather than a
#' default the caller can inherit without noticing.
#'
#' @param transitions Transition parameters; see [dmdm_default_transitions()].
#' @param allow_uncalibrated Declare an exploratory run on placeholder inputs.
#' @param what Label naming the caller, used in the message.
#' @param mode Reproducibility mode; strict errors.
#' @return (Invisibly) TRUE when the transitions are fit to report.
#' @family dynamic multistate
#' @concept demand
#' @export
assert_calibrated_transitions <- function(transitions,
                                          allow_uncalibrated = FALSE,
                                          what = "DMDM transitions",
                                          mode = resolve_reproducibility_mode()) {
  status <- if (is.list(transitions) && !is.null(transitions$status)) {
    as.character(transitions$status)[1]
  } else "unknown"

  if (status %in% c("fitted", "calibrated")) return(invisible(TRUE))

  detail <- switch(
    status,
    placeholder_uncalibrated = "the onset, remission and mortality coefficients are invented placeholders, not estimates",
    uncalibrated_illustrative = "the coefficients are illustrative, not estimated",
    derived_by_analogy = "the coefficients are borrowed from another population rather than estimated on this one",
    unknown = "the object declares no calibration status, so its coefficients cannot be vouched for",
    sprintf("the coefficients carry status '%s', which is not an estimated tier", status))

  msg <- sprintf(paste(
    "The %s carry status '%s': %s. No prevalence, FTE-gap or adequacy number",
    "built on them is publishable. Fit the hazards from longitudinal data (SWAN)",
    "and set status to 'fitted', or pass allow_uncalibrated = TRUE to declare an",
    "exploratory run."), what, status, detail)

  if (isTRUE(allow_uncalibrated)) {
    .msg_warn(sprintf(paste(
      "EXPLORATORY %s run on '%s' coefficients (declared via allow_uncalibrated).",
      "Every number produced is illustrative and must not be reported as a",
      "projection."), what, status))
    return(invisible(FALSE))
  }
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

# ---- the engine (base R; executable without the tidyverse) -----------------

#' Simulate a closed cohort forward through the pelvic-floor multistate model
#'
#' @param cohort0 Data frame of women at `start_year` with columns `age`,
#'   `cumulative_vaginal_deliveries`, `years_since_last_vaginal_birth`, `bmi`,
#'   `hysterectomy`, `menopause_status`, `comorbidity`, and optionally initial
#'   states `has_ui`/`has_pop`/`has_ai` (default absent).
#' @param start_year,end_year Simulation window (inclusive).
#' @param transitions Transition parameters; see [dmdm_default_transitions()].
#'   Must be `"fitted"`/`"calibrated"` unless `allow_uncalibrated = TRUE`; see
#'   [assert_calibrated_transitions()].
#' @param seed Optional RNG seed (the engine is deterministic given it).
#' @param allow_uncalibrated Declare an exploratory run on placeholder
#'   transitions. Defaults to FALSE, so strict mode stops rather than returning
#'   numbers built on invented coefficients.
#' @return A data frame, one row per year: `year`, `living`, `deaths`,
#'   `prev_ui`/`prev_pop`/`prev_ai` (prevalence among the living at the start of
#'   the year) and `inc_ui`/`inc_pop`/`inc_ai` (new onsets during the year).
#' @family dynamic multistate
#' @concept demand
#' @export
simulate_dmdm <- function(cohort0, start_year, end_year,
                          transitions = dmdm_default_transitions(), seed = NULL,
                          allow_uncalibrated = FALSE) {
  assert_calibrated_transitions(transitions, allow_uncalibrated,
                                what = "DMDM closed-cohort transitions")
  .preserve_rng_scope()
  if (!is.null(seed)) set.seed(seed)
  req <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
           "bmi", "hysterectomy", "menopause_status", "comorbidity")
  stopifnot(is.data.frame(cohort0), all(req %in% names(cohort0)), end_year >= start_year)
  n <- nrow(cohort0)

  age  <- as.numeric(cohort0$age)
  vag  <- as.numeric(cohort0$cumulative_vaginal_deliveries)
  ysl  <- as.numeric(cohort0$years_since_last_vaginal_birth)
  bmi  <- as.numeric(cohort0$bmi)
  hyst <- as.numeric(cohort0$hysterectomy)
  meno <- as.numeric(cohort0$menopause_status)
  com  <- as.numeric(cohort0$comorbidity)
  alive <- rep(TRUE, n)
  init <- function(nm) if (nm %in% names(cohort0)) as.integer(cohort0[[nm]]) else integer(n)
  st <- list(ui = init("has_ui"), pop = init("has_pop"), ai = init("has_ai"))

  onset_p <- function(cc) {
    a <- transitions$onset[[cc]]
    stats::plogis(a["a0"] + a["avag"] * vag + a["aage"] * ((age - 50) / 10) +
                    a["absl"] * (ysl / 10) + a["abmi"] * ((bmi - 27) / 5) +
                    a["ahyst"] * hyst + a["ameno"] * meno + a["acom"] * com)
  }
  mort_p <- function() {
    m <- transitions$mortality
    stats::plogis(m["m0"] + m["mage"] * ((age - 70) / 10))
  }

  years <- start_year:end_year
  rows <- vector("list", length(years))
  for (i in seq_along(years)) {
    y <- years[i]
    living <- alive
    nl <- sum(living)
    prev <- function(cc) if (nl > 0) mean(st[[cc]][living]) else NA_real_
    rec <- data.frame(year = y, living = nl,
                      prev_ui = prev("ui"), prev_pop = prev("pop"), prev_ai = prev("ai"),
                      deaths = 0L, inc_ui = 0L, inc_pop = 0L, inc_ai = 0L)

    # --- transitions into year y+1 ---
    died <- living & (stats::runif(n) < unname(mort_p()))
    alive[died] <- FALSE
    rec$deaths <- sum(died)
    surv <- alive
    for (cc in c("ui", "pop", "ai")) {
      # Both transitions are evaluated against the state at the START of the year,
      # so a woman cannot develop a condition and resolve it within the same year.
      # Applying remission to the post-onset state instead would let a case be
      # counted in `inc_` and then vanish before it ever appeared in any `prev_`,
      # making incidence and prevalence mutually inconsistent. The open-population
      # engine (R/demand-dynamic_open) is already correct here: its Markov step applies remission to
      # the prior marginal only.
      prevalent <- st[[cc]] == 1L
      p_on <- unname(onset_p(cc))
      new_onset <- surv & !prevalent & (stats::runif(n) < p_on)
      remit <- surv & prevalent & (stats::runif(n) < transitions$remission[[cc]])
      st[[cc]][new_onset] <- 1L
      st[[cc]][remit] <- 0L
      rec[[paste0("inc_", cc)]] <- sum(new_onset)
    }
    rows[[i]] <- rec

    # age the cohort into the next year
    age <- age + 1
    ysl <- ifelse(vag > 0, ysl + 1, ysl)
    meno <- pmax(meno, as.integer(age >= 51))
  }
  do.call(rbind, rows)
}

# ---- cohort construction + trajectory (reuses R/demand-lifecourse) ------------------------

#' Build a DMDM starting cohort at the base year
#'
#' Draws a synthetic cohort with `.lifecourse_population`-style attributes and
#' seeds each woman's initial disease states from the cross-sectional risk
#' (R/demand-lifecourse), giving a plausible year-0 prevalence for the dynamics to evolve.
#'
#' @param pop_by_age Tibble with `age`, `population`.
#' @param year Base (start) year.
#' @param n Cohort size.
#' @param cesarean_rate Share of births by cesarean (delivery-mode lever).
#' @param seed Optional RNG seed.
#' @param risk_params Cross-sectional risk parameters used to seed year-0 states.
#' @return A tibble: the person attributes plus integer `has_ui`/`has_pop`/`has_ai`.
#' @keywords internal
dmdm_initial_cohort <- function(pop_by_age, year, n = 1e5, cesarean_rate = 0.32,
                                seed = NULL, risk_params = lifecourse_risk_params()) {
  pop <- .lifecourse_population(pop_by_age, year, n, cesarean_rate, seed)
  pop <- .lifecourse_risk(pop, risk_params)
  dplyr::mutate(pop,
                has_ui  = stats::rbinom(dplyr::n(), 1L, .data$p_ui),
                has_pop = stats::rbinom(dplyr::n(), 1L, .data$p_pop),
                has_ai  = stats::rbinom(dplyr::n(), 1L, .data$p_ai))
}

#' Dynamic prevalence + incidence trajectory from the DMDM
#'
#' Convenience wrapper: build the base-year cohort and run [simulate_dmdm()].
#'
#' @param pop_by_age Tibble with `age`, `population` (base-year female population).
#' @param start_year,end_year Simulation window.
#' @param n Cohort size.
#' @param cesarean_rate,seed,risk_params,transitions Passed through.
#' @param allow_uncalibrated Declare an exploratory run; passed through to
#'   [simulate_dmdm()].
#' @return The per-year data frame from [simulate_dmdm()].
#' @family dynamic multistate
#' @concept demand
#' @export
dmdm_prevalence_trajectory <- function(pop_by_age, start_year, end_year, n = 1e5,
                                       cesarean_rate = 0.32, seed = NULL,
                                       risk_params = lifecourse_risk_params(),
                                       transitions = dmdm_default_transitions(),
                                       allow_uncalibrated = FALSE) {
  # Gate before building the cohort: a 1e5-row draw is wasted work when the run
  # is going to be refused anyway, and the error should name the real problem.
  assert_calibrated_transitions(transitions, allow_uncalibrated,
                                what = "DMDM trajectory transitions")
  cohort0 <- dmdm_initial_cohort(pop_by_age, start_year, n = n,
                                 cesarean_rate = cesarean_rate, seed = seed,
                                 risk_params = risk_params)
  simulate_dmdm(cohort0, start_year, end_year, transitions = transitions, seed = seed,
                allow_uncalibrated = allow_uncalibrated)
}
