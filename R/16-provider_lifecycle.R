# Provider Lifecycle: Roster, Hours Worked, Retirement, Career Change ----
#
# Replaces the illustrative provider agents and stepwise productivity weights in
# R/12-provider_microsimulation.R with the empirical lifecycle structure used by
# the Dall-family workforce models.
#
# Sources for the structure (not the placeholder numbers):
#   * Dall TM et al. Supply and demand analysis of the current and future US
#     neurology workforce. Neurology 2013;81:470-478.
#       - individual-level provider records; FTE defined as an hours threshold
#         (42.3 patient-care hrs/wk); patient-care hours by age and sex;
#         retirement ages from members whose status changed to "Senior";
#         mortality for professional occupations ~25% lower than national for
#         men and ~15% lower for women.
#   * Dall TM et al. The Physiatry Workforce in 2019 and Beyond, Part 2.
#     Am J Phys Med Rehabil 2021;100:877-884.
#       - FTE = 37.2 patient-care hrs/wk (survey mean); OLS of weekly patient-care
#         hours on age and sex; age-dependent annual attrition for age >= 50 from
#         survey retirement intentions adjusted for mortality; all retired by 75.
#   * Zarek P et al. Current and Projected Future Supply and Demand for Physical
#     Therapists From 2022 to 2037. Phys Ther 2025;105:pzaf014.
#       - multistate-licensure de-duplication (344,920 licenses -> 246,440
#         resident licensees); FTE = 40 hrs/wk; OLS hours by sex and age group
#         (males +5.1 hrs/wk; flat to ~55 then declining); a SEPARATE 1.42%/yr
#         career-change hazard for those under 50, estimated from CPS ASEC by the
#         Wolf & Lockard occupational-separations method.
#   * IHS Markit / Dall TM et al. Health Workforce Microsimulation Model
#     Documentation v5.19.20, May 2020 (HWSM chapter).
#       - "Each occupation has a maximum age at which the retirement probability
#         is set to 1... In general, this maximum age is 75 for most occupations
#         and 90 FOR PHYSICIANS AND DENTISTS."
#       - Exhibit 17 (Florida physician survey 2012-2013): of 100 physicians
#         active at 50, ~80 are still active at 60 and ~30 at 70. Female
#         physicians retire slightly earlier than male colleagues.
#       - Exhibit 14: weekly patient-care hours are modelled by OLS on age-group
#         dummies, sex, AGE-BY-SEX INTERACTIONS and state; hours are flat to the
#         late 50s and fall only from 60 (R^2 = 0.09 -- demographics explain
#         little of the variation).
#       - Labour-force participation is modelled only for clinicians under 50;
#         at 50 and above the model switches to PERMANENT retirement.
#   * Fraher EP, Knapton A. Nephrologist Workforce Trends in the United States.
#     Cecil G. Sheps Center, UNC, October 2017 (FutureDocs model).
#       - independent physician microsimulation: all physicians retire before 80;
#         survival flat to ~54, ~0.5 at ~68, ~0.1 at ~78.
#       - FTE assigned CATEGORICALLY (1.0 full time / 0.5 part time / 0 no
#         patient care) with probabilities by age and sex.
#
# The two attrition processes are deliberately kept apart: retirement is
# age-graded and estimated from retirement intentions; career change is
# approximately age-flat and estimated from labour-force separations. Applying a
# single retirement-shaped gradient to a 38-year-old is a category error.
#
# IMPORTANT: the PHYSICIAN retirement curve is far longer than the allied-health
# curve. Physical therapists are >90% retired by 70 and all by 75; physicians are
# only ~70% retired by 70 and the HWSM does not force retirement until 90. Using
# an allied-health curve for a surgical subspecialty overstates attrition badly.

# ---- FTE definition -------------------------------------------------------

# FTE is an HOURS THRESHOLD, not a productivity weight -- but the threshold is
# NOT comparable across studies:
#   physiatry (Dall 2021)  37.2 clinical hrs/wk
#   PT (Zarek 2025)        40   professional hrs/wk
#   neurology (Dall 2013)  42.3 patient-care hrs/wk
#   FutureDocs (UNC 2017)  70   patient-care hrs/wk  <- a different construct
# We adopt the physiatry convention for a surgical subspecialty. Any FTE compared
# across models must first be restated on a common threshold; `fte_definition()`
# carries the threshold so a mismatch is visible rather than silent.
URPS_FTE_CLINICAL_HOURS_PER_WEEK <- 37.2

#' Describe the FTE definition in force
#'
#' @param hours Weekly clinical hours defining 1.0 FTE.
#' @return List with the hours threshold and a human-readable label.
#' @export
fte_definition <- function(hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK) {
  list(hours_per_week = hours,
       basis = "clinical hours",
       label = sprintf("1.0 FTE = %.1f clinical hrs/wk", hours))
}

#' Restate an FTE count from one hours threshold onto another
#'
#' Guards the cross-model comparison trap: 8,853 physiatrist FTE at 37.2 hrs/wk
#' and 6,533 nephrologist FTE at 70 hrs/wk are not on the same scale.
#'
#' @param fte Numeric FTE count.
#' @param from_hours Hours threshold the count is expressed on.
#' @param to_hours Hours threshold to restate onto.
#' @return Numeric restated FTE.
#' @export
restate_fte <- function(fte, from_hours, to_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK) {
  assertthat::assert_that(is.numeric(from_hours), from_hours > 0,
                          is.numeric(to_hours), to_hours > 0)
  if (!isTRUE(all.equal(from_hours, to_hours))) {
    .msg_info(sprintf("Restating FTE from a %.1f hr/wk to a %.1f hr/wk definition",
                      from_hours, to_hours))
  }
  fte * from_hours / to_hours
}

# Terminal age at which retirement probability is set to 1. HWSM uses 90 for
# physicians and dentists (75 for most other occupations); FutureDocs uses 80.
# 90 is the default so the tail is governed by the hazard schedule rather than by
# a truncation the data do not support.
MICROSIM_TERMINAL_AGE <- 90L
MICROSIM_TERMINAL_AGE_ALLIED <- 75L
MICROSIM_TERMINAL_AGE_FUTUREDOCS <- 80L

# ---- Retirement hazard schedules ------------------------------------------

# PHYSICIAN schedule. Single-year hazards from 50, chosen to reproduce the
# survival profile shared by HWSM Exhibit 17 and the FutureDocs curve:
#   S(60) ~ 0.80   S(65) ~ 0.55   S(70) ~ 0.30   S(75) ~ 0.12   S(80) ~ 0.03
# `test-provider-lifecycle.R` asserts these anchors.
RETIREMENT_HAZARD_PHYSICIAN <- c(
  stats::setNames(rep(0.022, 5L), 50:54),   # S(55) ~ 0.895
  stats::setNames(rep(0.023, 5L), 55:59),   # S(60) ~ 0.797
  stats::setNames(rep(0.073, 5L), 60:64),   # S(65) ~ 0.548
  stats::setNames(rep(0.115, 5L), 65:69),   # S(70) ~ 0.301
  stats::setNames(rep(0.170, 5L), 70:74),   # S(75) ~ 0.123
  stats::setNames(rep(0.240, 5L), 75:79),   # S(80) ~ 0.030
  stats::setNames(rep(0.350, 10L), 80:89)
)

# ALLIED-HEALTH schedule (physical therapy; Zarek 2025): ~60% of those aged 50
# retired by 65, >90% by 70, all by 75. Retained for comparison and for modelling
# the APP side of the delegation matrix -- NOT for physicians.
RETIREMENT_HAZARD_ALLIED <- c(
  stats::setNames(rep(0.020, 5L), 50:54),
  stats::setNames(rep(0.040, 5L), 55:59),
  stats::setNames(rep(0.115, 5L), 60:64),
  stats::setNames(rep(0.275, 5L), 65:69),
  stats::setNames(rep(0.350, 5L), 70:74)
)

# Default for this repo: URPS providers are physicians.
RETIREMENT_HAZARD_BY_AGE <- RETIREMENT_HAZARD_PHYSICIAN

# Female physicians retire slightly earlier than male colleagues (HWSM Exhibit
# 17; the two curves nearly overlap, so the effect is small by design).
RETIREMENT_SEX_HAZARD_MULTIPLIER <- c(male = 1.00, female = 1.06)

RETIREMENT_MIN_AGE <- 50L

# Annual probability that a provider under RETIREMENT_MIN_AGE stops delivering
# clinical care. HWSM models this as LABOUR-FORCE PARTICIPATION (temporary, with
# re-entry) rather than permanent exit; Zarek's 1.42%/yr CPS ASEC estimate is a
# permanent occupational separation. Both are represented: `career_change_hazard`
# is the permanent component, `reentry_probability` lets the temporary component
# return.
CAREER_CHANGE_HAZARD_UNDER_50 <- 0.0142
LABOR_FORCE_REENTRY_PROBABILITY <- 0.0

# Professional-occupation mortality adjustment (Dall 2013): mortality among
# professional occupations runs about 25% below national rates for men and 15%
# below for women. Applied to any external life table supplied by the caller.
PROFESSIONAL_MORTALITY_MULTIPLIER <- c(male = 0.75, female = 0.85)

#' Annual departure hazard for a provider of a given age
#'
#' Combines the two distinct processes: an age-graded retirement hazard from
#' `RETIREMENT_MIN_AGE`, and an age-flat career-change hazard below it. Everyone
#' at or above `MICROSIM_TERMINAL_AGE` departs with probability 1.
#'
#' @param age Numeric age(s).
#' @param sex Character sex ("male"/"female"), recycled to length of `age`.
#'   Female physicians retire slightly earlier (HWSM Exhibit 17).
#' @param retirement_schedule Named numeric vector of hazards keyed by single
#'   year of age (see `RETIREMENT_HAZARD_PHYSICIAN`).
#' @param career_change_hazard Annual hazard applied below `RETIREMENT_MIN_AGE`.
#' @param terminal_age Age at which departure is certain.
#' @param sex_multiplier Named multiplier on the retirement hazard by sex.
#' @return Numeric hazard(s) in the unit interval.
#' @export
departure_hazard <- function(age,
                             sex = "female",
                             retirement_schedule = RETIREMENT_HAZARD_BY_AGE,
                             career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50,
                             terminal_age = MICROSIM_TERMINAL_AGE,
                             sex_multiplier = RETIREMENT_SEX_HAZARD_MULTIPLIER) {
  age_i <- floor(as.numeric(age))
  sex <- tolower(as.character(rep_len(sex, length(age_i))))
  mult <- unname(sex_multiplier[sex])
  mult[is.na(mult)] <- 1

  h <- rep(NA_real_, length(age_i))

  below <- !is.na(age_i) & age_i < RETIREMENT_MIN_AGE
  h[below] <- career_change_hazard

  retiring <- !is.na(age_i) & age_i >= RETIREMENT_MIN_AGE & age_i < terminal_age
  if (any(retiring)) {
    keys <- as.character(age_i[retiring])
    hz <- unname(retirement_schedule[keys])
    # Ages inside the retirement window but absent from the schedule take the
    # highest tabulated hazard rather than silently becoming NA.
    hz[is.na(hz)] <- max(retirement_schedule)
    # No compounding with career change above RETIREMENT_MIN_AGE: HWSM models
    # labour-force participation only for clinicians under 50 and "switches to
    # permanent retirement as the activity status changes for clinicians age 50
    # and over". The retirement schedule IS all-cause attrition above 50, so
    # adding a separate career-change hazard would double-count exits.
    h[retiring] <- pmin(hz * mult[retiring], 1)
  }

  h[!is.na(age_i) & age_i >= terminal_age] <- 1
  h[is.na(age_i)] <- career_change_hazard
  pmin(pmax(h, 0), 1)
}

#' Shift the retirement schedule earlier or later on the age axis
#'
#' The retirement scenarios in every Dall-family paper are expressed as "retire
#' two years earlier / later", not as a multiplicative hazard knob. Shifting the
#' age axis preserves the shape of the retirement curve; a multiplier does not
#' (it can push a 50-year-old's hazard above a 70-year-old's).
#'
#' @param delta_years Positive = retire LATER (schedule shifts to older ages);
#'   negative = retire earlier.
#' @param schedule Retirement hazard schedule to shift.
#' @return A shifted named hazard vector.
#' @export
shift_retirement_schedule <- function(delta_years, schedule = RETIREMENT_HAZARD_BY_AGE) {
  assertthat::assert_that(is.numeric(delta_years), length(delta_years) == 1L)
  if (delta_years == 0) return(schedule)
  ages <- as.integer(names(schedule)) + as.integer(delta_years)
  out <- stats::setNames(unname(schedule), as.character(ages))

  # Retiring later leaves a gap at the young end; fill it with the lowest hazard.
  # Retiring earlier pushes hazards below RETIREMENT_MIN_AGE; those are dropped
  # (career change already covers that age range).
  keep <- as.integer(names(out)) >= RETIREMENT_MIN_AGE
  out <- out[keep]
  gap_ages <- setdiff(RETIREMENT_MIN_AGE:(min(as.integer(names(out))) - 1L), integer(0))
  if (length(gap_ages) > 0) {
    filler <- stats::setNames(rep(min(schedule), length(gap_ages)), as.character(gap_ages))
    out <- c(filler, out)
  }
  out[order(as.integer(names(out)))]
}

#' Survival of a cohort aged `from_age` to each subsequent age
#'
#' Diagnostic used to check the hazard schedule against the published survival
#' anchors. For physicians (HWSM Exhibit 17 / FutureDocs Figure 10) those are
#' S(60) ~ 0.80, S(65) ~ 0.55, S(70) ~ 0.30, S(75) ~ 0.12, S(80) ~ 0.03.
#'
#' @param from_age Starting age.
#' @param to_ages Ages at which to report survival.
#' @param sex Sex to evaluate; defaults to the reference group (male), which is
#'   what the published survival curves are drawn against.
#' @param ... Passed to [departure_hazard()].
#' @return Named numeric vector of survival probabilities.
#' @export
retirement_survival <- function(from_age = 50, to_ages = c(60, 65, 70, 75, 80),
                                sex = "male", ...) {
  max_age <- max(to_ages)
  ages <- seq(from_age, max_age - 1L)
  h <- departure_hazard(ages, sex = sex, ...)
  surv <- cumprod(1 - h)
  names(surv) <- as.character(ages + 1L)
  out <- surv[as.character(to_ages)]
  out[is.na(out)] <- 0
  stats::setNames(out, as.character(to_ages))
}

#' Crude annual departure rate implied by a cohort's age structure
#'
#' The overall departure rate is an OUTPUT of the age structure and the hazard
#' schedule, never an input. Reporting it makes the model comparable to published
#' crude retirement rates without letting a crude rate drive the age gradient.
#'
#' @param ages Numeric ages of the active cohort.
#' @param ... Passed to [departure_hazard()].
#' @return Numeric crude annual departure probability.
#' @export
implied_annual_departure_rate <- function(ages, ...) {
  if (length(ages) == 0) return(NA_real_)
  mean(departure_hazard(ages, ...), na.rm = TRUE)
}

#' Rescale a retirement schedule so a given cohort's crude rate hits a target
#'
#' Use only when an external crude rate must be reproduced exactly. Unlike a
#' blind shape-scaling, this solves for the multiplier against the ACTUAL age
#' distribution of the cohort, so the resulting crude rate is the target by
#' construction rather than by assumption.
#'
#' @param ages Ages of the cohort the target rate refers to.
#' @param target_rate Crude annual departure probability to reproduce.
#' @param schedule Retirement hazard schedule to rescale.
#' @param career_change_hazard Career-change hazard (held fixed).
#' @return A rescaled retirement schedule.
#' @export
calibrate_retirement_schedule <- function(ages, target_rate,
                                          schedule = RETIREMENT_HAZARD_BY_AGE,
                                          career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50) {
  assertthat::assert_that(length(ages) > 0, target_rate > 0, target_rate < 1)
  objective <- function(mult) {
    implied_annual_departure_rate(
      ages,
      retirement_schedule = pmin(schedule * mult, 1),
      career_change_hazard = career_change_hazard
    ) - target_rate
  }
  lo <- objective(1e-6)
  hi <- objective(50)
  if (is.na(lo) || is.na(hi) || lo > 0 || hi < 0) {
    .msg_warn(sprintf(
      "calibrate_retirement_schedule: target %.4f not attainable for this cohort; returning schedule unchanged",
      target_rate
    ))
    return(schedule)
  }
  mult <- stats::uniroot(objective, c(1e-6, 50))$root
  pmin(schedule * mult, 1)
}

# ---- Clinical hours worked ------------------------------------------------

# Reference weekly PATIENT-CARE hours, transcribed from HWSM Exhibit 14 (OLS on
# 95,555 physician licensure-survey responses from FL, SC, NY and MD; the
# published example is general internal medicine). Coefficients are relative to
# the reference group: male, age <35, practising in Florida.
#
# Two properties matter and are easy to get wrong:
#   (1) hours are FLAT (very slightly rising) until 59 and fall only from 60 --
#       not from 50;
#   (2) the model carries AGE-BY-SEX INTERACTIONS, so the female penalty is not
#       constant across the age range.
# R^2 was 0.09: demographics explain little of the variation in hours worked, so
# these are a defensible central tendency and nothing more.
HWSM_HOURS_INTERCEPT <- 46.29          # male, age <35, Florida

HWSM_HOURS_AGE_EFFECT <- c(
  "<35"   =  0.00,
  "35-44" =  0.52,
  "45-54" =  0.30,
  "55-59" =  0.17,
  "60-64" = -2.04,
  "65-69" = -5.65,
  "70-74" = -8.37,
  "75+"   = -14.91
)

HWSM_HOURS_FEMALE_MAIN <- -3.53

HWSM_HOURS_FEMALE_INTERACTION <- c(
  "<35"   =  0.00,
  "25-34" =  0.00,
  "35-44" = -2.20,
  "45-54" = -2.03,
  "55-59" =  0.00,
  "60-64" =  0.00,
  "65-69" =  0.00,
  "70-74" =  0.00,
  "75+"   =  0.00
)

HWSM_HOURS_AGE_BANDS <- c(0, 35, 45, 55, 60, 65, 70, 75, Inf)
HWSM_HOURS_AGE_LABELS <- c("<35", "35-44", "45-54", "55-59", "60-64",
                           "65-69", "70-74", "75+")

# The intercept is a general-internal-medicine level in Florida. URPS hours must
# come from a URPS practice survey before publication; this gate says so.
REFERENCE_HOURS_CALIBRATION_STATUS <- "uncalibrated_illustrative"

#' Reference weekly patient-care hours from the HWSM age x sex specification
#'
#' @param age Numeric age(s).
#' @param sex Character sex ("male"/"female").
#' @param intercept Reference-group weekly hours.
#' @return Numeric weekly patient-care hours.
#' @export
hwsm_reference_hours <- function(age, sex = "female", intercept = HWSM_HOURS_INTERCEPT) {
  pmax(intercept + .hwsm_hours_offset(age, sex), 0)
}

# Age/sex contribution to weekly hours, WITHOUT the intercept and WITHOUT the
# non-negativity floor. Kept separate because calibrate_hours_intercept() needs
# the untruncated offset: evaluating hwsm_reference_hours(intercept = 0) would
# return mostly zeros through pmax() and silently mis-solve the intercept.
.hwsm_hours_offset <- function(age, sex = "female") {
  age <- as.numeric(age)
  sex <- tolower(as.character(rep_len(sex, length(age))))
  band <- as.character(cut(age, breaks = HWSM_HOURS_AGE_BANDS,
                           labels = HWSM_HOURS_AGE_LABELS,
                           right = FALSE, include.lowest = TRUE))

  age_eff <- unname(HWSM_HOURS_AGE_EFFECT[band])
  age_eff[is.na(age_eff)] <- 0

  female <- sex == "female"
  fem_main <- ifelse(female, HWSM_HOURS_FEMALE_MAIN, 0)
  fem_int <- unname(HWSM_HOURS_FEMALE_INTERACTION[band])
  fem_int[is.na(fem_int)] <- 0
  fem_int <- ifelse(female, fem_int, 0)

  age_eff + fem_main + fem_int
}

#' Calibration status of the reference hours schedule
#' @return Character status string.
#' @export
reference_hours_status <- function() REFERENCE_HOURS_CALIBRATION_STATUS

#' Fit weekly clinical hours on age and sex
#'
#' The Dall-family models all estimate hours worked by ordinary least squares on
#' provider age and sex from their own practice survey. This wraps that fit with
#' a natural spline in age so the mid-50s inflection is captured without imposing
#' a step function.
#'
#' @param survey Data frame with `age`, `sex`, `clinical_hours`, and optionally
#'   `weight`, `pathway`, `practice_setting`.
#' @param df Spline degrees of freedom for age.
#' @param extra_terms Character vector of additional right-hand-side terms.
#' @return An `lm` object with class `urps_hours_model` prepended.
#' @export
fit_clinical_hours_model <- function(survey, df = 4L, extra_terms = character(0)) {
  assertthat::assert_that(is.data.frame(survey))
  required <- c("age", "sex", "clinical_hours")
  missing <- setdiff(required, names(survey))
  if (length(missing) > 0) {
    stop(sprintf("fit_clinical_hours_model: survey missing column(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  n_age <- length(unique(stats::na.omit(survey$age)))
  df <- max(1L, min(as.integer(df), n_age - 1L))
  rhs <- c(sprintf("splines::ns(age, df = %d)", df), "sex", extra_terms)
  form <- stats::as.formula(paste("clinical_hours ~", paste(rhs, collapse = " + ")))
  w <- if ("weight" %in% names(survey)) survey$weight else NULL
  fit <- stats::lm(form, data = survey, weights = w)
  class(fit) <- c("urps_hours_model", class(fit))
  fit
}

#' Predict weekly clinical hours for providers
#'
#' Falls back to the HWSM age x sex reference specification when no fitted model
#' is supplied.
#'
#' @param age Numeric age(s).
#' @param sex Character sex ("male"/"female"); recycled to length of `age`.
#' @param model Optional model from [fit_clinical_hours_model()].
#' @param intercept Reference-group hours for the fallback specification.
#' @return Numeric weekly clinical hours.
#' @export
predict_clinical_hours <- function(age, sex = "female", model = NULL,
                                   intercept = HWSM_HOURS_INTERCEPT) {
  age <- as.numeric(age)
  sex <- tolower(as.character(rep_len(sex, length(age))))
  sex[!sex %in% c("male", "female")] <- "female"

  if (!is.null(model)) {
    newdata <- data.frame(age = age, sex = sex, stringsAsFactors = FALSE)
    return(pmax(as.numeric(stats::predict(model, newdata = newdata)), 0))
  }

  hwsm_reference_hours(age, sex, intercept)
}

# ---- Categorical participation (FutureDocs / UNC alternative) --------------

# The UNC FutureDocs model assigns each physician-year one of three states
# rather than a continuous hours value: full time (FTE 1.0), part time (0.5), or
# no patient care (0), with probabilities varying by age and sex.
#
# The single-year-of-age probabilities below are the values digitised from
# FutureDocs Figure 9 by ASN Data Analytics and published under an MIT licence in
# github.com/ASNDataAnalytics/wf_supply_modeling (00_bg/fte_prob_tbl.csv, commit
# b6c93f3, 2025-02-28). They are a pixel-extraction of a published chart, so they
# carry digitisation error; the authors of the original model also caution that
# the FTE calculations for women over 50 rest on a small number of physicians.
#
# Two features are load-bearing and neither survives a coarse age banding:
#   * MALE expected FTE is nearly FLAT across the whole age range (0.61 at 30 to
#     0.58 at 80): for men the shift is full-time to part-time, not out of
#     patient care.
#   * FEMALE expected FTE PEAKS AT ~50 (0.675) and then collapses (0.13 by 80),
#     driven by P(no patient care) rising from 0.04 at 50 to 0.75 at 80.
# A model that applies one age-productivity curve to both sexes cannot represent
# this, and it matters increasingly as the URPS workforce feminises.
#
# Cross-check on the FTE definition: mean expected FTE ~0.6 on the FutureDocs
# 70 hr/wk basis is ~42 clinical hrs/wk, which agrees with the Dall patient-care
# hours estimates (42.3 in neurology). The two constructs reconcile.

FUTUREDOCS_PARTICIPATION_AGES <- 30:85

.futuredocs_p_full_male <- c(
  0.3620, 0.3717, 0.3761, 0.3804, 0.3848, 0.3880, 0.3880, 0.3978,
  0.4033, 0.4043, 0.4087, 0.4130, 0.4141, 0.4152, 0.4163, 0.4196,
  0.4185, 0.4185, 0.4207, 0.4196, 0.4185, 0.4152, 0.4163, 0.4130,
  0.4120, 0.4076, 0.4043, 0.4033, 0.3989, 0.3935, 0.3891, 0.3804,
  0.3739, 0.3685, 0.3685, 0.3565, 0.3522, 0.3446, 0.3337, 0.3337,
  0.3261, 0.3163, 0.3033, 0.3033, 0.2902, 0.2804, 0.2663, 0.2543,
  0.2435, 0.2435, 0.2315, 0.2315, 0.2076, 0.1891, 0.1783, 0.1685
)

.futuredocs_p_half_male <- c(
  0.5033, 0.4859, 0.4815, 0.4772, 0.4685, 0.4620, 0.4598, 0.4467,
  0.4402, 0.4391, 0.4337, 0.4283, 0.4261, 0.4228, 0.4217, 0.4185,
  0.4152, 0.4174, 0.4141, 0.4152, 0.4174, 0.4228, 0.4207, 0.4261,
  0.4272, 0.4337, 0.4380, 0.4402, 0.4467, 0.4543, 0.4609, 0.4707,
  0.4804, 0.4891, 0.4924, 0.5076, 0.5163, 0.5283, 0.5402, 0.5435,
  0.5576, 0.5739, 0.5880, 0.5935, 0.6130, 0.6239, 0.6413, 0.6598,
  0.6761, 0.6848, 0.7000, 0.7065, 0.7413, 0.7663, 0.7837, 0.8000
)

.futuredocs_p_full_female <- c(
  0.3499, 0.3642, 0.3722, 0.3770, 0.3786, 0.3834, 0.3866, 0.3913,
  0.3913, 0.3961, 0.3977, 0.3977, 0.3993, 0.3993, 0.3945, 0.3961,
  0.3977, 0.3929, 0.3945, 0.3913, 0.3866, 0.3834, 0.3802, 0.3738,
  0.3642, 0.3595, 0.3467, 0.3403, 0.3340, 0.3244, 0.3180, 0.3021,
  0.2989, 0.2846, 0.2782, 0.2639, 0.2511, 0.2352, 0.2161, 0.2081,
  0.1922, 0.1730, 0.1635, 0.1412, 0.1300, 0.1077, 0.0982, 0.0743,
  0.0551, 0.0344, 0.0201, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000
)

.futuredocs_p_half_female <- c(
  0.4334, 0.4350, 0.4525, 0.4732, 0.4796, 0.5019, 0.5099, 0.5178,
  0.5274, 0.5338, 0.5370, 0.5529, 0.5577, 0.5609, 0.5688, 0.5736,
  0.5736, 0.5768, 0.5752, 0.5768, 0.5768, 0.5784, 0.5736, 0.5688,
  0.5672, 0.5688, 0.5688, 0.5593, 0.5529, 0.5449, 0.5354, 0.5306,
  0.5147, 0.5099, 0.4971, 0.4828, 0.4716, 0.4589, 0.4509, 0.4318,
  0.4222, 0.4031, 0.4015, 0.3776, 0.3474, 0.3394, 0.3027, 0.2964,
  0.2677, 0.2581, 0.2263, 0.2033, 0.1714, 0.1045, 0.0567, 0.0000
)

# Built without the pipe: this runs at package-collate time, before imported
# infix operators are available.
FUTUREDOCS_PARTICIPATION <- local({
  df <- data.frame(
    sex = rep(c("male", "female"), each = length(FUTUREDOCS_PARTICIPATION_AGES)),
    age = rep(FUTUREDOCS_PARTICIPATION_AGES, 2L),
    p_full = c(.futuredocs_p_full_male, .futuredocs_p_full_female),
    p_part = c(.futuredocs_p_half_male, .futuredocs_p_half_female),
    stringsAsFactors = FALSE
  )
  df$p_none <- 1 - df$p_full - df$p_part
  tibble::as_tibble(df)
})

FUTUREDOCS_PARTICIPATION_SOURCE <- paste(
  "Digitised from Fraher & Knapton (2017) FutureDocs Figure 9 by ASN Data",
  "Analytics; github.com/ASNDataAnalytics/wf_supply_modeling, MIT licence,",
  "00_bg/fte_prob_tbl.csv @ b6c93f3 (2025-02-28)"
)

#' Expected clinical FTE under the FutureDocs categorical participation model
#'
#' Ages outside the tabulated 30-85 range are clamped to the nearest tabulated
#' age rather than silently returning NA.
#'
#' @param age Numeric age(s).
#' @param sex Character sex.
#' @param table Participation probability table.
#' @return Numeric expected FTE (1.0 * p_full + 0.5 * p_part).
#' @export
participation_fte <- function(age, sex = "female", table = FUTUREDOCS_PARTICIPATION) {
  age <- as.numeric(age)
  sex <- tolower(as.character(rep_len(sex, length(age))))
  sex[!sex %in% c("male", "female")] <- "female"

  lo <- min(table$age); hi <- max(table$age)
  key_age <- pmin(pmax(round(age), lo), hi)

  idx <- match(paste(sex, key_age), paste(table$sex, table$age))
  out <- table$p_full[idx] + 0.5 * table$p_part[idx]
  out[is.na(out)] <- 0.5
  out
}

#' Probability a provider delivers no patient care
#'
#' The FutureDocs finding that most distinguishes the sexes; reported separately
#' because it is the mechanism behind the divergence in expected FTE.
#'
#' @param age Numeric age(s).
#' @param sex Character sex.
#' @param table Participation probability table.
#' @return Numeric probability.
#' @export
participation_p_no_patient_care <- function(age, sex = "female",
                                            table = FUTUREDOCS_PARTICIPATION) {
  age <- as.numeric(age)
  sex <- tolower(as.character(rep_len(sex, length(age))))
  sex[!sex %in% c("male", "female")] <- "female"
  key_age <- pmin(pmax(round(age), min(table$age)), max(table$age))
  idx <- match(paste(sex, key_age), paste(table$sex, table$age))
  out <- table$p_none[idx]
  out[is.na(out)] <- 0
  out
}

#' Validate that categorical participation probabilities sum to 1
#' @param table Participation probability table.
#' @return (Invisibly) the table.
#' @export
validate_participation_table <- function(table = FUTUREDOCS_PARTICIPATION) {
  s <- table$p_full + table$p_part + table$p_none
  if (any(abs(s - 1) > 1e-6)) {
    stop("validate_participation_table: probabilities must sum to 1 in every row",
         call. = FALSE)
  }
  if (any(table$p_full < 0 | table$p_part < 0 | table$p_none < 0)) {
    stop("validate_participation_table: negative probability", call. = FALSE)
  }
  invisible(table)
}

#' Calibrate the hours intercept so the cohort mean equals the FTE threshold
#'
#' The FTE threshold and the hours schedule must be internally consistent. Every
#' Dall-family study defines 1.0 FTE as the SURVEY MEAN weekly hours for that
#' occupation -- physiatry "37.2 hours per week in patient care activities, the
#' average reported in the 2019 survey". Pairing a 37.2 hr threshold with the
#' HWSM general-internal-medicine intercept of 46.29 would make the average
#' provider 1.24 FTE and base-year FTE supply exceed headcount, which no
#' published table shows (Zarek: 237,500 headcount -> 233,890 FTE).
#'
#' This solves for the intercept that makes mean predicted hours over the
#' base-year cohort equal the FTE threshold, so base-year FTE tracks headcount
#' and all subsequent movement comes from the changing age and sex composition.
#'
#' @param age Ages of the base-year cohort.
#' @param sex Sexes of the base-year cohort.
#' @param fte_hours Weekly clinical hours defining 1.0 FTE.
#' @return Numeric intercept for [hwsm_reference_hours()].
#' @export
calibrate_hours_intercept <- function(age, sex = "female",
                                      fte_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK) {
  offset <- mean(.hwsm_hours_offset(age, sex), na.rm = TRUE)
  intercept <- fte_hours - offset
  .msg_debug(sprintf("Calibrated hours intercept to %.2f (cohort mean hours -> %.1f)",
                     intercept, fte_hours))
  intercept
}

#' Convert weekly clinical hours to clinical FTE
#'
#' FTE is an hours threshold (Dall-family convention), so a provider working
#' fewer hours than the threshold contributes less than one FTE and the base-year
#' FTE supply is legitimately smaller than base-year headcount.
#'
#' @param age Numeric age(s).
#' @param sex Character sex.
#' @param model Optional fitted hours model.
#' @param fte_hours Weekly clinical hours defining 1.0 FTE.
#' @param ... Passed to [predict_clinical_hours()].
#' @return Numeric clinical FTE per provider.
#' @export
predict_clinical_fte <- function(age, sex = "female", model = NULL,
                                 fte_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK, ...) {
  assertthat::assert_that(is.numeric(fte_hours), fte_hours > 0)
  predict_clinical_hours(age, sex, model, ...) / fte_hours
}

# ---- Provider roster contract ---------------------------------------------

# Fields a PRODUCTION provider roster must carry. Synthetic agents are permitted
# only for examples and tests; `require_roster = TRUE` in the orchestrator makes
# that explicit rather than letting an illustrative age draw reach a published
# number.
PROVIDER_ROSTER_REQUIRED <- c(
  "provider_id", "pathway", "age", "sex", "state",
  "certification_year", "last_confirmed_active_year"
)

PROVIDER_ROSTER_OPTIONAL <- c(
  "birth_year", "lat", "lon", "first_observed_year", "retirement_evidence",
  "clinical_fte", "practice_setting", "license_state"
)

#' Validate a production provider roster (fail-closed)
#'
#' @param roster Data frame of provider records.
#' @param require_coords Require `lat`/`lon` (needed for the access layer).
#' @return (Invisibly) the roster, on success.
#' @export
validate_provider_roster <- function(roster, require_coords = FALSE) {
  assertthat::assert_that(is.data.frame(roster))
  need <- PROVIDER_ROSTER_REQUIRED
  if (require_coords) need <- c(need, "lat", "lon")
  missing <- setdiff(need, names(roster))
  if (length(missing) > 0) {
    stop(sprintf("validate_provider_roster: missing required field(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  if (nrow(roster) == 0) {
    stop("validate_provider_roster: roster has zero rows", call. = FALSE)
  }
  if (anyNA(roster$provider_id)) {
    stop("validate_provider_roster: provider_id contains NA", call. = FALSE)
  }
  bad_age <- !is.na(roster$age) & (roster$age < 18 | roster$age > 100)
  if (any(bad_age)) {
    stop(sprintf("validate_provider_roster: %d record(s) with implausible age",
                 sum(bad_age)), call. = FALSE)
  }
  invisible(roster)
}

#' Collapse a multi-licence roster to unique providers
#'
#' Zarek 2025 found 344,920 physical-therapy licences held by 246,440 resident
#' licensees: counting licences rather than people overstates the workforce by
#' 29%. Any board- or licensure-derived URPS roster needs the same collapse, and
#' the inflation factor belongs in the audit trail.
#'
#' @param roster Data frame with a person-level key.
#' @param person_key Column identifying the individual (e.g. NPI).
#' @param keep Strategy for choosing the surviving row per person: "first" or a
#'   column name to order by descending (e.g. "last_confirmed_active_year").
#' @return List with `roster` (de-duplicated) and `audit` (counts + inflation).
#' @export
deduplicate_provider_roster <- function(roster, person_key = "provider_id",
                                        keep = "last_confirmed_active_year") {
  assertthat::assert_that(is.data.frame(roster), person_key %in% names(roster))
  n_records <- nrow(roster)

  ordered <- roster
  if (!identical(keep, "first") && keep %in% names(roster)) {
    ordered <- roster[order(roster[[person_key]], -xtfrm(roster[[keep]])), , drop = FALSE]
  }
  deduped <- ordered[!duplicated(ordered[[person_key]]), , drop = FALSE]

  audit <- list(
    n_records = n_records,
    n_unique_providers = nrow(deduped),
    n_dropped = n_records - nrow(deduped),
    inflation_factor = if (nrow(deduped) > 0) n_records / nrow(deduped) else NA_real_
  )
  if (audit$n_dropped > 0) {
    .msg_info(sprintf(
      "Roster de-duplication: %d records -> %d providers (inflation %.2fx)",
      audit$n_records, audit$n_unique_providers, audit$inflation_factor
    ))
  }
  list(roster = deduped, audit = audit)
}

#' Build the microsimulation agent table from a production roster
#'
#' @param roster Validated provider roster.
#' @param baseline_year Calendar year of the roster snapshot.
#' @param hours_model Optional fitted hours model.
#' @param drop_terminal Drop providers at or beyond `MICROSIM_TERMINAL_AGE`
#'   (Zarek excludes licensees aged 75+ from the starting supply).
#' @return Agent tibble compatible with [simulate_provider_career_once()].
#' @export
agents_from_roster <- function(roster, baseline_year,
                               hours_model = NULL,
                               drop_terminal = TRUE) {
  validate_provider_roster(roster)
  if (isTRUE(drop_terminal)) {
    n_before <- nrow(roster)
    roster <- roster[!is.na(roster$age) & roster$age < MICROSIM_TERMINAL_AGE, , drop = FALSE]
    if (nrow(roster) < n_before) {
      .msg_info(sprintf("Excluded %d provider(s) aged %d+ from starting supply",
                        n_before - nrow(roster), MICROSIM_TERMINAL_AGE))
    }
  }

  entry_age <- if ("certification_year" %in% names(roster)) {
    pmax(roster$age - (baseline_year - roster$certification_year), MICROSIM_AGE_AT_CERT)
  } else {
    rep(MICROSIM_ENTRY_AGE, nrow(roster))
  }

  tibble::tibble(
    provider_id = as.character(roster$provider_id),
    subspecialty = if ("pathway" %in% names(roster)) as.character(roster$pathway) else "FPMRS",
    sex = if ("sex" %in% names(roster)) tolower(as.character(roster$sex)) else "female",
    state = if ("state" %in% names(roster)) as.character(roster$state) else NA_character_,
    age = as.numeric(roster$age),
    entry_year = baseline_year - pmax(as.numeric(roster$age) - entry_age, 0),
    retirement_year = NA_real_,
    origin_cohort = "roster",
    clinical_fte = predict_clinical_fte(roster$age,
                                        if ("sex" %in% names(roster)) roster$sex else "female",
                                        hours_model)
  )
}

# ---- Base-year supply from the versioned SSOT -----------------------------

#' Base-year URPS supply from the mufflyaccess contract
#'
#' No workforce count may be hard-coded. CONUS is the correct denominator for the
#' geographic access layer (the travel model covers the contiguous states);
#' national is the correct figure for national supply reporting. The two are
#' returned together and labelled so they cannot be silently interchanged.
#'
#' @param year Snapshot year.
#' @param include_urology Include the ABU pathway alongside ABOG.
#' @param measure Measure name passed through to `mufflyaccess::urps_count()`.
#' @return List with `national`, `conus`, and contract provenance.
#' @export
urps_baseline_supply <- function(year = 2023L, include_urology = TRUE,
                                 measure = "board_certified_active") {
  if (!requireNamespace("mufflyaccess", quietly = TRUE)) {
    stop("urps_baseline_supply(): package 'mufflyaccess' is required; ",
         "base-year workforce counts must come from the versioned contract, ",
         "not from a hard-coded literal.", call. = FALSE)
  }
  fetch <- function(geo) {
    mufflyaccess::urps_count(year = year, measure = measure, geography = geo,
                             include_urology = include_urology, details = TRUE)
  }
  nat <- fetch("national")
  conus <- fetch("conus")

  .msg_info(sprintf("URPS baseline supply %d: national %d, CONUS %d (contract v%s)",
                    year, nat$count, conus$count, nat$contract_version))

  list(
    national = nat$count,
    conus = conus$count,
    year = year,
    measure = measure,
    include_urology = include_urology,
    board_pathway = nat$board_pathway,
    contract_version = nat$contract_version,
    artifact_version = nat$artifact_version,
    canonical_release = nat$canonical_release,
    snapshot_date = as.character(nat$snapshot_date),
    source_git_commit = nat$source_git_commit
  )
}
