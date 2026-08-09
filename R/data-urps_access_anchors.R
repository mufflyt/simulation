# Observed URPS Access Anchors ------------------------------------------------
#
# WHAT THIS FILE IS FOR. `access_validation_targets()` ships every target
# unpopulated on purpose: the machinery refuses to invent an observation. This
# file supplies the observations, from URPS mystery-caller audits, with the
# citation attached to each number.
#
# WHY IT IS URPS DATA AND NOT A BORROWED SPECIALTY. An access model fitted on
# another specialty's audit answers a different question. In this project's own
# data the two disagree on the headline: an ENT-derived hurdle model puts the
# Medicaid disadvantage at the appointment-obtained stage, while the URPS audit
# below finds appointment obtainment essentially equal by insurance (32.9% vs
# 31.5%) and locates the disadvantage in outright refusal and in a
# scenario-specific wait. Calibrating URPS access to a non-URPS audit would have
# produced a confident answer in the wrong direction.
#
# THE MODEL IS BUSINESS-DAY NATIVE, and that is a deliberate choice rather than
# a convenience. Both URPS audits report elapsed BUSINESS days, because that is
# what a scheduler quotes. Converting a published mean into calendar days is not
# possible without the individual call dates: weekends and federal holidays fall
# where they fall, so 23.1 business days maps to a different calendar span
# depending on when each wait started. A flat 7/5 factor was used here
# originally and was wrong for exactly that reason, silently biasing the anchor.
#
# mysterycall ships the correct machinery (mysterycall_us_federal_calendar(),
# mysterycall_count_business_days()), but it converts DATES, and the Rabice
# individual-level call and appointment dates are not in this project: the
# submission archive carries summary tables only. So the anchor stays in the
# unit it was measured in, and the access layer adopts that unit.
#
# CONSEQUENCE FOR CALLERS. `clear_access()` is unit-agnostic: `wait_scale`,
# `wait_time` and `appointment_window` all carry whatever unit the observed wait
# was fitted in. Fit against this anchor and every one of them is in business
# days. Passing a calendar-day `appointment_window` alongside it silently
# compares two different clocks.

#' Observed URPS appointment wait times from mystery-caller audits
#'
#' One row per reported wait. `status` separates a published peer-reviewed
#' observation from a preliminary one; only `calibrated` rows are eligible to
#' anchor the model (see [urps_observed_wait_days()]).
#'
#' @format Tibble with `study`, `data_year`, `scenario`, `insurance`,
#'   `wait_business_days`, `n_offices`, `status`, `citation`.
#' @family access
#' @concept validation
#' @export
URPS_WAIT_OBSERVATIONS <- tibble::tribble(
  ~study,     ~data_year, ~scenario,       ~insurance,  ~wait_business_days, ~n_offices, ~status,       ~citation,
  "Rabice",   2019L,      "prolapse",      "Medicare",  23.1,                226L,       "calibrated",  "Rabice SR, Schultz C, Muffly TM. Appointment wait times in female pelvic medicine and reconstructive surgery: a mystery caller study. Female Pelvic Med Reconstr Surg. 2021;27:681-685.",
  "Acosta",   2026L,      "prolapse",      "BCBS",      35,                  150L,       "preliminary", "Acosta L, et al. Diagnosis-based disparities in urogynecology appointment wait times. Draft 2026-07-06; COMIRB approved.",
  "Acosta",   2026L,      "prolapse",      "Medicaid",  46,                  150L,       "preliminary", "Acosta L, et al. Draft 2026-07-06.",
  "Acosta",   2026L,      "incontinence",  "BCBS",      51,                  150L,       "preliminary", "Acosta L, et al. Draft 2026-07-06.",
  "Acosta",   2026L,      "incontinence",  "Medicaid",  41,                  150L,       "preliminary", "Acosta L, et al. Draft 2026-07-06."
)

#' The observed national URPS wait, in business days
#'
#' Returns the single national wait used to fit `wait_scale`. Defaults to the
#' Rabice 2019 audit: it is peer reviewed, national (427 offices called across
#' 46 states and DC), and reports one mean rather than a set of
#' scenario-conditional predictions.
#'
#' @details
#' WHY NOT THE 2026 AUDIT, WHICH IS CLOSER TO THE MODEL BASE YEAR. The 2026
#' study is preliminary and reports predicted waits by scenario and insurance
#' rather than a single national mean, so a pooled figure cannot be recovered
#' from it without the underlying data. It is registered here for drift
#' assessment, not as the anchor. Pass `study = "Acosta"` to see what a
#' later-vintage, scenario-specific value would do; the result is explicitly
#' preliminary and must not be published as calibrated.
#'
#' The two vintages are not directly comparable: 2019 called with Medicare, 2026
#' with commercial and Medicaid. The prolapse scenario is common to both.
#'
#' @param study Which audit to draw from. Default "Rabice".
#' @param scenario Clinical scenario. Default "prolapse".
#' @param insurance Optional insurance filter; `NULL` averages available rows.
#' @param observations Observation table; defaults to [URPS_WAIT_OBSERVATIONS].
#' @return A list with `business_days` (the model-scale value), `unit`,
#'   `status`, `n_rows` and `citation`.
#' @family access
#' @concept validation
#' @export
#' @examples
#' urps_observed_wait_days()
urps_observed_wait_days <- function(study = "Rabice",
                                    scenario = "prolapse",
                                    insurance = NULL,
                                    observations = URPS_WAIT_OBSERVATIONS) {
  d <- observations[observations$study == study & observations$scenario == scenario, ,
                    drop = FALSE]
  if (!is.null(insurance)) d <- d[d$insurance == insurance, , drop = FALSE]
  if (nrow(d) == 0L) {
    stop(sprintf("urps_observed_wait_days(): no observation for study '%s', scenario '%s'%s.",
                 study, scenario,
                 if (is.null(insurance)) "" else sprintf(", insurance '%s'", insurance)),
         call. = FALSE)
  }
  bd <- mean(d$wait_business_days)
  list(
    business_days = bd,
    unit = URPS_ACCESS_TIME_UNIT,
    status = if (all(d$status == "calibrated")) "calibrated" else "preliminary",
    n_rows = nrow(d),
    citation = paste(unique(d$citation), collapse = " ")
  )
}

#' Time unit the URPS access layer works in
#'
#' Every wait quantity fitted from [URPS_WAIT_OBSERVATIONS] is in business days,
#' including `wait_scale`, the resulting `wait_time`, and any
#' `appointment_window` compared against it.
#' @family access
#' @concept validation
#' @export
URPS_ACCESS_TIME_UNIT <- "business_days"

#' Appointment window for URPS access outcomes, in business days
#'
#' @details
#' A DECLARED BENCHMARK, NOT A CONVERSION. The conventional access measure is an
#' appointment within one month, and `clear_access()` defaults to 30 in
#' calendar-day usage. Rescaling 30 calendar days into business days would
#' reintroduce the flat factor this file exists to remove, so the window is
#' stated directly instead: four working weeks. It is close to the conventional
#' benchmark and is defensible on its own terms rather than derived from one.
#'
#' @return Numeric, business days.
#' @family access
#' @concept validation
#' @export
urps_appointment_window <- function() 20

#' Share of URPS capacity that will accept a given insurance
#'
#' `clear_access()` takes `insurance_fraction`, the share of capacity that will
#' see the patient at all. The 2026 URPS audit measured this directly: among
#' Medicaid calls reaching a definite response, 23% were refused outright
#' because the office does not accept Medicaid, a barrier commercially insured
#' callers did not encounter.
#'
#' @details
#' THIS IS A REFUSAL, NOT A WAIT, and the distinction is the point. Pooled
#' across scenarios the 2026 audit found no insurance effect on wait time
#' (IRR 1.05, 95% CI 0.90 to 1.22) and near-identical appointment obtainment
#' (32.9% commercial vs 31.5% Medicaid). Modelling the Medicaid disadvantage as
#' a longer queue would therefore miss it. It is a door that does not open.
#'
#' @param insurance "commercial" or "medicaid".
#' @return Share of capacity accepting that insurance, in 0 to 1.
#' @family access
#' @concept validation
#' @keywords internal
#' @noRd
urps_insurance_fraction <- function(insurance = c("commercial", "medicaid")) {
  insurance <- match.arg(insurance)
  switch(insurance,
    commercial = 1,
    medicaid   = 1 - URPS_MEDICAID_OUTRIGHT_REFUSAL_SHARE)
}

# Among 2026 audit Medicaid calls reaching a definite response, the share
# refused outright because the office does not accept Medicaid. Preliminary.
URPS_MEDICAID_OUTRIGHT_REFUSAL_SHARE <- 0.23

#' Access validation targets with the URPS wait anchor populated
#'
#' [access_validation_targets()] with `wait_time` filled from
#' [urps_observed_wait_days()] and stamped with that observation's status.
#' `panel_size` is deliberately left unpopulated: no URPS panel-size
#' observation exists in this project, and a benchmark borrowed from another
#' specialty is the error this file was written to avoid.
#'
#' @param ... Passed to [urps_observed_wait_days()].
#' @return A targets tibble for [validate_access_outcomes()].
#' @family access
#' @concept validation
#' @keywords internal
#' @noRd
urps_access_targets <- function(...) {
  w <- urps_observed_wait_days(...)
  t <- set_access_target(access_validation_targets(), "wait_time",
                         observed = w$business_days, status = w$status)
  # Record the unit ON the target. A bare number cannot say which clock it is
  # on, and the whole defect this file corrects was a unit mismatch.
  t$unit[t$target == "wait_time"] <- w$unit
  t
}
