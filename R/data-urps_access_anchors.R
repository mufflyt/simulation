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
# UNITS ARE THE TRAP. Both audits report BUSINESS days. `clear_access()` works
# in the same unit as `appointment_window`, whose default of 30 is CALENDAR
# days. A 23.1 business-day wait entered unconverted understates the true wait
# by about nine days, and nothing in the pipeline would have caught it. The
# conversion is done once, here, by a named function.

#' Business-to-calendar-day conversion for reported wait times
#'
#' Mystery-caller studies report elapsed BUSINESS days; the access-clearing
#' layer works in calendar days. Five business days span seven calendar days,
#' so the factor is 7/5. Public holidays would lengthen the calendar span
#' slightly further, so this conversion is mildly conservative.
#'
#' @param business_days Numeric vector of business days.
#' @return Calendar days.
#' @family access
#' @concept validation
#' @export
#' @examples
#' business_days_to_calendar(23.1)
business_days_to_calendar <- function(business_days) {
  stopifnot(is.numeric(business_days))
  business_days * 7 / 5
}

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

#' The observed national URPS wait, in calendar days
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
#' @return A list with `calendar_days`, `business_days`, `status`, `citation`.
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
    calendar_days = business_days_to_calendar(bd),
    business_days = bd,
    status = if (all(d$status == "calibrated")) "calibrated" else "preliminary",
    n_rows = nrow(d),
    citation = paste(unique(d$citation), collapse = " ")
  )
}

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
  set_access_target(access_validation_targets(), "wait_time",
                    observed = w$calendar_days, status = w$status)
}
