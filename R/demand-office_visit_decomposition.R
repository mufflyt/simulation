################################################################################
# R/demand-office_visit_decomposition.R
# Decompose predicted ambulatory visits, and assert the incident/prevalent
# distinction the pathway currently collapses.
#
# THE DEFECT THIS EXISTS TO CATCH. condition_service_pathway() assigns
# new_consultation = 1.0 and return_visit = 1.5 at the CONSERVATIVE stage, to
# every treated patient, for UI, POP and AI alike. With 6.18M treated patients
# that is 15.44M visits -- 95.2% of the model's entire 16.2M ambulatory
# prediction -- generated before any clinically interesting transition occurs.
# The downstream cascade barely participates in the office-volume mismatch.
#
# Worse than the magnitude is the semantics. `treated` is a PREVALENT annual
# cohort, yet every member receives one `new_consultation` every year. A
# prevalent patient cannot be a new patient annually. The correct structure
# splits the care-engaged population:
#
#   care_engaged
#     |- newly_entering_care   -> new_consultation, first-year follow-up
#     |- continuing_care       -> continuing follow-up intensity
#
# Against the NAMCS anchor the model's floor is 2.50 visits per treated woman
# where the observation implies 0.78. Note that "offices host only ~31% of
# pelvic-floor visits" is NOT independent evidence about setting coverage --
# 4,814,760 / 15,440,770 = 31.2% is the same 2.5 assumption restated.
################################################################################

#' Decompose predicted ambulatory visits by cascade component
#'
#' @param treated Named numeric vector of treated patients per condition.
#' @param pathway A table from [condition_service_pathway()].
#' @return Tibble of `component`, `volume`, `share`.
#' @export
office_visit_decomposition <- function(treated,
                                       pathway = condition_service_pathway()) {
  ent <- pathway_stage_entrants(treated, pathway)
  v <- dplyr::inner_join(pathway, ent, by = c("condition", "stage"))
  v <- dplyr::filter(v, .data$service %in% c("new_consultation", "return_visit"))
  v <- dplyr::mutate(v, volume = .data$entering * .data$per_entering)
  v <- dplyr::mutate(v, component = dplyr::case_when(
    .data$stage == "conservative" & .data$service == "new_consultation" ~ "new_consultation",
    .data$stage == "conservative" & .data$service == "return_visit"     ~ "conservative_return",
    .data$stage == "testing"                                            ~ "testing",
    .data$stage == "procedure"                                          ~ "procedure_related",
    .data$stage == "followup"                                           ~ "followup",
    .data$stage == "recurrence"                                         ~ "recurrence",
    TRUE ~ .data$stage))
  out <- dplyr::summarise(dplyr::group_by(v, .data$component),
                          volume = base::sum(.data$volume), .groups = "drop")
  out$share <- out$volume / base::sum(out$volume)
  dplyr::arrange(out, dplyr::desc(.data$volume))
}

#' Assert that new consultations cannot exceed the treated cohort
#'
#' The semantic check the pathway currently fails: a PREVALENT treated patient
#' cannot also be a NEW patient every year, so new_consultation volume must be
#' strictly below the treated total. Under the shipped table it equals it
#' exactly, which is the signature of the incident/prevalent collapse.
#'
#' @param treated Named numeric vector of treated patients per condition.
#' @param pathway A table from [condition_service_pathway()].
#' @param strict If TRUE, stop rather than warn.
#' @return Invisibly, TRUE if the assertion holds.
#' @export
assert_incident_not_prevalent <- function(treated,
                                          pathway = condition_service_pathway(),
                                          strict = FALSE) {
  d <- office_visit_decomposition(treated, pathway)
  new_consults <- base::sum(d$volume[d$component == "new_consultation"])
  treated_total <- base::sum(treated)

  # AN EMPTY COHORT IS OUTSIDE THE INVARIANT'S DOMAIN, not a violation of it.
  # The claim is "a prevalent patient must not be recounted as incident"; with
  # no patients there is nothing to recount. Zero treated yielding zero new
  # consultations is degenerate but correct, and the naive test fails it twice
  # over -- `0 < 0` is FALSE, and the ratio in the message is 0/0 = NaN, so it
  # refused with "ratio NaN" and no interpretable diagnostic.
  #
  # Found by the property-based worlds in adversarial/metamorphic.R, which
  # generates empty cohorts precisely because they are the boundary nobody
  # writes a test for. This is a DOMAIN condition, not a repair: no value is
  # coerced, clipped or imputed, and a non-empty cohort is judged exactly as
  # before.
  if (!base::is.finite(treated_total) || treated_total <= 0) {
    return(base::invisible(TRUE))
  }
  ok <- new_consults < treated_total

  msg <- base::sprintf(
    paste0("new_consultation volume is %s against a treated cohort of %s ",
           "(ratio %.2f). Every prevalent treated patient is being counted as ",
           "a NEW patient annually. Split care_engaged into newly_entering_care ",
           "and continuing_care before calibrating visit intensity."),
    base::format(base::round(new_consults), big.mark = ","),
    base::format(base::round(treated_total), big.mark = ","),
    new_consults / treated_total)

  if (!ok) {
    if (base::isTRUE(strict)) base::stop(msg, call. = FALSE) else base::warning(msg, call. = FALSE)
  }
  base::invisible(ok)
}


#' Look up CMS PSPS 2024 CPT Setting Mix (Facility vs Office)
#'
#' @param category Clinical category filter: `"all"`, `"slings"`, `"prolapse"`, `"urodynamics"`, or `"cystoscopy"`.
#'
#' @return Tibble of setting mix ratios per CPT.
#' @family demand
#' @concept claims
#' @export
cpt_setting_mix <- function(category = base::c("all", "slings", "prolapse", "urodynamics", "cystoscopy")) {
  category <- base::match.arg(category)

  tbl <- tibble::tribble(
    ~cpt, ~category, ~p_facility, ~p_office, ~total_services, ~calibration_status, ~source,
    "57288", "slings", 0.9807, 0.0193, 15420L, "calibrated", "CMS_PSPS_2024",
    "51840", "slings", 0.9750, 0.0250, 850L, "calibrated", "CMS_PSPS_2024",
    "57280", "prolapse", 0.9850, 0.0150, 4200L, "calibrated", "CMS_PSPS_2024",
    "57425", "prolapse", 0.9884, 0.0116, 12500L, "calibrated", "CMS_PSPS_2024",
    "57240", "prolapse", 0.9810, 0.0190, 15600L, "calibrated", "CMS_PSPS_2024",
    "57250", "prolapse", 0.9820, 0.0180, 8400L, "calibrated", "CMS_PSPS_2024",
    "57260", "prolapse", 0.9840, 0.0160, 9500L, "calibrated", "CMS_PSPS_2024",
    "57265", "prolapse", 0.9860, 0.0140, 2416L, "calibrated", "CMS_PSPS_2024",
    "64590", "neuromodulation", 0.9500, 0.0500, 5600L, "calibrated", "CMS_PSPS_2024",
    "64561", "neuromodulation", 0.8200, 0.1800, 7800L, "calibrated", "CMS_PSPS_2024",
    "51715", "neuromodulation", 0.7500, 0.2500, 4100L, "calibrated", "CMS_PSPS_2024",
    "51726", "urodynamics", 0.1200, 0.8800, 19200L, "calibrated", "CMS_PSPS_2024",
    "51729", "urodynamics", 0.1015, 0.8985, 24500L, "calibrated", "CMS_PSPS_2024",
    "52000", "cystoscopy", 0.3500, 0.6500, 45000L, "calibrated", "CMS_PSPS_2024",
    "52287", "cystoscopy", 0.5091, 0.4909, 18200L, "calibrated", "CMS_PSPS_2024",
    "52332", "cystoscopy", 0.8800, 0.1200, 22000L, "calibrated", "CMS_PSPS_2024",
    "52204", "cystoscopy", 0.4000, 0.6000, 8900L, "calibrated", "CMS_PSPS_2024",
    "52005", "cystoscopy", 0.8500, 0.1500, 4100L, "calibrated", "CMS_PSPS_2024",
    "52224", "cystoscopy", 0.4500, 0.5500, 6200L, "calibrated", "CMS_PSPS_2024",
    "52260", "cystoscopy", 0.9000, 0.1000, 3100L, "calibrated", "CMS_PSPS_2024"
  )

  if (category != "all") {
    tbl <- dplyr::filter(tbl, .data$category == !!category)
  }

  tbl
}

#' Summary Facility/Office Ratios for Prolapse Procedures
#'
#' @return List of empirical prolapse setting ratios.
#' @family demand
#' @concept claims
#' @export
prolapse_setting_mix <- function() {
  base::list(
    p_facility = 0.9841,
    p_office = 0.0159,
    total_services = 52616L,
    cpt_codes = c("57280", "57425", "57240", "57250", "57260", "57265")
  )
}
