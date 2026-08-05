# The URPS Practice Survey: One Instrument, Two Open Items ----
#
# Two calibration inputs in this model are borrowed from other specialties, and
# neither can be resolved from any data the repository has or could derive. They
# are registered here TOGETHER because a single fielded instrument answers both,
# and fielding two surveys to answer one questionnaire's worth of questions is
# how neither gets done.
#
#   1. BASE-YEAR CAPACITY ANCHOR   `capacity_status()`
#      Currently `capacity_survey_adequacy(example_capacity_survey())`, whose
#      evidence is a PHYSICAL-THERAPY capacity distribution (Zarek 2025) used as
#      a stand-in. It sets the LEVEL of required FTE and is the only remaining
#      input that can change the SIGN of the projected gap: adequacy of 0.708
#      instead of 0.948 erases the 2050 surplus entirely.
#
#   2. HOURS-TO-FTE CURVE          `fte_curve_status()`
#      Currently `hwsm_reference_hours(age, sex)`, the HWSM/Dall curve fitted on
#      a different physician population, and reported by
#      `reference_hours_status()` as "uncalibrated_illustrative".
#
# WHY THE SECOND ONE IS NOT ABSORBED, WHEN SO MUCH ELSE IS. Delegation shares,
# demand levels and the NAMCS calibration scalar all cancel out of required FTE,
# because `calibrate_wrvu_per_fte()` solves productivity against the base-year
# anchor. `calibrate_hours_intercept()` does the same to the hours curve's
# LEVEL -- mean clinical FTE is 1.000 at base year by construction. But it
# cannot absorb the curve's SHAPE. As the cohort ages, FTE per head falls
# 1.0000 -> 0.9597 -> 0.9231 across 2025/2035/2050, which removes 176 FTE from
# the 2050 supply. That is larger than the entire demand-growth lever across the
# full Census low-to-high range, and it rests on another specialty's gradient.
#
# DO NOT "FIX" R/40-hrsa_fte_calibration.R. It carries TODO-FTE-001 and the
# `derived_by_analogy` tier, so it looks like the thing to replace. It is
# dormant: `apply_hrsa_surgical_fte()` is called by nothing but its own tests.
# Replacing it would change no output. The live target is
# `hwsm_reference_hours()` and the `reference_hours_status()` gate.

#' Items a fielded URPS practice survey must collect
#'
#' The single instrument. `resolves` records which open calibration item each
#' variable serves, so the questionnaire can be read as one document while each
#' status function reports only what bears on it.
#'
#' @param resolves Optional filter: "capacity_anchor", "fte_curve", or NULL for
#'   the whole instrument.
#' @return Tibble with `variable`, `resolves`, `why_needed`.
#' @export
urps_practice_survey_requirements <- function(resolves = NULL) {
  items <- tibble::tribble(
    ~variable,                  ~resolves,          ~why_needed,
    # ---- capacity anchor ----
    "clinical_fte",             "both",             "Denominator for every per-provider quantity, and the only check on the model's assertion that 1.0 FTE is 37.2 clinical hours/week.",
    "annual_visits",            "capacity_anchor",  "Separates delivered volume from capacity. The gap between them IS the adequacy the anchor asserts, and claims can only ever show the delivered side.",
    "annual_procedures",        "capacity_anchor",  "Office and diagnostic volume, which claims undercount because incident-to work bills under the supervising physician.",
    "new_patient_capacity",     "capacity_anchor",  "Distinguishes a full panel from an accessible one; a workforce can be entirely busy and still inaccessible.",
    "panel_size",               "capacity_anchor",  "Converts prevalence-based demand into providers without routing through work RVUs, giving an independent check on the wRVU path.",
    "wait_time",               "capacity_anchor",  "The only direct observable of unmet demand, and the natural external validation target for the whole model.",
    "payer_mix_constraints",    "capacity_anchor",  "Capacity is not fungible across payers; Medicaid and uninsured access can bind where total capacity does not.",
    # ---- hours-to-FTE curve ----
    "weekly_clinical_hours",    "fte_curve",        "The quantity the curve predicts. Currently taken from HWSM, fitted on a different physician population.",
    "age",                      "fte_curve",        "The gradient covariate. The curve's SHAPE survives intercept calibration and removes 176 FTE from 2050 supply, so an age effect borrowed from another specialty is load-bearing.",
    "sex",                      "fte_curve",        "Every Dall-family model finds a sex gap in hours that VARIES with age; a single main effect cannot represent it.",
    "or_hours_per_week",        "fte_curve",        "Operating time must be separable from clinic time. In a surgical subspecialty the age decline may be driven by giving up operating rather than by seeing fewer patients, and the current curve cannot distinguish the two.",
    "clinic_sessions_per_week", "fte_curve",        "The other half of that separation.",
    "call_burden",              "fte_curve",        "Call is displaced clinical capacity and is not visible in any claims-derived measure.",
    "practice_setting",         "fte_curve",        "Academic, employed and private practice differ in both hours and their age trajectory; the model currently applies one curve to all.",
    # ---- both ----
    "operative_volume",         "both",             "Theatre throughput is the binding constraint on surgical capacity AND the most likely driver of the age gradient, so it serves both items.",
    "practice_constraints",     "both",             "Call, academic and administrative obligations that reduce deliverable clinical FTE below contracted FTE."
  )
  if (is.null(resolves)) return(items)
  resolves <- match.arg(resolves, c("capacity_anchor", "fte_curve"))
  items[items$resolves %in% c(resolves, "both"), , drop = FALSE]
}

#' Status of the hours-to-FTE curve
#'
#' @return List with `resolved`, the current source, why it is unresolved, its
#'   leverage, and the variables that would resolve it.
#' @export
fte_curve_status <- function() {
  list(
    resolved = FALSE,
    current_source = paste(
      "hwsm_reference_hours(age, sex) -- the HWSM/Dall reference curve, fitted",
      "on a different physician population. reference_hours_status() reports it",
      "as 'uncalibrated_illustrative'."),
    why_unresolved = paste(
      "No URPS-specific hours data exists in or derivable from this repository.",
      "Medicare claims show delivered services, not hours, and cannot separate a",
      "provider working fewer hours from one billing less per hour."),
    leverage = paste(
      "The curve's LEVEL is absorbed by calibrate_hours_intercept() -- mean",
      "clinical FTE is 1.000 at base year by construction. Its SHAPE is not:",
      "FTE per head falls to 0.9231 by 2050, removing 176 FTE from supply. This",
      "does NOT cancel, unlike delegation and demand calibration."),
    do_not_fix = paste(
      "R/40-hrsa_fte_calibration.R carries TODO-FTE-001 and looks like the",
      "target. It is dormant -- apply_hrsa_surgical_fte() is called by nothing",
      "but its own tests -- so replacing it would change no output."),
    resolved_by = urps_practice_survey_requirements("fte_curve")$variable
  )
}

#' Every calibration item still resolved by analogy rather than measurement
#'
#' The one place to ask "what is still borrowed, and does it matter?". Ordered
#' by leverage: an item that cancels out of the reported estimand is a
#' provenance problem, and an item that does not is a results problem.
#'
#' @return Tibble with `item`, `resolved`, `cancels_out`, `leverage`.
#' @export
unresolved_calibration_items <- function() {
  tibble::tribble(
    ~item,               ~resolved, ~cancels_out, ~leverage,
    "capacity_anchor",   FALSE,     FALSE,        "Sets the LEVEL of required FTE. The only input that can still change the SIGN of the projected gap (adequacy 0.708 vs 0.948 erases the 2050 surplus).",
    "fte_curve",         FALSE,     FALSE,        "Level absorbed by intercept calibration; SHAPE removes 176 FTE from 2050 supply. Larger than the demand-growth lever across the full Census range.",
    "delegation_matrix", FALSE,     TRUE,         "Cancels: calibrate_wrvu_per_fte() solves productivity against the anchor, so required FTE is invariant to it. A provenance problem, not a results problem.",
    "demand_calibration", TRUE,     TRUE,         "Fitted to NAMCS (scalar 0.467), and cancels for the same reason -- a 2.1x level correction moved 2050 required FTE by 0.25%."
  )
}
