# The Open-Items Register ----
#
# Three items are open, and they are open in two different WAYS. Keeping that
# distinction visible is the point of the file: the first two are answered by
# collecting data the model has never had, the third by connecting data it
# already holds.
#
# TWO INPUTS BORROWED FROM OTHER SPECIALTIES, both answerable by ONE fielded
# instrument -- registered together because fielding two surveys to collect one
# questionnaire's worth of questions is how neither gets done:
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
# ONE CAPABILITY THAT IS BUILT BUT UNREACHABLE:
#
#   3. GEOGRAPHIC ACCESS           `geographic_access_status()`
#      Not an analogy and not a wrong value -- an absent one. The layer exists
#      and is called by nothing, so it cannot be miscalibrated, only missing.
#      No questionnaire fixes it; see that function for the ordering trap.
#
# DO NOT "FIX" R/calibration-hrsa_fte.R. It carries TODO-FTE-001 and the
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
#' @family practice survey
#' @concept data
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
#' @family practice survey
#' @concept data
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
      "R/calibration-hrsa_fte.R carries TODO-FTE-001 and looks like the",
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
#' `in_reported_estimand` separates two things that both read as "does not
#' affect the answer" and are not the same. Delegation IS in the estimand and
#' cancels arithmetically. Geographic access is not in it at all, because the
#' layer is dormant -- so it cannot be wrong, only absent.
#'
#' @return Tibble with `item`, `resolved`, `in_reported_estimand`,
#'   `cancels_out`, `resolved_by`, `leverage`.
#' @family practice survey
#' @concept data
#' @export
unresolved_calibration_items <- function() {
  tibble::tribble(
    ~item,                ~resolved, ~in_reported_estimand, ~cancels_out, ~resolved_by,        ~leverage,
    "capacity_anchor",    FALSE,     TRUE,                  FALSE,        "practice survey",   "Sets the LEVEL of required FTE. The only input that can still change the SIGN of the projected gap (adequacy 0.708 vs 0.948 erases the 2050 surplus).",
    "fte_curve",          FALSE,     TRUE,                  FALSE,        "practice survey",   "Level absorbed by intercept calibration; SHAPE removes 176 FTE from 2050 supply. Larger than the demand-growth lever across the full Census range.",
    "geographic_access",  FALSE,     FALSE,                 NA,           "data integration",  "Not in any reported result: R/geography-spatial_access_e2sfca.R is loaded and called by nothing. Cannot be wrong, only absent. Basis for every distributional claim the model might make. See geographic_access_status().",
    "delegation_matrix",  FALSE,     TRUE,                  TRUE,         "practice survey",   "Cancels: calibrate_wrvu_per_fte() solves productivity against the anchor, so required FTE is invariant to it. A provenance problem, not a results problem.",
    "demand_calibration", TRUE,      TRUE,                  TRUE,         "NAMCS (done)",      "Fitted to NAMCS (scalar 0.467), and cancels for the same reason -- a 2.1x level correction moved 2050 required FTE by 0.25%."
  )
}

# ---- Geographic access -----------------------------------------------------
#
# A different KIND of open item from the two above, and the register has to say
# so or it will be "fixed" wrongly. The capacity anchor and the FTE curve are
# inputs resolved BY ANALOGY: they have values, and the values come from another
# specialty. Geographic access has no value at all -- the layer is built, loaded,
# and reachable from nothing. It is a capability gap, not a calibration error.

#' Status of the geographic access layer
#'
#' `docs/DEMAND_METHODS.md` says production use "requires tract-level population,
#' provider locations and drive-time isochrones". Two of those three now exist,
#' so the remaining work is cross-repo INTEGRATION and WIRING, not construction.
#'
#' THE ORDERING TRAP, which is why this is registered rather than left as a TODO.
#' `R/geography-spatial_access_e2sfca.R` is loaded by the orchestrator's module list and
#' called by nothing outside its own tests. Wiring it up is a small change and
#' looks like the obvious first step. Do it BEFORE provider coordinates exist and
#' the layer runs on state-level centroids, producing a geographic access ratio
#' that is fully plausible and means nothing. A dormant module produces no
#' number; a wired one running on the wrong geometry produces a publishable one.
#' Coordinates and isochrones come first.
#'
#' @return List with `resolved`, per-component state, the trap, and what remains.
#' @family practice survey
#' @concept data
#' @export
geographic_access_status <- function() {
  components <- tibble::tribble(
    ~component,              ~state,     ~detail,
    "tract_population",      "PRESENT",  "data-raw/spatial/acs5_2023_tract_female_by_ageband.csv -- 84,400 tracts, 83.5M women 40+, md5 df69beefcead6aa84d629ca9862ba011, regenerated by scripts/data_acquisition/08_download_acs_tracts.R.",
    "tract_centroids",       "PRESENT",  "data-raw/spatial/tract_fem65_centroids.csv, joined to the ACS table by GEOID.",
    "demand_machinery",      "WIRED",    "R/geography-demand.R: tract_need_from_population(), demand_by_travel_band(), need_weighted_access(), isochrone_demand_from_tracts(). Called from scripts/run_demand_pipeline.R.",
    "supply_machinery",      "WIRED",    "R/geography-spatial_access_e2sfca.R: compute_access(), match_points_to_isochrones(), compare_access_methods(), access_moe_ci(). run_geographic_access() is now the fail-closed entry point and run_workforce_microsimulation() calls it, attaching $geographic_access. It computes only when the membership artifact + a coordinate-bearing roster exist; otherwise resolved = FALSE with a reason, NEVER on fallback geometry.",
    "provider_coordinates",  "PRESENT",  "Assembled from five geocoding runs in mufflyt/isochrones: the primary ABOG run, the separate data/abu_urology/ run it omits, and three earlier production runs. 1,324 of the 1,339 model baseline carry a point (98.9%): ABOG 98.8%, ABU 99.0%. Above the 95% floor with no pathway hole, so provider_coordinate_coverage() no longer blocks.",
    "drive_time_isochrones", "MISSING",  "Absent from this repository entirely. They exist in mufflyt/isochrones and were expensive to generate; import rather than recompute.",
    "validation_gate",       "WIRED",    "validation_report() reports geographic_access_validated (an external check reading this status object): FALSE until isochrones land, so a run records the geographic gap instead of asserting geography was checked. It deliberately does NOT run the access layer -- that awaits isochrones (the ordering trap below). two_method_agreement() still compares geographic adequacy rankings and is called with geographic data by nothing."
  )

  list(
    resolved = FALSE,
    components = components,
    n_present = sum(components$state %in% c("PRESENT", "WIRED")),
    n_missing = sum(components$state %in% c("MISSING", "DORMANT")),
    why_unresolved = paste(
      "Two of the three inputs the methods document names are now present and",
      "checksummed. What is missing is provider point locations and drive-time",
      "isochrones -- both of which exist in mufflyt/isochrones -- plus the wiring",
      "that would make geography a gate rather than a downstream feature."),
    ordering_trap = paste(
      "Do NOT wire R/geography-spatial_access_e2sfca.R first. Running it before provider coordinates exist",
      "falls back to state-level geometry and yields a plausible access ratio",
      "that means nothing. A dormant module produces no number; a wired one on",
      "the wrong geometry produces a publishable one."),
    # Deliberately NOT resolved by the practice survey: this is an integration
    # task, and listing it there would imply a questionnaire could fix it.
    # The third historical item -- "a geographic check added to
    # validation_report()") and the orchestrator wiring
    # (run_geographic_access(), called from run_workforce_microsimulation()) are
    # both DONE. The ONE remaining blocker is the data step: import the isochrone
    # polygons and build the membership table. The moment that artifact exists,
    # the already-wired layer resolves.
    resolved_by = c("drive-time isochrones imported from mufflyt/isochrones")
  )
}
