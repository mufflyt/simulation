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
#' @param agents Optional cohort with `age` and `sex`. When supplied, the
#'   leverage is MEASURED for this cohort via [fte_curve_gradient_leverage()]
#'   and returned in `leverage_measured`, rather than being read off the prose
#'   below. Supply it whenever the number matters: the prose figure went stale
#'   once already, and a hardcoded magnitude for an input that does not cancel
#'   is the specific failure this argument exists to remove.
#' @return List with `resolved`, the current source, why it is unresolved, its
#'   leverage, and the variables that would resolve it. Plus `leverage_measured`
#'   when `agents` is supplied.
#' @family practice survey
#' @concept data
#' @export
fte_curve_status <- function(agents = NULL) {
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
      "clinical FTE is 1.000 at base year by construction. Its SHAPE is not, and",
      "does NOT cancel, unlike delegation and demand calibration. Two magnitudes,",
      "both measured by fte_curve_gradient_leverage() rather than asserted:",
      "ageing the base cohort 25 years with no renewal costs 20.0% of FTE per",
      "head (-200 FTE per 1,000), which is the gradient's own effect; in the full",
      "model young entrants absorb about half of it and FTE per head lands at",
      "0.911 in 2050, some 169 FTE. Earlier text here said 0.9231/176 FTE and the",
      "README said ~3%: three numbers for one quantity, none computed."),
    do_not_fix = paste(
      "R/calibration-hrsa_fte.R carries TODO-FTE-001 and looks like the",
      "target. It is dormant -- apply_hrsa_surgical_fte() is called by nothing",
      "but its own tests -- so replacing it would change no output."),
    resolved_by = urps_practice_survey_requirements("fte_curve")$variable,
    leverage_measured = if (is.null(agents)) NULL else {
      fte_curve_gradient_leverage(agents, horizon_years = 25L,
                                  gradient_scale = c(0, 1))
    }
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
  # DERIVED, not declared. This row read a hardcoded "MISSING" while the
  # artifacts sat verifiable on disk, so the status object could not tell you
  # whether the blocker had cleared -- the same defect as a hardcoded leverage
  # figure. Presence-only here (no 1.6 GB hash on a status call); identity is
  # established by assert_canonical_isochrones() at the point of use.
  iso_v <- verify_canonical_isochrones(verify_checksums = FALSE)
  iso <- if (isTRUE(iso_v$ok)) {
    list(state = "PRESENT", detail = sprintf(paste(
      "Canonical run %s (Valhalla %s, cohort %d NPIs), bands %s, referenced in",
      "place. Registry-matched; SHA-256 verified at point of use by",
      "assert_canonical_isochrones(). Quarantined, superseded and",
      "SUCCESS-marker-contradicting artifacts in the same source tree are",
      "refused by name -- see ISOCHRONE_REFUSED_ARTIFACTS. Location comes from",
      "the configured isochrones_root; verify_canonical_isochrones()$dir",
      "reports the resolved directory. It is deliberately NOT interpolated",
      "here: it lives in a different repository, and a foreign absolute path in",
      "status text reads as repo-relative to any reader or scanner."),
      ISOCHRONE_CANONICAL_RUN_ID, ISOCHRONE_CANONICAL_VALHALLA,
      ISOCHRONE_CANONICAL_COHORT$n_npis,
      paste(ISOCHRONE_CANONICAL_BANDS, collapse = "/")))
  } else {
    list(state = "MISSING", detail = paste(
      "They exist in mufflyt/isochrones and were expensive to generate; import",
      "rather than recompute. Not currently resolvable:", iso_v$reason))
  }

  # Ask the anchor rather than describing it, so this row cannot drift from the
  # observation it reports.
  .wa <- urps_observed_wait_days()
  components <- tibble::tribble(
    ~component,              ~state,     ~detail,
    "tract_population",      "PRESENT",  "data-raw/spatial/acs5_2023_tract_female_by_ageband.csv -- 84,400 tracts, 83.5M women 40+, md5 df69beefcead6aa84d629ca9862ba011, regenerated by scripts/data_acquisition/08_download_acs_tracts.R.",
    "tract_centroids",       "PRESENT",  "data-raw/spatial/tract_fem65_centroids.csv, joined to the ACS table by GEOID.",
    "demand_machinery",      "WIRED",    "R/geography-demand.R: tract_need_from_population(), demand_by_travel_band(), need_weighted_access(), isochrone_demand_from_tracts(). Called from scripts/run_demand_pipeline.R.",
    "supply_machinery",      "WIRED",    "R/geography-spatial_access_e2sfca.R: compute_access(), match_points_to_isochrones(), compare_access_methods(), access_moe_ci(). run_geographic_access() is now the fail-closed entry point and run_workforce_microsimulation() calls it, attaching $geographic_access. It computes only when the membership artifact + a coordinate-bearing roster exist; otherwise resolved = FALSE with a reason, NEVER on fallback geometry.",
    "provider_coordinates",  "PRESENT",  "Assembled from five geocoding runs in mufflyt/isochrones: the primary ABOG run, the separate data/abu_urology/ run it omits, and three earlier production runs. 1,324 of the 1,339 model baseline carry a point (98.9%): ABOG 98.8%, ABU 99.0%. Above the 95% floor with no pathway hole, so provider_coordinate_coverage() no longer blocks.",
    "drive_time_isochrones", iso$state, iso$detail,
    "wait_time_anchor",      "PRESENT",  sprintf("Observed URPS wait %.1f business days (%.1f calendar) from %s. Supplied by urps_observed_wait_days(); populates the wait_time target that ships unpopulated.", .wa$business_days, .wa$calendar_days, sub("\\..*", "", .wa$citation)),
    "validation_gate",       "WIRED",    "validation_report() reports geographic_access_validated (an external check reading this status object): FALSE until isochrones land, so a run records the geographic gap instead of asserting geography was checked. It deliberately does NOT run the access layer -- that awaits isochrones (the ordering trap below). two_method_agreement() still compares geographic adequacy rankings and is called with geographic data by nothing."
  )

  # Resolved is now DERIVED from the components, not asserted. The comment below
  # states the repository's own design rule -- "The ONE remaining blocker is the
  # data step ... the moment that artifact exists, the already-wired layer
  # resolves" -- so hardcoding FALSE would have contradicted it the moment the
  # isochrones landed.

  # E2SFCA SURFACE VALIDATION IS A SEPARATE, WEAKER CLAIM than the layer being
  # wired, so it is reported as fields rather than as a component. Adding it to
  # `components` would let an unset E2SFCA_SURFACE_DIR drag `resolved` back to
  # FALSE, which would undo the derivation rule above for a reason that has
  # nothing to do with whether the isochrones are present.
  .contemporary <- tryCatch(access_surface_for_demand(), error = function(e) NULL)

  list(
    surface_validated = !is.null(.contemporary),
    resolved = !any(components$state %in% c("MISSING", "DORMANT")),
    components = components,
    n_present = sum(components$state %in% c("PRESENT", "WIRED")),
    n_missing = sum(components$state %in% c("MISSING", "DORMANT")),
    why_unresolved = if (!any(components$state %in% c("MISSING", "DORMANT"))) {
      NA_character_
    } else paste(
      "Missing:", paste(components$component[components$state %in% c("MISSING", "DORMANT")],
                        collapse = ", "),
      "-- see the component detail for what each needs."),
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
    # Ascertainment is a MEASURED property of each yearly surface, not a
    # blocker. It is surfaced here so no caller can read an access number
    # without also being able to read how much of the workforce is behind it.
    ascertainment = tryCatch({
      fl <- access_provider_flow()
      cy <- contemporary_access_year(fl)
      list(contemporary_year = cy$year, reason = cy$reason,
           by_year = fl[, c("analysis_year", "eligible_provider_n",
                            "usable_coordinate_n", "surface_provider_n",
                            "usable_coordinate_rate", "surface_rate")],
           all_losses_explained = all(fl$spatially_eligible_provider_n ==
                                        fl$surface_provider_n, na.rm = TRUE))
    }, error = function(e) list(available = FALSE, reason = conditionMessage(e))),
    resolved_by = c("drive-time isochrones imported from mufflyt/isochrones")
  )
}


# ---- Hours-curve shape sensitivity ----------------------------------------
#
# WHY THIS EXISTS. `fte_curve_status()` says the borrowed hours curve's LEVEL
# cancels and its SHAPE does not. That distinction is right, but the shape's
# magnitude was a hardcoded sentence, and it went stale: the recorded "0.9231 by
# 2050, removing 176 FTE" reads 0.9115 and 169 FTE against the current model,
# while README described the same quantity as "~3%" when it is nearer 9%. Three
# numbers for one quantity, none of them computed.
#
# The LEVEL cancels because calibrate_hours_intercept() solves the intercept so
# mean clinical FTE is exactly 1.000 in the base year. Nothing does that for the
# age GRADIENT, so every year after the base year applies the age profile of a
# general-internal-medicine curve to urogynaecologists.
#
# The cohort is held fixed and aged: no retirement, no entrants. That is
# deliberate. The full model replaces retirees with young entrants, which offsets
# part of the ageing and makes the curve look less consequential than it is.
# Holding the cohort fixed answers "what does the borrowed SHAPE do", which is
# the question the analogy actually raises.

# Hours offset with the age gradient scaled. Deliberately NOT implemented by
# reassigning HWSM_HOURS_AGE_EFFECT: a package namespace is locked after load,
# and a sensitivity helper that mutates a published constant would be a worse
# defect than the one it measures.
.hours_offset_scaled <- function(age, sex, gradient_scale) {
  age <- as.numeric(age)
  sex <- tolower(as.character(rep_len(sex, length(age))))
  band <- as.character(cut(age, breaks = HWSM_HOURS_AGE_BANDS,
                           labels = HWSM_HOURS_AGE_LABELS,
                           right = FALSE, include.lowest = TRUE))

  age_eff <- unname(HWSM_HOURS_AGE_EFFECT[band]) * gradient_scale
  age_eff[is.na(age_eff)] <- 0

  female <- sex == "female"
  # The female INTERACTION is age-varying, so it is part of the gradient and
  # scales with it. The female MAIN effect is a level and does not.
  fem_int <- unname(HWSM_HOURS_FEMALE_INTERACTION[band]) * gradient_scale
  fem_int[is.na(fem_int)] <- 0

  age_eff + ifelse(female, HWSM_HOURS_FEMALE_MAIN, 0) + ifelse(female, fem_int, 0)
}

#' Leverage of the borrowed hours curve's age gradient
#'
#' Scales the published HWSM age gradient by `gradient_scale` and reports mean
#' clinical FTE per head after ageing the cohort `horizon_years`.
#' `gradient_scale = 0` is a flat curve (hours independent of age); `1` is the
#' published gradient; `>1` steepens it.
#'
#' The intercept is re-solved for every scale, so each row is normalised to
#' 1.000 FTE per head in the base year and the rows differ ONLY in shape. That
#' is the point: the level is not what is borrowed in a way that matters.
#'
#' @param agents Cohort with `age` and `sex`.
#' @param horizon_years Years to age the cohort by.
#' @param gradient_scale Multipliers on the published age gradient.
#' @param fte_hours Clinical hours defining 1.0 FTE.
#' @return Tibble of `gradient_scale`, `fte_per_head_base`,
#'   `fte_per_head_horizon`, `drift_pct`, `fte_delta_per_1000_head`.
#' @family practice survey
#' @concept data
#' @export
fte_curve_gradient_leverage <- function(agents,
                                        horizon_years = 25L,
                                        gradient_scale = c(0, 0.5, 1, 1.5),
                                        fte_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK) {
  assertthat::assert_that(is.data.frame(agents),
                          all(c("age", "sex") %in% names(agents)))
  assertthat::assert_that(is.numeric(horizon_years), length(horizon_years) == 1L,
                          is.finite(horizon_years), horizon_years >= 0)
  assertthat::assert_that(is.numeric(gradient_scale), length(gradient_scale) >= 1L,
                          all(is.finite(gradient_scale)))

  rows <- lapply(gradient_scale, function(k) {
    off_base <- .hours_offset_scaled(agents$age, agents$sex, k)
    # Solve the intercept so mean FTE per head is exactly 1.000 at base year,
    # mirroring calibrate_hours_intercept() for the scaled gradient.
    ic <- fte_hours - mean(off_base)
    fte_at <- function(extra) {
      mean(pmax(ic + .hours_offset_scaled(agents$age + extra, agents$sex, k), 0)) / fte_hours
    }
    base <- fte_at(0)
    horiz <- fte_at(horizon_years)
    tibble::tibble(
      gradient_scale = k,
      fte_per_head_base = base,
      fte_per_head_horizon = horiz,
      drift_pct = 100 * (horiz / base - 1),
      fte_delta_per_1000_head = 1000 * (horiz - base)
    )
  })
  dplyr::bind_rows(rows)
}
