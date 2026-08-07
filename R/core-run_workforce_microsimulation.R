# Integrated Workforce Microsimulation Orchestrator ----
#
# Ties the modules into ONE reproducible run following the Dall-family sequence:
#
#   population -> healthcare use -> provider staffing patterns -> provider demand
#   provider roster -> hours + retirement + migration -> provider supply
#   base-year adequacy -> absolute gap (FTE on both sides) -> scenarios
#
# What changed relative to the previous orchestrator, and why:
#
#  * BASE-YEAR EQUILIBRIUM IS NO LONGER ASSUMED. The run requires a
#    `baseline_gap` estimate and refuses (strict) or warns (relaxed) without one.
#    The rebased growth curves are still produced but are labelled
#    `growth_adequacy` and can never again be read as absolute coverage.
#  * SUPPLY AND DEMAND ARE COMPARED IN FTE ON BOTH SIDES, via the work-RVU
#    workload conversion, instead of dividing FTE by case counts.
#  * BASE-YEAR SUPPLY COMES FROM THE VERSIONED CONTRACT (mufflyaccess), never a
#    hard-coded literal, and national vs CONUS are kept distinguishable.
#  * RETIREMENT SCENARIOS SHIFT THE AGE AXIS (+/- 2 years) rather than scaling a
#    hazard, matching every published Dall-family study.
#  * The Monte-Carlo loop is no longer duplicated: this calls
#    run_supply_microsimulation().
#
# Load order (plain sourced files):
#   10 provenance, 11 canonical/joins, 12 supply microsim, 13 demand,
#   14 spatial access, 16 lifecycle, 17 workload->FTE, 18 baseline gap,
#   19 scenarios, 20 geography, 21 calibration/validation

MICROSIM_MODULES <- c(
  "core-paths.R",
  "core-repro_provenance.R",
  "core-canonical_and_joins.R",
  "supply-provider_lifecycle.R",
  "supply-provider_microsimulation.R",
  "demand-urps.R",
  "geography-spatial_access_e2sfca.R",
  "supply-workload_to_fte.R",
  "reporting-baseline_gap.R",
  "reporting-scenario_registry.R",
  "geography-provider_geography.R",
  "calibration-validation.R",
  "core-legacy_loader.R",
  "reporting-urps_projection.R"
)

#' Load every microsimulation module in dependency order (deprecated)
#'
#' This repository is now an R package: use `library(urpssim)`, or
#' `pkgload::load_all()` in a source checkout. Sourcing the module files by hand
#' bypasses the namespace and masks the package's own exports. Retained so older
#' scripts keep working.
#'
#' @param r_dir Directory holding the numbered R modules.
#' @return (Invisibly) the sourced file paths.
#' @keywords internal
load_workforce_microsimulation <- function(r_dir = "R") {
  .msg_warn("load_workforce_microsimulation() is deprecated: use library(urpssim) ",
            "or pkgload::load_all() instead.")
  paths <- file.path(r_dir, MICROSIM_MODULES)
  paths <- paths[file.exists(paths)]
  for (p in paths) source(p)
  invisible(paths)
}

# ---- Example inputs (run end-to-end without external data) ----------------

#' Example age-banded female population series
#'
#' Age bands grow at different rates so the three demand estimands diverge, which
#' is the whole point of giving each its own age profile. Replace with ACS/Census
#' projections resolved through [resolve_canonical()] for production runs.
#'
#' @param years Integer years.
#' @param base_pop Named base-year population by age band.
#' @param growth Named annual growth rate by age band.
#' @return Tibble `year`, `age_band`, `female_pop`.
#' @export
example_female_population_by_band <- function(
    years = 2025:2050,
    base_pop = c("20-39" = 43.0e6, "40-59" = 41.5e6, "60-64" = 11.0e6,
                 "65-79" = 22.0e6, "80+" = 7.5e6),
    growth   = c("20-39" = 0.001,  "40-59" = 0.004,  "60-64" = 0.008,
                 "65-79" = 0.016,  "80+" = 0.031)) {
  tidyr::expand_grid(year = years, age_band = names(base_pop)) %>%
    dplyr::mutate(
      female_pop = unname(base_pop[.data$age_band]) *
        (1 + unname(growth[.data$age_band]))^(.data$year - min(years))
    )
}

#' Example base-year service volumes for the workload conversion
#'
#' Volumes are per year, national. Replace with MEPS-derived ambulatory volumes
#' and a claims/SASD-derived procedure basket for production runs.
#'
#' @param demand_long Long demand tibble; D2 drives consultations and D3 drives
#'   procedures, so volumes track the demand projection.
#' @return Tibble `year`, `service`, `volume`.
#' @export
example_service_volumes <- function(demand_long) {
  consults <- dplyr::filter(demand_long, .data$estimand == "D2")
  surgery <- dplyr::filter(demand_long, .data$estimand == "D3")
  prevalent <- dplyr::filter(demand_long, .data$estimand == "D1")

  dplyr::bind_rows(
    tibble::tibble(year = consults$year, service = "new_consultation",
                   volume = consults$demand_cases),
    tibble::tibble(year = consults$year, service = "return_visit",
                   volume = consults$demand_cases * 2.4),
    tibble::tibble(year = prevalent$year, service = "pessary_care",
                   volume = prevalent$demand_cases * 0.035),
    tibble::tibble(year = consults$year, service = "urodynamics",
                   volume = consults$demand_cases * 0.30),
    tibble::tibble(year = consults$year, service = "cystoscopy",
                   volume = consults$demand_cases * 0.18),
    tibble::tibble(year = prevalent$year, service = "botox_bladder",
                   volume = prevalent$demand_cases * 0.004),
    tibble::tibble(year = prevalent$year, service = "ptns",
                   volume = prevalent$demand_cases * 0.010),
    tibble::tibble(year = prevalent$year, service = "bladder_instillation",
                   volume = prevalent$demand_cases * 0.006),
    tibble::tibble(year = surgery$year, service = "sling_procedure",
                   volume = surgery$demand_cases * 0.55),
    tibble::tibble(year = surgery$year, service = "prolapse_procedure",
                   volume = surgery$demand_cases * 0.45),
    tibble::tibble(year = surgery$year, service = "postoperative_care",
                   volume = surgery$demand_cases * 2.0)
  )
}

#' Example capacity-survey responses (Zarek 2025 published distribution)
#'
#' Stands in for a URPS practice-capacity survey until one is fielded. Using the
#' physical-therapy distribution for urogynaecology is an explicit assumption:
#' a gap built on it must be constructed with
#' `calibration_status = "derived_by_analogy"`, which is what stops the run
#' artifacts from reporting it as a survey fielded in urogynaecology.
#'
#' The choice of donor specialty is not incidental. On the 1,306-FTE base-year
#' supply this distribution implies a 71 FTE shortfall; Dall's physiatry survey
#' implies 155 and Dall's neurology assumption 161. Nothing in the model narrows
#' that range -- see [published_baseline_gaps()].
#'
#' @return Tibble in the shape [capacity_survey_adequacy()] expects.
#' @export
example_capacity_survey <- function() {
  tibble::tribble(
    ~category,         ~n,  ~seen, ~additional,
    "equilibrium",    343,     NA,          NA,
    "surplus",        268,     25,           5,
    "shortage_hours", 449,     31,           4,
    "shortage_unmet", 363,     35,           5
  )
}

# ---- Main orchestrator ----------------------------------------------------

#' Run the integrated workforce microsimulation
#'
#' @param baseline_supply Base-year supplied FTE, or NULL to read it from the
#'   `mufflyaccess` contract. Never hard-code a workforce count.
#' @param supply_geography "national" or "conus": national for supply reporting,
#'   CONUS for the geographic access layer.
#' @param roster Optional production provider roster; when NULL the base cohort
#'   is built from the observed certification series where available.
#' @param use_certification_cohorts Build the base cohort from
#'   `mufflyaccess::urps_counts_long()` when no roster is supplied.
#' @param years Integer projection years.
#' @param subspecialty Subspecialty label.
#' @param pop_by_band Age-banded female population. When NULL, resolved from the
#'   canonical Census-NPP series ([resolve_demand_population()]), falling back to
#'   [example_female_population_by_band()] if the file is absent.
#' @param population_series Census-NPP series when `pop_by_band` is NULL: one of
#'   "mid" (default), "low", "hi".
#' @param supply_scenarios,demand_scenarios Scenario registries.
#' @param baseline_gap_estimate A [baseline_gap()] object; NULL triggers the
#'   fail-closed guard.
#' @param n_iterations Monte-Carlo replicates per scenario.
#' @param baseline_entrants Baseline annual entrants. `NULL` (default) resolves
#'   the real NRMP-matched value via [nrmp_entrants()] (70/yr for URPS) and
#'   records `entrants_source = "nrmp_matched"` in the run metadata. Pass a
#'   number to override; `55` reproduces runs made before the default became
#'   measured rather than assumed.
#' @param retirement_source Base retirement hazard: "urps_empirical" (default,
#'   cliff's observed URPS hazards for ages 50-69 with the HWSM tail past 70) or
#'   "hwsm" (the HWSM/FutureDocs literature curve, an external analogue fitted on
#'   a different physician population). Scenario age-shifts apply on top of
#'   whichever base is chosen. The empirical hazards are preferred whenever they
#'   are derivable: they are measured on this subspecialty rather than borrowed.
#'   Selecting "hwsm" is an affirmative choice to model URPS attrition with
#'   another population's curve, and is recorded in the run metadata.
#' @param calibration Demand calibration. `NULL` (default) leaves demand
#'   uncalibrated, which [assert_demand_calibrated()] warns about and strict
#'   mode refuses -- this is the uncalibrated comparator. The string `"namcs"`
#'   fits scalars internally from the base-year volumes against the NAMCS
#'   national anchor ([namcs_demand_calibration()]). A tibble from
#'   [fit_calibration_scalars()] is used as supplied. Scalars are APPLIED to the
#'   comparable service rows, not merely recorded.
#' @param placement_shares Optional tibble of `geo` and `share` that enables the
#'   geographic layer: entrants are placed by this distribution and providers may
#'   migrate mid-career. Build it with [opportunity_placement_shares()] for the
#'   HWSM rule (demand growth plus retirements, which lets existing
#'   maldistribution persist), [historical_placement_shares()] for the
#'   reproduce-today's-distribution comparison, or [blend_placement_shares()].
#'   Requires a cohort carrying `state`.
#' @param seed_base_geography When `placement_shares` is supplied but the cohort
#'   has no `state` column, draw the base cohort's states from those shares
#'   rather than leaving the layer inert. A declared assumption, not an
#'   observation: geographic output is then conditional on it.
#' @param parameter_spec Optional [supply_parameter_spec()]; defaults to one
#'   built from the observed certification series.
#' @param allow_analogy Permit inputs derived by analogy from another specialty:
#'   the delegation matrix, and a base-year gap whose `calibration_status` is
#'   `derived_by_analogy` or `assumed_with_evidence`. Declared in the run
#'   metadata either way. Defaults to FALSE: passing TRUE suppresses the
#'   strict-mode stop in [assert_publishable_workload()] and
#'   [assert_baseline_gap_estimated()], so a publication-facing run must opt in
#'   deliberately rather than inherit the exemption from the default.
#' @param brfss_cells Optional population cell table from
#'   [build_urps_population_cells()].  When non-NULL, a fourth demand estimand
#'   D4 (BRFSS survey-weighted UI care-seeking demand) is appended to
#'   `demand_long` and flows through concordance assessment alongside D1-D3.
#'   Requires that [brfss_pfd_prevalence_for_demand_bands()] is available (i.e.
#'   that R/data-urps_population.R is loaded).
#' @param prevention_scenario Character key in [URPS_PREVENTION_SCENARIOS], or
#'   `NULL` (default). When non-NULL, service volumes are adjusted by
#'   [apply_named_prevention_scenario()] before [convert_workload_to_fte()],
#'   modelling conservative PFD management (PT/pessary) as a demand-side
#'   multiplier. Use `"baseline"` to confirm the no-shift reference.
#' @param setting_scenario Character key in [URPS_SETTING_SCENARIOS], or `NULL`
#'   (default). When non-NULL, [apply_setting_scenario()] redistributes service
#'   volumes across care-delivery settings and applies per-setting productivity
#'   adjustments before [convert_workload_to_fte()]. Scenarios include
#'   `"telehealth_10pct"`, `"asc_migration_30pct"`, `"combined_shift"`.
#' @param output_dir If non-NULL, write provenance-tagged artifacts here.
#' @param seed RNG seed.
#' @param verbose Logical.
#' @return List with supply, demand, required FTE, gap, growth adequacy,
#'   concordance, outlook, validation, and run metadata.
#' @export
run_workforce_microsimulation <- function(baseline_supply = NULL,
                                          supply_geography = c("national", "conus"),
                                          roster = NULL,
                                          use_certification_cohorts = TRUE,
                                          years = 2025:2050,
                                          subspecialty = "FPMRS",
                                          pop_by_band = NULL,
                                          population_series = "mid",
                                          supply_scenarios = NULL,
                                          demand_scenarios = NULL,
                                          baseline_gap_estimate = NULL,
                                          n_iterations = 200,
                                          baseline_entrants = NULL,
                                          retirement_source = c("urps_empirical", "hwsm"),
                                          calibration = NULL,
                                          placement_shares = NULL,
                                          seed_base_geography = FALSE,
                                          parameter_spec = NULL,
                                          allow_analogy = FALSE,
                                          brfss_cells = NULL,
                                          prevention_scenario = NULL,
                                          setting_scenario = NULL,
                                          output_dir = NULL,
                                          seed = 20260801L,
                                          verbose = TRUE) {
  supply_geography <- match.arg(supply_geography)
  retirement_source <- match.arg(retirement_source)
  mode <- resolve_reproducibility_mode()
  seed_microsimulation(seed, mode)
  run_id <- make_run_id(paste0("workforce_", tolower(subspecialty)), seed, mode)

  # Base retirement schedule: cliff's empirical URPS hazards (observed 50-69 +
  # HWSM tail past 70) by default, because they are measured on this subspecialty.
  # "hwsm" is the external analogue and must now be asked for by name; taking it
  # is a modelling choice, so say so rather than letting it pass silently.
  if (identical(retirement_source, "hwsm")) {
    .msg_info(paste(
      "retirement_source='hwsm': modelling URPS attrition with the HWSM/FutureDocs",
      "literature curve, fitted on a different physician population, in place of",
      "the observed URPS hazards (declared)."))
  }
  base_retirement_schedule <- if (identical(retirement_source, "urps_empirical")) {
    tryCatch(
      urps_empirical_retirement_schedule(mode = mode),
      error = function(e) {
        if (identical(mode, "strict")) {
          stop(sprintf("retirement_source='urps_empirical' unavailable: %s",
                       conditionMessage(e)), call. = FALSE)
        }
        .msg_warn(sprintf("Empirical retirement schedule unavailable (%s); using HWSM",
                          conditionMessage(e)))
        RETIREMENT_HAZARD_BY_AGE
      }
    )
  } else {
    RETIREMENT_HAZARD_BY_AGE
  }

  # --- Base-year supply from the versioned contract ------------------------
  contract <- NULL
  if (is.null(baseline_supply)) {
    contract <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
    baseline_supply <- contract[[supply_geography]]
  }
  base_year <- min(years)

  # Prefer the real Census-NPP female population (canonical source); fall back to
  # the example series when the file is absent (strict mode re-raises instead).
  if (is.null(pop_by_band)) {
    demand_pop <- resolve_demand_population(years, series = population_series, mode = mode)
    pop_by_band <- demand_pop$pop_by_band
    population_source <- demand_pop$source
  } else {
    population_source <- "caller_supplied"
  }
  # --- Baseline entrants: measured, not assumed ----------------------------
  # The shipped default was a round 55/yr that matched no source. NRMP filled
  # fellowship positions are the actual entry pathway into this subspecialty and
  # are already carried as a canonical input, so resolve them and say so. The
  # observed certification flow corroborates the magnitude independently: 2021-23
  # averaged 69/yr against NRMP's 70. `baseline_entrants = 55` remains available
  # to reproduce any earlier run.
  entrants_source <- "caller_supplied"
  if (is.null(baseline_entrants)) {
    baseline_entrants <- tryCatch({
      n <- nrmp_entrants(subspecialty, mode = mode)
      entrants_source <- "nrmp_matched"
      if (verbose) {
        .msg_info(sprintf("Baseline entrants: %d/yr from the NRMP match (observed).", n))
      }
      n
    }, error = function(e) {
      if (identical(mode, "strict")) {
        stop(sprintf(paste("baseline_entrants could not be resolved from the NRMP",
                           "match (%s), and strict mode will not substitute an",
                           "assumption. Pass baseline_entrants explicitly."),
                     conditionMessage(e)), call. = FALSE)
      }
      entrants_source <<- "assumed_fallback"
      .msg_warn(sprintf(paste("NRMP entrants unavailable (%s); falling back to the",
                              "legacy assumption of 55/yr, which matches no source."),
                        conditionMessage(e)))
      55
    })
  }

  if (is.null(supply_scenarios)) supply_scenarios <- supply_scenario_registry(baseline_entrants)
  if (is.null(demand_scenarios)) demand_scenarios <- demand_scenario_registry()
  validate_scenario_registry(supply_scenarios, "supply")
  # Downstream consumers validate scenario ids against the mufflyaccess
  # registry, so an unregistered id fails validate_urps_projection() later.
  assert_scenarios_registered(names(supply_scenarios), mode)
  validate_scenario_registry(demand_scenarios, "demand")

  if (verbose) {
    .msg_info(sprintf("=== Workforce microsimulation %s (mode = %s) ===", run_id, mode))
    .msg_info(sprintf("%s: base-year %s supply %s FTE; %d supply scenarios x %d iterations",
                      subspecialty, supply_geography, format(baseline_supply, big.mark = ","),
                      length(supply_scenarios), n_iterations))
  }

  # --- Starting cohort -----------------------------------------------------
  if (!is.null(roster)) {
    dedup <- deduplicate_provider_roster(roster)
    agents <- agents_from_roster(dedup$roster, base_year)
  } else if (isTRUE(use_certification_cohorts) && has_mufflyaccess()) {
    # Better than a synthetic draw: fellowship-graduate cohorts get an age
    # derived from their observed certification year; only the 2013
    # backlog-clearance cohort is assumed. Still not a roster -- the contract
    # ships aggregate counts with no age, sex or state.
    agents <- agents_from_certification_cohorts(
      baseline_year = base_year, geography = supply_geography,
      subspecialty = subspecialty
    )
  } else {
    .msg_warn("No provider roster and no certification series: generating a ",
              "SYNTHETIC cohort. Examples and tests only.")
    agents <- initialize_provider_agents(baseline_supply, subspecialty, base_year)
    agents$sex <- ifelse(stats::runif(nrow(agents)) < 0.55, "female", "male")
  }
  cohort <- cohort_provenance(agents)
  example_only <- !cohort$is_production
  if (example_only) {
    # Only agents_from_roster() yields a production cohort. The certification
    # series is a CUMULATIVE certification count -- n_retired is 0 in every row
    # and n_active == n_ever_certified -- so a cohort built from it has never had
    # attrition removed, and the synthetic draw is a placeholder outright. Both
    # are legitimate for examples and tests; neither may seed a run whose numbers
    # are meant to be published, which is exactly what strict mode declares.
    msg <- paste0(
      "Cohort source '", cohort$source, "' is not a production cohort: ",
      cohort$note, " Supply agents_from_roster() with a true active roster, or ",
      "treat every output of this run as exploratory."
    )
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  # Keep the hours schedule and the FTE threshold internally consistent.
  # --- Geographic placement ------------------------------------------------
  # The geography layer (opportunity_placement_shares(), entrant placement,
  # mid-career migration) has existed in R/geography-provider_geography since the port and was reachable
  # from nothing: this orchestrator never passed `placement_shares`, so every
  # run was national-headcount-only. Wiring it is most of the work; the rest is
  # making its precondition explicit.
  #
  # The engine keys entrant placement and migration off a `state` column. A
  # cohort built from the certification contract has none -- the contract ships
  # aggregate counts with no geography -- so passing shares alone would leave the
  # layer silently inert, which is the failure mode this repository keeps
  # rediscovering. Say so, and offer an explicit, labelled way to seed it.
  placement_active <- !is.null(placement_shares)
  if (placement_active && !"state" %in% names(agents)) {
    if (isTRUE(seed_base_geography)) {
      agents$state <- assign_entrant_geography(nrow(agents), placement_shares)
      .msg_warn("seed_base_geography = TRUE: the base cohort's states were DRAWN ",
                "from placement_shares, not observed. Geographic results are then ",
                "conditional on that assumption and must not be read as the ",
                "observed distribution of the current workforce.")
    } else {
      placement_active <- FALSE
      .msg_warn("placement_shares was supplied but the cohort has no `state` ",
                "column, so entrant placement and migration would do nothing. ",
                "Supply a roster via agents_from_roster(), or pass ",
                "seed_base_geography = TRUE to draw the base cohort's states ",
                "from the shares as a declared assumption.")
    }
  }
  if (!placement_active) placement_shares <- NULL

  hours_intercept <- calibrate_hours_intercept(agents$age, agents$sex)

  # Parameter uncertainty: the entrant rate is drawn from the observed series'
  # own sampling distribution each iteration. Without this the intervals are
  # sampling noise only, which the 2020->2023 back-test showed to be 6.5-8.2x
  # too narrow.
  # Centred on the resolved `baseline_entrants` so the documented argument
  # actually controls the run; the series supplies only the sampling spread.
  param_spec <- if (is.null(parameter_spec) && has_mufflyaccess()) {
    tryCatch(entrant_spec_from_series(agents, entrant_mean = baseline_entrants),
             error = function(e) NULL)
  } else parameter_spec
  # Unconditional, for the same reason as in run_supply_microsimulation(): a NULL
  # spec is the case the guard exists to catch, and here it is reachable whenever
  # the contract is unavailable or the spec fit fails. Failing at the orchestrator
  # is a clearer error than failing inside the first scenario's engine call.
  assert_parameter_uncertainty(param_spec, mode)

  # --- Supply: one Monte-Carlo microsimulation per scenario ----------------
  supply_by_scenario <- purrr::imap_dfr(supply_scenarios, function(params, scenario_name) {
    if (verbose) .msg_info(sprintf("  supply scenario: %s", params$label))
    sim <- run_supply_microsimulation(
      initial_workforce = agents,
      years = years,
      entrants_per_year = params$entrants,
      subspecialty = subspecialty,
      n_iterations = n_iterations,
      conversion_floor = params$conversion %||% 1.0,
      retirement_schedule = scenario_retirement_schedule(params, base_retirement_schedule),
      # Burnout acts on the age-flat early-exit (career-change) hazard, NOT the
      # age-graded retirement curve, so a scenario scales it here rather than
      # multiplying the retirement schedule (which the registry forbids). The
      # neutral default (multiplier 1) leaves the base hazard untouched.
      career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50 *
        (params$career_change_multiplier %||% 1),
      hours_multiplier = params$hours_multiplier %||% 1.0,
      hours_intercept = hours_intercept,
      # Re-centred on THIS scenario's entrant level. `entrant_mean` takes
      # precedence over `entrants_per_year` inside the engine, so sharing one
      # spec across scenarios silently overrode every scenario's entrant value:
      # "Fellowship output +10%" and "-10%" returned results identical to
      # Baseline to the last digit. The scenario now sets the LEVEL and the
      # observed series still sets the SPREAD.
      param_spec = recentre_entrant_spec(param_spec, params$entrants),
      late_career_fte_factor = params$late_career_fte_factor %||% 1.0,
      late_career_fte_onset_age = params$late_career_fte_onset_age %||% NA_real_,
      placement_shares = placement_shares,
      seed = seed,
      verbose = FALSE
    )
    dplyr::mutate(sim$summary, scenario = scenario_name, scenario_label = params$label)
  })

  # --- Demand: three estimands with DISTINCT age profiles ------------------
  demand_long <- compute_demand_denominators(pop_by_band)

  # D4: BRFSS-derived UI care-seeking demand (appended when cells supplied).
  # assert_estimands_independent() is re-run after appending so the four-
  # estimand concordance check is not skipped.
  if (!is.null(brfss_cells)) {
    d4 <- tryCatch(
      compute_brfss_demand_estimand(pop_by_band, brfss_cells),
      error = function(e) {
        .msg_warn("D4 BRFSS estimand failed (", conditionMessage(e), "); using D1-D3 only")
        NULL
      }
    )
    if (!is.null(d4)) {
      demand_long <- dplyr::bind_rows(demand_long, d4)
      if (verbose) {
        d4_2025 <- d4$demand_cases[d4$year == min(years)]
        .msg_info(sprintf("  D4 (BRFSS UI): %.0f cases in %d", d4_2025[1], min(years)))
      }
    }
  }

  assert_estimands_independent(demand_long, "demand_cases", mode)

  # Demand totals must be anchored to an independent national estimate, which is
  # the HDMM's own step (Exhibit 11: scalar = observed / model-predicted). The
  # machinery has been here all along -- fit_calibration_scalars() is used by
  # R/calibration-demand_lifecourse -- but nothing in a workforce run ever called the guard, so `calibration`
  # was accepted, stored in the metadata, and never checked. An uncalibrated
  # demand total is not anchored to any observed quantity, so it is gated here
  # like the base-year gap and the estimand independence check.
  # The guard moved below the volume build: `calibration = "namcs"` fits the
  # scalar FROM those volumes, so it cannot be checked before they exist.

  # --- Workload -> required FTE (FTE on both sides) ------------------------
  assert_publishable_workload(mode = mode)
  assert_publishable_workload(URPS_DELEGATION_STATUS, allow_analogy = allow_analogy,
                              what = "delegation matrix", mode = mode)
  volumes <- if (!is.null(prevention_scenario)) {
    if (verbose) .msg_info(sprintf("  prevention scenario: %s", prevention_scenario))
    prevention_demand_trajectory(demand_long, scenario_id = prevention_scenario)
  } else {
    example_service_volumes(demand_long)
  }

  # --- Demand calibration against an independent national anchor -----------
  # HDMM Exhibit 11: scalar = observed / model-predicted, fitted on the base
  # year. `calibration` was previously accepted, stored and CHECKED but never
  # APPLIED -- `apply_calibration_scalars()` was called by nothing in a
  # workforce run -- so a caller who supplied scalars silently got uncalibrated
  # output that reported itself as calibrated.
  if (identical(calibration, "namcs")) {
    calibration <- tryCatch(
      namcs_demand_calibration(volumes, base_year = base_year),
      error = function(e) {
        if (identical(mode, "strict")) {
          stop(sprintf("calibration = 'namcs' could not be fitted: %s",
                       conditionMessage(e)), call. = FALSE)
        }
        .msg_warn("NAMCS calibration unavailable (", conditionMessage(e),
                  "); continuing UNCALIBRATED.")
        NULL
      })
  }
  assert_demand_calibrated(calibration, mode)
  if (is.data.frame(calibration) && nrow(calibration) > 0) {
    volumes <- apply_demand_calibration(volumes, calibration)
    if (verbose) {
      .msg_info(sprintf(
        "Demand calibrated to %s: scalar %.3f on %s (anchor %d, %d records).",
        attr(calibration, "provenance")$source %||% "an independent anchor",
        calibration$scalar[1],
        paste(attr(calibration, "services") %||% "visit services", collapse = " + "),
        attr(calibration, "anchor_year") %||% NA_integer_,
        attr(calibration, "anchor_records") %||% NA_integer_))
    }
  }

  # Setting mix is applied AFTER the level correction: calibration fixes how
  # many encounters there are, the setting scenario redistributes them.
  if (!is.null(setting_scenario)) {
    if (verbose) .msg_info(sprintf("  setting scenario: %s", setting_scenario))
    volumes <- apply_setting_scenario(volumes, scenario_id = setting_scenario)
  }

  base_wrvu <- service_volume_to_wrvu(dplyr::filter(volumes, .data$year == base_year))

  gap <- baseline_gap_estimate
  # allow_analogy gates the delegation matrix above; the base-year gap needs the
  # same opt-in and needs it more, because it is the one analogy that reaches the
  # headline shortage undamped.
  assert_baseline_gap_estimated(gap, mode, allow_analogy = allow_analogy)
  base_required <- if (inherits(gap, "urps_baseline_gap")) {
    gap$required_fte
  } else {
    .msg_warn("Falling back to base-year equilibrium (required = supplied). ",
              "This is the assumption the model exists to avoid.")
    baseline_supply
  }

  wrvu_per_fte <- calibrate_wrvu_per_fte(base_wrvu$work_rvu, base_required)
  productivity_ok <- check_productivity_plausible(wrvu_per_fte, mode = mode)
  required <- convert_workload_to_fte(volumes, wrvu_per_fte = wrvu_per_fte)
  required_by_setting <- convert_workload_to_fte(volumes, wrvu_per_fte = wrvu_per_fte,
                                                  by_setting = TRUE)

  # --- Absolute gap, status quo -------------------------------------------
  reference_id <- if ("baseline" %in% names(supply_scenarios)) "baseline" else "status_quo"
  status_quo <- dplyr::filter(supply_by_scenario, .data$scenario == reference_id)
  fte_gap <- compute_fte_gap(status_quo, required, supply_col = "effective_fte_median")

  # --- Relative growth adequacy (explicitly labelled) ---------------------
  growth <- compute_growth_adequacy(status_quo, demand_long, base_year = base_year)
  concordance <- assess_demand_concordance(growth, demand_long)

  # --- Demand scenarios: double-count guard + insurance/income lift ----------
  # When brfss_cells is supplied, project_urps_demand() provides an access-
  # scenario-aware demand estimate for each registered demand scenario.  The
  # lift ratio (scenario / status_quo) is applied to `required` so every demand
  # scenario produces its own gap series against the status-quo supply.
  sq_demand_fte <- if (!is.null(brfss_cells)) {
    sum(project_urps_demand(brfss_cells, access_scenario = "status_quo",
                            verbose = FALSE)$demand_fte, na.rm = TRUE)
  } else NULL

  demand_scenario_gaps <- NULL
  for (nm in names(demand_scenarios)) {
    s <- demand_scenarios[[nm]]
    comps <- s$access_components
    if (length(comps)) assert_access_not_double_counted(gap, comps, mode)

    acc <- s$access_scenario
    if (!is.null(sq_demand_fte) && !is.null(acc) && !identical(nm, "status_quo")) {
      scen_fte <- sum(
        project_urps_demand(brfss_cells, access_scenario = acc,
                            verbose = FALSE)$demand_fte,
        na.rm = TRUE
      )
      demand_lift <- scen_fte / sq_demand_fte
      required_s  <- dplyr::mutate(required,
                                   required_fte = .data$required_fte * demand_lift)
      gap_s <- compute_fte_gap(status_quo, required_s,
                               supply_col = "effective_fte_median")
      gap_s$demand_scenario       <- nm
      gap_s$demand_scenario_label <- s$label
      gap_s$demand_lift           <- demand_lift
      demand_scenario_gaps <- dplyr::bind_rows(demand_scenario_gaps, gap_s)
      if (verbose) {
        .msg_info(sprintf("  demand scenario '%s': +%.1f%% demand vs status quo",
                          s$label, 100 * (demand_lift - 1)))
      }
    }
  }

  # --- Replacement-ratio outlook per supply scenario -----------------------
  crude_rate <- implied_annual_departure_rate(agents$age, agents$sex)
  entrant_check <- if (has_mufflyaccess()) {
    tryCatch(implied_gross_entrants(agents, assumed = baseline_entrants),
             error = function(e) NULL)
  } else NULL
  outlook <- purrr::imap_dfr(supply_scenarios, function(params, scenario_name) {
    sched <- scenario_retirement_schedule(params, base_retirement_schedule)
    rate <- implied_annual_departure_rate(agents$age, agents$sex, retirement_schedule = sched)
    departures <- baseline_supply * rate
    rr <- (params$entrants * (params$conversion %||% 1.0)) / departures
    tibble::tibble(
      scenario = scenario_name,
      scenario_label = params$label,
      annual_entrants_effective = params$entrants * (params$conversion %||% 1.0),
      implied_departure_rate = rate,
      expected_annual_departures = departures,
      replacement_ratio = rr,
      outlook = classify_workforce_outlook(rr)
    )
  })

  projection <- if (has_mufflyaccess()) {
    tryCatch(as_urps_projection(supply_by_scenario, specialty = subspecialty,
                                geography_type = supply_geography,
                                geography_id = if (supply_geography == "conus") "CONUS" else "US"),
             error = function(e) { .msg_warn("Projection contract: ", conditionMessage(e)); NULL })
  } else NULL

  # Extended gap projection: supply + demand + gap in one validated table.
  # Built before validation_report() so the contract check runs as an internal
  # validation gate rather than a post-hoc summary.
  gap_projection <- tryCatch(
    gap_projections_all_scenarios(
      supply_by_scenario, required,
      specialty      = subspecialty,
      geography_type = supply_geography,
      geography_id   = if (supply_geography == "conus") "CONUS" else "US",
      # The cohort's provenance travels INSIDE the exported table, not only in
      # scenario_meta. Every column here is a supply number or derived from one,
      # so a projection that has been saved or handed on must still be able to
      # say whether its supply was measured or reconstructed.
      cohort_basis   = cohort$source,
      observed_share = cohort$observed_share,
      mode           = mode
    ),
    error = function(e) {
      .msg_warn("Gap projection contract failed (", conditionMessage(e), "); skipped.")
      NULL
    }
  )

  validation <- validation_report(supply_by_scenario, required, gap,
                                  gap_projection = gap_projection)
  assert_validation_passed(validation, mode)

  result <- list(
    supply = supply_by_scenario,
    projection = projection,
    gap_projection = gap_projection,
    demand = demand_long,
    service_volumes = volumes,
    required_fte = required,
    required_fte_by_setting = required_by_setting,
    fte_gap = fte_gap,
    demand_scenario_gaps = demand_scenario_gaps,
    baseline_gap = gap,
    growth_adequacy = growth,
    concordance = concordance,
    outlook = outlook,
    validation = validation,
    calibration = calibration,
    run_id = run_id,
    scenario_meta = list(
      subspecialty = subspecialty,
      baseline_supply = baseline_supply,
      supply_geography = supply_geography,
      population_source = population_source,
      retirement_source = retirement_source,
      # The @param has always said allow_analogy is declared in the metadata; it
      # was not actually carried here. It is the one argument that can suppress a
      # strict-mode stop, so a reader of the artifact must be able to see whether
      # it was set without re-reading the call that produced it.
      allow_analogy = allow_analogy,
      supply_contract = contract,
      example_only = example_only,
      cohort_provenance = cohort,
      cohort_composition = cohort_composition(agents),
      # Whether the run was national-only or geographically resolved, and on what
      # basis -- a reader cannot tell from the supply panel alone.
      geographic_placement = list(
        active = placement_active,
        n_geographies = if (placement_active) nrow(placement_shares) else 0L,
        base_geography_seeded = placement_active && isTRUE(seed_base_geography)
      ),
      demand_calibrated = is.data.frame(calibration) && nrow(calibration) > 0,
      fte_definition = fte_definition(),
      hours_intercept = hours_intercept,
      parameter_spec = param_spec,
      wrvu_per_fte = wrvu_per_fte,
      productivity_plausible = productivity_ok,
      crude_departure_rate = crude_rate,
      baseline_entrants = baseline_entrants,
      entrants_source = entrants_source,
      entrant_reconciliation = entrant_check,
      years = range(years),
      n_iterations = n_iterations,
      scenario_registry_version = ssot_scenario_registry_version() %||% SCENARIO_REGISTRY_VERSION,
      reference_scenario = reference_id,
      ssot_provenance = ssot_provenance(),
      ssot_coverage = ssot_coverage_report(),
      workload_status = urps_service_workload_status(),
      # Validation status of the engine that produced `supply`, carried on the
      # run itself so it reaches anyone reading the projection or the gap.
      backtest = backtest_status(),
      interval_label = interval_label(),
      calibration = calibration_status_report(),
      hours_status = reference_hours_status(),
      mode = mode,
      seed = seed,
      prevention_scenario = prevention_scenario,
      setting_scenario = setting_scenario
    )
  )

  if (!is.null(output_dir)) {
    artifact_path <- file.path(output_dir, sprintf("workforce_microsim_%s.rds", run_id))
    write_artifact_with_provenance(
      result, artifact_path,
      inputs = list(baseline_supply, years, supply_scenarios, pop_by_band, gap),
      code_paths = file.path("R", MICROSIM_MODULES),
      run_id = run_id, mode = mode,
      source = sprintf("workforce microsimulation (%s)", subspecialty)
    )
  }

  if (verbose) {
    final_year <- max(years)
    if (!is.null(gap_projection)) {
      fin <- dplyr::filter(gap_projection,
                           .data$year == final_year,
                           .data$scenario_id == reference_id)
      if (nrow(fin) == 0) fin <- dplyr::filter(gap_projection, .data$year == final_year)[1, ]
      .msg_info(sprintf(
        "Status quo %d: %.0f hc / %.0f FTE supplied  vs  %.0f hc / %.0f FTE required  (gap %.0f FTE, %.1f%%)",
        final_year,
        fin$supply_headcount, fin$supply_clinical_fte,
        fin$demand_headcount,  fin$demand_clinical_fte,
        fin$gap_fte,
        if (!is.null(fin$gap_pct) && !is.na(fin$gap_pct)) fin$gap_pct else
          100 * fin$gap_fte / fin$demand_clinical_fte
      ))
    } else {
      fin <- dplyr::filter(fte_gap, .data$year == final_year)
      .msg_info(sprintf("Status quo %d: %.0f FTE supplied vs %.0f required (gap %.0f FTE, %.1f%%)",
                        final_year, fin$supplied_fte, fin$required_fte, fin$gap_fte, fin$gap_pct))
    }
    .msg_info(sprintf("Demand concordance informative = %s", concordance$informative))
  }

  result
}
