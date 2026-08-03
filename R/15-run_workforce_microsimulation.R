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
  "00-paths.R",
  "10-repro_provenance.R",
  "11-canonical_and_joins.R",
  "16-provider_lifecycle.R",
  "12-provider_microsimulation.R",
  "13-demand_urps.R",
  "14-spatial_access_e2sfca.R",
  "17-workload_to_fte.R",
  "18-baseline_gap.R",
  "19-scenario_registry.R",
  "20-provider_geography.R",
  "21-calibration_validation.R",
  "22-legacy_loader.R"
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
#' @export
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
#' physical-therapy distribution for urogynaecology is an explicit assumption and
#' is recorded as one in the returned gap object's evidence ledger.
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
#' @param baseline_entrants Baseline annual entrants. `nrmp_entrants("URPS")`
#'   supplies the real NRMP-matched value (70).
#' @param retirement_source Base retirement hazard: "hwsm" (default, the
#'   HWSM/FutureDocs literature curve) or "urps_empirical" (cliff's observed
#'   URPS hazards for ages 50-69 with the HWSM tail past 70). Scenario age-shifts
#'   apply on top of whichever base is chosen.
#' @param calibration Optional calibration scalars from
#'   [fit_calibration_scalars()].
#' @param parameter_spec Optional [supply_parameter_spec()]; defaults to one
#'   built from the observed certification series.
#' @param allow_analogy Permit inputs derived by analogy from another specialty
#'   (currently the delegation matrix). Declared in the run metadata either way.
#' @param brfss_cells Optional population cell table from
#'   [build_urps_population_cells()].  When non-NULL, a fourth demand estimand
#'   D4 (BRFSS survey-weighted UI care-seeking demand) is appended to
#'   `demand_long` and flows through concordance assessment alongside D1-D3.
#'   Requires that [brfss_pfd_prevalence_for_demand_bands()] is available (i.e.
#'   that R/44-urps_population.R is loaded).
#' @param prevention_scenario Character key in [URPS_PREVENTION_SCENARIOS], or
#'   `NULL` (default). When non-NULL, service volumes are adjusted by
#'   [apply_named_prevention_scenario()] before [convert_workload_to_fte()],
#'   modelling conservative PFD management (PT/pessary) as a demand-side
#'   multiplier. Use `"baseline"` to confirm the no-shift reference.
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
                                          baseline_entrants = 55,
                                          retirement_source = c("hwsm", "urps_empirical"),
                                          calibration = NULL,
                                          parameter_spec = NULL,
                                          allow_analogy = TRUE,
                                          brfss_cells = NULL,
                                          prevention_scenario = NULL,
                                          output_dir = NULL,
                                          seed = 20260801L,
                                          verbose = TRUE) {
  supply_geography <- match.arg(supply_geography)
  retirement_source <- match.arg(retirement_source)
  mode <- resolve_reproducibility_mode()
  seed_microsimulation(seed, mode)
  run_id <- make_run_id(paste0("workforce_", tolower(subspecialty)), seed, mode)

  # Base retirement schedule: the HWSM/FutureDocs literature curve by default, or
  # cliff's empirical URPS hazards (observed 50-69 + HWSM tail past 70) opt-in.
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
  if (example_only) .msg_warn("Cohort source '", cohort$source, "': ", cohort$note)

  # Keep the hours schedule and the FTE threshold internally consistent.
  hours_intercept <- calibrate_hours_intercept(agents$age, agents$sex)

  # Parameter uncertainty: the entrant rate is drawn from the observed series'
  # own sampling distribution each iteration. Without this the intervals are
  # sampling noise only, which the 2020->2023 back-test showed to be 6.5-8.2x
  # too narrow.
  param_spec <- if (is.null(parameter_spec) && has_mufflyaccess()) {
    tryCatch(entrant_spec_from_series(agents), error = function(e) NULL)
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
      hours_multiplier = params$hours_multiplier %||% 1.0,
      hours_intercept = hours_intercept,
      param_spec = param_spec,
      late_career_fte_factor = params$late_career_fte_factor %||% 1.0,
      late_career_fte_onset_age = params$late_career_fte_onset_age %||% NA_real_,
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
  base_wrvu <- service_volume_to_wrvu(dplyr::filter(volumes, .data$year == base_year))

  gap <- baseline_gap_estimate
  assert_baseline_gap_estimated(gap, mode)
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

  # --- Absolute gap, status quo -------------------------------------------
  reference_id <- if ("baseline" %in% names(supply_scenarios)) "baseline" else "status_quo"
  status_quo <- dplyr::filter(supply_by_scenario, .data$scenario == reference_id)
  fte_gap <- compute_fte_gap(status_quo, required, supply_col = "effective_fte_median")

  # --- Relative growth adequacy (explicitly labelled) ---------------------
  growth <- compute_growth_adequacy(status_quo, demand_long, base_year = base_year)
  concordance <- assess_demand_concordance(growth, demand_long)

  # --- Demand scenarios: guard against access double-counting --------------
  for (nm in names(demand_scenarios)) {
    comps <- demand_scenarios[[nm]]$access_components
    if (length(comps)) assert_access_not_double_counted(gap, comps, mode)
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

  validation <- validation_report(supply_by_scenario, required, gap)
  assert_validation_passed(validation, mode)

  projection <- if (has_mufflyaccess()) {
    tryCatch(as_urps_projection(supply_by_scenario, specialty = subspecialty,
                                geography_type = supply_geography,
                                geography_id = if (supply_geography == "conus") "CONUS" else "US"),
             error = function(e) { .msg_warn("Projection contract: ", conditionMessage(e)); NULL })
  } else NULL

  result <- list(
    supply = supply_by_scenario,
    projection = projection,
    demand = demand_long,
    service_volumes = volumes,
    required_fte = required,
    fte_gap = fte_gap,
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
      supply_contract = contract,
      example_only = example_only,
      cohort_provenance = cohort,
      cohort_composition = cohort_composition(agents),
      fte_definition = fte_definition(),
      hours_intercept = hours_intercept,
      parameter_spec = param_spec,
      wrvu_per_fte = wrvu_per_fte,
      productivity_plausible = productivity_ok,
      crude_departure_rate = crude_rate,
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
      prevention_scenario = prevention_scenario
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
    fin <- dplyr::filter(fte_gap, .data$year == final_year)
    .msg_info(sprintf("Status quo %d: %.0f FTE supplied vs %.0f required (gap %.0f FTE, %.1f%%)",
                      final_year, fin$supplied_fte, fin$required_fte, fin$gap_fte, fin$gap_pct))
    .msg_info(sprintf("Demand concordance informative = %s", concordance$informative))
  }

  result
}
