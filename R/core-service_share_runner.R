# Service-share aware end-to-end runner -------------------------------------
#
# This file collates after `core-run_end_to_end_simulation.R`. Capture the exact
# pre-service-share implementation so `service_share_engine = "legacy_matrix"`
# is a true regression-preserving dispatch, not a reimplementation.
.run_end_to_end_simulation_legacy <- run_end_to_end_simulation


.service_share_empirical_value <- function(parameters, parameter, fallback) {
  if (parameter %in% base::names(parameters)) {
    estimate <- parameters[[parameter]]
    if (base::is.finite(estimate)) {
      base::message(
        "Empirical input: ", parameter, " = ",
        base::format(estimate, big.mark = ",", scientific = FALSE)
      )
      return(estimate)
    }
  }
  fallback
}


.service_share_validate_runner_inputs <- function(
    start_year,
    end_year,
    initial_provider_count,
    fellowship_entrants,
    app_delegation_rate,
    geography_engine,
    entrant_engine,
    productivity_engine,
    county_market_tbl,
    entrant_donor_pool,
    productivity_panel,
    productivity_model) {
  if (!base::is.numeric(start_year) || !base::is.numeric(end_year) ||
      base::length(start_year) != 1L || base::length(end_year) != 1L ||
      base::is.na(start_year) || base::is.na(end_year) ||
      start_year > end_year) {
    base::stop(
      "`start_year` must be less than or equal to `end_year`.",
      call. = FALSE
    )
  }
  if (!base::is.numeric(initial_provider_count) ||
      initial_provider_count < 1) {
    base::stop("`initial_provider_count` must be positive.", call. = FALSE)
  }
  if (!base::is.numeric(fellowship_entrants) || fellowship_entrants < 0) {
    base::stop("`fellowship_entrants` must be nonnegative.", call. = FALSE)
  }
  if (!base::is.numeric(app_delegation_rate) ||
      app_delegation_rate < 0 || app_delegation_rate > 0.30) {
    base::stop(
      "`app_delegation_rate` must be between 0 and 0.30.",
      call. = FALSE
    )
  }
  if (geography_engine == "county_endogenous" &&
      !base::is.data.frame(county_market_tbl)) {
    base::stop(
      "`county_market_tbl` is required for county endogenous geography.",
      call. = FALSE
    )
  }
  if (entrant_engine == "joint_donor_sampling" &&
      !base::is.data.frame(entrant_donor_pool)) {
    base::stop(
      "`entrant_donor_pool` is required for joint donor sampling.",
      call. = FALSE
    )
  }
  if (productivity_engine == "lmer_fitted" &&
      base::is.null(productivity_model) &&
      !base::is.data.frame(productivity_panel)) {
    base::stop(
      "Supply `productivity_model` or `productivity_panel` for lmer_fitted.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}


.service_share_initial_provider_cohort <- function(initial_provider_count) {
  tibble::tibble(
    provider_id = base::sprintf(
      "NPI%07d",
      base::seq_len(initial_provider_count)
    ),
    age = stats::runif(initial_provider_count, 32, 68),
    years_certified = base::pmax(
      0,
      stats::runif(initial_provider_count, 0, 35)
    ),
    academic_setting = stats::rbinom(
      initial_provider_count,
      1L,
      0.22
    ) == 1L,
    hospital_outpatient = stats::rbinom(
      initial_provider_count,
      1L,
      0.35
    ) == 1L,
    hrr_code = base::sprintf(
      "HRR%03d",
      base::sample.int(306L, initial_provider_count, replace = TRUE)
    ),
    county_fips = NA_character_,
    state_abbr = "US",
    svi = stats::runif(initial_provider_count, 0.1, 0.9),
    fte = stats::runif(initial_provider_count, 0.7, 1.0),
    active = TRUE
  )
}


.service_share_provider_capacity <- function(
    active_providers,
    simulation_year,
    productivity_engine,
    fitted_productivity_model,
    productivity_predictor) {
  if (productivity_engine == "benchmark") {
    return(active_providers |>
      dplyr::transmute(
        provider_id = .data$provider_id,
        annual_patient_capacity = .data$fte * 1600
      ))
  }

  if (base::is.function(productivity_predictor)) {
    predicted <- productivity_predictor(
      fitted_productivity_model,
      active_providers,
      simulation_year
    )
  } else {
    bridged <- .lmer_fitted_predictor_bridge(active_providers)
    predicted_panel <- predict_provider_capacity(
      fitted_productivity_model,
      bridged
    )
    if (!base::identical(
      predicted_panel$capacity_outcome[[1L]],
      "encounters_per_clinical_fte"
    )) {
      base::stop(
        "Calibrated runner requires an encounters_per_clinical_fte ",
        "productivity outcome for patient-capacity gating.",
        call. = FALSE
      )
    }
    predicted <- predicted_panel |>
      dplyr::transmute(
        provider_id = .data$provider_id,
        predicted_capacity = .data$predicted_capacity
      )
  }

  if (base::is.data.frame(predicted)) {
    capacity_columns <- base::intersect(
      base::c("annual_patient_capacity", "predicted_capacity", "capacity"),
      base::names(predicted)
    )
    if (!"provider_id" %in% base::names(predicted) ||
        base::length(capacity_columns) == 0L) {
      base::stop(
        "Productivity prediction must contain provider_id and capacity.",
        call. = FALSE
      )
    }
    capacity_column <- capacity_columns[[1L]]
    result <- predicted |>
      dplyr::transmute(
        provider_id = .data$provider_id,
        annual_patient_capacity = base::as.numeric(
          .data[[capacity_column]]
        )
      )
  } else {
    if (base::length(predicted) != base::nrow(active_providers)) {
      base::stop(
        "Numeric productivity prediction length does not match providers.",
        call. = FALSE
      )
    }
    result <- active_providers |>
      dplyr::transmute(
        provider_id = .data$provider_id,
        annual_patient_capacity = base::as.numeric(predicted)
      )
  }

  if (base::any(!base::is.finite(result$annual_patient_capacity)) ||
      base::any(result$annual_patient_capacity < 0)) {
    base::stop(
      "Fitted productivity predictions must be finite and nonnegative.",
      call. = FALSE
    )
  }
  result
}


.service_share_policy_bridge <- function(
    years,
    base_population_2025,
    fellowship_entrants,
    provider_cohort,
    policy_migration_scenario,
    policy_evidence_db,
    seed) {
  active <- !base::identical(policy_migration_scenario, "baseline") &&
    !base::is.null(policy_evidence_db)
  if (!active) {
    return(base::list(
      active = FALSE,
      multipliers = NULL,
      summary = NULL,
      calibration = NULL
    ))
  }

  state_population_shares <- national_older_female_population_by_state()
  state_crosswalk <- state_population_shares |>
    dplyr::select(.data$state, .data$state_fips)
  initial_provider_fte <- base::sum(provider_cohort$fte, na.rm = TRUE)
  baseline <- purrr::map_dfr(years, function(year) {
    population <- base_population_2025 * 1.006^(year - 2025L)
    cases <- population * (0.245 + 0.001 * (year - 2025L))
    state_population_shares |>
      dplyr::transmute(
        state = .data$state,
        year = year,
        female_older_population = .data$share * population,
        pfd_demand = .data$share * cases,
        provider_fte = .data$share * initial_provider_fte,
        fellowship_applications = .data$share * fellowship_entrants
      )
  })

  con <- open_policy_migration_duckdb(policy_evidence_db)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  evidence <- build_policy_migration_evidence(con, state_crosswalk)
  calibration <- calibrate_legislative_relocation(
    state_year_history = baseline,
    evidence = evidence
  )
  simulated <- simulate_policy_migration_scenarios(
    baseline,
    evidence = evidence,
    scenario = policy_migration_scenario,
    seed = seed,
    calibration = calibration
  )
  multipliers <- simulated |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      demand_multiplier = base::sum(.data$pfd_demand_scenario) /
        base::sum(.data$pfd_demand),
      provider_multiplier = base::sum(.data$provider_fte_scenario) /
        base::sum(.data$provider_fte),
      application_multiplier = base::sum(
        .data$fellowship_applications_scenario
      ) / base::sum(.data$fellowship_applications),
      .groups = "drop"
    )

  base::list(
    active = TRUE,
    multipliers = multipliers,
    summary = summarize_policy_migration_scenarios(simulated),
    calibration = calibration
  )
}


.service_share_spatial_balance <- function(
    geography_engine,
    active_providers,
    required_fte,
    simulation_year,
    county_market_tbl) {
  if (geography_engine == "hrr_balance") {
    hrr_reference <- tibble::tibble(
      hrr_code = base::sprintf("HRR%03d", base::seq_len(306L)),
      hrr_name = base::paste("HRR Region", base::seq_len(306L))
    )
    hrr_demand <- tibble::tibble(
      hrr_code = hrr_reference$hrr_code,
      demand_fte = required_fte / 306
    )
    return(aggregate_hrr_workforce_balance(
      provider_roster = active_providers,
      hrr_demand_tbl = hrr_demand,
      hrr_reference_tbl = hrr_reference,
      shortage_threshold = 0.20,
      expected_hrr_n = 306L
    ) |>
      dplyr::mutate(
        year = simulation_year,
        geography_engine = geography_engine
      ))
  }

  year_market <- if ("year" %in% base::names(county_market_tbl)) {
    county_market_tbl |>
      dplyr::filter(.data$year == simulation_year)
  } else {
    county_market_tbl
  }
  if (!base::all(c("county_fips", "demand_weight") %in%
      base::names(year_market))) {
    base::stop(
      "County markets must contain county_fips and demand_weight.",
      call. = FALSE
    )
  }
  county_demand <- year_market |>
    dplyr::mutate(
      demand_share = .data$demand_weight /
        base::sum(.data$demand_weight, na.rm = TRUE),
      demand_fte = required_fte * .data$demand_share
    )
  active_providers |>
    dplyr::group_by(.data$county_fips) |>
    dplyr::summarise(
      supplied_fte = base::sum(.data$fte, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::full_join(
      county_demand |>
        dplyr::select(.data$county_fips, .data$demand_fte),
      by = "county_fips"
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::c(.data$supplied_fte, .data$demand_fte),
        ~ tidyr::replace_na(.x, 0)
      ),
      fte_gap = .data$supplied_fte - .data$demand_fte,
      year = simulation_year,
      geography_engine = geography_engine
    )
}


.service_share_practice_economics <- function(
    active_providers,
    provider_workload,
    payer_mix,
    app_delegation_rate,
    draws,
    seed,
    simulation_year) {
  practice_tbl <- active_providers |>
    dplyr::select(
      .data$provider_id,
      .data$academic_setting,
      .data$hospital_outpatient,
      .data$fte
    ) |>
    dplyr::inner_join(provider_workload, by = "provider_id") |>
    dplyr::transmute(
      practice_id = .data$provider_id,
      year = simulation_year,
      clinical_fte = .data$clinical_fte,
      annual_wrvu = .data$annual_wrvu,
      medicare_share = payer_mix$medicare_share,
      medicaid_share = payer_mix$medicaid_share,
      commercial_share = payer_mix$commercial_share,
      self_pay_share = payer_mix$self_pay_share,
      practice_setting = dplyr::case_when(
        .data$academic_setting ~ "academic",
        .data$hospital_outpatient ~ "hospital_employed",
        TRUE ~ "independent"
      ),
      app_fte = .data$fte * app_delegation_rate
    )

  result <- simulate_practice_economics(
    practice_tbl,
    draws = draws,
    seed = seed
  )
  result_draws <- result$draws
  summary <- result$summary |>
    dplyr::summarise(
      year = simulation_year,
      n_practices = dplyr::n(),
      input_annual_wrvu = base::sum(practice_tbl$annual_wrvu),
      provider_workload_wrvu = base::sum(provider_workload$annual_wrvu),
      mean_wrvu_per_fte = base::sum(practice_tbl$annual_wrvu) /
        base::sum(practice_tbl$clinical_fte),
      mean_revenue_per_fte = base::mean(
        result_draws$gross_revenue / result_draws$clinical_fte
      ),
      mean_expense_per_fte = base::mean(
        result_draws$operating_cost / result_draws$clinical_fte
      ),
      mean_operating_margin = base::mean(
        .data$median_operating_margin,
        na.rm = TRUE
      ),
      mean_loss_probability = base::mean(
        .data$loss_probability,
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  base::list(summary = summary, result = result)
}


.run_end_to_end_simulation_calibrated <- function(
    start_year,
    end_year,
    n_agents,
    initial_provider_count,
    fellowship_entrants,
    app_delegation_rate,
    medicaid_fee_ratio,
    geography_engine,
    entrant_engine,
    productivity_engine,
    county_market_tbl,
    entrant_donor_pool,
    productivity_panel,
    productivity_model,
    geography_solver,
    entrant_simulator,
    productivity_fitter,
    productivity_predictor,
    geography_control,
    evidence_db,
    empirical_parameters,
    policy_migration_scenario,
    policy_evidence_db,
    run_practice_economics,
    practice_payer_mix,
    practice_economics_draws,
    seed,
    save_outputs,
    output_dir,
    service_share_bundle,
    service_share_draw) {
  validate_service_share_bundle(service_share_bundle)
  .service_share_validate_runner_inputs(
    start_year,
    end_year,
    initial_provider_count,
    fellowship_entrants,
    app_delegation_rate,
    geography_engine,
    entrant_engine,
    productivity_engine,
    county_market_tbl,
    entrant_donor_pool,
    productivity_panel,
    productivity_model
  )

  if (!base::is.null(evidence_db) && base::is.null(empirical_parameters)) {
    empirical_parameters <- read_urps_empirical_parameters(evidence_db)
  }
  if (base::is.null(empirical_parameters)) {
    empirical_parameters <- stats::setNames(base::numeric(), base::character())
  }

  years <- base::seq.int(base::as.integer(start_year), base::as.integer(end_year))
  base::set.seed(base::as.integer(seed))
  provider_cohort <- .service_share_initial_provider_cohort(
    initial_provider_count
  )
  base::message(
    "Starting calibrated service-share simulation for ",
    start_year, "-", end_year, "."
  )

  fitted_productivity_model <- productivity_model
  if (productivity_engine == "lmer_fitted" &&
      base::is.null(fitted_productivity_model)) {
    fitted_productivity_model <- productivity_fitter(productivity_panel)
  }

  base_population_2025 <- .service_share_empirical_value(
    empirical_parameters,
    "female_population_20plus",
    132500000
  )
  fellowship_entrants <- .service_share_empirical_value(
    empirical_parameters,
    "annual_fellowship_entrants",
    fellowship_entrants
  )

  available_draws <- base::sort(base::unique(
    service_share_bundle$share_draws$draw_id
  ))
  if (base::is.null(service_share_draw)) {
    draw_index <- (base::abs(base::as.integer(seed)) %%
      base::length(available_draws)) + 1L
    service_share_draw <- available_draws[[draw_index]]
  }
  if (!service_share_draw %in% available_draws) {
    base::stop(
      "Requested service_share_draw is absent from service_share_bundle.",
      call. = FALSE
    )
  }

  policy <- .service_share_policy_bridge(
    years,
    base_population_2025,
    fellowship_entrants,
    provider_cohort,
    policy_migration_scenario,
    policy_evidence_db,
    seed
  )
  if (run_practice_economics && base::is.null(practice_payer_mix)) {
    practice_payer_mix <- practice_payer_mix_defaults(
      include_crosscheck = FALSE
    )
  }

  audit_rows <- base::vector("list", base::length(years))
  balance_rows <- base::vector("list", base::length(years))
  engine_rows <- base::vector("list", base::length(years))
  service_share_rows <- base::vector("list", base::length(years))
  provider_workload_rows <- base::vector("list", base::length(years))
  service_workload_rows <- base::vector("list", base::length(years))
  practice_rows <- base::vector("list", base::length(years))
  policy_rows <- base::vector("list", base::length(years))

  for (year_index in base::seq_along(years)) {
    simulation_year <- years[[year_index]]
    base::message("Running calibrated year ", simulation_year, ".")

    population_total <- base_population_2025 *
      1.006^(simulation_year - 2025L)
    prevalence_rate <- 0.245 + 0.001 * (simulation_year - 2025L)
    incident_rate <- 0.022
    prevalent_cases <- population_total * prevalence_rate
    incident_cases <- population_total * incident_rate

    policy_demand_multiplier <- 1
    policy_provider_multiplier <- 1
    policy_application_multiplier <- 1
    if (policy$active) {
      multiplier <- policy$multipliers |>
        dplyr::filter(.data$year == simulation_year)
      if (base::nrow(multiplier) == 1L) {
        policy_demand_multiplier <- multiplier$demand_multiplier[[1L]]
        policy_provider_multiplier <- multiplier$provider_multiplier[[1L]]
        policy_application_multiplier <-
          multiplier$application_multiplier[[1L]]
      }
      prevalent_cases <- prevalent_cases * policy_demand_multiplier
      incident_cases <- incident_cases * policy_demand_multiplier
    }

    care_seeking_count <- prevalent_cases * 0.380
    referred_count <- care_seeking_count * 0.420
    active_providers <- provider_cohort |>
      dplyr::filter(.data$active)

    geography_iterations <- NA_integer_
    geography_converged <- NA
    if (geography_engine == "county_endogenous") {
      year_markets <- if ("year" %in% base::names(county_market_tbl)) {
        county_market_tbl |>
          dplyr::filter(.data$year == simulation_year)
      } else {
        county_market_tbl
      }
      geography_arguments <- base::c(
        base::list(
          provider_roster = active_providers,
          county_market_tbl = year_markets,
          year = simulation_year
        ),
        geography_control
      )
      solution <- base::do.call(geography_solver, geography_arguments)
      if (base::is.data.frame(solution)) {
        relocated <- solution
      } else {
        relocated <- solution$provider_roster
        geography_iterations <- .urps_null_or(
          solution$iterations,
          NA_integer_
        )
        geography_converged <- .urps_null_or(solution$converged, NA)
      }
      if (!base::all(c("provider_id", "county_fips") %in%
          base::names(relocated))) {
        base::stop(
          "Geography solver must return provider_id and county_fips.",
          call. = FALSE
        )
      }
      provider_cohort <- provider_cohort |>
        dplyr::select(-dplyr::any_of("county_fips")) |>
        dplyr::left_join(
          relocated |>
            dplyr::select(.data$provider_id, .data$county_fips),
          by = "provider_id"
        )
      active_providers <- provider_cohort |>
        dplyr::filter(.data$active)
    }

    reachable_count <- referred_count * 0.885
    medicaid_acceptance <- predict_medicaid_acceptance(
      academic_setting = active_providers$academic_setting,
      hospital_outpatient = active_providers$hospital_outpatient,
      medicaid_fee_ratio = medicaid_fee_ratio,
      svi = active_providers$svi,
      years_certified = active_providers$years_certified
    )
    mean_medicaid_acceptance <- base::mean(
      medicaid_acceptance,
      na.rm = TRUE
    )
    medicaid_eligible_count <- reachable_count * 0.20
    appointment_requests <- reachable_count * 0.80 +
      medicaid_eligible_count * mean_medicaid_acceptance

    supplied_fte <- base::sum(active_providers$fte, na.rm = TRUE)
    provider_capacity <- .service_share_provider_capacity(
      active_providers,
      simulation_year,
      productivity_engine,
      fitted_productivity_model,
      productivity_predictor
    )
    # Calibrated routing already contains the APP share of delivered services.
    # Applying the historical APP capacity multiplier here would delegate twice.
    clinical_capacity <- base::sum(
      provider_capacity$annual_patient_capacity,
      na.rm = TRUE
    )
    served_patients <- base::pmin(appointment_requests, clinical_capacity)
    unserved_delayed <- base::pmax(
      0,
      appointment_requests - served_patients
    )
    if (!base::isTRUE(base::all.equal(
      served_patients + unserved_delayed,
      appointment_requests,
      tolerance = 1e-6
    ))) {
      base::stop(
        "Patient-flow conservation failed in year ",
        simulation_year, ".",
        call. = FALSE
      )
    }

    treated_counts <- base::c(
      ui = served_patients * 0.45,
      pop = served_patients * 0.35,
      ai = served_patients * 0.20
    )
    routing <- service_share_routing_for_year(
      service_share_bundle,
      year = simulation_year,
      draw_id = service_share_draw,
      required_services = service_share_required_routing_services()
    )
    routed_services <- pathway_provider_service_volumes(
      treated = treated_counts,
      year = simulation_year,
      routing = routing,
      prior_only = "apply"
    )

    route_check <- routed_services |>
      dplyr::group_by(.data$service) |>
      dplyr::summarise(
        demand_volume = dplyr::first(.data$volume),
        allocated_volume = base::sum(.data$provider_volume),
        .groups = "drop"
      )
    routed_volume_error <- base::sum(
      route_check$allocated_volume - route_check$demand_volume
    )
    if (base::abs(routed_volume_error) > 1e-6) {
      base::stop(
        "Provider routing failed service-volume conservation in year ",
        simulation_year, ".",
        call. = FALSE
      )
    }

    workload <- allocate_urps_service_workload(routed_services)
    delivered_services <- workload$total_urps_services
    wrvu_total <- workload$total_urps_wrvu
    physician_minutes <- wrvu_total * 42
    required_fte <- wrvu_total / 1400
    fte_gap <- supplied_fte - required_fte
    adequacy_ratio <- supplied_fte / required_fte

    provider_workload <- allocate_urps_workload_to_active_providers(
      active_providers,
      total_urps_wrvu = wrvu_total,
      year = simulation_year
    )
    provider_workload_rows[[year_index]] <- provider_workload
    service_workload_rows[[year_index]] <- workload$service_workload

    balance_rows[[year_index]] <- .service_share_spatial_balance(
      geography_engine,
      active_providers,
      required_fte,
      simulation_year,
      county_market_tbl
    )

    wait_days <- 14 + dplyr::if_else(
      appointment_requests > 0,
      unserved_delayed / appointment_requests * 45,
      0
    )
    travel_minutes <- 22.5 +
      (1 - supplied_fte / required_fte) * 8
    mean_provider_capacity <- base::mean(
      provider_capacity$annual_patient_capacity,
      na.rm = TRUE
    )

    audit_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      population = population_total,
      incident_cases = incident_cases,
      prevalent_cases = prevalent_cases,
      care_seeking_n = care_seeking_count,
      referred_n = referred_count,
      reachable_n = reachable_count,
      medicaid_eligible_n = medicaid_eligible_count,
      appointment_requests_n = appointment_requests,
      served_patients_n = served_patients,
      unserved_delayed_n = unserved_delayed,
      delivered_services_n = delivered_services,
      wrvu_total = wrvu_total,
      physician_minutes_total = physician_minutes,
      required_fte = required_fte,
      supplied_fte = supplied_fte,
      fte_gap = fte_gap,
      adequacy_ratio = adequacy_ratio,
      mean_wait_days = wait_days,
      mean_travel_mins = travel_minutes,
      mean_provider_capacity = mean_provider_capacity,
      geography_engine = geography_engine,
      entrant_engine = entrant_engine,
      productivity_engine = productivity_engine
    )

    provider_totals <- routed_services |>
      dplyr::group_by(.data$provider_group) |>
      dplyr::summarise(
        volume = base::sum(.data$provider_volume),
        .groups = "drop"
      )
    provider_volume <- function(group) {
      value <- provider_totals$volume[
        provider_totals$provider_group == group
      ]
      if (base::length(value) == 0L) 0 else base::sum(value)
    }
    total_allocated <- base::sum(provider_totals$volume)
    service_share_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      service_share_draw = service_share_draw,
      evidence_year_min = base::min(routing$evidence_year),
      evidence_year_max = base::max(routing$evidence_year),
      pathway_service_volume = base::sum(route_check$demand_volume),
      allocated_service_volume = total_allocated,
      urps_service_volume = provider_volume("urps"),
      app_service_volume = provider_volume("app"),
      other_service_volume = total_allocated -
        provider_volume("urps") - provider_volume("app"),
      routed_volume_error = routed_volume_error,
      urps_wrvu = wrvu_total,
      provider_wrvu_error = base::sum(provider_workload$annual_wrvu) -
        wrvu_total,
      app_capacity_multiplier_applied = FALSE,
      condition_marginalization = "evidence-weighted within service"
    )

    if (run_practice_economics) {
      practice <- .service_share_practice_economics(
        active_providers,
        provider_workload,
        practice_payer_mix,
        app_delegation_rate,
        practice_economics_draws,
        seed + year_index,
        simulation_year
      )
      practice_rows[[year_index]] <- practice$summary
    }

    provider_cohort <- provider_cohort |>
      dplyr::mutate(
        age = .data$age + 1,
        years_certified = .data$years_certified +
          base::as.numeric(.data$active)
      )
    exit_probability <- 0.015 + (provider_cohort$age / 100)^3.5
    exits <- stats::rbinom(
      base::nrow(provider_cohort),
      1L,
      exit_probability
    ) == 1L
    provider_cohort$active[exits] <- FALSE

    entrant_count <- base::as.integer(base::round(
      fellowship_entrants * policy_application_multiplier
    ))
    if (entrant_engine == "parametric") {
      new_entrants <- tibble::tibble(
        provider_id = base::sprintf(
          "NPI%07d",
          initial_provider_count + year_index * 100L +
            base::seq_len(entrant_count)
        ),
        age = stats::runif(entrant_count, 31, 34),
        years_certified = 0,
        academic_setting = stats::rbinom(
          entrant_count,
          1L,
          0.30
        ) == 1L,
        hospital_outpatient = stats::rbinom(
          entrant_count,
          1L,
          0.40
        ) == 1L,
        hrr_code = base::sprintf(
          "HRR%03d",
          base::sample.int(306L, entrant_count, replace = TRUE)
        ),
        county_fips = NA_character_,
        state_abbr = "US",
        svi = stats::runif(entrant_count, 0.1, 0.9),
        fte = 1,
        active = TRUE
      )
    } else {
      new_entrants <- entrant_simulator(
        n_entrants = entrant_count,
        donor_pool = entrant_donor_pool,
        entry_year = simulation_year,
        seed = seed + year_index
      ) |>
        dplyr::mutate(
          provider_id = base::sprintf(
            "NPI%07d",
            initial_provider_count + year_index * 100L +
              dplyr::row_number()
          ),
          years_certified = 0,
          active = TRUE
        )
      missing_provider_columns <- base::setdiff(
        base::names(provider_cohort),
        base::names(new_entrants)
      )
      if (base::length(missing_provider_columns) > 0L) {
        new_entrants[missing_provider_columns] <- NA
      }
      new_entrants <- new_entrants |>
        dplyr::select(dplyr::all_of(base::names(provider_cohort)))
    }
    provider_cohort <- dplyr::bind_rows(provider_cohort, new_entrants)

    engine_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      active_provider_n = base::sum(provider_cohort$active),
      entrant_n = base::nrow(new_entrants),
      exit_n = base::sum(exits),
      geography_iterations = geography_iterations,
      geography_converged = geography_converged
    )
    policy_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      policy_migration_active = policy$active,
      demand_multiplier = policy_demand_multiplier,
      provider_multiplier = policy_provider_multiplier,
      application_multiplier = policy_application_multiplier,
      relocation_empirical = if (policy$active) {
        policy$calibration$empirical
      } else {
        NA
      },
      relocation_method = if (policy$active) {
        policy$calibration$method
      } else {
        NA_character_
      }
    )
  }

  audit <- dplyr::bind_rows(audit_rows)
  balance <- dplyr::bind_rows(balance_rows)
  engine_diagnostics <- dplyr::bind_rows(engine_rows)
  service_share_diagnostics <- dplyr::bind_rows(service_share_rows)
  provider_workload <- dplyr::bind_rows(provider_workload_rows)
  service_workload <- dplyr::bind_rows(service_workload_rows)
  practice_diagnostics <- dplyr::bind_rows(practice_rows)
  policy_diagnostics <- dplyr::bind_rows(policy_rows)

  bundle <- base::list(
    audit_ledger_tbl = audit,
    annual_hrr_balance = if (geography_engine == "hrr_balance") {
      balance
    } else {
      NULL
    },
    annual_county_balance = if (geography_engine == "county_endogenous") {
      balance
    } else {
      NULL
    },
    final_provider_cohort = provider_cohort,
    engine_diagnostics = engine_diagnostics,
    fitted_productivity_model = fitted_productivity_model,
    policy_migration_diagnostics = policy_diagnostics,
    policy_migration_summary_tbl = policy$summary,
    practice_economics_diagnostics = practice_diagnostics,
    service_share_diagnostics = service_share_diagnostics,
    service_share_provider_workload = provider_workload,
    service_share_service_workload = service_workload,
    service_share_provenance = service_share_bundle$provenance,
    simulation_config = base::list(
      start_year = start_year,
      end_year = end_year,
      n_agents = n_agents,
      initial_provider_count = initial_provider_count,
      fellowship_entrants = fellowship_entrants,
      app_delegation_rate = app_delegation_rate,
      medicaid_fee_ratio = medicaid_fee_ratio,
      geography_engine = geography_engine,
      entrant_engine = entrant_engine,
      productivity_engine = productivity_engine,
      seed = seed,
      evidence_db = evidence_db,
      empirical_parameter_names = base::names(empirical_parameters),
      policy_migration_scenario = policy_migration_scenario,
      policy_evidence_db = policy_evidence_db,
      run_practice_economics = run_practice_economics,
      practice_economics_draws = practice_economics_draws,
      service_share_engine = "calibrated",
      service_share_draw = base::as.integer(service_share_draw),
      app_capacity_multiplier_applied = FALSE
    ),
    empirical_parameter_provenance = base::attr(
      empirical_parameters,
      "provenance"
    )
  )

  if (base::isTRUE(save_outputs)) {
    base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    outputs <- base::list(
      audit_ledger = audit,
      spatial_balance = balance,
      engine_diagnostics = engine_diagnostics,
      service_share_diagnostics = service_share_diagnostics,
      service_share_provider_workload = provider_workload,
      service_share_service_workload = service_workload
    )
    purrr::iwalk(outputs, function(data, name) {
      path <- base::file.path(
        output_dir,
        base::paste0(name, "_", timestamp, ".csv")
      )
      readr::write_csv(data, path)
      base::message("Saved ", name, ": ", base::normalizePath(path))
    })
  }

  base::message("Calibrated service-share simulation completed successfully.")
  bundle
}


# Public dispatch wrapper. New arguments are appended after the historical
# signature so positional callers retain their old meaning.
run_end_to_end_simulation <- function(
    start_year = 2025L,
    end_year = 2035L,
    n_agents = 1000L,
    initial_provider_count = 1200L,
    fellowship_entrants = 55L,
    app_delegation_rate = 0.15,
    medicaid_fee_ratio = 0.75,
    geography_engine = c("hrr_balance", "county_endogenous"),
    entrant_engine = c("parametric", "joint_donor_sampling"),
    productivity_engine = c("benchmark", "lmer_fitted"),
    county_market_tbl = NULL,
    entrant_donor_pool = NULL,
    productivity_panel = NULL,
    productivity_model = NULL,
    geography_solver = solve_endogenous_geography,
    entrant_simulator = simulate_joint_entrant_characteristics,
    productivity_fitter = fit_provider_productivity_model,
    productivity_predictor = NULL,
    geography_control = base::list(),
    evidence_db = NULL,
    empirical_parameters = NULL,
    policy_migration_scenario = "baseline",
    policy_evidence_db = NULL,
    run_practice_economics = FALSE,
    practice_payer_mix = NULL,
    practice_economics_draws = 100L,
    seed = 20260821L,
    save_outputs = TRUE,
    output_dir = "artifacts/end_to_end",
    service_share_engine = c("legacy_matrix", "calibrated"),
    service_share_bundle = NULL,
    service_share_draw = NULL) {
  service_share_engine <- base::match.arg(service_share_engine)
  geography_engine <- base::match.arg(geography_engine)
  entrant_engine <- base::match.arg(entrant_engine)
  productivity_engine <- base::match.arg(productivity_engine)
  policy_migration_scenario <- base::match.arg(
    policy_migration_scenario,
    c(
      "baseline", "observed_migration", "migration_stress",
      "legislative_climate", "combined_stress"
    )
  )

  if (service_share_engine == "legacy_matrix") {
    return(.run_end_to_end_simulation_legacy(
      start_year = start_year,
      end_year = end_year,
      n_agents = n_agents,
      initial_provider_count = initial_provider_count,
      fellowship_entrants = fellowship_entrants,
      app_delegation_rate = app_delegation_rate,
      medicaid_fee_ratio = medicaid_fee_ratio,
      geography_engine = geography_engine,
      entrant_engine = entrant_engine,
      productivity_engine = productivity_engine,
      county_market_tbl = county_market_tbl,
      entrant_donor_pool = entrant_donor_pool,
      productivity_panel = productivity_panel,
      productivity_model = productivity_model,
      geography_solver = geography_solver,
      entrant_simulator = entrant_simulator,
      productivity_fitter = productivity_fitter,
      productivity_predictor = productivity_predictor,
      geography_control = geography_control,
      evidence_db = evidence_db,
      empirical_parameters = empirical_parameters,
      policy_migration_scenario = policy_migration_scenario,
      policy_evidence_db = policy_evidence_db,
      run_practice_economics = run_practice_economics,
      practice_payer_mix = practice_payer_mix,
      practice_economics_draws = practice_economics_draws,
      seed = seed,
      save_outputs = save_outputs,
      output_dir = output_dir
    ))
  }

  if (base::is.null(service_share_bundle)) {
    base::stop(
      "`service_share_bundle` is required when ",
      "service_share_engine = \"calibrated\".",
      call. = FALSE
    )
  }

  .run_end_to_end_simulation_calibrated(
    start_year = start_year,
    end_year = end_year,
    n_agents = n_agents,
    initial_provider_count = initial_provider_count,
    fellowship_entrants = fellowship_entrants,
    app_delegation_rate = app_delegation_rate,
    medicaid_fee_ratio = medicaid_fee_ratio,
    geography_engine = geography_engine,
    entrant_engine = entrant_engine,
    productivity_engine = productivity_engine,
    county_market_tbl = county_market_tbl,
    entrant_donor_pool = entrant_donor_pool,
    productivity_panel = productivity_panel,
    productivity_model = productivity_model,
    geography_solver = geography_solver,
    entrant_simulator = entrant_simulator,
    productivity_fitter = productivity_fitter,
    productivity_predictor = productivity_predictor,
    geography_control = geography_control,
    evidence_db = evidence_db,
    empirical_parameters = empirical_parameters,
    policy_migration_scenario = policy_migration_scenario,
    policy_evidence_db = policy_evidence_db,
    run_practice_economics = run_practice_economics,
    practice_payer_mix = practice_payer_mix,
    practice_economics_draws = practice_economics_draws,
    seed = seed,
    save_outputs = save_outputs,
    output_dir = output_dir,
    service_share_bundle = service_share_bundle,
    service_share_draw = service_share_draw
  )
}
