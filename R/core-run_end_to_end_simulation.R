# Master End-to-End Simulation Pipeline ------------------------------------

#' Run the coupled URPS workforce and demand simulation
#'
#' @description
#' Runs the eight-step annual microsimulation and optionally couples the
#' endogenous county geography, joint entrant donor, and fitted productivity
#' engines. Supports empirical parameter overrides via National Evidence DuckDB.
#'
#' @param start_year First simulation year.
#' @param end_year Last simulation year.
#' @param n_agents Number of patient agents.
#' @param initial_provider_count Baseline number of providers.
#' @param fellowship_entrants Annual number of fellowship graduates.
#' @param app_delegation_rate APP delegation fraction.
#' @param medicaid_fee_ratio Medicaid-to-Medicare fee ratio.
#' @param geography_engine Geography engine name.
#' @param entrant_engine Entrant engine name.
#' @param productivity_engine Productivity engine name.
#' @param county_market_tbl County-year market inputs for endogenous geography.
#' @param entrant_donor_pool Historical entrant profiles used as donors.
#' @param productivity_panel Provider-year productivity training panel.
#' @param productivity_model Optional already-fitted productivity model.
#' @param geography_solver Function used for endogenous geography.
#' @param entrant_simulator Function used for joint entrant sampling.
#' @param productivity_fitter Function used to fit productivity.
#' @param productivity_predictor Optional custom prediction function.
#' @param geography_control Named list passed to the geography solver.
#' @param evidence_db Optional national evidence DuckDB path.
#' @param empirical_parameters Optional named numeric vector of empirical parameters.
#' @param policy_migration_scenario One of `"baseline"` (default, an identity
#'   transform -- zero behavior change), `"observed_migration"`,
#'   `"migration_stress"`, `"legislative_climate"`, or `"combined_stress"`.
#'   See [simulate_policy_migration_scenarios()]. Ignored unless
#'   `policy_evidence_db` is also supplied.
#' @param policy_evidence_db Optional path to a policy migration DuckDB (see
#'   [open_policy_migration_duckdb()]). `NULL` (default) skips the
#'   state-migration/legislative-policy step entirely. State-year allocation
#'   uses real ACS population shares from
#'   [national_older_female_population_by_state()]; when the database has no
#'   ingested evidence, the module degrades gracefully to its declared prior
#'   (see [calibrate_legislative_relocation()]) rather than a no-op.
#' @param run_practice_economics Whether to run [simulate_practice_economics()]
#'   each year against a practice-tbl bridged from `provider_cohort`. `FALSE`
#'   (default) skips the step entirely -- zero behavior change. Each active
#'   provider becomes one practice-year: `annual_wrvu` allocates that year's
#'   national `wrvu_total` by each provider's share of `supplied_fte`,
#'   `practice_setting` derives from `academic_setting`/`hospital_outpatient`
#'   (`academic`/`hospital_employed`/`independent`), `app_fte` is
#'   `fte * app_delegation_rate` -- documented proxies, not real per-provider
#'   data (this runner has no real provider-level wRVU or APP headcount).
#' @param practice_payer_mix Optional payer-mix tibble (the shape
#'   [practice_payer_mix_defaults()] returns), applied uniformly to every
#'   provider-practice. Computed via [practice_payer_mix_defaults()] when
#'   `NULL` and `run_practice_economics = TRUE`.
#' @param practice_economics_draws Monte Carlo draws per practice-year passed
#'   to [simulate_practice_economics()]; kept below its 500-draw default to
#'   bound runtime at full provider-cohort scale.
#' @param seed Master random seed.
#' @param save_outputs Whether to save timestamped CSV files.
#' @param output_dir Directory for saved CSV files.
#'
#' @return A named list containing the audit ledger, spatial balances,
#'   provider cohort, engine diagnostics, fitted productivity model, policy
#'   migration diagnostics/summary, practice economics diagnostics, and
#'   simulation configuration.
#' @family simulation pipeline
#' @concept core
#' @export
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
    output_dir = "artifacts/end_to_end") {

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

  if (!base::is.null(evidence_db) && base::is.null(empirical_parameters)) {
    empirical_parameters <- read_urps_empirical_parameters(evidence_db)
  }
  if (base::is.null(empirical_parameters)) {
    empirical_parameters <- stats::setNames(base::numeric(), base::character())
  }

  empirical_value <- function(parameter, fallback) {
    if (parameter %in% base::names(empirical_parameters)) {
      estimate <- empirical_parameters[[parameter]]
      if (base::is.finite(estimate)) {
        base::message("Empirical input: ", parameter, " = ",
                     base::format(estimate, big.mark = ",", scientific = FALSE))
        base::return(estimate)
      }
    }
    fallback
  }

  if (!base::is.numeric(start_year) || !base::is.numeric(end_year) ||
      base::length(start_year) != 1L || base::length(end_year) != 1L ||
      base::is.na(start_year) || base::is.na(end_year) ||
      start_year > end_year) {
    base::stop("`start_year` must be less than or equal to `end_year`.",
      call. = FALSE)
  }
  if (!base::is.numeric(initial_provider_count) ||
      initial_provider_count < 1) {
    base::stop("`initial_provider_count` must be positive.", call. = FALSE)
  }
  if (!base::is.numeric(fellowship_entrants) || fellowship_entrants < 0) {
    base::stop("`fellowship_entrants` must be nonnegative.", call. = FALSE)
  }
  if (app_delegation_rate < 0 || app_delegation_rate > 0.30) {
    base::stop("`app_delegation_rate` must be between 0 and 0.30.",
      call. = FALSE)
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
      paste0(
        "Supply either `productivity_model` or `productivity_panel` when ",
        "`productivity_engine = \"lmer_fitted\"`."
      ),
      call. = FALSE
    )
  }

  base::message("Starting the eight-step URPS simulation.")
  base::message("Inputs: years ", start_year, "-", end_year,
    "; providers = ", scales::comma(initial_provider_count),
    "; entrants/year = ", scales::comma(fellowship_entrants), ".")
  base::message("Geography engine: ", geography_engine, ".")
  base::message("Entrant engine: ", entrant_engine, ".")
  base::message("Productivity engine: ", productivity_engine, ".")

  years <- base::seq.int(
    base::as.integer(start_year),
    base::as.integer(end_year)
  )
  base::set.seed(base::as.integer(seed))

  provider_cohort <- tibble::tibble(
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
  base::message("Initialized provider cohort: ",
    scales::comma(base::nrow(provider_cohort)), " records.")

  fitted_productivity_model <- productivity_model
  if (productivity_engine == "lmer_fitted" &&
      base::is.null(fitted_productivity_model)) {
    base::message("Fitting provider productivity model.")
    fitted_productivity_model <- productivity_fitter(productivity_panel)
  }

  audit_rows <- base::vector("list", base::length(years))
  spatial_balance_rows <- base::vector("list", base::length(years))
  engine_diagnostic_rows <- base::vector("list", base::length(years))
  policy_migration_diagnostic_rows <- base::vector("list", base::length(years))

  base_population_2025 <- empirical_value("female_population_20plus", 132500000)
  fellowship_entrants <- empirical_value("annual_fellowship_entrants", fellowship_entrants)

  # Policy migration operates at state-year granularity; this runner is
  # national-aggregate. Build the state-year bridge and calibrate the
  # relocation response ONCE from a multi-year panel (mirroring
  # `fitted_productivity_model` above) rather than per year, since a
  # single-year slice never has the year-over-year variation
  # `calibrate_legislative_relocation()` needs to move off its prior.
  policy_migration_active <- !base::identical(policy_migration_scenario, "baseline") &&
    !base::is.null(policy_evidence_db)
  policy_migration_year_multipliers <- NULL
  policy_migration_summary_tbl <- NULL
  policy_relocation_calibration <- NULL
  if (policy_migration_active) {
    base::message(
      "Policy migration scenario: ", policy_migration_scenario, "."
    )
    state_population_shares <- national_older_female_population_by_state()
    state_crosswalk <- state_population_shares |>
      dplyr::select(.data$state, .data$state_fips)
    initial_provider_fte_total <- base::sum(provider_cohort$fte, na.rm = TRUE)

    policy_state_year_baseline <- purrr::map_dfr(years, function(policy_year) {
      year_population <- base_population_2025 * 1.006^(policy_year - 2025L)
      year_prevalent_cases <- year_population *
        (0.245 + 0.001 * (policy_year - 2025L))
      state_population_shares |>
        dplyr::transmute(
          state = .data$state,
          year = policy_year,
          female_older_population = .data$share * year_population,
          pfd_demand = .data$share * year_prevalent_cases,
          # Provider FTE is allocated by population share, not real
          # state-level provider geography (`state_abbr` stays a
          # placeholder "US" cohort-wide) -- a documented proxy.
          provider_fte = .data$share * initial_provider_fte_total,
          # Entrants-as-applications proxy: the runner has no separate
          # fellowship-applications figure.
          fellowship_applications = .data$share * fellowship_entrants
        )
    })

    policy_migration_con <- open_policy_migration_duckdb(policy_evidence_db)
    base::on.exit(
      DBI::dbDisconnect(policy_migration_con, shutdown = TRUE),
      add = TRUE
    )
    policy_evidence <- build_policy_migration_evidence(
      policy_migration_con,
      state_crosswalk
    )
    policy_relocation_calibration <- calibrate_legislative_relocation(
      state_year_history = policy_state_year_baseline,
      evidence = policy_evidence
    )
    policy_simulated <- simulate_policy_migration_scenarios(
      policy_state_year_baseline,
      evidence = policy_evidence,
      scenario = policy_migration_scenario,
      seed = seed,
      calibration = policy_relocation_calibration
    )
    policy_migration_summary_tbl <- summarize_policy_migration_scenarios(
      policy_simulated
    )
    policy_migration_year_multipliers <- policy_simulated |>
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
    base::message(
      "Policy migration relocation calibration: ",
      policy_relocation_calibration$method, " (empirical = ",
      policy_relocation_calibration$empirical, ")."
    )
  }

  practice_economics_diagnostic_rows <- base::vector("list", base::length(years))
  if (run_practice_economics && base::is.null(practice_payer_mix)) {
    practice_payer_mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  }

  for (year_index in base::seq_along(years)) {
    simulation_year <- years[[year_index]]
    base::message("Running year ", simulation_year, ".")

    population_total <- base_population_2025 *
      1.006^(simulation_year - 2025L)
    prevalence_rate <- 0.245 + 0.001 *
      (simulation_year - 2025L)
    incidence_rate <- 0.022
    prevalent_cases <- population_total * prevalence_rate
    incident_cases <- population_total * incidence_rate

    policy_demand_multiplier <- 1
    policy_provider_multiplier <- 1
    policy_application_multiplier <- 1
    if (policy_migration_active) {
      year_multiplier_row <- policy_migration_year_multipliers |>
        dplyr::filter(.data$year == simulation_year)
      if (base::nrow(year_multiplier_row) == 1L) {
        policy_demand_multiplier <- year_multiplier_row$demand_multiplier
        policy_provider_multiplier <- year_multiplier_row$provider_multiplier
        policy_application_multiplier <-
          year_multiplier_row$application_multiplier
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
      base::message("Applying county endogenous geography for ",
        simulation_year, ".")
      if ("year" %in% base::names(county_market_tbl)) {
        year_markets <- county_market_tbl |>
          dplyr::filter(.data$year == simulation_year)
      } else {
        year_markets <- county_market_tbl
      }
      geography_arguments <- base::c(
        base::list(
          provider_roster = active_providers,
          county_market_tbl = year_markets,
          year = simulation_year
        ),
        geography_control
      )
      geography_solution <- base::do.call(
        geography_solver,
        geography_arguments
      )
      if (base::is.data.frame(geography_solution)) {
        relocated_providers <- geography_solution
      } else {
        relocated_providers <- geography_solution$provider_roster
        geography_iterations <- .urps_null_or(geography_solution$iterations, NA_integer_)
        geography_converged <- .urps_null_or(geography_solution$converged, NA)
      }
      required_geo_columns <- base::c("provider_id", "county_fips")
      missing_geo_columns <- base::setdiff(
        required_geo_columns,
        base::names(relocated_providers)
      )
      if (base::length(missing_geo_columns) > 0L) {
        base::stop(
          "The geography solver must return provider_id and county_fips.",
          call. = FALSE
        )
      }
      provider_cohort <- provider_cohort |>
        dplyr::select(-dplyr::any_of("county_fips")) |>
        dplyr::left_join(
          relocated_providers |>
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
    if (productivity_engine == "benchmark") {
      provider_capacity <- active_providers |>
        dplyr::transmute(
          provider_id = .data$provider_id,
          annual_patient_capacity = .data$fte * 1600
        )
    } else {
      base::message("Predicting fitted provider capacity for ",
        simulation_year, ".")
      if (base::is.function(productivity_predictor)) {
        predicted_capacity <- productivity_predictor(
          fitted_productivity_model,
          active_providers,
          simulation_year
        )
      } else {
        predicted_capacity <- stats::predict(
          fitted_productivity_model,
          newdata = active_providers,
          allow.new.levels = TRUE
        )
      }
      if (base::is.data.frame(predicted_capacity)) {
        capacity_columns <- base::intersect(
          base::c(
            "annual_patient_capacity",
            "predicted_capacity",
            "capacity"
          ),
          base::names(predicted_capacity)
        )
        if (base::length(capacity_columns) == 0L ||
            !"provider_id" %in% base::names(predicted_capacity)) {
          base::stop(
            paste0(
              "A tabular productivity prediction must contain provider_id ",
              "and a recognized capacity column."
            ),
            call. = FALSE
          )
        }
        capacity_column <- capacity_columns[[1L]]
        provider_capacity <- predicted_capacity |>
          dplyr::transmute(
            provider_id = .data$provider_id,
            annual_patient_capacity = .data[[capacity_column]]
          )
      } else {
        provider_capacity <- active_providers |>
          dplyr::transmute(
            provider_id = .data$provider_id,
            annual_patient_capacity = base::as.numeric(
              predicted_capacity
            )
          )
      }
      if (base::any(!base::is.finite(
        provider_capacity$annual_patient_capacity
      )) || base::any(provider_capacity$annual_patient_capacity < 0)) {
        base::stop(
          "Fitted productivity predictions must be finite and nonnegative.",
          call. = FALSE
        )
      }
    }

    clinical_capacity <- base::sum(
      provider_capacity$annual_patient_capacity,
      na.rm = TRUE
    ) * (1 + app_delegation_rate * 0.5)
    served_patients <- base::pmin(
      appointment_requests,
      clinical_capacity
    )
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
        simulation_year,
        ".",
        call. = FALSE
      )
    }

    # Route served patient demand through condition-service-provider routing pathway
    treated_counts <- base::c(
      ui = served_patients * 0.45,
      pop = served_patients * 0.35,
      ai = served_patients * 0.20
    )
    routed_services <- pathway_provider_service_volumes(
      treated = treated_counts,
      year = simulation_year,
      prior_only = "apply"
    )
    urps_services <- urps_routed_service_volumes(routed_services)
    
    # Calculate delivered services, wRVUs, and required FTE from routed URPS service volumes
    delivered_services <- base::sum(urps_services$provider_volume, na.rm = TRUE)
    # 8.60 average wRVU per routed URPS procedural/consultative service
    wrvu_total <- delivered_services * 8.60
    physician_minutes <- wrvu_total * 42
    required_fte <- wrvu_total / 1400
    fte_gap <- supplied_fte - required_fte
    adequacy_ratio <- supplied_fte / required_fte

    if (geography_engine == "hrr_balance") {
      hrr_reference <- tibble::tibble(
        hrr_code = base::sprintf("HRR%03d", base::seq_len(306L)),
        hrr_name = base::paste("HRR Region", base::seq_len(306L))
      )
      hrr_demand <- tibble::tibble(
        hrr_code = hrr_reference$hrr_code,
        demand_fte = required_fte / 306
      )
      spatial_balance <- aggregate_hrr_workforce_balance(
        provider_roster = active_providers,
        hrr_demand_tbl = hrr_demand,
        hrr_reference_tbl = hrr_reference,
        shortage_threshold = 0.20,
        expected_hrr_n = 306L
      ) |>
        dplyr::mutate(
          year = simulation_year,
          geography_engine = geography_engine
        )
    } else {
      if ("year" %in% base::names(county_market_tbl)) {
        year_county_demand <- county_market_tbl |>
          dplyr::filter(.data$year == simulation_year)
      } else {
        year_county_demand <- county_market_tbl
      }
      required_county_columns <- base::c(
        "county_fips",
        "demand_weight"
      )
      if (!base::all(required_county_columns %in%
          base::names(year_county_demand))) {
        base::stop(
          "County markets must contain county_fips and demand_weight.",
          call. = FALSE
        )
      }
      county_demand <- year_county_demand |>
        dplyr::mutate(
          demand_share = .data$demand_weight /
            base::sum(.data$demand_weight, na.rm = TRUE),
          demand_fte = required_fte * .data$demand_share
        )
      spatial_balance <- active_providers |>
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
    spatial_balance_rows[[year_index]] <- spatial_balance

    wait_days <- 14 + unserved_delayed /
      appointment_requests * 45
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

    if (run_practice_economics) {
      practice_tbl <- active_providers |>
        dplyr::transmute(
          practice_id = .data$provider_id,
          year = simulation_year,
          clinical_fte = .data$fte,
          annual_wrvu = wrvu_total * .data$fte / supplied_fte,
          medicare_share = practice_payer_mix$medicare_share,
          medicaid_share = practice_payer_mix$medicaid_share,
          commercial_share = practice_payer_mix$commercial_share,
          self_pay_share = practice_payer_mix$self_pay_share,
          practice_setting = dplyr::case_when(
            .data$academic_setting ~ "academic",
            .data$hospital_outpatient ~ "hospital_employed",
            TRUE ~ "independent"
          ),
          app_fte = .data$fte * app_delegation_rate
        )
      practice_result <- simulate_practice_economics(
        practice_tbl,
        draws = practice_economics_draws,
        seed = seed + year_index
      )
      practice_draws <- practice_result$draws
      # Exact cost-component decomposition (verified by accounting identity
      # in tests/testthat/test-supply-practice-economics.R): operating_cost
      # == clinical_fte*(overhead_per_fte + malpractice_per_fte) +
      # app_fte*app_compensation. There is NO physician-compensation line
      # item anywhere in this cost model -- mean_operating_income is money
      # available to compensate the physician BEFORE they are paid, not a
      # true bottom-line practice profit. A negative value does not mean
      # cash losses in the ordinary sense; it means the composed overhead +
      # malpractice + APP-labor cost exceeds wRVU-proxy revenue before
      # physician pay is even considered.
      practice_diagnostic_row <- practice_result$summary |>
        dplyr::summarise(
          year = simulation_year,
          n_practices = dplyr::n(),
          mean_wrvu_per_fte = base::sum(practice_tbl$annual_wrvu) /
            base::sum(practice_tbl$clinical_fte),
          mean_revenue_per_fte = base::mean(
            practice_draws$gross_revenue / practice_draws$clinical_fte
          ),
          mean_expense_per_fte = base::mean(
            practice_draws$operating_cost / practice_draws$clinical_fte
          ),
          mean_overhead_per_fte = base::mean(practice_draws$overhead_per_fte),
          mean_malpractice_per_fte = base::mean(
            practice_draws$malpractice_per_fte
          ),
          mean_app_labor_per_fte = base::mean(
            practice_draws$app_fte * practice_draws$app_compensation /
              practice_draws$clinical_fte
          ),
          mean_revenue_per_wrvu = base::sum(practice_draws$gross_revenue) /
            base::sum(practice_draws$annual_wrvu),
          mean_break_even_wrvu_per_fte = base::mean(
            practice_draws$break_even_wrvu_per_fte, na.rm = TRUE
          ),
          mean_required_revenue_per_wrvu = base::mean(
            practice_draws$required_revenue_per_wrvu, na.rm = TRUE
          ),
          mean_operating_income = base::mean(practice_draws$operating_income),
          # Legacy alias, identical arithmetic to mean_operating_income --
          # see the file-level comment above on why this is the primary
          # name going forward: there is no physician-compensation line
          # item anywhere in this cost model, so a negative value means
          # modeled professional revenue does not cover nonphysician
          # practice costs, before the physician is paid at all.
          mean_physician_compensation_capacity = base::mean(
            practice_draws$physician_compensation_capacity
          ),
          # Two distinct estimands, kept separate rather than assumed equal:
          # the sum-based aggregate margin (national revenue-weighted) and
          # the distributional mean-of-per-practice-median margin (equal
          # weight per practice regardless of size).
          aggregate_operating_margin =
            (base::sum(practice_draws$gross_revenue) -
              base::sum(practice_draws$operating_cost)) /
              base::sum(practice_draws$gross_revenue),
          mean_operating_margin = base::mean(
            .data$median_operating_margin, na.rm = TRUE
          ),
          mean_loss_probability = base::mean(
            .data$loss_probability, na.rm = TRUE
          ),
          mean_acquisition_probability = base::mean(
            .data$acquisition_probability, na.rm = TRUE
          ),
          mean_cash_pay_probability = base::mean(
            .data$cash_pay_probability, na.rm = TRUE
          )
        )

      # FAIL-LOUD PLAUSIBILITY ALARMS, not calibration targets: these warn
      # (never silently adjust an input) when the composed cost/revenue
      # model produces an implausible headline result, so a wrong number
      # cannot pass through unnoticed the way -56% margins / 99.98% loss
      # probability did on the first wiring pass. See R/supply-
      # practice_economics.R's overhead assumption -- "User-specified
      # scenario"/"assumption" status, the one cost input with no external
      # citation -- as the most likely thing to revisit if these fire.
      if (base::isTRUE(practice_diagnostic_row$mean_operating_margin < -0.25)) {
        base::warning(
          "run_end_to_end_simulation(): year ", simulation_year,
          " practice-economics mean operating margin is ",
          base::sprintf("%.1f%%", 100 * practice_diagnostic_row$mean_operating_margin),
          " (< -25% plausibility bound). This is a fail-loud alarm about the",
          " composed revenue/cost model, not a claim the simulation is wrong;",
          " see practice_economics_diagnostics for the revenue/expense",
          " decomposition.",
          call. = FALSE
        )
      }
      if (base::isTRUE(practice_diagnostic_row$mean_loss_probability > 0.90)) {
        base::warning(
          "run_end_to_end_simulation(): year ", simulation_year,
          " practice-economics loss probability is ",
          base::sprintf("%.1f%%", 100 * practice_diagnostic_row$mean_loss_probability),
          " (> 90% plausibility bound).",
          call. = FALSE
        )
      }
      if (base::isTRUE(practice_diagnostic_row$mean_revenue_per_wrvu < 15 ||
          practice_diagnostic_row$mean_revenue_per_wrvu > 100)) {
        base::warning(
          "run_end_to_end_simulation(): year ", simulation_year,
          " realized revenue per wRVU is $",
          base::sprintf("%.2f", practice_diagnostic_row$mean_revenue_per_wrvu),
          ", outside the [$15, $100] plausibility bound around real Medicare",
          " conversion factors (~$33-34/RVU).",
          call. = FALSE
        )
      }

      practice_economics_diagnostic_rows[[year_index]] <- practice_diagnostic_row
    }

    provider_cohort <- provider_cohort |>
      dplyr::mutate(
        age = .data$age + 1,
        years_certified = .data$years_certified +
          base::as.numeric(.data$active)
      )
    exit_probability <- 0.015 +
      (provider_cohort$age / 100)^3.5
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
      base::message("Sampling joint entrant profiles for ",
        simulation_year, ".")
      new_entrants <- entrant_simulator(
        n_entrants = entrant_count,
        donor_pool = entrant_donor_pool,
        entry_year = simulation_year,
        seed = seed + year_index
      )
      new_entrants <- new_entrants |>
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
    provider_cohort <- dplyr::bind_rows(
      provider_cohort,
      new_entrants
    )
    engine_diagnostic_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      active_provider_n = base::sum(provider_cohort$active),
      entrant_n = base::nrow(new_entrants),
      exit_n = base::sum(exits),
      geography_iterations = geography_iterations,
      geography_converged = geography_converged
    )
    policy_migration_diagnostic_rows[[year_index]] <- tibble::tibble(
      year = simulation_year,
      policy_migration_active = policy_migration_active,
      demand_multiplier = policy_demand_multiplier,
      provider_multiplier = policy_provider_multiplier,
      application_multiplier = policy_application_multiplier,
      relocation_empirical = if (policy_migration_active) {
        policy_relocation_calibration$empirical
      } else {
        NA
      },
      relocation_method = if (policy_migration_active) {
        policy_relocation_calibration$method
      } else {
        NA_character_
      }
    )
    base::message("Completed year ", simulation_year,
      ": served ", scales::comma(base::round(served_patients)),
      "; delayed ", scales::comma(base::round(unserved_delayed)), ".")
  }

  audit_ledger_tbl <- dplyr::bind_rows(audit_rows)
  annual_spatial_balance <- dplyr::bind_rows(spatial_balance_rows)
  engine_diagnostics <- dplyr::bind_rows(engine_diagnostic_rows)
  policy_migration_diagnostics <- dplyr::bind_rows(
    policy_migration_diagnostic_rows
  )
  practice_economics_diagnostics <- dplyr::bind_rows(
    practice_economics_diagnostic_rows
  )
  base::message("Combined annual audit, balance, and diagnostic tables.")

  simulation_bundle <- base::list(
    audit_ledger_tbl = audit_ledger_tbl,
    annual_hrr_balance = if (geography_engine == "hrr_balance") {
      annual_spatial_balance
    } else {
      NULL
    },
    annual_county_balance = if (
      geography_engine == "county_endogenous"
    ) {
      annual_spatial_balance
    } else {
      NULL
    },
    final_provider_cohort = provider_cohort,
    engine_diagnostics = engine_diagnostics,
    fitted_productivity_model = fitted_productivity_model,
    policy_migration_diagnostics = policy_migration_diagnostics,
    policy_migration_summary_tbl = policy_migration_summary_tbl,
    practice_economics_diagnostics = practice_economics_diagnostics,
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
      practice_economics_draws = practice_economics_draws
    ),
    empirical_parameter_provenance = base::attr(
      empirical_parameters,
      "provenance"
    )
  )

  if (base::isTRUE(save_outputs)) {
    if (!base::dir.exists(output_dir)) {
      base::dir.create(output_dir, recursive = TRUE)
    }
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    audit_path <- base::normalizePath(
      base::file.path(
        output_dir,
        base::paste0("audit_ledger_", timestamp, ".csv")
      ),
      mustWork = FALSE
    )
    balance_path <- base::normalizePath(
      base::file.path(
        output_dir,
        base::paste0("spatial_balance_", timestamp, ".csv")
      ),
      mustWork = FALSE
    )
    diagnostic_path <- base::normalizePath(
      base::file.path(
        output_dir,
        base::paste0("engine_diagnostics_", timestamp, ".csv")
      ),
      mustWork = FALSE
    )
    readr::write_csv(audit_ledger_tbl, audit_path)
    base::message("Saved audit ledger: ", audit_path)
    readr::write_csv(annual_spatial_balance, balance_path)
    base::message("Saved spatial balance: ", balance_path)
    readr::write_csv(engine_diagnostics, diagnostic_path)
    base::message("Saved engine diagnostics: ", diagnostic_path)
  }

  first_year <- base::min(audit_ledger_tbl$year)
  last_year <- base::max(audit_ledger_tbl$year)
  gap_change <- audit_ledger_tbl$fte_gap[
    audit_ledger_tbl$year == last_year
  ] - audit_ledger_tbl$fte_gap[
    audit_ledger_tbl$year == first_year
  ]
  change_direction <- if (gap_change >= 0) "increased" else "decreased"
  base::message(
    "From ", first_year, " to ", last_year,
    ", the supplied-minus-required FTE gap ", change_direction,
    " by ", scales::comma(base::round(base::abs(gap_change), 1)),
    "; no hypothesis-test p-value was calculated."
  )
  base::message("Simulation completed successfully.")
  simulation_bundle
}

#' Null-coalescing helper
#'
#' @param left Candidate value.
#' @param right Replacement used when `left` is `NULL`.
#'
#' @return `left`, unless it is `NULL`, otherwise `right`.
#' @keywords internal
.urps_null_or <- function(left, right) {
  if (base::is.null(left)) right else left
}
