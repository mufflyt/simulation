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
#' @param seed Master random seed.
#' @param save_outputs Whether to save timestamped CSV files.
#' @param output_dir Directory for saved CSV files.
#'
#' @return A named list containing the audit ledger, spatial balances,
#'   provider cohort, engine diagnostics, fitted productivity model, and
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
    seed = 20260821L,
    save_outputs = TRUE,
    output_dir = "artifacts/end_to_end") {

  geography_engine <- base::match.arg(geography_engine)
  entrant_engine <- base::match.arg(entrant_engine)
  productivity_engine <- base::match.arg(productivity_engine)

  if (!base::is.null(evidence_db) && base::is.null(empirical_parameters)) {
    empirical_parameters <- read_urps_empirical_parameters(evidence_db)
  }
  if (base::is.null(empirical_parameters)) {
    empirical_parameters <- base::setNames(base::numeric(), base::character())
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

  base_population_2025 <- empirical_value("female_population_20plus", 132500000)
  fellowship_entrants <- empirical_value("annual_fellowship_entrants", fellowship_entrants)

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

    delivered_services <- served_patients * 2.15
    wrvu_total <- served_patients * 18.50
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

    entrant_count <- base::as.integer(fellowship_entrants)
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
    base::message("Completed year ", simulation_year,
      ": served ", scales::comma(base::round(served_patients)),
      "; delayed ", scales::comma(base::round(unserved_delayed)), ".")
  }

  audit_ledger_tbl <- dplyr::bind_rows(audit_rows)
  annual_spatial_balance <- dplyr::bind_rows(spatial_balance_rows)
  engine_diagnostics <- dplyr::bind_rows(engine_diagnostic_rows)
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
      empirical_parameter_names = base::names(empirical_parameters)
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
