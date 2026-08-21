#' Validate inputs for the endogenous geography model
#'
#' @param providers Provider-level tibble.
#' @param markets County-year market tibble.
#' @param choice_set Provider-county choice-set tibble.
#' @return Invisibly returns TRUE.
validate_geography_inputs <- function(providers, markets, choice_set) {
  base::message("Validating endogenous geography inputs.")

  provider_required <- c(
    "provider_id", "current_county_fips", "clinical_fte"
  )
  market_required <- c(
    "county_fips", "year", "population", "required_fte",
    "income_index", "academic_center"
  )
  choice_required <- c(
    "provider_id", "county_fips", "training_tie",
    "distance_penalty", "historical_tie"
  )

  missing_provider <- base::setdiff(
    provider_required,
    base::names(providers)
  )
  missing_market <- base::setdiff(
    market_required,
    base::names(markets)
  )
  missing_choice <- base::setdiff(
    choice_required,
    base::names(choice_set)
  )

  if (base::length(missing_provider) > 0L) {
    base::stop(
      "Missing provider columns: ",
      base::paste(missing_provider, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::length(missing_market) > 0L) {
    base::stop(
      "Missing market columns: ",
      base::paste(missing_market, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::length(missing_choice) > 0L) {
    base::stop(
      "Missing choice-set columns: ",
      base::paste(missing_choice, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::anyDuplicated(providers$provider_id) > 0L) {
    base::stop("provider_id must be unique.", call. = FALSE)
  }
  if (base::anyDuplicated(markets[c("county_fips", "year")]) > 0L) {
    base::stop(
      "Each county_fips-year market row must be unique.",
      call. = FALSE
    )
  }
  if (base::anyDuplicated(choice_set[c("provider_id", "county_fips")]) >
      0L) {
    base::stop(
      "Each provider-county choice row must be unique.",
      call. = FALSE
    )
  }
  if (base::any(!providers$provider_id %in% choice_set$provider_id)) {
    base::stop(
      "Every provider must appear in the choice set.",
      call. = FALSE
    )
  }
  if (base::any(providers$clinical_fte < 0, na.rm = TRUE)) {
    base::stop("clinical_fte cannot be negative.", call. = FALSE)
  }
  if (base::any(markets$required_fte < 0, na.rm = TRUE)) {
    base::stop("required_fte cannot be negative.", call. = FALSE)
  }

  base::message("Input validation complete.")
  base::invisible(TRUE)
}

#' Apply county-level policy changes
#'
#' @param markets One-year county market tibble.
#' @param policy_shocks Optional tibble of county-year shocks.
#' @return County market tibble with policy variables.
apply_geography_policy <- function(markets, policy_shocks = NULL) {
  base::message("Applying county-level geography policy inputs.")

  market_policy <- markets |>
    dplyr::mutate(
      rural_incentive = 0,
      new_program = 0,
      fellowship_slots = 0
    )

  if (base::is.null(policy_shocks)) {
    base::message("No policy shocks supplied.")
    return(market_policy)
  }

  shock_required <- c(
    "county_fips", "year", "rural_incentive", "new_program",
    "fellowship_slots"
  )
  shock_missing <- base::setdiff(
    shock_required,
    base::names(policy_shocks)
  )
  if (base::length(shock_missing) > 0L) {
    base::stop(
      "Missing policy columns: ",
      base::paste(shock_missing, collapse = ", "),
      call. = FALSE
    )
  }

  market_policy <- market_policy |>
    dplyr::select(
      -rural_incentive,
      -new_program,
      -fellowship_slots
    ) |>
    dplyr::left_join(
      policy_shocks |>
        dplyr::select(dplyr::all_of(shock_required)),
      by = c("county_fips", "year")
    ) |>
    dplyr::mutate(
      dplyr::across(
        c(rural_incentive, new_program, fellowship_slots),
        ~ tidyr::replace_na(.x, 0)
      )
    )

  base::message("Policy inputs applied to county markets.")
  market_policy
}

#' Calculate numerically stable destination probabilities
#'
#' @param utility_table Provider-destination utility tibble.
#' @return Utility tibble with conditional-logit probabilities.
calculate_destination_probabilities <- function(utility_table) {
  base::message("Calculating provider destination probabilities.")

  probability_table <- utility_table |>
    dplyr::group_by(provider_id) |>
    dplyr::mutate(
      centered_utility = utility - base::max(utility),
      probability_numerator = base::exp(centered_utility),
      destination_probability = probability_numerator /
        base::sum(probability_numerator)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      -centered_utility,
      -probability_numerator
    )

  probability_check <- probability_table |>
    dplyr::group_by(provider_id) |>
    dplyr::summarise(
      probability_sum = base::sum(destination_probability),
      .groups = "drop"
    )

  if (base::any(base::abs(probability_check$probability_sum - 1) >
      1e-10)) {
    base::stop("Destination probabilities do not sum to one.",
      call. = FALSE
    )
  }

  base::message("Destination probabilities calculated.")
  probability_table
}

#' Construct provider-county utilities
#'
#' @param providers Provider-level tibble.
#' @param markets One-year county market tibble.
#' @param choice_set Provider-county choice-set tibble.
#' @param coefficients Named numeric vector of utility coefficients.
#' @return Provider-county utility and probability tibble.
build_geography_utilities <- function(
    providers,
    markets,
    choice_set,
    coefficients) {
  base::message("Building provider-county utility table.")

  coefficient_names <- c(
    "training_tie", "income", "unmet_demand", "academic_center",
    "current_county", "historical_tie", "distance_penalty",
    "rural_incentive", "new_program", "fellowship_slots"
  )
  missing_coefficients <- base::setdiff(
    coefficient_names,
    base::names(coefficients)
  )
  if (base::length(missing_coefficients) > 0L) {
    base::stop(
      "Missing coefficients: ",
      base::paste(missing_coefficients, collapse = ", "),
      call. = FALSE
    )
  }

  utility_table <- choice_set |>
    dplyr::inner_join(
      providers |>
        dplyr::select(
          provider_id,
          current_county_fips,
          clinical_fte
        ),
      by = "provider_id"
    ) |>
    dplyr::inner_join(markets, by = "county_fips") |>
    dplyr::mutate(
      current_county = base::as.integer(
        county_fips == current_county_fips
      ),
      utility = county_intercept +
        coefficients[["training_tie"]] * training_tie +
        coefficients[["income"]] * base::log(income_index) +
        coefficients[["unmet_demand"]] * unmet_demand_rate +
        coefficients[["academic_center"]] * academic_center +
        coefficients[["current_county"]] * current_county +
        coefficients[["historical_tie"]] * historical_tie +
        coefficients[["distance_penalty"]] * distance_penalty +
        coefficients[["rural_incentive"]] * rural_incentive +
        coefficients[["new_program"]] * new_program +
        coefficients[["fellowship_slots"]] * fellowship_slots
    )

  calculate_destination_probabilities(utility_table)
}

#' Update county supply and unmet demand from expected provider choices
#'
#' @param probability_table Provider-county probability tibble.
#' @param markets One-year county market tibble.
#' @param accessibility Optional county-to-county E2SFCA weight tibble.
#' @return Updated county market tibble.
update_county_market <- function(
    probability_table,
    markets,
    accessibility = NULL) {
  base::message("Updating expected county supply.")

  direct_supply <- probability_table |>
    dplyr::group_by(county_fips) |>
    dplyr::summarise(
      direct_supply_fte = base::sum(
        clinical_fte * destination_probability
      ),
      .groups = "drop"
    )

  market_update <- markets |>
    dplyr::select(-dplyr::any_of(c(
      "direct_supply_fte", "accessible_supply_fte",
      "unmet_demand_fte", "unmet_demand_rate", "adequacy"
    ))) |>
    dplyr::left_join(direct_supply, by = "county_fips") |>
    dplyr::mutate(
      direct_supply_fte = tidyr::replace_na(direct_supply_fte, 0)
    )

  if (base::is.null(accessibility)) {
    base::message("Using direct county supply; no E2SFCA weights supplied.")
    market_update <- market_update |>
      dplyr::mutate(accessible_supply_fte = direct_supply_fte)
  } else {
    base::message("Applying E2SFCA cross-county accessibility weights.")
    access_required <- c(
      "origin_county_fips", "provider_county_fips", "access_weight"
    )
    access_missing <- base::setdiff(
      access_required,
      base::names(accessibility)
    )
    if (base::length(access_missing) > 0L) {
      base::stop(
        "Missing accessibility columns: ",
        base::paste(access_missing, collapse = ", "),
        call. = FALSE
      )
    }

    accessible_supply <- accessibility |>
      dplyr::left_join(
        direct_supply,
        by = c("provider_county_fips" = "county_fips")
      ) |>
      dplyr::mutate(
        direct_supply_fte = tidyr::replace_na(direct_supply_fte, 0),
        weighted_supply_fte = direct_supply_fte * access_weight
      ) |>
      dplyr::group_by(origin_county_fips) |>
      dplyr::summarise(
        accessible_supply_fte = base::sum(weighted_supply_fte),
        .groups = "drop"
      )

    market_update <- market_update |>
      dplyr::left_join(
        accessible_supply,
        by = c("county_fips" = "origin_county_fips")
      ) |>
      dplyr::mutate(
        accessible_supply_fte = tidyr::replace_na(
          accessible_supply_fte,
          0
        )
      )
  }

  market_update <- market_update |>
    dplyr::mutate(
      unmet_demand_fte = base::pmax(
        required_fte - accessible_supply_fte,
        0
      ),
      unmet_demand_rate = dplyr::if_else(
        required_fte > 0,
        unmet_demand_fte / required_fte,
        0
      ),
      adequacy = dplyr::if_else(
        required_fte > 0,
        base::pmin(accessible_supply_fte / required_fte, 1),
        1
      )
    )

  base::message("County supply and unmet demand updated.")
  market_update
}

#' Solve the endogenous county-location equilibrium for one year
#'
#' @param providers Provider-level tibble.
#' @param markets County-year market tibble.
#' @param choice_set Provider-county choice-set tibble.
#' @param coefficients Named numeric vector.
#' @param year_value Simulation year.
#' @param accessibility Optional E2SFCA accessibility weights.
#' @param policy_shocks Optional county-year policy tibble.
#' @param relocation_model Optional fitted relocation model.
#' @param tolerance Maximum permitted demand-rate change.
#' @param max_iterations Maximum fixed-point iterations.
#' @param damping Weight placed on the newly calculated demand rate.
#' @return Named list containing county markets and choice probabilities.
solve_endogenous_geography <- function(
    providers,
    markets,
    choice_set,
    coefficients,
    year_value,
    accessibility = NULL,
    policy_shocks = NULL,
    relocation_model = NULL,
    tolerance = 1e-6,
    max_iterations = 200L,
    damping = 0.35) {
  base::message(
    "Starting endogenous geography equilibrium for year ",
    scales::comma(year_value), "."
  )
  base::message(
    "Inputs: ", scales::comma(base::nrow(providers)),
    " providers; ", scales::comma(base::nrow(choice_set)),
    " provider-county alternatives."
  )

  validate_geography_inputs(providers, markets, choice_set)
  if (damping <= 0 || damping > 1) {
    base::stop("damping must be in (0, 1].", call. = FALSE)
  }

  county_market <- markets |>
    dplyr::filter(year == year_value) |>
    apply_geography_policy(policy_shocks = policy_shocks)

  if (!"county_intercept" %in% base::names(county_market)) {
    county_market$county_intercept <- 0
  }
  if (!"initial_supply_fte" %in% base::names(county_market)) {
    county_market$initial_supply_fte <- 0
  }

  county_market <- county_market |>
    dplyr::mutate(
      county_intercept = tidyr::replace_na(county_intercept, 0),
      initial_supply_fte = tidyr::replace_na(initial_supply_fte, 0),
      unmet_demand_rate = dplyr::if_else(
        required_fte > 0,
        base::pmax(required_fte - initial_supply_fte, 0) /
          required_fte,
        0
      )
    )

  if (base::nrow(county_market) == 0L) {
    base::stop("No market rows found for year_value.", call. = FALSE)
  }

  converged <- FALSE
  iteration <- 0L
  maximum_change <- Inf
  probability_table <- NULL

  while (iteration < max_iterations && !converged) {
    iteration <- iteration + 1L
    previous_rate <- county_market |>
      dplyr::select(county_fips, previous_unmet = unmet_demand_rate)

    probability_table <- build_geography_utilities(
      providers = providers,
      markets = county_market,
      choice_set = choice_set,
      coefficients = coefficients
    ) |>
      apply_relocation_probabilities(
        providers = providers,
        relocation_model = relocation_model
      )
    calculated_market <- update_county_market(
      probability_table = probability_table,
      markets = county_market,
      accessibility = accessibility
    ) |>
      dplyr::left_join(previous_rate, by = "county_fips") |>
      dplyr::mutate(
        undamped_unmet = unmet_demand_rate,
        unmet_demand_rate = damping * undamped_unmet +
          (1 - damping) * previous_unmet,
        rate_change = base::abs(
          unmet_demand_rate - previous_unmet
        )
      )

    maximum_change <- base::max(
      calculated_market$rate_change,
      na.rm = TRUE
    )
    county_market <- calculated_market |>
      dplyr::select(-previous_unmet, -undamped_unmet, -rate_change)
    converged <- maximum_change < tolerance

    base::message(
      "Iteration ", scales::comma(iteration),
      ": maximum unmet-demand change = ",
      scales::number(maximum_change, accuracy = 0.000001), "."
    )
  }

  if (!converged) {
    base::warning(
      "Geography equilibrium did not converge after ",
      max_iterations, " iterations.",
      call. = FALSE
    )
  }

  probability_table <- build_geography_utilities(
    providers = providers,
    markets = county_market,
    choice_set = choice_set,
    coefficients = coefficients
  ) |>
    apply_relocation_probabilities(
      providers = providers,
      relocation_model = relocation_model
    )
  county_market <- update_county_market(
    probability_table = probability_table,
    markets = county_market,
    accessibility = accessibility
  )

  diagnostics <- tibble::tibble(
    year = year_value,
    converged = converged,
    iterations = iteration,
    maximum_change = maximum_change,
    national_required_fte = base::sum(county_market$required_fte),
    national_direct_supply_fte = base::sum(
      county_market$direct_supply_fte
    ),
    national_unmet_demand_fte = base::sum(
      county_market$unmet_demand_fte
    )
  )

  base::message(
    "Equilibrium complete: ",
    scales::comma(diagnostics$national_direct_supply_fte, accuracy = 0.1),
    " expected clinical FTE and ",
    scales::comma(diagnostics$national_unmet_demand_fte, accuracy = 0.1),
    " unmet FTE."
  )

  base::list(
    county_markets = county_market,
    destination_probabilities = probability_table,
    diagnostics = diagnostics
  )
}

#' Draw realized provider counties from equilibrium probabilities
#'
#' @param probability_table Provider-county probability tibble.
#' @param seed Integer random-number seed.
#' @return One realized county per provider.
draw_provider_locations <- function(probability_table, seed) {
  base::message("Drawing realized provider locations with seed ", seed, ".")

  withr::with_seed(
    seed,
    probability_table |>
      dplyr::group_by(provider_id) |>
      dplyr::slice_sample(
        n = 1L,
        weight_by = destination_probability
      ) |>
      dplyr::ungroup() |>
      dplyr::transmute(
        provider_id,
        county_fips,
        clinical_fte,
        selected_probability = destination_probability
      )
  )
}

#' Aggregate county geography measures to state and national levels
#'
#' @param county_markets Solved county market tibble.
#' @return Named list with state and national summaries.
aggregate_geography_markets <- function(county_markets) {
  base::message("Aggregating county markets to state and national levels.")

  prepared_counties <- county_markets |>
    dplyr::mutate(
      state_fips = base::substr(county_fips, 1L, 2L)
    )

  state_summary <- prepared_counties |>
    dplyr::group_by(year, state_fips) |>
    dplyr::summarise(
      population = base::sum(population),
      required_fte = base::sum(required_fte),
      direct_supply_fte = base::sum(direct_supply_fte),
      unmet_demand_fte = base::sum(unmet_demand_fte),
      adequacy = dplyr::if_else(
        required_fte > 0,
        1 - unmet_demand_fte / required_fte,
        1
      ),
      .groups = "drop"
    )

  national_summary <- prepared_counties |>
    dplyr::group_by(year) |>
    dplyr::summarise(
      population = base::sum(population),
      required_fte = base::sum(required_fte),
      direct_supply_fte = base::sum(direct_supply_fte),
      unmet_demand_fte = base::sum(unmet_demand_fte),
      adequacy = dplyr::if_else(
        required_fte > 0,
        1 - unmet_demand_fte / required_fte,
        1
      ),
      .groups = "drop"
    )

  base::message("State and national aggregation complete.")
  base::list(
    state_markets = state_summary,
    national_markets = national_summary
  )
}

#' Save endogenous geography artifacts
#'
#' @param geography_model Return value from solve_endogenous_geography().
#' @param directory Existing destination directory.
#' @param prefix Filename prefix.
#' @return Invisibly returns exact saved paths.
save_geography_artifacts <- function(
    geography_model,
    directory,
    prefix = "endogenous_geography") {
  base::message("Preparing endogenous geography artifacts for saving.")

  if (!base::dir.exists(directory)) {
    base::stop("directory must already exist.", call. = FALSE)
  }
  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  county_path <- base::file.path(
    directory,
    base::paste0(prefix, "_county_", timestamp, ".csv")
  )
  probability_path <- base::file.path(
    directory,
    base::paste0(prefix, "_probabilities_", timestamp, ".csv")
  )
  diagnostic_path <- base::file.path(
    directory,
    base::paste0(prefix, "_diagnostics_", timestamp, ".csv")
  )

  readr::write_csv(geography_model$county_markets, county_path)
  base::message("Saved county markets to: ", county_path)
  readr::write_csv(
    geography_model$destination_probabilities,
    probability_path
  )
  base::message("Saved destination probabilities to: ", probability_path)
  readr::write_csv(geography_model$diagnostics, diagnostic_path)
  base::message("Saved diagnostics to: ", diagnostic_path)

  saved_paths <- c(
    county_path = county_path,
    probability_path = probability_path,
    diagnostic_path = diagnostic_path
  )
  base::invisible(saved_paths)
}
#' Declare the empirical evidence expected by the geography engine
#'
#' @return Tibble describing each source, model role, and evidence tier.
geography_evidence_manifest <- function() {
  base::message("Building endogenous geography evidence manifest.")

  tibble::tribble(
    ~domain, ~source, ~measure, ~model_role, ~evidence_tier,
    "provider", "NPPES monthly files", "practice county changes",
    "observed destination and relocation timing", "primary",
    "provider", "CMS Doctors and Clinicians", "practice location",
    "location confirmation and organization ties", "primary",
    "provider", "Medicare claims", "services and work RVUs",
    "clinical FTE, productivity, and local supply", "primary",
    "provider", "board certification roster", "active status",
    "FPMRS cohort membership and exits", "primary",
    "training", "ACGME program data", "fellowship county and slots",
    "training ties and policy counterfactuals", "primary",
    "training", "NRMP appointment data", "matched fellows",
    "entrant cohort size and training-region exposure", "primary",
    "demand", "ACS five-year estimates", "female population",
    "county demand denominators and covariates", "primary",
    "demand", "CDC PLACES and BRFSS", "health risk prevalence",
    "county morbidity and demand adjustment", "primary",
    "demand", "claims utilization", "pelvic-floor services",
    "age-specific realized service demand", "primary",
    "access", "mystery-caller study", "availability and wait",
    "observed unmet-demand calibration targets", "primary",
    "access", "Valhalla isochrones", "travel-time catchments",
    "E2SFCA cross-county accessibility weights", "primary",
    "market", "BEA regional accounts", "personal income",
    "county income and opportunity measure", "primary",
    "market", "BLS QCEW or OEWS", "health-care wages",
    "county labor-market attractiveness", "primary",
    "market", "CMS geographic indices", "practice-cost indices",
    "real purchasing-power adjustment", "primary",
    "market", "USDA rural-urban codes", "rurality",
    "rural heterogeneity and policy targeting", "primary",
    "academic", "teaching-hospital and program rosters",
    "academic center status", "academic destination utility", "primary",
    "validation", "historical E2SFCA surfaces", "accessibility",
    "spatial out-of-sample validation", "validation",
    "validation", "state licenses and archived directories",
    "independent practice locations", "movement validation", "validation"
  ) |>
    dplyr::mutate(
      required = TRUE,
      observed = TRUE,
      imputed = FALSE,
      assumption = FALSE
    )
}

#' Audit evidence coverage before fitting geography models
#'
#' @param evidence_log Tibble with source-level availability information.
#' @param minimum_primary_fraction Minimum primary-source coverage.
#' @return One-row evidence audit tibble.
audit_geography_evidence <- function(
    evidence_log,
    minimum_primary_fraction = 0.80) {
  base::message("Auditing empirical evidence coverage.")

  required_columns <- c(
    "source", "evidence_tier", "available", "row_count",
    "year_min", "year_max", "version", "retrieved_at"
  )
  missing_columns <- base::setdiff(
    required_columns,
    base::names(evidence_log)
  )
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Missing evidence-log columns: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  evidence_audit <- evidence_log |>
    dplyr::summarise(
      source_count = dplyr::n(),
      available_count = base::sum(available),
      primary_count = base::sum(evidence_tier == "primary"),
      primary_available = base::sum(
        evidence_tier == "primary" & available
      ),
      primary_fraction = primary_available / primary_count,
      total_rows = base::sum(row_count[available], na.rm = TRUE),
      earliest_year = base::min(year_min[available], na.rm = TRUE),
      latest_year = base::max(year_max[available], na.rm = TRUE)
    )

  if (evidence_audit$primary_fraction < minimum_primary_fraction) {
    base::stop(
      "Primary evidence coverage is ",
      scales::percent(evidence_audit$primary_fraction, accuracy = 0.1),
      "; required coverage is ",
      scales::percent(minimum_primary_fraction, accuracy = 0.1),
      ".",
      call. = FALSE
    )
  }

  base::message(
    "Evidence audit passed with ",
    scales::comma(evidence_audit$total_rows),
    " source rows spanning ", evidence_audit$earliest_year,
    "–", evidence_audit$latest_year, "."
  )
  evidence_audit
}

#' Fit relocation and destination models from observed provider histories
#'
#' @param provider_years Provider-year panel with observed moves.
#' @param historical_choices Provider-year-county alternative table.
#' @param training_ties Provider-county training relationships.
#' @param market_history County-year covariates and unmet demand.
#' @return Fitted relocation and conditional destination models.
fit_endogenous_geography_models <- function(
    provider_years,
    historical_choices,
    training_ties,
    market_history) {
  base::message("Fitting empirical relocation and destination models.")

  provider_required <- c(
    "provider_id", "year", "moved_next_year", "age", "sex",
    "years_since_fellowship", "academic", "clinical_fte"
  )
  choice_required <- c(
    "provider_id", "year", "county_fips", "chosen",
    "distance_penalty", "historical_tie"
  )
  market_required <- c(
    "county_fips", "year", "income_index", "unmet_demand_rate",
    "academic_center", "rural_incentive", "new_program",
    "fellowship_slots"
  )

  input_specification <- base::list(
    provider_years = base::setdiff(
      provider_required,
      base::names(provider_years)
    ),
    historical_choices = base::setdiff(
      choice_required,
      base::names(historical_choices)
    ),
    market_history = base::setdiff(
      market_required,
      base::names(market_history)
    )
  )
  missing_text <- purrr::imap_chr(
    input_specification,
    ~ base::paste(.y, base::paste(.x, collapse = ", "), sep = ": ")
  )
  missing_text <- missing_text[
    purrr::map_lgl(input_specification, ~ base::length(.x) > 0L)
  ]
  if (base::length(missing_text) > 0L) {
    base::stop(
      "Missing empirical-model columns: ",
      base::paste(missing_text, collapse = "; "),
      call. = FALSE
    )
  }

  base::message(
    "Relocation panel: ", scales::comma(base::nrow(provider_years)),
    " provider-years."
  )
  relocation_model <- stats::glm(
    moved_next_year ~ splines::ns(age, df = 4) + sex +
      splines::ns(years_since_fellowship, df = 4) + academic +
      clinical_fte + factor(year),
    data = provider_years,
    family = stats::binomial()
  )

  destination_panel <- historical_choices |>
    dplyr::left_join(
      training_ties,
      by = c("provider_id", "county_fips")
    ) |>
    dplyr::left_join(
      market_history,
      by = c("county_fips", "year")
    ) |>
    dplyr::mutate(
      training_tie = tidyr::replace_na(training_tie, 0),
      provider_year_id = base::interaction(
        provider_id,
        year,
        drop = TRUE
      )
    ) |>
    dplyr::group_by(provider_year_id) |>
    dplyr::filter(base::sum(chosen) == 1L) |>
    dplyr::ungroup()

  base::message(
    "Destination panel: ",
    scales::comma(base::nrow(destination_panel)),
    " provider-county alternatives."
  )
  destination_model <- survival::clogit(
    chosen ~ training_tie + base::log(income_index) +
      unmet_demand_rate + academic_center + historical_tie +
      distance_penalty + rural_incentive + new_program +
      fellowship_slots + strata(provider_year_id),
    data = destination_panel,
    method = "efron"
  )

  base::message("Empirical geography models fitted.")
  base::list(
    relocation_model = relocation_model,
    destination_model = destination_model,
    destination_panel = destination_panel,
    relocation_rows = stats::nobs(relocation_model),
    destination_rows = stats::nobs(destination_model)
  )
}

#' Extract simulation coefficients from an empirical choice model
#'
#' @param fitted_models Return value from fit_endogenous_geography_models().
#' @return Named coefficient vector compatible with the solver.
extract_geography_coefficients <- function(fitted_models) {
  base::message("Extracting empirically fitted geography coefficients.")

  fitted_values <- stats::coef(fitted_models$destination_model)
  coefficient_map <- c(
    training_tie = "training_tie",
    income = "base::log(income_index)",
    unmet_demand = "unmet_demand_rate",
    academic_center = "academic_center",
    historical_tie = "historical_tie",
    distance_penalty = "distance_penalty",
    rural_incentive = "rural_incentive",
    new_program = "new_program",
    fellowship_slots = "fellowship_slots"
  )
  extracted_values <- fitted_values[coefficient_map]
  base::names(extracted_values) <- base::names(coefficient_map)

  c(
    extracted_values,
    current_county = 0
  )
}

#' Combine relocation hazards with conditional destination probabilities
#'
#' @param probability_table Provider-county conditional probabilities.
#' @param providers Provider-level prediction tibble.
#' @param relocation_model Fitted binomial relocation model or NULL.
#' @return Provider-county unconditional transition probabilities.
apply_relocation_probabilities <- function(
    probability_table,
    providers,
    relocation_model = NULL) {
  if (base::is.null(relocation_model)) {
    base::message("No relocation model supplied; using choice probabilities.")
    return(probability_table)
  }

  base::message("Applying empirical provider relocation hazards.")
  relocation_table <- providers |>
    dplyr::mutate(
      relocation_probability = stats::predict(
        relocation_model,
        newdata = providers,
        type = "response"
      )
    ) |>
    dplyr::select(provider_id, relocation_probability)

  transition_table <- probability_table |>
    dplyr::left_join(relocation_table, by = "provider_id") |>
    dplyr::mutate(
      is_current = county_fips == current_county_fips,
      mover_weight = dplyr::if_else(
        is_current,
        0,
        destination_probability
      )
    ) |>
    dplyr::group_by(provider_id) |>
    dplyr::mutate(
      mover_weight_sum = base::sum(mover_weight),
      destination_probability = dplyr::case_when(
        mover_weight_sum == 0 & is_current ~ 1,
        mover_weight_sum == 0 ~ 0,
        is_current ~ 1 - relocation_probability,
        TRUE ~ relocation_probability * mover_weight /
          mover_weight_sum
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-is_current, -mover_weight, -mover_weight_sum)

  transition_table
}
