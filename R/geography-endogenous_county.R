# County-level Endogenous Provider Geography & Spatial Equilibrium -------
#
# Solves county-level provider destination choices using a conditional logit framework
# coupled with iterative fixed-point supply-demand equilibrium.

#' Validate inputs for the endogenous geography model
#'
#' @param providers Provider-level tibble.
#' @param markets County-year market tibble.
#' @param choice_set Provider-county choice-set tibble.
#' @return Invisibly returns TRUE.
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
#' @param tolerance Maximum permitted demand-rate change.
#' @param max_iterations Maximum fixed-point iterations.
#' @param damping Weight placed on the newly calculated demand rate.
#' @return Named list containing county markets and choice probabilities.
#' @family geography
#' @concept geography
#' @export
solve_endogenous_geography <- function(
    providers,
    markets,
    choice_set,
    coefficients,
    year_value,
    accessibility = NULL,
    policy_shocks = NULL,
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
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
#' @family geography
#' @concept geography
#' @export
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
