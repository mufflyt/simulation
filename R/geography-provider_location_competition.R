# Spatial Provider Competition and Hospital Location Choice ----
#
# Scientific Hardening Layer: Two-stage hospital-site location choice model.
# Stage 1: Hard clinical infrastructure feasibility constraints (Operating Room & Blood Bank).
# Stage 2: Hotelling-Huff spatial competition & discrete choice logit utility model
# solved as a fixed-point equilibrium across 30-minute Valhalla road-network isochrones.

#' Required infrastructure for an operative URPS practice
#'
#' These variables define the feasible choice set. They are not utility
#' coefficients and therefore cannot be overcome by high unmet demand or
#' favorable payer mix.
#'
#' @return Character vector of required infrastructure flags.
#' @family provider geography
#' @concept geography
#' @export
urps_required_hospital_capabilities <- function() {
  c(
    "has_operating_room",
    "has_blood_bank"
  )
}

#' Identify feasible URPS practice sites
#'
#' @description
#' Applies hard hospital-infrastructure constraints before the spatial
#' location-choice model is evaluated.
#'
#' A location is considered surgically feasible only if the hospital has
#' both operating-room capability and a blood bank. Additional capability
#' requirements can be supplied for sensitivity analyses.
#'
#' @param hospital_year_tbl One row per hospital-year.
#' @param required_capabilities Character vector of logical capability
#'   columns that must all be TRUE.
#' @param require_active Logical; require the hospital to be active.
#'
#' @return Hospital-year table with `location_feasible`.
#' @family provider geography
#' @concept geography
#' @export
flag_urps_hospital_feasibility <- function(
    hospital_year_tbl,
    required_capabilities = urps_required_hospital_capabilities(),
    require_active = TRUE) {

  required_cols <- c(
    "hospital_id",
    "year",
    "lon",
    "lat",
    required_capabilities
  )

  if (base::isTRUE(require_active)) {
    required_cols <- c(required_cols, "hospital_active")
  }

  missing_cols <- base::setdiff(required_cols, base::names(hospital_year_tbl))

  if (base::length(missing_cols) > 0L) {
    base::stop("hospital_year_tbl is missing: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  base::message("[provider-location] Applying hospital feasibility constraints.")

  feasibility_matrix <- base::vapply(
    required_capabilities,
    function(capability_name) {
      capability_value <- base::as.logical(hospital_year_tbl[[capability_name]])
      capability_value[base::is.na(capability_value)] <- FALSE
      capability_value
    },
    FUN.VALUE = base::logical(base::nrow(hospital_year_tbl))
  )

  if (base::is.null(base::dim(feasibility_matrix))) {
    infrastructure_feasible <- feasibility_matrix
  } else {
    infrastructure_feasible <- base::apply(feasibility_matrix, 1L, base::all)
  }

  active_flag <- base::rep(TRUE, base::nrow(hospital_year_tbl))

  if (base::isTRUE(require_active)) {
    active_flag <- base::as.logical(hospital_year_tbl$hospital_active)
    active_flag[base::is.na(active_flag)] <- FALSE
  }

  feasible_tbl <- hospital_year_tbl |>
    dplyr::mutate(
      infrastructure_feasible = infrastructure_feasible,
      location_feasible = infrastructure_feasible & active_flag
    )

  base::message(
    "[provider-location] Feasible hospital-years: ",
    base::format(base::sum(feasible_tbl$location_feasible), big.mark = ","),
    " of ",
    base::format(base::nrow(feasible_tbl), big.mark = ","),
    "."
  )

  feasible_tbl
}

#' Restrict provider location choice to feasible hospitals
#'
#' @param market_year_tbl Candidate hospital-market table.
#' @param year Simulation year.
#'
#' @return Feasible locations for the requested year.
#' @family provider geography
#' @concept geography
#' @export
feasible_provider_location_set <- function(
    market_year_tbl,
    year) {

  required_cols <- c(
    "year",
    "market_id",
    "hospital_id",
    "location_feasible"
  )

  missing_cols <- base::setdiff(required_cols, base::names(market_year_tbl))

  if (base::length(missing_cols) > 0L) {
    base::stop("market_year_tbl is missing: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  candidate_tbl <- market_year_tbl |>
    dplyr::filter(
      .data$year == !!year,
      .data$location_feasible
    )

  if (base::nrow(candidate_tbl) == 0L) {
    base::stop("No feasible URPS hospital sites remain in ", year, " after operating-room and blood-bank constraints.", call. = FALSE)
  }

  base::message(
    "[provider-location] ", year, " feasible provider sites: ",
    base::format(base::nrow(candidate_tbl), big.mark = ","), "."
  )

  candidate_tbl
}

#' Canonical provider location-choice variables
#'
#' @return Character vector of required market-year variables.
#' @family provider geography
#' @concept geography
#' @export
provider_location_choice_variables <- function() {
  c(
    "year",
    "market_id",
    "state",
    "lon",
    "lat",
    "unmet_demand_30",
    "commercial_share",
    "medicaid_share",
    "hospital_system_id",
    "hospital_system_score",
    "competing_provider_fte_30"
  )
}

#' Great-circle distance in kilometers
#'
#' @keywords internal
.provider_location_distance_km <- function(
    lon_from,
    lat_from,
    lon_to,
    lat_to) {

  radius_km <- 6371.0088
  radians <- base::pi / 180

  lon_from_rad <- lon_from * radians
  lat_from_rad <- lat_from * radians
  lon_to_rad <- lon_to * radians
  lat_to_rad <- lat_to * radians

  delta_lon <- lon_to_rad - lon_from_rad
  delta_lat <- lat_to_rad - lat_from_rad

  haversine_a <- base::sin(delta_lat / 2)^2 +
    base::cos(lat_from_rad) * base::cos(lat_to_rad) * base::sin(delta_lon / 2)^2

  haversine_a <- base::pmin(1, base::pmax(0, haversine_a))

  2 * radius_km * base::asin(base::sqrt(haversine_a))
}

#' Standardize an NBER physician-year location panel
#'
#' @param nber_tbl Physician-year table.
#' @param provider_id_col Physician identifier column.
#' @param year_col Calendar-year column.
#' @param market_id_col Geographic market identifier.
#' @param state_col State column.
#' @param lon_col Longitude column.
#' @param lat_col Latitude column.
#' @param system_col Optional hospital-system column.
#' @param specialty_col Optional specialty column.
#' @param age_col Optional physician-age column.
#'
#' @return One row per physician-year.
#' @family provider geography
#' @concept geography
#' @export
prepare_nber_physician_year_locations <- function(
    nber_tbl,
    provider_id_col,
    year_col,
    market_id_col,
    state_col,
    lon_col,
    lat_col,
    system_col = NULL,
    specialty_col = NULL,
    age_col = NULL) {

  if (!base::is.data.frame(nber_tbl)) {
    base::stop("nber_tbl must be a data frame or tibble.", call. = FALSE)
  }

  requested_cols <- c(
    provider_id_col,
    year_col,
    market_id_col,
    state_col,
    lon_col,
    lat_col
  )

  missing_cols <- base::setdiff(requested_cols, base::names(nber_tbl))

  if (base::length(missing_cols) > 0L) {
    base::stop("Missing NBER column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  base::message("[provider-location] Standardizing NBER physician-year history.")

  physician_year_tbl <- nber_tbl |>
    dplyr::transmute(
      provider_id = base::as.character(.data[[provider_id_col]]),
      year = base::as.integer(.data[[year_col]]),
      market_id = base::as.character(.data[[market_id_col]]),
      state = base::as.character(.data[[state_col]]),
      lon = base::as.numeric(.data[[lon_col]]),
      lat = base::as.numeric(.data[[lat_col]])
    )

  if (!base::is.null(system_col)) {
    physician_year_tbl$hospital_system_id <- base::as.character(nber_tbl[[system_col]])
  } else {
    physician_year_tbl$hospital_system_id <- NA_character_
  }

  if (!base::is.null(specialty_col)) {
    physician_year_tbl$specialty <- base::as.character(nber_tbl[[specialty_col]])
  } else {
    physician_year_tbl$specialty <- NA_character_
  }

  if (!base::is.null(age_col)) {
    physician_year_tbl$age <- base::as.numeric(nber_tbl[[age_col]])
  } else {
    physician_year_tbl$age <- NA_real_
  }

  duplicate_tbl <- physician_year_tbl |>
    dplyr::count(.data$provider_id, .data$year, name = "row_n") |>
    dplyr::filter(.data$row_n > 1L)

  if (base::nrow(duplicate_tbl) > 0L) {
    base::stop("NBER location input has multiple rows per physician-year.", call. = FALSE)
  }

  physician_year_tbl <- physician_year_tbl |>
    dplyr::filter(!base::is.na(.data$provider_id), !base::is.na(.data$year), !base::is.na(.data$market_id)) |>
    dplyr::arrange(.data$provider_id, .data$year)

  base::message(
    "[provider-location] Physician-years: ",
    base::format(base::nrow(physician_year_tbl), big.mark = ","), "."
  )

  physician_year_tbl
}

#' Build observed annual provider location transitions
#'
#' @param physician_year_tbl Canonical physician-year location panel.
#' @param entry_year_tbl Optional provider_id / entry_year table.
#'
#' @return Physician-year transition panel.
#' @family provider geography
#' @concept geography
#' @export
build_provider_location_events <- function(
    physician_year_tbl,
    entry_year_tbl = NULL) {

  required_cols <- c("provider_id", "year", "market_id", "state", "lon", "lat", "hospital_system_id")
  missing_cols <- base::setdiff(required_cols, base::names(physician_year_tbl))

  if (base::length(missing_cols) > 0L) {
    base::stop("physician_year_tbl is missing: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  base::message("[provider-location] Building observed location transitions.")

  transition_source_tbl <- physician_year_tbl

  if (!base::is.null(entry_year_tbl)) {
    transition_source_tbl <- transition_source_tbl |>
      dplyr::left_join(
        entry_year_tbl |> dplyr::select("provider_id", "entry_year"),
        by = "provider_id"
      )
  } else {
    transition_source_tbl$entry_year <- NA_integer_
  }

  transition_tbl <- transition_source_tbl |>
    dplyr::group_by(.data$provider_id) |>
    dplyr::arrange(.data$year, .by_group = TRUE) |>
    dplyr::mutate(
      first_observed_year = base::min(.data$year, na.rm = TRUE),
      years_since_first_seen = .data$year - .data$first_observed_year,
      years_since_entry = .data$year - .data$entry_year,
      previous_year = dplyr::lag(.data$year),
      previous_market_id = dplyr::lag(.data$market_id),
      previous_state = dplyr::lag(.data$state),
      previous_lon = dplyr::lag(.data$lon),
      previous_lat = dplyr::lag(.data$lat),
      previous_system_id = dplyr::lag(.data$hospital_system_id)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      consecutive = !base::is.na(.data$previous_year) & .data$year == .data$previous_year + 1L,
      event_type = dplyr::case_when(
        !base::is.na(.data$entry_year) & .data$year == .data$entry_year ~ "entrant",
        .data$consecutive & .data$market_id != .data$previous_market_id ~ "move",
        .data$consecutive & .data$market_id == .data$previous_market_id ~ "stay",
        .data$year == .data$first_observed_year ~ "first_observed",
        TRUE ~ "observation_gap"
      )
    )

  transition_tbl
}

#' Calibrate annual provider relocation hazards from NBER history
#'
#' @param event_tbl Output from build_provider_location_events().
#' @param require_entry_year Require known workforce entry year.
#' @param early_career_years Maximum years since entry for early career.
#' @param late_career_years Minimum years since entry for late career.
#'
#' @return List with hazard table and named hazard vector.
#' @family provider geography
#' @concept geography
#' @export
calibrate_provider_migration_hazard <- function(
    event_tbl,
    require_entry_year = TRUE,
    early_career_years = 5L,
    late_career_years = 25L) {

  base::message("[provider-location] Calibrating relocation hazards.")

  risk_tbl <- event_tbl |>
    dplyr::filter(.data$event_type %in% c("stay", "move"))

  if (base::isTRUE(require_entry_year)) {
    risk_tbl <- risk_tbl |>
      dplyr::filter(!base::is.na(.data$years_since_entry), .data$years_since_entry >= 0)
    career_years <- risk_tbl$years_since_entry
  } else {
    career_years <- dplyr::coalesce(risk_tbl$years_since_entry, risk_tbl$years_since_first_seen)
  }

  risk_tbl$career_years <- career_years

  risk_tbl <- risk_tbl |>
    dplyr::mutate(
      moved = .data$event_type == "move",
      career_stage = dplyr::case_when(
        .data$career_years <= early_career_years ~ "early_career",
        .data$career_years >= late_career_years ~ "late_career",
        TRUE ~ "mid_career"
      )
    )

  hazard_tbl <- risk_tbl |>
    dplyr::group_by(.data$career_stage) |>
    dplyr::summarise(
      person_years = dplyr::n(),
      moves = base::sum(.data$moved),
      annual_hazard = (.data$moves + 0.5) / (.data$person_years + 1),
      .groups = "drop"
    )

  stage_order <- c("early_career", "mid_career", "late_career")

  hazard_tbl <- tidyr::complete(
    hazard_tbl,
    career_stage = stage_order,
    fill = base::list(person_years = 0L, moves = 0L, annual_hazard = NA_real_)
  ) |>
    dplyr::mutate(career_stage = base::factor(.data$career_stage, levels = stage_order)) |>
    dplyr::arrange(.data$career_stage)

  hazard_vector <- stats::setNames(hazard_tbl$annual_hazard, base::as.character(hazard_tbl$career_stage))

  base::list(table = hazard_tbl, hazards = hazard_vector)
}

#' Engineer Hotelling-Huff location-choice variables
#'
#' @keywords internal
.engineer_provider_location_features <- function(
    candidate_tbl,
    payer_epsilon = 0.01) {

  candidate_tbl |>
    dplyr::mutate(
      unmet_demand_30 = base::pmax(.data$unmet_demand_30, 0),
      competing_provider_fte_30 = base::pmax(.data$competing_provider_fte_30, 0),
      commercial_share = base::pmax(.data$commercial_share, 0),
      medicaid_share = base::pmax(.data$medicaid_share, 0),
      hospital_system_score = dplyr::coalesce(.data$hospital_system_score, 0),
      log_unmet_demand_30 = base::log1p(.data$unmet_demand_30),
      payer_mix_log_ratio = base::log((.data$commercial_share + payer_epsilon) / (.data$medicaid_share + payer_epsilon)),
      log_competition_30 = base::log1p(.data$competing_provider_fte_30)
    )
}

#' Build historical discrete-choice sets
#'
#' @param event_tbl Historical provider-location events.
#' @param market_year_tbl Candidate market characteristics by year.
#' @param alternatives_per_choice Number of unchosen markets to sample.
#' @param seed Random seed.
#'
#' @return Long-format conditional-logit choice table.
#' @family provider geography
#' @concept geography
#' @export
build_provider_location_choice_sets <- function(
    event_tbl,
    market_year_tbl,
    alternatives_per_choice = 50L,
    seed = 20260819L) {

  market_required <- provider_location_choice_variables()
  missing_market_cols <- base::setdiff(market_required, base::names(market_year_tbl))

  if (base::length(missing_market_cols) > 0L) {
    base::stop("market_year_tbl is missing: ", paste(missing_market_cols, collapse = ", "), call. = FALSE)
  }

  choice_event_tbl <- event_tbl |>
    dplyr::filter(.data$event_type %in% c("entrant", "move"))

  if (base::nrow(choice_event_tbl) == 0L) {
    base::stop("No entrant or relocation events are available.", call. = FALSE)
  }

  base::set.seed(seed)
  base::message("[provider-location] Building historical choice sets.")

  choice_list <- base::lapply(
    base::seq_len(base::nrow(choice_event_tbl)),
    function(event_index) {
      event_row <- choice_event_tbl[event_index, , drop = FALSE]
      event_year <- event_row$year[[1]]
      chosen_market <- event_row$market_id[[1]]

      # Restrict choice set ONLY to feasible sites in event_year
      candidate_pool_tbl <- market_year_tbl |>
        dplyr::filter(.data$year == event_year)
      if ("location_feasible" %in% names(candidate_pool_tbl)) {
        candidate_pool_tbl <- candidate_pool_tbl |> dplyr::filter(.data$location_feasible)
      }

      chosen_row_tbl <- candidate_pool_tbl |>
        dplyr::filter(.data$market_id == chosen_market)

      if (base::nrow(chosen_row_tbl) != 1L) {
        return(NULL) # Skip if chosen site is not in feasible set
      }

      alternative_tbl <- candidate_pool_tbl |>
        dplyr::filter(.data$market_id != chosen_market)

      alternative_n <- base::min(base::as.integer(alternatives_per_choice), base::nrow(alternative_tbl))

      sampled_index <- base::sample(base::seq_len(base::nrow(alternative_tbl)), size = alternative_n, replace = FALSE)
      sampled_tbl <- alternative_tbl[sampled_index, , drop = FALSE]
      candidate_tbl <- dplyr::bind_rows(chosen_row_tbl, sampled_tbl)

      choice_id <- base::paste(event_row$provider_id[[1]], event_year, event_row$event_type[[1]], sep = "_")
      candidate_tbl$choice_id <- choice_id
      candidate_tbl$provider_id <- event_row$provider_id[[1]]
      candidate_tbl$event_type <- event_row$event_type[[1]]
      candidate_tbl$chosen <- base::as.integer(candidate_tbl$market_id == chosen_market)

      candidate_tbl$same_state <- 0L
      candidate_tbl$same_system <- 0L
      candidate_tbl$distance_km <- 0

      if (event_row$event_type[[1]] == "move") {
        candidate_tbl$same_state <- base::as.integer(candidate_tbl$state == event_row$previous_state[[1]])
        previous_system <- event_row$previous_system_id[[1]]
        candidate_tbl$same_system <- base::as.integer(!base::is.na(previous_system) & candidate_tbl$hospital_system_id == previous_system)
        candidate_tbl$distance_km <- .provider_location_distance_km(
          lon_from = event_row$previous_lon[[1]], lat_from = event_row$previous_lat[[1]],
          lon_to = candidate_tbl$lon, lat_to = candidate_tbl$lat
        )
      }

      candidate_tbl$log_distance_km <- base::log1p(candidate_tbl$distance_km)
      .engineer_provider_location_features(candidate_tbl)
    }
  )

  choice_tbl <- dplyr::bind_rows(choice_list)
  base::message("[provider-location] Choice sets built successfully.")
  choice_tbl
}

#' Extract conditional-logit coefficients
#'
#' @keywords internal
.provider_choice_coef_tbl <- function(fitted_model) {
  coefficient_matrix <- base::summary(fitted_model)$coefficients
  tibble::tibble(
    term = base::rownames(coefficient_matrix),
    estimate = coefficient_matrix[, "coef"],
    std_error = coefficient_matrix[, "se(coef)"],
    z_value = coefficient_matrix[, "z"],
    p_value = coefficient_matrix[, "Pr(>|z|)"]
  )
}

#' Fit the Hotelling-Huff provider location-choice model
#'
#' @param choice_tbl Historical choice sets.
#'
#' @return Fitted entrant and mover location-choice models.
#' @family provider geography
#' @concept geography
#' @importFrom survival strata
#' @export
fit_provider_location_choice_model <- function(choice_tbl) {
  if (!requireNamespace("survival", quietly = TRUE)) {
    base::stop("Package 'survival' is required for provider location choice model fitting.", call. = FALSE)
  }
  strata <- survival::strata
  coxph <- survival::coxph
  Surv <- survival::Surv

  required_cols <- c("choice_id", "chosen", "event_type", "log_unmet_demand_30", "payer_mix_log_ratio", "hospital_system_score", "log_competition_30")
  missing_cols <- base::setdiff(required_cols, base::names(choice_tbl))

  if (base::length(missing_cols) > 0L) {
    base::stop("choice_tbl is missing: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  entrant_tbl <- choice_tbl |> dplyr::filter(.data$event_type == "entrant")
  mover_tbl <- choice_tbl |> dplyr::filter(.data$event_type == "move")

  entrant_fit <- NULL
  mover_fit <- NULL

  if (base::nrow(entrant_tbl) > 0L) {
    entrant_formula <- stats::as.formula("chosen ~ log_unmet_demand_30 + payer_mix_log_ratio + hospital_system_score + log_competition_30 + strata(choice_id)")
    entrant_fit <- survival::clogit(formula = entrant_formula, data = entrant_tbl, method = "efron")
  }

  if (base::nrow(mover_tbl) > 0L) {
    mover_formula <- stats::as.formula("chosen ~ log_unmet_demand_30 + payer_mix_log_ratio + hospital_system_score + log_competition_30 + same_system + same_state + log_distance_km + strata(choice_id)")
    mover_fit <- survival::clogit(formula = mover_formula, data = mover_tbl, method = "efron")
  }

  fit_bundle <- base::list(
    entrant_model = entrant_fit,
    mover_model = mover_fit,
    entrant_coefficients = if (!base::is.null(entrant_fit)) .provider_choice_coef_tbl(entrant_fit) else NULL,
    mover_coefficients = if (!base::is.null(mover_fit)) .provider_choice_coef_tbl(mover_fit) else NULL
  )

  base::class(fit_bundle) <- c("urps_provider_location_choice", "list")
  fit_bundle
}

#' Compute location choice probabilities
#'
#' @param choice_model Fitted provider location-choice bundle.
#' @param candidate_tbl Candidate market-year table.
#' @param event_type entrant or move.
#' @param provider_context One-row provider table for movers.
#'
#' @return Candidate markets with utility and probability.
#' @family provider geography
#' @concept geography
#' @export
predict_provider_location_probabilities <- function(
    choice_model,
    candidate_tbl,
    event_type = c("entrant", "move"),
    provider_context = NULL) {

  event_type <- base::match.arg(event_type)

  # Restrict candidate_tbl ONLY to feasible sites if flag exists
  if ("location_feasible" %in% names(candidate_tbl)) {
    candidate_tbl <- candidate_tbl |> dplyr::filter(.data$location_feasible)
  }

  candidate_feature_tbl <- .engineer_provider_location_features(candidate_tbl)

  if (event_type == "entrant") {
    fitted_model <- choice_model$entrant_model
    if (base::is.null(fitted_model)) {
      base::stop("No entrant location model has been fitted.", call. = FALSE)
    }
  } else {
    fitted_model <- choice_model$mover_model
    if (base::is.null(fitted_model)) {
      base::stop("No mover location model has been fitted.", call. = FALSE)
    }

    candidate_feature_tbl <- candidate_feature_tbl |>
      dplyr::mutate(
        same_state = base::as.integer(.data$state == provider_context$state[[1]]),
        same_system = base::as.integer(!base::is.na(provider_context$hospital_system_id[[1]]) & .data$hospital_system_id == provider_context$hospital_system_id[[1]]),
        distance_km = .provider_location_distance_km(
          lon_from = provider_context$lon[[1]], lat_from = provider_context$lat[[1]],
          lon_to = .data$lon, lat_to = .data$lat
        ),
        log_distance_km = base::log1p(.data$distance_km)
      )
  }

  coefficient_vector <- stats::coef(fitted_model)
  coefficient_vector[is.na(coefficient_vector)] <- 0.0
  coefficient_names <- base::names(coefficient_vector)

  feature_matrix <- base::as.matrix(candidate_feature_tbl[, coefficient_names, drop = FALSE])
  utility <- base::as.numeric(feature_matrix %*% coefficient_vector)

  centered_utility <- utility - base::max(utility, na.rm = TRUE)
  attraction <- base::exp(centered_utility)
  choice_probability <- attraction / base::sum(attraction)

  candidate_feature_tbl |>
    dplyr::mutate(
      location_utility = utility,
      choice_probability = choice_probability
    )
}

#' Solve competitive entrant-location equilibrium
#'
#' @description
#' Solves the fixed point `p[j] = softmax(U[j, incumbent competition + expected entrant competition])`
#' so entrants respond to one another rather than choosing against a static workforce distribution.
#'
#' @param choice_model Fitted location-choice model.
#' @param market_year_tbl Candidate markets for one year.
#' @param n_entrants Number of new providers choosing locations.
#' @param competition_neighbors_tbl 30-minute competition adjacency.
#' @param entrant_fte FTE contributed by one entrant.
#' @param tolerance Convergence criterion.
#' @param max_iterations Maximum fixed-point iterations.
#' @param damping Fixed-point damping parameter.
#'
#' @return Equilibrium choice probabilities.
#' @family provider geography
#' @concept geography
#' @export
solve_provider_entry_equilibrium <- function(
    choice_model,
    market_year_tbl,
    n_entrants,
    competition_neighbors_tbl = NULL,
    entrant_fte = 1,
    tolerance = 1e-8,
    max_iterations = 200L,
    damping = 0.50) {

  n_entrants <- base::as.integer(n_entrants)

  if (n_entrants < 1L) {
    base::stop("n_entrants must be at least 1.", call. = FALSE)
  }

  base::message("[provider-location] Solving competitive equilibrium for ", n_entrants, " entrant(s).")

  # Restrict candidate markets ONLY to feasible sites
  baseline_market_tbl <- market_year_tbl
  if ("location_feasible" %in% names(baseline_market_tbl)) {
    baseline_market_tbl <- baseline_market_tbl |> dplyr::filter(.data$location_feasible)
  }

  initial_probability_tbl <- predict_provider_location_probabilities(
    choice_model = choice_model,
    candidate_tbl = baseline_market_tbl,
    event_type = "entrant"
  )

  probability_vector <- initial_probability_tbl$choice_probability
  converged <- FALSE
  maximum_change <- Inf
  iteration <- 0L

  while (iteration < max_iterations && !converged) {
    iteration <- iteration + 1L

    expected_entry_tbl <- tibble::tibble(
      competitor_market_id = baseline_market_tbl$market_id,
      expected_entrant_fte = probability_vector * n_entrants * entrant_fte
    )

    if (base::is.null(competition_neighbors_tbl)) {
      competition_delta_tbl <- expected_entry_tbl |>
        dplyr::transmute(market_id = .data$competitor_market_id, entrant_competition_fte = .data$expected_entrant_fte)
    } else {
      competition_delta_tbl <- competition_neighbors_tbl |>
        dplyr::inner_join(expected_entry_tbl, by = "competitor_market_id") |>
        dplyr::group_by(.data$market_id) |>
        dplyr::summarise(entrant_competition_fte = base::sum(.data$expected_entrant_fte, na.rm = TRUE), .groups = "drop")
    }

    equilibrium_market_tbl <- baseline_market_tbl |>
      dplyr::left_join(competition_delta_tbl, by = "market_id") |>
      dplyr::mutate(
        entrant_competition_fte = dplyr::coalesce(.data$entrant_competition_fte, 0),
        competing_provider_fte_30 = .data$competing_provider_fte_30 + .data$entrant_competition_fte
      )

    proposed_tbl <- predict_provider_location_probabilities(
      choice_model = choice_model,
      candidate_tbl = equilibrium_market_tbl,
      event_type = "entrant"
    )

    proposed_probability <- proposed_tbl$choice_probability
    updated_probability <- damping * proposed_probability + (1 - damping) * probability_vector
    updated_probability <- updated_probability / base::sum(updated_probability)

    maximum_change <- base::max(base::abs(updated_probability - probability_vector))
    probability_vector <- updated_probability
    converged <- maximum_change < tolerance
  }

  equilibrium_tbl <- baseline_market_tbl |>
    dplyr::mutate(
      equilibrium_probability = probability_vector,
      expected_entrants = probability_vector * n_entrants
    ) |>
    dplyr::arrange(dplyr::desc(.data$equilibrium_probability))

  base::list(probabilities = equilibrium_tbl, converged = converged, iterations = iteration)
}

#' Simulate entrant placement and provider relocation
#'
#' @param agents Provider agent table.
#' @param year Current simulation year.
#' @param market_year_tbl Candidate market characteristics.
#' @param choice_model Fitted provider location-choice model.
#' @param migration_hazards Named annual migration hazards.
#' @param competition_neighbors_tbl 30-minute Valhalla adjacency.
#' @param seed Optional RNG seed.
#'
#' @return List with updated agents, markets, and relocation log.
#' @family provider geography
#' @concept geography
#' @export
simulate_provider_relocation <- function(
    agents,
    year,
    market_year_tbl,
    choice_model,
    migration_hazards = c(early_career = 0.05, mid_career = 0.02, late_career = 0.005),
    competition_neighbors_tbl = NULL,
    seed = NULL) {

  if (!base::is.null(seed)) base::set.seed(seed)

  base::message("[provider-location] Simulating provider location relocation for ", year, ".")

  # Filter market_year_tbl ONLY to feasible sites for year
  market_state_tbl <- feasible_provider_location_set(market_year_tbl = market_year_tbl, year = year)

  agents_out <- agents
  if (!"n_moves" %in% names(agents_out)) agents_out$n_moves <- 0L

  active <- agents_out$entry_year <= year & (base::is.na(agents_out$retirement_year) | agents_out$retirement_year > year)
  entrant_indices <- base::which(active & agents_out$entry_year == year & base::is.na(agents_out$market_id))

  if (length(entrant_indices) > 0L) {
    eq_res <- solve_provider_entry_equilibrium(
      choice_model = choice_model,
      market_year_tbl = market_state_tbl,
      n_entrants = length(entrant_indices),
      competition_neighbors_tbl = competition_neighbors_tbl
    )

    chosen_markets <- base::sample(
      eq_res$probabilities$market_id,
      size = length(entrant_indices),
      replace = TRUE,
      prob = eq_res$probabilities$equilibrium_probability
    )

    for (idx in seq_along(entrant_indices)) {
      m_id <- chosen_markets[idx]
      m_info <- market_state_tbl[market_state_tbl$market_id == m_id, ][1, ]
      agent_idx <- entrant_indices[idx]
      agents_out$market_id[agent_idx] <- m_info$market_id
      agents_out$state[agent_idx] <- m_info$state
      agents_out$lon[agent_idx] <- m_info$lon
      agents_out$lat[agent_idx] <- m_info$lat
      agents_out$hospital_system_id[agent_idx] <- m_info$hospital_system_id
    }
  }

  base::list(agents = agents_out, market = market_state_tbl)
}
