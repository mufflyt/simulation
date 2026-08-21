# Patient Travel and Spatial Gravity Choice Engine ----
#
# Complements E2SFCA by converting origin-level demand into expected flows to
# individual destinations. The engine retains cross-county and cross-HRR travel,
# estimates coefficients from observed choices when available, and separates
# structural access from capacity-constrained service delivery.

#' Default patient-destination choice coefficients
#'
#' These values are deliberately labelled priors / scenario defaults rather
#' than empirical urogynecology estimates. Use
#' [estimate_patient_destination_choice()] whenever observed visits are
#' available.
#'
#' @return Named numeric vector on the utility scale.
#' @family patient destination choice
#' @concept geography
#' @export
patient_choice_default_coefficients <- function() {
  c(
    log_fte = 0.70,
    travel_time = -0.035,
    wait_days = -0.020,
    subspecialty = 0.80
  )
}

#' Validate patient-destination choice inputs
#'
#' @param choice_set Long data frame with one row per feasible origin and
#'   destination pair.
#' @param require_demand Whether `origin_demand` is required.
#' @return `choice_set`, invisibly.
#' @keywords internal
validate_patient_choice_set <- function(choice_set,
                                        require_demand = TRUE) {
  needed <- c(
    "origin_id", "destination_id", "fte", "travel_time_min",
    "wait_days", "subspecialty"
  )
  if (isTRUE(require_demand)) {
    needed <- c(needed, "origin_demand")
  }
  if (!base::is.data.frame(choice_set)) {
    base::stop("`choice_set` must be a data frame.", call. = FALSE)
  }
  missing_names <- base::setdiff(needed, base::names(choice_set))
  if (base::length(missing_names) > 0L) {
    base::stop(
      "Missing required column(s): ",
      base::paste(missing_names, collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (base::anyNA(choice_set$origin_id) ||
      base::anyNA(choice_set$destination_id)) {
    base::stop("Origin and destination IDs cannot be missing.",
               call. = FALSE)
  }
  pair_key <- base::paste(
    choice_set$origin_id,
    choice_set$destination_id,
    sep = "\r"
  )
  if (base::anyDuplicated(pair_key)) {
    base::stop("Each origin-destination pair must be unique.",
               call. = FALSE)
  }
  numeric_names <- c("fte", "travel_time_min", "wait_days")
  if (isTRUE(require_demand)) {
    numeric_names <- c(numeric_names, "origin_demand")
  }
  invalid_numeric <- base::vapply(
    choice_set[numeric_names],
    function(value) {
      !base::is.numeric(value) ||
        base::any(!base::is.finite(value)) ||
        base::any(value < 0)
    },
    logical(1)
  )
  if (base::any(invalid_numeric)) {
    base::stop(
      "Numeric inputs must be finite, non-negative numbers: ",
      base::paste(base::names(invalid_numeric)[invalid_numeric],
                  collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (base::any(choice_set$fte <= 0)) {
    base::stop("`fte` must be greater than zero before log transformation.",
               call. = FALSE)
  }
  invisible(choice_set)
}

#' Calculate Huff gravity / multinomial-logit destination probabilities
#'
#' Utility is
#' `b_fte * log(FTE) + b_time * travel time + b_wait * wait days +`
#' `b_subspecialty * subspecialty`. Probabilities use a grouped log-sum-exp
#' calculation to avoid numerical overflow.
#'
#' @param choice_set Long origin-destination choice set. Required columns are
#'   `origin_id`, `destination_id`, `fte`, `travel_time_min`, `wait_days`,
#'   `subspecialty`, and `origin_demand`.
#' @param coefficients Named numeric vector with `log_fte`, `travel_time`,
#'   `wait_days`, and `subspecialty`.
#' @param max_travel_min Optional maximum feasible travel time. Rows beyond it
#'   are excluded before normalizing probabilities.
#' @return Choice set with `utility` and `choice_probability`.
#' @family patient destination choice
#' @concept geography
#' @export
predict_patient_destination_choice <- function(
    choice_set,
    coefficients = patient_choice_default_coefficients(),
    max_travel_min = Inf) {
  base::message(
    "predict_patient_destination_choice(): received ",
    scales::comma(base::nrow(choice_set)), " origin-destination pairs."
  )
  validate_patient_choice_set(choice_set, require_demand = TRUE)
  coefficient_names <- c(
    "log_fte", "travel_time", "wait_days", "subspecialty"
  )
  if (!base::is.numeric(coefficients) ||
      !base::all(coefficient_names %in% base::names(coefficients)) ||
      base::any(!base::is.finite(coefficients[coefficient_names]))) {
    base::stop(
      "`coefficients` must be finite and name: ",
      base::paste(coefficient_names, collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (!base::is.numeric(max_travel_min) ||
      base::length(max_travel_min) != 1L ||
      base::is.na(max_travel_min) || max_travel_min <= 0) {
    base::stop("`max_travel_min` must be one positive number.",
               call. = FALSE)
  }
  base::message(
    "predict_patient_destination_choice(): applying travel feasibility and ",
    "calculating utilities."
  )
  eligible_choices <- choice_set |>
    dplyr::filter(.data$travel_time_min <= max_travel_min) |>
    dplyr::mutate(
      subspecialty_numeric = base::as.numeric(.data$subspecialty),
      utility = coefficients[["log_fte"]] * base::log(.data$fte) +
        coefficients[["travel_time"]] * .data$travel_time_min +
        coefficients[["wait_days"]] * .data$wait_days +
        coefficients[["subspecialty"]] * .data$subspecialty_numeric
    ) |>
    dplyr::group_by(.data$origin_id) |>
    dplyr::mutate(
      utility_centered = .data$utility - base::max(.data$utility),
      choice_probability = base::exp(.data$utility_centered) /
        base::sum(base::exp(.data$utility_centered))
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-"subspecialty_numeric", -"utility_centered")
  lost_origins <- base::setdiff(
    base::unique(choice_set$origin_id),
    base::unique(eligible_choices$origin_id)
  )
  if (base::length(lost_origins) > 0L) {
    base::stop(
      scales::comma(base::length(lost_origins)),
      " origin(s) have no feasible destination after the travel filter.",
      call. = FALSE
    )
  }
  probability_check <- eligible_choices |>
    dplyr::group_by(.data$origin_id) |>
    dplyr::summarise(
      probability_sum = base::sum(.data$choice_probability),
      .groups = "drop"
    )
  if (base::any(base::abs(probability_check$probability_sum - 1) > 1e-10)) {
    base::stop("Choice probabilities failed the sum-to-one invariant.",
               call. = FALSE)
  }
  base::message(
    "predict_patient_destination_choice(): returned ",
    scales::comma(base::nrow(eligible_choices)), " feasible pairs."
  )
  eligible_choices
}

#' Estimate destination-choice coefficients from observed patient choices
#'
#' Fits a conditional multinomial logit by maximizing the full choice-set
#' likelihood. All nonchosen alternatives remain in the denominator; dropping
#' them would bias travel and wait-time coefficients.
#'
#' @param observed_choices Long choice set with a binary `chosen` column and one
#'   choice per `choice_event_id`. Other required fields match
#'   [predict_patient_destination_choice()].
#' @param initial Starting coefficient vector.
#' @param method Optimization method passed to [stats::optim()].
#' @param control Optimization control list.
#' @return List with coefficients, covariance matrix, standard errors, fit
#'   statistics, convergence code, and the number of events and alternatives.
#' @family patient destination choice
#' @concept geography
#' @export
estimate_patient_destination_choice <- function(
    observed_choices,
    initial = patient_choice_default_coefficients(),
    method = "BFGS",
    control = list(maxit = 1000L, reltol = 1e-10)) {
  base::message(
    "estimate_patient_destination_choice(): validating observed choices."
  )
  validate_patient_choice_set(observed_choices, require_demand = FALSE)
  needed <- c("choice_event_id", "chosen")
  if (!base::all(needed %in% base::names(observed_choices))) {
    base::stop("Observed choices require `choice_event_id` and `chosen`.",
               call. = FALSE)
  }
  event_check <- observed_choices |>
    dplyr::group_by(.data$choice_event_id) |>
    dplyr::summarise(
      chosen_count = base::sum(base::as.integer(.data$chosen)),
      alternative_count = dplyr::n(),
      .groups = "drop"
    )
  if (base::any(event_check$chosen_count != 1L) ||
      base::any(event_check$alternative_count < 2L)) {
    base::stop(
      "Every choice event must have one chosen and at least two alternatives.",
      call. = FALSE
    )
  }
  design_matrix <- base::cbind(
    log_fte = base::log(observed_choices$fte),
    travel_time = observed_choices$travel_time_min,
    wait_days = observed_choices$wait_days,
    subspecialty = base::as.numeric(observed_choices$subspecialty)
  )
  event_index <- base::match(
    observed_choices$choice_event_id,
    base::unique(observed_choices$choice_event_id)
  )
  chosen_index <- base::as.logical(observed_choices$chosen)
  negative_log_likelihood <- function(parameter_values) {
    utility <- base::drop(design_matrix %*% parameter_values)
    event_maximum <- base::as.numeric(
      base::tapply(utility, event_index, base::max)
    )
    centered <- utility - event_maximum[event_index]
    event_denominator <- base::as.numeric(
      base::tapply(base::exp(centered), event_index, base::sum)
    )
    log_probability <- centered -
      base::log(event_denominator[event_index])
    -base::sum(log_probability[chosen_index])
  }
  base::message(
    "estimate_patient_destination_choice(): fitting ",
    scales::comma(base::nrow(event_check)), " choice events across ",
    scales::comma(base::nrow(observed_choices)), " alternatives."
  )
  fitted_model <- stats::optim(
    par = initial[base::colnames(design_matrix)],
    fn = negative_log_likelihood,
    method = method,
    hessian = TRUE,
    control = control
  )
  covariance_matrix <- tryCatch(
    base::solve(fitted_model$hessian),
    error = function(condition) {
      base::matrix(
        NA_real_,
        nrow = base::length(fitted_model$par),
        ncol = base::length(fitted_model$par)
      )
    }
  )
  base::dimnames(covariance_matrix) <- list(
    base::names(fitted_model$par),
    base::names(fitted_model$par)
  )
  standard_errors <- base::sqrt(base::diag(covariance_matrix))
  null_log_likelihood <- -base::sum(
    base::log(event_check$alternative_count)
  )
  base::message(
    "estimate_patient_destination_choice(): convergence code = ",
    fitted_model$convergence, "; log-likelihood = ",
    base::format(-fitted_model$value, digits = 6), "."
  )
  list(
    coefficients = fitted_model$par,
    covariance = covariance_matrix,
    standard_errors = standard_errors,
    log_likelihood = -fitted_model$value,
    null_log_likelihood = null_log_likelihood,
    mcfadden_r_squared = 1 -
      ((-fitted_model$value) / null_log_likelihood),
    convergence = fitted_model$convergence,
    message = fitted_model$message,
    n_choice_events = base::nrow(event_check),
    n_alternatives = base::nrow(observed_choices)
  )
}

#' Allocate origin demand to patient-selected destinations
#'
#' @param choice_probabilities Return value from
#'   [predict_patient_destination_choice()].
#' @param destination_capacity Optional table with `destination_id` and
#'   `annual_capacity`. If supplied, served workload is capped and excess is
#'   returned as unmet demand. This is an accounting constraint, not a second
#'   behavioral model.
#' @param local_wait_threshold Days defining an access-pressure origin.
#' @return List containing pair flows, destination totals, origin totals,
#'   system diagnostics, and a conservation audit.
#' @family patient destination choice
#' @concept geography
#' @export
allocate_patient_destination_flows <- function(
    choice_probabilities,
    destination_capacity = NULL,
    local_wait_threshold = 60) {
  base::message(
    "allocate_patient_destination_flows(): allocating origin demand."
  )
  validate_patient_choice_set(choice_probabilities, require_demand = TRUE)
  needed <- c("choice_probability", "utility")
  if (!base::all(needed %in% base::names(choice_probabilities))) {
    base::stop(
      "Input must come from `predict_patient_destination_choice()`.",
      call. = FALSE
    )
  }
  origin_demand_check <- choice_probabilities |>
    dplyr::group_by(.data$origin_id) |>
    dplyr::summarise(
      distinct_demand = dplyr::n_distinct(.data$origin_demand),
      .groups = "drop"
    )
  if (base::any(origin_demand_check$distinct_demand != 1L)) {
    base::stop("`origin_demand` must be constant within each origin.",
               call. = FALSE)
  }
  pair_flows <- choice_probabilities |>
    dplyr::mutate(
      expected_demand = .data$origin_demand * .data$choice_probability
    )
  has_county <- base::all(
    c("origin_county", "destination_county") %in%
      base::names(pair_flows)
  )
  has_hrr <- base::all(
    c("origin_hrr", "destination_hrr") %in% base::names(pair_flows)
  )
  pair_flows$cross_county <- if (has_county) {
    pair_flows$origin_county != pair_flows$destination_county
  } else {
    base::rep(NA, base::nrow(pair_flows))
  }
  pair_flows$cross_hrr <- if (has_hrr) {
    pair_flows$origin_hrr != pair_flows$destination_hrr
  } else {
    base::rep(NA, base::nrow(pair_flows))
  }
  destination_totals <- pair_flows |>
    dplyr::group_by(.data$destination_id) |>
    dplyr::summarise(
      expected_demand = base::sum(.data$expected_demand),
      demand_weighted_travel_min = stats::weighted.mean(
        .data$travel_time_min,
        w = .data$expected_demand,
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  if (!base::is.null(destination_capacity)) {
    if (!base::is.data.frame(destination_capacity) ||
        !base::all(c("destination_id", "annual_capacity") %in%
                    base::names(destination_capacity)) ||
        base::anyDuplicated(destination_capacity$destination_id) ||
        base::any(!base::is.finite(destination_capacity$annual_capacity)) ||
        base::any(destination_capacity$annual_capacity < 0)) {
      base::stop(
        "Capacity needs unique `destination_id` and non-negative ",
        "`annual_capacity`.",
        call. = FALSE
      )
    }
    destination_totals <- destination_totals |>
      dplyr::left_join(destination_capacity, by = "destination_id") |>
      dplyr::mutate(
        annual_capacity = dplyr::coalesce(.data$annual_capacity, 0),
        served_demand = base::pmin(
          .data$expected_demand,
          .data$annual_capacity
        ),
        unmet_demand = base::pmax(
          .data$expected_demand - .data$annual_capacity,
          0
        ),
        capacity_utilization = dplyr::if_else(
          .data$annual_capacity > 0,
          .data$served_demand / .data$annual_capacity,
          NA_real_
        )
      )
  } else {
    destination_totals <- destination_totals |>
      dplyr::mutate(
        annual_capacity = NA_real_,
        served_demand = .data$expected_demand,
        unmet_demand = 0,
        capacity_utilization = NA_real_
      )
  }
  if (has_county) {
    pair_flows$local_destination <-
      pair_flows$origin_county == pair_flows$destination_county
  } else {
    pair_flows <- pair_flows |>
      dplyr::group_by(.data$origin_id) |>
      dplyr::mutate(
        local_destination = .data$travel_time_min ==
          base::min(.data$travel_time_min)
      ) |>
      dplyr::ungroup()
  }
  local_access <- pair_flows |>
    dplyr::group_by(.data$origin_id) |>
    dplyr::summarise(
      origin_demand = dplyr::first(.data$origin_demand),
      has_local_subspecialist = base::any(
        .data$local_destination & base::as.logical(.data$subspecialty)
      ),
      minimum_local_wait = base::ifelse(
        base::any(.data$local_destination),
        base::min(.data$wait_days[.data$local_destination]),
        Inf
      ),
      expected_travel_min = base::sum(
        .data$choice_probability * .data$travel_time_min
      ),
      probability_cross_county = base::ifelse(
        base::all(base::is.na(.data$cross_county)),
        NA_real_,
        base::sum(.data$choice_probability[.data$cross_county],
                  na.rm = TRUE)
      ),
      probability_cross_hrr = base::ifelse(
        base::all(base::is.na(.data$cross_hrr)),
        NA_real_,
        base::sum(.data$choice_probability[.data$cross_hrr],
                  na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      access_pressure = !.data$has_local_subspecialist |
        .data$minimum_local_wait > local_wait_threshold
    )
  total_demand <- base::sum(local_access$origin_demand)
  allocated_demand <- base::sum(pair_flows$expected_demand)
  conservation_error <- allocated_demand - total_demand
  system_diagnostics <- tibble::tibble(
    total_demand = total_demand,
    allocated_demand = allocated_demand,
    served_demand = base::sum(destination_totals$served_demand),
    unmet_demand = base::sum(destination_totals$unmet_demand),
    demand_weighted_travel_min = stats::weighted.mean(
      local_access$expected_travel_min,
      w = local_access$origin_demand
    ),
    demand_share_under_access_pressure = base::sum(
      local_access$origin_demand[local_access$access_pressure]
    ) / total_demand,
    conservation_error = conservation_error,
    conserved = base::abs(conservation_error) <=
      1e-8 * base::max(1, total_demand)
  )
  base::message(
    "allocate_patient_destination_flows(): allocated ",
    scales::comma(allocated_demand), " demand units; conservation error = ",
    base::format(conservation_error, scientific = TRUE), "."
  )
  list(
    pair_flows = pair_flows,
    destination_totals = destination_totals,
    origin_totals = local_access,
    system_diagnostics = system_diagnostics
  )
}

#' Run the patient travel and spatial gravity engine
#'
#' @param choice_set,coefficients,max_travel_min,destination_capacity,
#'   local_wait_threshold Passed to the prediction and allocation functions.
#' @return Full allocation list with coefficients and model metadata appended.
#' @family patient destination choice
#' @concept geography
#' @export
run_patient_destination_choice <- function(
    choice_set,
    coefficients = patient_choice_default_coefficients(),
    max_travel_min = Inf,
    destination_capacity = NULL,
    local_wait_threshold = 60) {
  base::message(
    "run_patient_destination_choice(): starting gravity-choice engine."
  )
  choice_probabilities <- predict_patient_destination_choice(
    choice_set = choice_set,
    coefficients = coefficients,
    max_travel_min = max_travel_min
  )
  allocation <- allocate_patient_destination_flows(
    choice_probabilities = choice_probabilities,
    destination_capacity = destination_capacity,
    local_wait_threshold = local_wait_threshold
  )
  allocation$coefficients <- coefficients
  allocation$metadata <- list(
    method = "Huff gravity / conditional multinomial logit",
    max_travel_min = max_travel_min,
    local_wait_threshold = local_wait_threshold
  )
  base::message(
    "run_patient_destination_choice(): completed ",
    scales::comma(base::nrow(allocation$pair_flows)), " patient-flow pairs."
  )
  allocation
}
