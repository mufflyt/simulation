# Patient Spatial Travel & Destination Choice Engine -----------------------

#' Default Spatial Choice Model Coefficients
#'
#' @return A named list of parameters for the Huff/MNL choice model.
#' @family geography
#' @concept choice
#' @export
default_destination_choice_coefficients <- function() {
  base::list(
    beta_fte = 0.65,
    beta_travel_time = 0.045, # Penalty per minute of travel time
    beta_wait_days = 0.025,   # Penalty per day of wait time
    beta_subspecialist = 0.85 # Premium for board-certified FPMRS/Urology subspecialist
  )
}

#' Calculate Huff Gravity / MNL Destination Choice Probabilities
#'
#' @description
#' Computes numerically stable destination choice probabilities for patients travelling from
#' origin $i$ to destination provider/county $j$:
#' \[V_{ij} = \beta_{\text{fte}} \log(\text{FTE}_j) - \beta_{\text{travel}} \text{TravelTime}_{ij} - \beta_{\text{wait}} \text{WaitDays}_j + \beta_{\text{subspec}} \text{Subspecialist}_j\]
#'
#' @param distance_matrix Matrix or data frame of origin-destination travel times (minutes).
#' @param destination_tbl Data frame of destination characteristics (`destination_id`, `clinical_fte`, `wait_days`, `has_subspecialist`).
#' @param coefficients Named list from [default_destination_choice_coefficients()].
#' @param max_travel_minutes Optional upper ceiling for feasible travel time (minutes).
#'
#' @return Matrix of origin-to-destination probabilities (rows = origins, cols = destinations).
#' @family geography
#' @concept choice
#' @export
calculate_patient_destination_probabilities <- function(
    distance_matrix,
    destination_tbl,
    coefficients = default_destination_choice_coefficients(),
    max_travel_minutes = NULL) {

  req_dest <- base::c("destination_id", "clinical_fte", "wait_days", "has_subspecialist")
  missing_dest <- base::setdiff(req_dest, base::names(destination_tbl))
  if (base::length(missing_dest) > 0L) {
    base::stop("Missing columns in destination_tbl: ", base::paste(missing_dest, collapse = ", "), call. = FALSE)
  }

  dist_mat <- base::as.matrix(distance_matrix)
  n_origins <- base::nrow(dist_mat)
  n_destinations <- base::ncol(dist_mat)

  if (n_destinations != base::nrow(destination_tbl)) {
    base::stop("Columns of distance_matrix must match rows of destination_tbl.", call. = FALSE)
  }

  log_fte <- base::log(base::pmax(0.01, destination_tbl$clinical_fte))
  subspec_flag <- base::as.numeric(destination_tbl$has_subspecialist)
  wait_days <- base::pmax(0, destination_tbl$wait_days)

  # Calculate systematic utility matrix V (n_origins x n_destinations)
  utility_mat <- base::matrix(0, nrow = n_origins, ncol = n_destinations)
  for (j in base::seq_len(n_destinations)) {
    utility_mat[, j] <- (coefficients$beta_fte * log_fte[[j]]) -
      (coefficients$beta_travel_time * dist_mat[, j]) -
      (coefficients$beta_wait_days * wait_days[[j]]) +
      (coefficients$beta_subspecialist * subspec_flag[[j]])
  }

  if (!base::is.null(max_travel_minutes)) {
    utility_mat[dist_mat > max_travel_minutes] <- -Inf
  }

  # Log-sum-exp stabilization for probabilities
  max_u <- base::apply(utility_mat, 1, base::max)
  exp_u <- base::exp(utility_mat - max_u)
  sum_exp <- base::rowSums(exp_u)

  prob_mat <- exp_u / sum_exp
  # Handle any origins with no feasible destinations
  prob_mat[base::is.nan(prob_mat)] <- 0

  base::colnames(prob_mat) <- destination_tbl$destination_id
  prob_mat
}

#' Predict Patient Destination Choice Flows and Capacity Allocation
#'
#' @description
#' Projects patient travel flows from origins to destinations, identifies cross-county and cross-HRR
#' boundary crossings, and clears demand against destination provider capacity constraints.
#'
#' @param origin_demand_tbl Tibble of patient origins (`origin_id`, `county_fips`, `hrr_code`, `patient_demand_n`).
#' @param destination_tbl Tibble of destinations (`destination_id`, `county_fips`, `hrr_code`, `clinical_fte`, `wait_days`, `has_subspecialist`, `capacity_patients_n`).
#' @param distance_matrix Matrix of travel times from origins to destinations.
#' @param coefficients Choice model coefficients.
#'
#' @return A list with flow predictions, boundary-crossing summary, and capacity clearing metrics.
#' @family geography
#' @concept choice
#' @export
predict_patient_destination_choice <- function(
    origin_demand_tbl,
    destination_tbl,
    distance_matrix,
    coefficients = default_destination_choice_coefficients()) {

  base::message("[patient-choice] Calculating Huff/MNL spatial destination choice probabilities.")
  prob_mat <- calculate_patient_destination_probabilities(
    distance_matrix = distance_matrix,
    destination_tbl = destination_tbl,
    coefficients = coefficients
  )

  n_origins <- base::nrow(origin_demand_tbl)
  n_destinations <- base::nrow(destination_tbl)

  # Expected unconstrained patient flow matrix (n_origins x n_destinations)
  expected_flows <- base::matrix(0, nrow = n_origins, ncol = n_destinations)
  for (i in base::seq_len(n_origins)) {
    expected_flows[i, ] <- origin_demand_tbl$patient_demand_n[[i]] * prob_mat[i, ]
  }

  # Aggregate demand received at each destination
  received_demand <- base::colSums(expected_flows)

  destination_summary <- destination_tbl |>
    dplyr::mutate(
      received_demand_n = received_demand,
      capacity_patients_n = dplyr::coalesce(.data$capacity_patients_n, .data$clinical_fte * 1600.0),
      served_demand_n = base::pmin(.data$received_demand_n, .data$capacity_patients_n),
      unmet_demand_n = base::pmax(0, .data$received_demand_n - .data$capacity_patients_n),
      capacity_utilization = dplyr::if_else(.data$capacity_patients_n > 0, .data$received_demand_n / .data$capacity_patients_n, 0)
    )

  # Boundary-crossing analysis
  dist_mat <- base::as.matrix(distance_matrix)
  boundary_flows <- base::vector("list", n_origins)

  for (i in base::seq_len(n_origins)) {
    orig_fips <- origin_demand_tbl$county_fips[[i]]
    orig_hrr  <- origin_demand_tbl$hrr_code[[i]]

    same_county <- destination_tbl$county_fips == orig_fips
    same_hrr    <- destination_tbl$hrr_code == orig_hrr

    boundary_flows[[i]] <- tibble::tibble(
      origin_id = origin_demand_tbl$origin_id[[i]],
      total_demand = origin_demand_tbl$patient_demand_n[[i]],
      same_county_flow = base::sum(expected_flows[i, same_county]),
      cross_county_flow = base::sum(expected_flows[i, !same_county]),
      same_hrr_flow = base::sum(expected_flows[i, same_hrr]),
      cross_hrr_flow = base::sum(expected_flows[i, !same_hrr]),
      mean_travel_mins = base::sum(expected_flows[i, ] * dist_mat[i, ]) / base::max(1, origin_demand_tbl$patient_demand_n[[i]])
    )
  }

  boundary_summary <- dplyr::bind_rows(boundary_flows)

  # Identify origins with severe access barriers (wait > 60 days or no local subspecialist)
  severe_access_origins <- origin_demand_tbl |>
    dplyr::mutate(
      high_wait_barrier = boundary_summary$mean_travel_mins > 45 | boundary_summary$cross_hrr_flow > (0.5 * origin_demand_tbl$patient_demand_n)
    )

  base::message("[patient-choice] Predicted flows across ", n_origins, " origins and ", n_destinations, " destinations.")
  base::list(
    probability_matrix = prob_mat,
    destination_summary = destination_summary,
    boundary_summary = boundary_summary,
    severe_access_origins = severe_access_origins
  )
}
