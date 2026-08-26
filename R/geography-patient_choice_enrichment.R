# Empirical enrichment for the patient destination choice engine ------------

#' Build an enriched origin-destination choice set from DuckDB
#'
#' This function performs the joins lazily with duckplyr-compatible dplyr
#' verbs and collects only the completed choice set. Provider volume is lagged
#' to avoid using the outcome year to define destination attractiveness.
#'
#' @param connection Readable DuckDB connection.
#' @param travel_table Qualified table with `origin_id`, `destination_id`, and
#'   `travel_time_min`.
#' @param origin_table Qualified table with `origin_id`, `origin_demand`, and
#'   optional ACS/context covariates.
#' @param destination_table Qualified table with `destination_id`, `npi`,
#'   `fte`, `wait_days`, and `subspecialty`.
#' @param volume_table Optional qualified provider-year volume table with
#'   `npi`, `year`, and `service_volume`.
#' @param model_year Year for which choices are constructed.
#' @param max_travel_min Maximum feasible travel time.
#' @return Enriched origin-destination tibble.
#' @family patient destination choice
#' @concept geography
#' @export
build_empirical_patient_choice_set <- function(
    connection,
    travel_table,
    origin_table,
    destination_table,
    volume_table = NULL,
    model_year,
    max_travel_min = 240) {
  base::message(
    "build_empirical_patient_choice_set(): model year = ", model_year,
    "; maximum travel = ", max_travel_min, " minutes."
  )
  table_reference <- function(qualified_name) {
    parts <- base::strsplit(qualified_name, "\\.")[[1]]
    if (base::length(parts) == 1L) {
      dplyr::tbl(connection, parts[[1]])
    } else {
      dplyr::tbl(
        connection,
        DBI::Id(schema = parts[[1]], table = parts[[2]])
      )
    }
  }
  travel_relation <- table_reference(travel_table) |>
    dplyr::filter(.data$travel_time_min <= max_travel_min)
  origin_relation <- table_reference(origin_table)
  destination_relation <- table_reference(destination_table)
  enriched_relation <- travel_relation |>
    dplyr::inner_join(origin_relation, by = "origin_id") |>
    dplyr::inner_join(destination_relation, by = "destination_id")
  if (!base::is.null(volume_table)) {
    volume_relation <- table_reference(volume_table) |>
      dplyr::filter(.data$year == model_year - 1L) |>
      dplyr::group_by(.data$npi) |>
      dplyr::summarise(
        lagged_service_volume = base::sum(
          .data$service_volume,
          na.rm = TRUE
        ),
        .groups = "drop"
      )
    enriched_relation <- enriched_relation |>
      dplyr::left_join(volume_relation, by = "npi") |>
      dplyr::mutate(
        lagged_service_volume = dplyr::coalesce(
          .data$lagged_service_volume,
          0
        )
      )
  }
  base::message(
    "build_empirical_patient_choice_set(): collecting joined choice set."
  )
  enriched_choices <- enriched_relation |>
    dplyr::collect() |>
    tibble::as_tibble()
  if (base::nrow(enriched_choices) == 0L) {
    base::stop("The empirical choice-set join returned zero rows.",
               call. = FALSE)
  }
  base::message(
    "build_empirical_patient_choice_set(): returned ",
    scales::comma(base::nrow(enriched_choices)), " alternatives for ",
    scales::comma(dplyr::n_distinct(enriched_choices$origin_id)),
    " origins."
  )
  enriched_choices
}

#' Add observed origin-specific travel-barrier interactions
#'
#' @param choice_set Origin-destination alternatives.
#' @return Choice set with centered ACS/context covariates and interactions.
#' @family patient destination choice
#' @concept geography
#' @export
add_patient_travel_barrier_features <- function(choice_set) {
  base::message(
    "add_patient_travel_barrier_features(): constructing interactions."
  )
  defaults <- list(
    no_vehicle_share = 0,
    disability_share = 0,
    poverty_share = 0,
    transportation_barrier_share = 0,
    rural_share = 0,
    lagged_service_volume = 0
  )
  enriched_choices <- choice_set
  for (feature_name in base::names(defaults)) {
    if (!feature_name %in% base::names(enriched_choices)) {
      enriched_choices[[feature_name]] <- defaults[[feature_name]]
    }
  }
  enriched_choices |>
    dplyr::mutate(
      travel_no_vehicle = .data$travel_time_min *
        .data$no_vehicle_share,
      travel_disability = .data$travel_time_min *
        .data$disability_share,
      travel_poverty = .data$travel_time_min * .data$poverty_share,
      travel_transportation_barrier = .data$travel_time_min *
        .data$transportation_barrier_share,
      travel_rural = .data$travel_time_min * .data$rural_share,
      log_lagged_volume = base::log1p(
        dplyr::coalesce(.data$lagged_service_volume, 0)
      )
    )
}

#' Validate predicted flows against county or HRR utilization
#'
#' @param predicted_flows Data frame with geography and `expected_demand`.
#' @param cms_geographic_variation CMS geographic PUF with geography and an
#'   observed utilization measure.
#' @param geography_col,observed_col Column names.
#' @return Geography-level comparison and summary metrics.
#' @family patient destination choice
#' @concept validation
#' @export
validate_patient_flows_against_cms <- function(
    predicted_flows,
    cms_geographic_variation,
    geography_col = "destination_county",
    observed_col = "observed_utilization") {
  base::message(
    "validate_patient_flows_against_cms(): aggregating at ",
    geography_col, "."
  )
  predicted_summary <- predicted_flows |>
    dplyr::group_by(.data[[geography_col]]) |>
    dplyr::summarise(
      predicted_demand = base::sum(.data$expected_demand, na.rm = TRUE),
      .groups = "drop"
    )
  comparison <- predicted_summary |>
    dplyr::inner_join(
      cms_geographic_variation,
      by = geography_col
    ) |>
    dplyr::filter(
      base::is.finite(.data$predicted_demand),
      base::is.finite(.data[[observed_col]])
    )
  if (base::nrow(comparison) < 3L) {
    base::stop("Fewer than three comparable geographies.", call. = FALSE)
  }
  observed_values <- comparison[[observed_col]]
  summary_metrics <- tibble::tibble(
    n_geographies = base::nrow(comparison),
    pearson_correlation = stats::cor(
      comparison$predicted_demand,
      observed_values,
      method = "pearson"
    ),
    spearman_correlation = stats::cor(
      comparison$predicted_demand,
      observed_values,
      method = "spearman"
    ),
    mean_absolute_scaled_error = base::mean(
      base::abs(comparison$predicted_demand - observed_values)
    ) / base::mean(base::abs(observed_values))
  )
  list(comparison = comparison, metrics = summary_metrics)
}
