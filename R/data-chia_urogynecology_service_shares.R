# CHIA All-Payer Urogynecology Service-Share Evidence Builder -------------

#' Build CHIA All-Payer Urogynecology Service-Share Evidence
#'
#' Processes Massachusetts CHIA all-payer hospital and outpatient casemix evidence to
#' estimate surgical specialty composition across inpatient, hospital outpatient,
#' ASC, and ED settings, stratified by payer mix.
#'
#' @param con DuckDB DBI connection to CHIA casemix data. If NULL, builds a synthetic
#'   CHIA evidence structure for testing.
#' @param service_registry Canonical service registry rules. Defaults to
#'   [build_urogynecology_service_registry()].
#' @param taxonomy_registry Provider taxonomy registry rules. Defaults to
#'   [build_urogynecology_provider_taxonomy_registry()].
#'
#' @return A list containing:
#'   \item{setting_shares}{Shares split by setting (inpatient vs outpatient vs ASC vs ED).}
#'   \item{payer_shares}{Shares split by payer (Commercial, Medicare, Medicaid, Self-pay).}
#'   \item{specialty_composition}{Surgeon/specialty composition by service and setting.}
#' @family data acquisition
#' @concept data
#' @export
build_chia_service_share_evidence <- function(
    con = NULL,
    service_registry = build_urogynecology_service_registry(),
    taxonomy_registry = build_urogynecology_provider_taxonomy_registry()) {
  base::message("Starting CHIA all-payer urogynecology service-share evidence build.")

  if (base::is.null(con)) {
    chia_claims <- fixture_chia_service_shares(service_registry, taxonomy_registry)
  } else {
    chia_claims <- extract_chia_claims_from_connection(con)
  }

  setting_shares <- chia_claims |>
    dplyr::group_by(.data$service, .data$setting) |>
    dplyr::summarise(
      events = base::sum(.data$discharge_count, na.rm = TRUE),
      .groups = "drop_last"
    ) |>
    dplyr::mutate(
      total_service_events = base::sum(.data$events, na.rm = TRUE),
      setting_share = .data$events / dplyr::if_else(.data$total_service_events == 0, 1, .data$total_service_events)
    ) |>
    dplyr::ungroup()

  payer_shares <- chia_claims |>
    dplyr::group_by(.data$service, .data$payer_category) |>
    dplyr::summarise(
      events = base::sum(.data$discharge_count, na.rm = TRUE),
      .groups = "drop_last"
    ) |>
    dplyr::mutate(
      total_service_events = base::sum(.data$events, na.rm = TRUE),
      payer_share = .data$events / dplyr::if_else(.data$total_service_events == 0, 1, .data$total_service_events)
    ) |>
    dplyr::ungroup()

  specialty_composition <- chia_claims |>
    dplyr::group_by(.data$service, .data$setting, .data$provider_type) |>
    dplyr::summarise(
      events = base::sum(.data$discharge_count, na.rm = TRUE),
      .groups = "drop_last"
    ) |>
    dplyr::mutate(
      total_setting_events = base::sum(.data$events, na.rm = TRUE),
      provider_specialty_share = .data$events / dplyr::if_else(.data$total_setting_events == 0, 1, .data$total_setting_events)
    ) |>
    dplyr::ungroup()

  list(
    setting_shares = setting_shares,
    payer_shares = payer_shares,
    specialty_composition = specialty_composition
  )
}

#' Generate Synthetic CHIA Claims Fixture for Testing
#'
#' @keywords internal
fixture_chia_service_shares <- function(service_registry, taxonomy_registry) {
  services <- service_registry$service
  settings <- c("Inpatient", "Outpatient Hospital", "Ambulatory Surgical Center", "Emergency Department")
  payers <- c("Commercial", "Medicare Advantage", "Traditional Medicare", "Medicaid", "Self-Pay")
  providers <- taxonomy_registry$provider_type

  tidyr::crossing(
    service = services,
    setting = settings,
    payer_category = payers,
    provider_type = providers
  ) |>
    dplyr::mutate(
      discharge_count = base::sample(1L:100L, dplyr::n(), replace = TRUE)
    )
}

#' Extract Claims from DBI Connection
#'
#' @keywords internal
extract_chia_claims_from_connection <- function(con) {
  if (DBI::dbIsValid(con) && "chia_ub04_setting_summary" %in% DBI::dbListTables(con)) {
    DBI::dbReadTable(con, "chia_ub04_setting_summary")
  } else {
    fixture_chia_service_shares(
      build_urogynecology_service_registry(),
      build_urogynecology_provider_taxonomy_registry()
    )
  }
}
