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


.chia_share_require_columns <- function(data, required, label) {
  missing <- base::setdiff(required, base::names(data))
  if (base::length(missing) > 0L) {
    base::stop(
      label, " is missing: ", base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  base::invisible(TRUE)
}


.chia_normalize_npi <- function(x) {
  x <- stringr::str_trim(base::as.character(x))
  x[x %in% base::c("", "NA")] <- NA_character_
  stringr::str_pad(x, width = 10L, side = "left", pad = "0")
}


#' Read classified CHIA pelvic-floor service events
#'
#' This reader deliberately requires a pre-classified event table. Raw CHIA
#' discharge/observation tables do not contain HCPCS in the same form as CMS,
#' so silently treating all surgical discharges as a model service would be an
#' estimand error. The table must be created by a separately validated CHIA
#' service-classification step.
#'
#' @param con Open DBI connection.
#' @param schema Schema containing the classified table.
#' @param table Classified event table name.
#'
#' @return A tibble of classified CHIA events.
#' @family calibration
#' @concept model
#' @export
read_chia_service_share_events <- function(
    con,
    schema = "chia_casemix",
    table = "urogynecology_service_events") {
  identifier <- DBI::Id(schema = schema, table = table)
  if (!DBI::dbExistsTable(con, identifier)) {
    base::stop(
      "CHIA classified service-share table `", schema, ".", table,
      "` does not exist. Build it before estimating provider shares.",
      call. = FALSE
    )
  }
  out <- DBI::dbReadTable(con, identifier) |>
    tibble::as_tibble()
  .chia_share_require_columns(
    out,
    base::c(
      "encounter_id", "year", "rendering_npi", "service",
      "payer_group", "setting", "service_events"
    ),
    "CHIA service-share event table"
  )
  out
}


.chia_resolve_provider_groups <- function(
    npi_taxonomy,
    taxonomy_registry = urogynecology_provider_taxonomy_registry()) {
  .chia_share_require_columns(
    npi_taxonomy,
    base::c("rendering_npi", "taxonomy_code"),
    "npi_taxonomy"
  )
  if (!"is_primary" %in% base::names(npi_taxonomy)) {
    npi_taxonomy$is_primary <- FALSE
  }

  mapped <- npi_taxonomy |>
    dplyr::transmute(
      rendering_npi = .chia_normalize_npi(.data$rendering_npi),
      taxonomy_code = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$taxonomy_code))
      ),
      is_primary = dplyr::coalesce(
        base::as.logical(.data$is_primary),
        FALSE
      )
    ) |>
    dplyr::left_join(
      taxonomy_registry |>
        dplyr::select(
          .data$taxonomy_code,
          .data$provider_type,
          .data$provider_group
        ),
      by = "taxonomy_code"
    ) |>
    dplyr::mutate(
      mapped = !base::is.na(.data$provider_group),
      urps_priority = .data$provider_group == "urps",
      primary_priority = .data$is_primary & .data$mapped
    ) |>
    dplyr::arrange(
      .data$rendering_npi,
      dplyr::desc(.data$urps_priority),
      dplyr::desc(.data$primary_priority),
      dplyr::desc(.data$mapped),
      .data$taxonomy_code
    ) |>
    dplyr::group_by(.data$rendering_npi) |>
    dplyr::slice_head(n = 1L) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      .data$rendering_npi,
      .data$taxonomy_code,
      provider_type = dplyr::coalesce(
        .data$provider_type,
        "Unknown / unmapped"
      ),
      provider_group = dplyr::coalesce(
        .data$provider_group,
        "unknown"
      )
    )

  mapped
}


#' Classify CHIA service events by URPS/general-OB-GYN/other provider group
#'
#' CHIA is treated as Massachusetts all-payer hospital evidence, not pooled
#' with national CMS Medicare FFS. Roster membership overrides taxonomy
#' because the validated individual-level roster is more specific than a
#' broad NPPES branch.
#'
#' @param events Classified CHIA service events (see
#'   [read_chia_service_share_events()]).
#' @param npi_taxonomy NPI-to-taxonomy rows.
#' @param taxonomy_registry Canonical provider taxonomy registry.
#' @param service_registry Canonical pelvic-floor service registry.
#' @param urps_roster Optional validated roster containing `npi`.
#'
#' @return A list with provider shares, physician-conditional URPS shares,
#'   classified events, diagnostics, and source scope.
#' @family calibration
#' @concept model
#' @export
classify_chia_service_share_events <- function(
    events,
    npi_taxonomy,
    taxonomy_registry = urogynecology_provider_taxonomy_registry(),
    service_registry = urogynecology_service_share_registry(),
    urps_roster = NULL) {
  base::message("Building CHIA urogynecology service-share evidence.")
  .chia_share_require_columns(
    events,
    base::c(
      "encounter_id", "year", "rendering_npi", "service",
      "payer_group", "setting", "service_events"
    ),
    "events"
  )
  validate_service_share_registry(service_registry, strict = TRUE)

  known_services <- base::unique(service_registry$service)
  unknown_services <- base::setdiff(
    base::unique(events$service),
    known_services
  )
  if (base::length(unknown_services) > 0L) {
    base::stop(
      "CHIA events contain noncanonical service(s): ",
      base::paste(unknown_services, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  provider_lookup <- .chia_resolve_provider_groups(
    npi_taxonomy,
    taxonomy_registry
  )

  classified <- events |>
    dplyr::mutate(
      rendering_npi = .chia_normalize_npi(.data$rendering_npi),
      year = base::as.integer(.data$year),
      service_events = base::as.numeric(.data$service_events)
    ) |>
    dplyr::left_join(provider_lookup, by = "rendering_npi") |>
    dplyr::mutate(
      provider_type = dplyr::coalesce(
        .data$provider_type,
        "Unknown / unmapped"
      ),
      provider_group = dplyr::coalesce(
        .data$provider_group,
        "unknown"
      )
    )

  if (base::any(!base::is.finite(classified$service_events)) ||
      base::any(classified$service_events < 0)) {
    base::stop("CHIA service_events must be finite and nonnegative.",
      call. = FALSE)
  }

  roster_override_events <- 0
  if (!base::is.null(urps_roster)) {
    .chia_share_require_columns(urps_roster, "npi", "urps_roster")
    roster_npi <- .chia_normalize_npi(urps_roster$npi)
    override <- classified$rendering_npi %in% roster_npi &
      classified$provider_group != "urps"
    roster_override_events <- base::sum(
      classified$service_events[override],
      na.rm = TRUE
    )
    classified$provider_group[
      classified$rendering_npi %in% roster_npi
    ] <- "urps"
    classified$provider_type[
      classified$rendering_npi %in% roster_npi
    ] <- "URPS physician (validated roster)"
  }

  grouping <- base::c("service", "year", "payer_group", "setting")
  provider_shares <- classified |>
    dplyr::group_by(
      .data$service,
      .data$year,
      .data$payer_group,
      .data$setting,
      .data$provider_group
    ) |>
    dplyr::summarise(
      service_events = base::sum(.data$service_events, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(
      .data$service,
      .data$year,
      .data$payer_group,
      .data$setting
    ) |>
    dplyr::mutate(
      total_service_events = base::sum(.data$service_events),
      provider_share = dplyr::if_else(
        .data$total_service_events > 0,
        .data$service_events / .data$total_service_events,
        NA_real_
      )
    ) |>
    dplyr::ungroup()

  physician_groups <- base::c(
    "urps", "general_obgyn", "general_urology", "primary_care", "other"
  )
  physician_share <- classified |>
    dplyr::mutate(
      is_physician = .data$provider_group %in% physician_groups,
      is_urps = .data$provider_group == "urps"
    ) |>
    dplyr::group_by(
      .data$service,
      .data$year,
      .data$payer_group,
      .data$setting
    ) |>
    dplyr::summarise(
      urps_events = base::sum(
        .data$service_events * .data$is_urps,
        na.rm = TRUE
      ),
      physician_events = base::sum(
        .data$service_events * .data$is_physician,
        na.rm = TRUE
      ),
      urps_given_physician = dplyr::if_else(
        .data$physician_events > 0,
        .data$urps_events / .data$physician_events,
        NA_real_
      ),
      .groups = "drop"
    )

  share_check <- provider_shares |>
    dplyr::group_by(
      .data$service,
      .data$year,
      .data$payer_group,
      .data$setting
    ) |>
    dplyr::summarise(
      share_sum = base::sum(.data$provider_share),
      .groups = "drop"
    )
  if (base::any(base::abs(share_check$share_sum - 1) > 1e-10)) {
    base::stop("CHIA provider composition failed to sum to one.",
      call. = FALSE)
  }

  unknown_events <- classified |>
    dplyr::filter(.data$provider_group == "unknown") |>
    dplyr::summarise(total = base::sum(.data$service_events, na.rm = TRUE)) |>
    dplyr::pull(.data$total)
  if (base::length(unknown_events) == 0L || base::is.na(unknown_events)) {
    unknown_events <- 0
  }

  total_events <- base::sum(classified$service_events, na.rm = TRUE)
  diagnostics <- tibble::tibble(
    service_events = total_events,
    npi_n = dplyr::n_distinct(classified$rendering_npi),
    year_n = dplyr::n_distinct(classified$year),
    payer_group_n = dplyr::n_distinct(classified$payer_group),
    setting_n = dplyr::n_distinct(classified$setting),
    unknown_provider_events = unknown_events,
    mapped_event_share = dplyr::if_else(
      total_events > 0,
      1 - unknown_events / total_events,
      NA_real_
    ),
    roster_override_events = roster_override_events
  )

  base::list(
    provider_shares = provider_shares,
    physician_share = physician_share,
    classified_events = classified,
    diagnostics = diagnostics,
    provenance = base::list(
      events_sha256 = digest::digest(
        tibble::as_tibble(events),
        algo = "sha256",
        serialize = TRUE
      ),
      taxonomy_sha256 = digest::digest(
        tibble::as_tibble(npi_taxonomy),
        algo = "sha256",
        serialize = TRUE
      ),
      source = "Massachusetts CHIA Case Mix classified service events"
    ),
    estimand = base::list(
      parameter = "P(provider type | service, year, payer, setting)",
      geography = "Massachusetts",
      payer_scope = "all-payer CHIA Case Mix",
      transport_role = "separate evidence; not pooled with CMS"
    )
  )
}


#' Compare CHIA's URPS-given-physician share against the CMS bound interval
#'
#' The comparison is diagnostic: it does not average the sources. A CHIA
#' point estimate outside the CMS partial-identification interval ([lower_bound,
#' upper_bound] from [build_cms_service_share_evidence()]'s `service_bounds`)
#' increases the transport standard deviation used by later calibration,
#' combined with `baseline_transport_sd` in quadrature (independent variance
#' addition) rather than a simple sum -- agreement leaves the SD at its
#' baseline; growing disagreement widens it, never resolved by picking a side.
#'
#' @param chia_evidence Output from [classify_chia_service_share_events()].
#' @param cms_evidence Output from [build_cms_service_share_evidence()].
#' @param baseline_transport_sd Minimum transport standard deviation.
#'
#' @return A service-level comparison tibble.
#' @family calibration
#' @concept model
#' @export
compare_chia_to_cms_service_share_evidence <- function(
    chia_evidence,
    cms_evidence,
    baseline_transport_sd = 0.05) {
  if (!base::is.numeric(baseline_transport_sd) ||
      base::length(baseline_transport_sd) != 1L ||
      !base::is.finite(baseline_transport_sd) ||
      baseline_transport_sd <= 0) {
    base::stop("baseline_transport_sd must be positive.", call. = FALSE)
  }

  chia_service <- chia_evidence$physician_share |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      chia_urps_events = base::sum(.data$urps_events, na.rm = TRUE),
      chia_physician_events = base::sum(
        .data$physician_events,
        na.rm = TRUE
      ),
      chia_urps_given_physician = dplyr::if_else(
        .data$chia_physician_events > 0,
        .data$chia_urps_events / .data$chia_physician_events,
        NA_real_
      ),
      .groups = "drop"
    )

  cms_bounds <- cms_evidence$service_bounds |>
    dplyr::select(
      .data$service,
      cms_lower = .data$lower_bound,
      cms_upper = .data$upper_bound
    )

  chia_service |>
    dplyr::inner_join(cms_bounds, by = "service") |>
    dplyr::mutate(
      distance_to_cms_interval = dplyr::case_when(
        .data$chia_urps_given_physician < .data$cms_lower ~
          .data$cms_lower - .data$chia_urps_given_physician,
        .data$chia_urps_given_physician > .data$cms_upper ~
          .data$chia_urps_given_physician - .data$cms_upper,
        TRUE ~ 0
      ),
      outside_cms_interval = .data$distance_to_cms_interval > 0,
      transport_sd = base::sqrt(
        baseline_transport_sd^2 + .data$distance_to_cms_interval^2
      ),
      cms_interval_width = .data$cms_upper - .data$cms_lower
    )
}
