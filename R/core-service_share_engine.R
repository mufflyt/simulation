# Calibrated service-share engine -------------------------------------------

#' Services emitted by the current condition-to-service pathway
#'
#' @return Character vector of routing-service names.
#' @keywords internal
service_share_required_routing_services <- function() {
  base::c(
    "sling_procedure",
    "prolapse_surgery",
    "sacral_neuromodulation",
    "botox_injection",
    "ptns_procedure",
    "urodynamics",
    "pessary_fitting",
    "cystoscopy",
    "bladder_instillation",
    "new_consultation",
    "return_visit"
  )
}


.service_share_routing_crosswalk <- function() {
  tibble::tribble(
    ~service, ~workload_service,
    "sling_procedure", "sling_procedure",
    "prolapse_surgery", "prolapse_procedure",
    "sacral_neuromodulation", "sacral_neuromodulation",
    "botox_injection", "botox_bladder",
    "ptns_procedure", "ptns",
    "urodynamics", "urodynamics",
    "pessary_fitting", "pessary_care",
    "cystoscopy", "cystoscopy",
    "bladder_instillation", "bladder_instillation",
    "new_consultation", "new_consultation",
    "return_visit", "return_visit"
  )
}


#' Work RVUs aligned to provider-routing service names
#'
#' All ordinary services come from [urps_service_workload()]. Sacral
#' neuromodulation is explicitly bridged to CMS HCPCS 64581, which already
#' exists in `CMS_WORK_RVU` but is not exposed by the legacy modeled-service
#' basket. This avoids falling back to the runner's historical 8.60 average.
#'
#' @return Tibble with `service`, `work_rvu`, and `source`.
#' @keywords internal
service_share_routing_workload <- function() {
  crosswalk <- .service_share_routing_crosswalk()
  standard <- urps_service_workload() |>
    dplyr::select(
      workload_service = .data$service,
      .data$work_rvu,
      workload_source = .data$source
    ) |>
    dplyr::inner_join(
      crosswalk |>
        dplyr::filter(.data$service != "sacral_neuromodulation"),
      by = "workload_service"
    ) |>
    dplyr::transmute(
      .data$service,
      .data$work_rvu,
      source = base::paste0(
        .data$workload_source,
        "; mapped from `", .data$workload_service, "`"
      )
    )

  snm <- CMS_WORK_RVU |>
    dplyr::filter(.data$hcpcs == "64581")
  if (base::nrow(snm) != 1L ||
      !base::is.finite(snm$work_rvu[[1L]]) || snm$work_rvu[[1L]] <= 0) {
    base::stop(
      "CMS_WORK_RVU must contain one positive HCPCS 64581 row for the ",
      "sacral-neuromodulation workload bridge.",
      call. = FALSE
    )
  }
  snm_row <- tibble::tibble(
    service = "sacral_neuromodulation",
    work_rvu = snm$work_rvu[[1L]],
    source = base::paste0(
      CMS_RVU_RELEASE,
      "; HCPCS 64581 open sacral neurostimulator implant"
    )
  )

  out <- dplyr::bind_rows(standard, snm_row) |>
    dplyr::arrange(
      base::match(.data$service, service_share_required_routing_services())
    )
  missing <- base::setdiff(
    service_share_required_routing_services(),
    out$service
  )
  if (base::length(missing) > 0L) {
    base::stop(
      "Missing work-RVU bridge for routing service(s): ",
      base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  out
}


.service_share_to_routing_names <- function(service_names) {
  registry <- urogynecology_service_share_registry() |>
    dplyr::select(.data$service, .data$routing_service) |>
    dplyr::distinct(.data$service, .keep_all = TRUE)
  lookup <- stats::setNames(registry$routing_service, registry$service)
  mapped <- base::unname(lookup[service_names])
  mapped[base::is.na(mapped)] <- service_names[base::is.na(mapped)]
  mapped
}


#' Extract a calibrated provider-routing draw for a simulation year
#'
#' Evidence may be carried forward to later simulation years, but the function
#' refuses to backcast a service before its first observed year. Condition-level
#' compositions are marginalized using their empirical cell-event weights
#' because the current pathway exposes service volume, not diagnosis-specific
#' service lines.
#'
#' @param bundle Valid calibrated service-share bundle.
#' @param year Simulation year.
#' @param draw_id Joint calibration draw to use.
#' @param required_services Optional routing-service coverage requirement.
#'
#' @return Routing table accepted by [apply_provider_routing()].
#' @keywords internal
service_share_routing_for_year <- function(
    bundle,
    year,
    draw_id,
    required_services = NULL) {
  validate_service_share_bundle(bundle)
  if (!base::is.numeric(year) || base::length(year) != 1L ||
      !base::is.finite(year)) {
    base::stop("year must be one finite value.", call. = FALSE)
  }
  if (!base::is.numeric(draw_id) || base::length(draw_id) != 1L ||
      !base::is.finite(draw_id)) {
    base::stop("draw_id must be one finite value.", call. = FALSE)
  }

  draws <- bundle$share_draws |>
    dplyr::filter(.data$draw_id == base::as.integer(draw_id))
  if (base::nrow(draws) == 0L) {
    base::stop(
      "Requested service-share draw_id is absent: ", draw_id, ".",
      call. = FALSE
    )
  }

  coverage <- draws |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      first_year = base::min(.data$year),
      evidence_year = base::max(.data$year[.data$year <= year],
        na.rm = TRUE
      ),
      has_past_evidence = base::any(.data$year <= year),
      .groups = "drop"
    )
  if (base::any(!coverage$has_past_evidence)) {
    bad <- coverage$service[!coverage$has_past_evidence]
    base::stop(
      "Calibrated service shares cannot backcast before first evidence year: ",
      base::paste(bad, collapse = ", "), ".",
      call. = FALSE
    )
  }

  selected <- draws |>
    dplyr::inner_join(
      coverage |>
        dplyr::select(.data$service, .data$evidence_year),
      by = "service"
    ) |>
    dplyr::filter(.data$year == .data$evidence_year) |>
    dplyr::mutate(
      routing_service = .service_share_to_routing_names(.data$service),
      weighted_share = .data$share * .data$cell_events
    )

  denominators <- selected |>
    dplyr::distinct(
      .data$service,
      .data$routing_service,
      .data$condition,
      .data$evidence_year,
      .data$cell_events
    ) |>
    dplyr::group_by(.data$routing_service) |>
    dplyr::summarise(
      evidence_events = base::sum(.data$cell_events),
      evidence_year = base::max(.data$evidence_year),
      .groups = "drop"
    )

  routing <- selected |>
    dplyr::group_by(.data$routing_service, .data$provider_group) |>
    dplyr::summarise(
      weighted_share = base::sum(.data$weighted_share),
      .groups = "drop"
    ) |>
    dplyr::left_join(denominators, by = "routing_service") |>
    dplyr::transmute(
      service = .data$routing_service,
      .data$provider_group,
      probability = .data$weighted_share / .data$evidence_events,
      .data$evidence_year,
      evidence_events = .data$evidence_events,
      draw_id = base::as.integer(draw_id),
      evidence_status = "calibrated"
    )

  sums <- routing |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      probability_sum = base::sum(.data$probability),
      .groups = "drop"
    )
  if (base::any(base::abs(sums$probability_sum - 1) > 1e-8)) {
    base::stop(
      "Calibrated service routing does not sum to one.",
      call. = FALSE
    )
  }

  if (!base::is.null(required_services)) {
    missing <- base::setdiff(required_services, routing$service)
    if (base::length(missing) > 0L) {
      base::stop(
        "Calibrated path is missing calibrated evidence for routing service(s): ",
        base::paste(missing, collapse = ", "),
        ". No legacy fallback is permitted in calibrated mode.",
        call. = FALSE
      )
    }
  }
  routing
}


#' Convert routed URPS service volume to service-specific work RVUs
#'
#' @param routed Routed service table from [pathway_provider_service_volumes()].
#' @param workload Routing-service work-RVU table.
#'
#' @return List with service workload and exact totals.
#' @keywords internal
allocate_urps_service_workload <- function(
    routed,
    workload = service_share_routing_workload()) {
  required <- base::c("service", "provider_group", "provider_volume")
  missing <- base::setdiff(required, base::names(routed))
  if (base::length(missing) > 0L) {
    base::stop(
      "Routed service table is missing: ",
      base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  workload_required <- base::c("service", "work_rvu")
  missing_workload <- base::setdiff(workload_required, base::names(workload))
  if (base::length(missing_workload) > 0L) {
    base::stop(
      "Workload table is missing: ",
      base::paste(missing_workload, collapse = ", "), ".",
      call. = FALSE
    )
  }

  group_vars <- base::intersect(base::c("year", "service"), base::names(routed))
  urps <- routed |>
    dplyr::filter(.data$provider_group == "urps") |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_vars))) |>
    dplyr::summarise(
      urps_volume = base::sum(.data$provider_volume, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::left_join(
      workload |>
        dplyr::select(
          .data$service,
          .data$work_rvu,
          dplyr::any_of("source")
        ) |>
        dplyr::distinct(.data$service, .keep_all = TRUE),
      by = "service"
    )

  missing_rvu <- urps |>
    dplyr::filter(.data$urps_volume > 0, base::is.na(.data$work_rvu))
  if (base::nrow(missing_rvu) > 0L) {
    base::stop(
      "Positive URPS service volume lacks work RVU: ",
      base::paste(missing_rvu$service, collapse = ", "), ".",
      call. = FALSE
    )
  }
  service_workload <- urps |>
    dplyr::mutate(
      work_rvu_total = .data$urps_volume * .data$work_rvu
    )

  base::list(
    service_workload = service_workload,
    total_urps_services = base::sum(service_workload$urps_volume),
    total_urps_wrvu = base::sum(service_workload$work_rvu_total)
  )
}


#' Allocate calibrated URPS workload to the active provider cohort
#'
#' @param providers Provider cohort containing `provider_id` and `fte`.
#' @param total_urps_wrvu Aggregate URPS workload to allocate.
#' @param year Simulation year.
#'
#' @return Provider-year workload table.
#' @keywords internal
allocate_urps_workload_to_active_providers <- function(
    providers,
    total_urps_wrvu,
    year) {
  required <- base::c("provider_id", "fte")
  missing <- base::setdiff(required, base::names(providers))
  if (base::length(missing) > 0L) {
    base::stop(
      "Provider cohort is missing: ",
      base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  active <- providers
  if ("active" %in% base::names(active)) {
    active <- active |>
      dplyr::filter(.data$active)
  }
  active <- active |>
    dplyr::filter(base::is.finite(.data$fte), .data$fte > 0)
  total_fte <- base::sum(active$fte)
  if (!base::is.finite(total_fte) || total_fte <= 0) {
    base::stop("No positive active clinical FTE available for workload.",
      call. = FALSE
    )
  }
  if (!base::is.numeric(total_urps_wrvu) ||
      base::length(total_urps_wrvu) != 1L ||
      !base::is.finite(total_urps_wrvu) || total_urps_wrvu < 0) {
    base::stop("total_urps_wrvu must be finite and nonnegative.",
      call. = FALSE
    )
  }

  out <- active |>
    dplyr::transmute(
      .data$provider_id,
      year = base::as.integer(year),
      clinical_fte = .data$fte,
      workload_share = .data$fte / total_fte,
      annual_wrvu = total_urps_wrvu * .data$fte / total_fte
    )
  error <- base::sum(out$annual_wrvu) - total_urps_wrvu
  if (base::abs(error) > base::max(1e-8, total_urps_wrvu * 1e-12)) {
    base::stop("Provider-level URPS work-RVU allocation failed to close.",
      call. = FALSE
    )
  }
  out
}
