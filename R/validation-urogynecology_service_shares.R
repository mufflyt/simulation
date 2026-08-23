# Validation for calibrated urogynecology service shares -------------------

#' Validate frozen CMS service-share accounting
#'
#' @param cms_evidence CMS evidence bundle.
#' @param tolerance Numerical tolerance.
#'
#' @return Validation table. Stops when the frozen accounting identity fails.
#' @keywords internal
validate_cms_service_share_accounting <- function(
    cms_evidence,
    tolerance = 1e-8) {
  required <- base::c("service", "T_s", "U", "O", "N", "M")
  missing <- base::setdiff(required, base::names(cms_evidence$service_bounds))
  if (base::length(missing) > 0L) {
    base::stop(
      "CMS service_bounds is missing: ",
      base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  rows <- cms_evidence$service_bounds |>
    dplyr::mutate(
      accounting_error = .data$T_s -
        (.data$U + .data$O + .data$N + .data$M),
      passed = base::abs(.data$accounting_error) <= tolerance
    ) |>
    dplyr::transmute(
      check = base::paste0("cms_identity:", .data$service),
      max_abs_error = base::abs(.data$accounting_error),
      tolerance = tolerance,
      .data$passed
    )
  if (base::any(!rows$passed)) {
    base::stop(
      "CMS service-share accounting failed T = U + O + N + M.",
      call. = FALSE
    )
  }
  rows
}


.service_share_accounting_row <- function(check, error, tolerance) {
  tibble::tibble(
    check = check,
    max_abs_error = base::max(base::abs(error), na.rm = TRUE),
    tolerance = tolerance,
    passed = base::all(base::abs(error) <= tolerance)
  )
}


#' Validate end-to-end calibrated service-share accounting
#'
#' Recomputes the important identities from the returned objects instead of
#' trusting the diagnostics written during the simulation.
#'
#' @param simulation_result Calibrated end-to-end simulation result.
#' @param cms_evidence Optional CMS evidence bundle.
#' @param tolerance Numerical tolerance.
#'
#' @return Validation table. Stops on any failed identity.
#' @keywords internal
validate_service_share_accounting <- function(
    simulation_result,
    cms_evidence = NULL,
    tolerance = 1e-6) {
  required <- base::c(
    "audit_ledger_tbl", "service_share_diagnostics",
    "service_share_provider_workload", "service_share_service_workload",
    "simulation_config"
  )
  missing <- base::setdiff(required, base::names(simulation_result))
  if (base::length(missing) > 0L) {
    base::stop(
      "Simulation result is missing calibrated service-share object(s): ",
      base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (!base::identical(
    simulation_result$simulation_config$service_share_engine,
    "calibrated"
  )) {
    base::stop(
      "Service-share accounting validation requires calibrated mode.",
      call. = FALSE
    )
  }

  audit <- simulation_result$audit_ledger_tbl |>
    dplyr::select(.data$year, audit_wrvu = .data$wrvu_total)
  provider <- simulation_result$service_share_provider_workload |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      provider_wrvu = base::sum(.data$annual_wrvu),
      .groups = "drop"
    ) |>
    dplyr::right_join(audit, by = "year") |>
    dplyr::mutate(error = .data$provider_wrvu - .data$audit_wrvu)
  service <- simulation_result$service_share_service_workload |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      service_wrvu = base::sum(.data$work_rvu_total),
      urps_volume = base::sum(.data$urps_volume),
      .groups = "drop"
    ) |>
    dplyr::right_join(audit, by = "year") |>
    dplyr::mutate(error = .data$service_wrvu - .data$audit_wrvu)
  routed <- simulation_result$service_share_diagnostics |>
    dplyr::mutate(
      error = .data$allocated_service_volume -
        .data$pathway_service_volume
    )

  checks <- dplyr::bind_rows(
    .service_share_accounting_row(
      "provider work-RVU equals audit work-RVU",
      provider$error,
      tolerance
    ),
    .service_share_accounting_row(
      "service work-RVU equals audit work-RVU",
      service$error,
      tolerance
    ),
    .service_share_accounting_row(
      "routed service volume equals pathway demand",
      routed$error,
      tolerance
    ),
    .service_share_accounting_row(
      "stored routed-volume error is zero",
      routed$routed_volume_error,
      tolerance
    ),
    .service_share_accounting_row(
      "stored provider work-RVU error is zero",
      routed$provider_wrvu_error,
      tolerance
    )
  )

  if (!base::is.null(cms_evidence)) {
    checks <- dplyr::bind_rows(
      checks,
      validate_cms_service_share_accounting(
        cms_evidence,
        tolerance = tolerance
      )
    )
  }

  if (base::any(!checks$passed)) {
    failed <- checks$check[!checks$passed]
    base::stop(
      "Calibrated service-share accounting failed: ",
      base::paste(failed, collapse = "; "), ".",
      call. = FALSE
    )
  }
  checks
}


#' Stable reproducibility digest for a calibrated service-share bundle
#'
#' Runtime timestamps and git SHA are intentionally excluded. Evidence hashes,
#' calibrated draws, held-out scores, selected alpha values, source weights, and
#' model configuration are included.
#'
#' @param bundle Calibrated service-share bundle.
#'
#' @return SHA-256 digest string.
#' @keywords internal
service_share_reproducibility_digest <- function(bundle) {
  validate_service_share_bundle(bundle)
  stable_provenance <- bundle$provenance[
    base::grepl("sha256$", base::names(bundle$provenance))
  ]
  stable <- base::list(
    share_draws = bundle$share_draws |>
      dplyr::arrange(
        .data$service,
        .data$condition,
        .data$year,
        .data$draw_id,
        .data$provider_group
      ),
    selected_alpha = bundle$selected_alpha |>
      dplyr::arrange(.data$service),
    holdout_scores = bundle$holdout_scores |>
      dplyr::arrange(.data$service, .data$alpha),
    draw_weights = bundle$source_fit$draw_weights |>
      dplyr::arrange(.data$draw_id),
    provenance = stable_provenance,
    config = bundle$config
  )
  digest::digest(stable, algo = "sha256", serialize = TRUE)
}


.service_share_dropout_summary <- function(bundle, label) {
  latest <- bundle$share_draws |>
    dplyr::group_by(.data$service) |>
    dplyr::filter(.data$year == base::max(.data$year)) |>
    dplyr::ungroup() |>
    dplyr::filter(.data$provider_group == "urps") |>
    dplyr::group_by(.data$service, .data$year, .data$draw_id) |>
    dplyr::summarise(
      urps_share = stats::weighted.mean(
        .data$share,
        w = .data$cell_events
      ),
      .groups = "drop"
    )

  latest |>
    dplyr::group_by(.data$service, .data$year) |>
    dplyr::summarise(
      mean_urps_share = base::mean(.data$urps_share),
      sd_urps_share = stats::sd(.data$urps_share),
      median_urps_share = stats::median(.data$urps_share),
      p25_urps_share = stats::quantile(.data$urps_share, 0.25),
      p75_urps_share = stats::quantile(.data$urps_share, 0.75),
      .groups = "drop"
    ) |>
    dplyr::mutate(source_configuration = label, .before = 1)
}


#' Evaluate CMS/CHIA source-dropout sensitivity
#'
#' @param events Claims provider-composition event counts.
#' @param cms_evidence CMS partial-identification evidence.
#' @param chia_evidence CHIA transport evidence.
#' @param alpha_grid Candidate concentration values.
#' @param draws Number of draws per fit.
#' @param seed Common seed used for comparable fits.
#'
#' @return Summary of claims+CMS, claims+CHIA, and combined fits.
#' @keywords internal
evaluate_service_share_source_dropout <- function(
    events,
    cms_evidence,
    chia_evidence,
    alpha_grid = base::c(0.5, 1, 2, 5, 10, 20, 50, 100),
    draws = 500L,
    seed = 20260822L) {
  base::message("Running service-share source-dropout validation.")
  configurations <- base::list(
    "claims+cms" = base::list(cms = cms_evidence, chia = NULL),
    "claims+chia" = base::list(cms = NULL, chia = chia_evidence),
    "claims+cms+chia" = base::list(
      cms = cms_evidence,
      chia = chia_evidence
    )
  )

  purrr::imap_dfr(configurations, function(sources, label) {
    fitted <- calibrate_service_share_model(
      events = events,
      cms_evidence = sources$cms,
      chia_evidence = sources$chia,
      alpha_grid = alpha_grid,
      draws = draws,
      seed = seed
    )
    .service_share_dropout_summary(fitted, label)
  })
}


#' Build a machine-readable service-share provenance manifest
#'
#' @param bundle Calibrated service-share bundle.
#'
#' @return Two-column key/value tibble.
#' @keywords internal
service_share_provenance_manifest <- function(bundle) {
  validate_service_share_bundle(bundle)
  stringify <- function(value) {
    if (base::length(value) == 0L) {
      return(NA_character_)
    }
    base::paste(base::as.character(value), collapse = ";")
  }
  provenance <- tibble::tibble(
    key = base::names(bundle$provenance),
    value = base::vapply(
      bundle$provenance,
      stringify,
      FUN.VALUE = base::character(1)
    )
  )
  config <- tibble::tibble(
    key = base::names(bundle$config),
    value = base::vapply(
      bundle$config,
      stringify,
      FUN.VALUE = base::character(1)
    )
  )
  dplyr::bind_rows(provenance, config) |>
    dplyr::bind_rows(tibble::tibble(
      key = "reproducibility_digest",
      value = service_share_reproducibility_digest(bundle)
    )) |>
    dplyr::distinct(.data$key, .keep_all = TRUE)
}
