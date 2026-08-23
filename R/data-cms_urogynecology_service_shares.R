# CMS urogynecology service-share evidence ----------------------------------

.cms_require_columns <- function(data, required, label) {
  missing <- base::setdiff(required, base::names(data))
  if (base::length(missing) > 0L) {
    base::stop(
      label, " is missing: ", base::paste(missing, collapse = ", "), ".",
      call. = FALSE
    )
  }
  base::invisible(TRUE)
}


.cms_normalize_npi <- function(x) {
  x <- stringr::str_trim(base::as.character(x))
  x[x %in% base::c("", "NA")] <- NA_character_
  stringr::str_pad(x, width = 10L, side = "left", pad = "0")
}


.cms_hash_table <- function(data) {
  digest::digest(
    tibble::as_tibble(data),
    algo = "sha256",
    serialize = TRUE
  )
}


.cms_aggregate_bounds <- function(service_bounds, services, label) {
  data <- service_bounds |>
    dplyr::filter(.data$service %in% services)
  if (base::nrow(data) == 0L) {
    base::stop("No services available for aggregate: ", label, call. = FALSE)
  }

  denominator <- base::sum(
    (data$T_s - data$N) * data$work_rvu,
    na.rm = TRUE
  )
  if (!base::is.finite(denominator) || denominator <= 0) {
    base::stop(
      "Nonpositive physician-workload denominator for ", label, ".",
      call. = FALSE
    )
  }
  observed_denominator <- base::sum(
    (data$U + data$O) * data$work_rvu,
    na.rm = TRUE
  )

  tibble::tibble(
    aggregate = label,
    services = dplyr::n_distinct(data$service),
    lower_bound = base::sum(data$U * data$work_rvu) / denominator,
    upper_bound = base::sum(
      (data$U + data$M) * data$work_rvu
    ) / denominator,
    observed_cell_share = dplyr::if_else(
      observed_denominator > 0,
      base::sum(data$U * data$work_rvu) / observed_denominator,
      NA_real_
    ),
    capture_share = base::sum(
      (data$U + data$O + data$N) * data$work_rvu
    ) / base::sum(data$T_s * data$work_rvu)
  )
}


#' Paths to the real CMS service-share inputs, honoring env-var overrides
#'
#' The single source of truth for where
#' [build_cms_service_share_evidence()]'s default inputs live on disk.
#' Extracted from `scripts/calibration/build_cms_urogynecology_service_share_evidence.R`
#' (which used to be the only place these paths were written down) so that
#' [default_cms_service_share_evidence()] and the script stay in sync instead
#' of drifting the way the same path list drifted across the old claim-line
#' test fixtures once this function moved to real CMS PUF inputs.
#'
#' @return Named character vector of file paths: `provider_service`,
#'   `geography_service`, `roster`, `provider_type_map`.
#' @keywords internal
.cms_service_share_input_paths <- function() {
  base::c(
    provider_service = Sys.getenv(
      "URPS_CMS_PROVIDER_SERVICE",
      file.path("data-raw", "cms_psps", "PHY_R26_P05_V10_D24_Prov_Svc.csv")
    ),
    geography_service = Sys.getenv(
      "URPS_CMS_GEOGRAPHY_SERVICE",
      file.path("data-raw", "cms_psps", "MUP_PHY_R26_P05_V10_D24_Geo.csv")
    ),
    roster = Sys.getenv(
      "URPS_LINKAGE_ROSTER_2024",
      file.path("data-raw", "urps_roster", "urps_linkage_roster_2024.csv")
    ),
    provider_type_map = Sys.getenv(
      "URPS_CMS_PROVIDER_TYPE_MAP",
      file.path("scripts", "validation", "mappings", "cms_provider_type_class.csv")
    )
  )
}


#' Build CMS service-share evidence from the real default inputs on disk
#'
#' [calibrate_service_share_model()]'s `cms_evidence = NULL` fallback used to
#' call [build_cms_service_share_evidence()] with zero arguments, which can
#' never succeed -- that function is deliberately fail-closed with no default
#' data (real Medicare data or nothing, never a silent placeholder). This is
#' the loading step the standalone calibration script always had; factored out
#' here so the library function's default path actually does what its own
#' `cms_evidence = NULL` signature implies, and fails with a clear "which
#' file is missing" message instead of a confusing multi-frame stack trace
#' from deep inside `.cms_require_columns()`.
#'
#' Does NOT verify the frozen roster SHA-256 the standalone script checks --
#' that is a publication-provenance guard, not a precondition for the
#' function to run, and duplicating it here would be a second copy of the
#' pin to drift out of sync with the script's.
#'
#' @return Same shape as [build_cms_service_share_evidence()].
#' @keywords internal
default_cms_service_share_evidence <- function() {
  paths <- .cms_service_share_input_paths()
  missing <- paths[!base::file.exists(paths)]
  if (base::length(missing) > 0L) {
    base::stop(
      "No cms_evidence supplied and the real default CMS service-share ",
      "input(s) are not present:\n  ",
      base::paste(base::names(missing), "->", missing, collapse = "\n  "),
      "\nSupply cms_evidence explicitly, or place the real files at the ",
      "path(s) above (or point URPS_CMS_PROVIDER_SERVICE / ",
      "URPS_CMS_GEOGRAPHY_SERVICE / URPS_LINKAGE_ROSTER_2024 / ",
      "URPS_CMS_PROVIDER_TYPE_MAP at them).",
      call. = FALSE
    )
  }

  registry <- urogynecology_service_share_registry()
  hcpcs_keep <- registry$hcpcs

  provider_service <- readr::read_csv(
    paths[["provider_service"]],
    col_select = dplyr::all_of(base::c(
      "Rndrng_NPI", "Rndrng_Prvdr_Type", "HCPCS_Cd", "Tot_Srvcs"
    )),
    show_col_types = FALSE,
    progress = FALSE
  ) |>
    dplyr::filter(.data$HCPCS_Cd %in% hcpcs_keep)

  geography_service <- readr::read_csv(
    paths[["geography_service"]],
    col_select = dplyr::all_of(base::c(
      "Rndrng_Prvdr_Geo_Lvl", "HCPCS_Cd", "Tot_Srvcs"
    )),
    show_col_types = FALSE,
    progress = FALSE
  ) |>
    dplyr::filter(
      .data$Rndrng_Prvdr_Geo_Lvl == "National",
      .data$HCPCS_Cd %in% hcpcs_keep
    )

  roster <- readr::read_csv(
    paths[["roster"]],
    show_col_types = FALSE,
    progress = FALSE
  )
  if (!"npi" %in% base::names(roster)) {
    npi_candidate <- base::intersect(
      base::names(roster),
      base::c("NPI", "rendering_npi", "Rndrng_NPI")
    )
    if (base::length(npi_candidate) != 1L) {
      base::stop("Linkage roster must contain an `npi` column.", call. = FALSE)
    }
    roster <- roster |>
      dplyr::rename(npi = dplyr::all_of(npi_candidate[[1L]]))
  }

  provider_type_map <- readr::read_csv(
    paths[["provider_type_map"]],
    comment = "#",
    show_col_types = FALSE,
    progress = FALSE
  )

  build_cms_service_share_evidence(
    provider_service = provider_service,
    geography_service = geography_service,
    roster = roster,
    provider_type_map = provider_type_map,
    service_registry = registry,
    workload = urps_service_workload()
  )
}


#' Build CMS partial-identification evidence for URPS service shares
#'
#' Reproduces the frozen 2024 Medicare FFS estimand in
#' `docs/PRESPEC_URPS_SHARE.md`. The provider file contributes retained
#' provider-service cells; the Geography file contributes national totals before
#' provider-cell suppression. The unidentified remainder is retained as `M`.
#'
#' @param provider_service CMS Provider and Service rows.
#' @param geography_service CMS Geography service rows.
#' @param roster Frozen linkage roster containing `npi`.
#' @param provider_type_map Mapping with `cms_provider_type` and
#'   `provider_class`, where class is `physician` or `nonphysician`.
#' @param service_registry Canonical HCPCS service registry.
#' @param workload Service-to-work-RVU table.
#' @param tolerance Numeric tolerance for the accounting identity.
#'
#' @return A list containing service bounds, aggregate bounds, diagnostics,
#'   provenance, and the estimand scope.
#' @keywords internal
build_cms_service_share_evidence <- function(
    provider_service,
    geography_service,
    roster,
    provider_type_map,
    service_registry = urogynecology_service_share_registry(),
    workload = urps_service_workload(),
    tolerance = 1e-8) {
  base::message("Building CMS urogynecology service-share evidence.")
  validate_service_share_registry(service_registry, strict = TRUE)

  .cms_require_columns(
    provider_service,
    base::c("Rndrng_NPI", "Rndrng_Prvdr_Type", "HCPCS_Cd", "Tot_Srvcs"),
    "provider_service"
  )
  .cms_require_columns(
    geography_service,
    base::c("Rndrng_Prvdr_Geo_Lvl", "HCPCS_Cd", "Tot_Srvcs"),
    "geography_service"
  )
  .cms_require_columns(roster, "npi", "roster")
  .cms_require_columns(
    provider_type_map,
    base::c("cms_provider_type", "provider_class"),
    "provider_type_map"
  )
  .cms_require_columns(workload, base::c("service", "work_rvu"), "workload")

  invalid_classes <- base::setdiff(
    base::unique(provider_type_map$provider_class),
    base::c("physician", "nonphysician")
  )
  if (base::length(invalid_classes) > 0L) {
    base::stop(
      "Unknown provider_class values: ",
      base::paste(invalid_classes, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  registry <- service_registry |>
    dplyr::mutate(
      hcpcs = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$hcpcs))
      )
    )

  roster_npi <- roster |>
    dplyr::transmute(npi = .cms_normalize_npi(.data$npi)) |>
    dplyr::filter(!base::is.na(.data$npi)) |>
    dplyr::distinct(.data$npi)

  national <- geography_service |>
    dplyr::mutate(
      hcpcs = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$HCPCS_Cd))
      ),
      national_level = stringr::str_to_lower(
        stringr::str_trim(base::as.character(.data$Rndrng_Prvdr_Geo_Lvl))
      )
    ) |>
    dplyr::filter(
      .data$national_level == "national",
      .data$hcpcs %in% registry$hcpcs
    )

  missing_codes <- base::setdiff(
    registry$hcpcs,
    base::unique(national$hcpcs)
  )
  if (base::length(missing_codes) > 0L) {
    base::stop(
      "CMS Geography national denominator missing HCPCS: ",
      base::paste(missing_codes, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  national_totals <- national |>
    dplyr::transmute(
      hcpcs = .data$hcpcs,
      services = base::as.numeric(.data$Tot_Srvcs)
    ) |>
    dplyr::inner_join(
      registry |>
        dplyr::select(
          .data$hcpcs,
          .data$service,
          .data$cms_tier,
          .data$calibration_role
        ),
      by = "hcpcs"
    ) |>
    dplyr::group_by(
      .data$service,
      .data$cms_tier,
      .data$calibration_role
    ) |>
    dplyr::summarise(
      T_s = base::sum(.data$services, na.rm = TRUE),
      .groups = "drop"
    )

  provider_rows <- provider_service |>
    dplyr::mutate(
      npi = .cms_normalize_npi(.data$Rndrng_NPI),
      hcpcs = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$HCPCS_Cd))
      ),
      cms_provider_type = base::as.character(.data$Rndrng_Prvdr_Type),
      services = base::as.numeric(.data$Tot_Srvcs)
    ) |>
    dplyr::filter(.data$hcpcs %in% registry$hcpcs) |>
    dplyr::left_join(
      provider_type_map |>
        dplyr::select(
          .data$cms_provider_type,
          .data$provider_class
        ) |>
        dplyr::distinct(.data$cms_provider_type, .keep_all = TRUE),
      by = "cms_provider_type"
    )

  unmapped <- provider_rows |>
    dplyr::filter(base::is.na(.data$provider_class)) |>
    dplyr::distinct(.data$cms_provider_type) |>
    dplyr::pull(.data$cms_provider_type)
  if (base::length(unmapped) > 0L) {
    base::stop(
      "Found unmapped CMS provider type(s): ",
      base::paste(unmapped, collapse = "; "),
      ". Classify them before using the evidence.",
      call. = FALSE
    )
  }

  provider_rows <- provider_rows |>
    dplyr::inner_join(
      registry |>
        dplyr::select(.data$hcpcs, .data$service),
      by = "hcpcs"
    ) |>
    dplyr::mutate(
      on_roster = .data$npi %in% roster_npi$npi,
      bucket = dplyr::case_when(
        .data$on_roster ~ "U",
        .data$provider_class == "physician" ~ "O",
        TRUE ~ "N"
      )
    )

  roster_nonphysician_services <- provider_rows |>
    dplyr::filter(
      .data$on_roster,
      .data$provider_class != "physician"
    ) |>
    dplyr::summarise(total = base::sum(.data$services, na.rm = TRUE)) |>
    dplyr::pull(.data$total)
  if (base::length(roster_nonphysician_services) == 0L ||
      base::is.na(roster_nonphysician_services)) {
    roster_nonphysician_services <- 0
  }

  components <- provider_rows |>
    dplyr::group_by(.data$service, .data$bucket) |>
    dplyr::summarise(
      services = base::sum(.data$services, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      names_from = .data$bucket,
      values_from = .data$services,
      values_fill = 0
    )

  for (component in base::c("U", "O", "N")) {
    if (!component %in% base::names(components)) {
      components[[component]] <- 0
    }
  }

  workload_lookup <- workload |>
    dplyr::select(.data$service, .data$work_rvu) |>
    dplyr::distinct(.data$service, .keep_all = TRUE)

  service_bounds <- national_totals |>
    dplyr::left_join(
      components |>
        dplyr::select(.data$service, .data$U, .data$O, .data$N),
      by = "service"
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(base::c("U", "O", "N")),
        ~ tidyr::replace_na(.x, 0)
      ),
      M = .data$T_s - .data$U - .data$O - .data$N
    )

  if (base::any(service_bounds$M < -tolerance)) {
    bad <- service_bounds$service[service_bounds$M < -tolerance]
    base::stop(
      "CMS evidence produced negative unidentified volume for: ",
      base::paste(bad, collapse = ", "),
      ". The identity T = U + O + N + M is broken.",
      call. = FALSE
    )
  }
  service_bounds$M <- base::pmax(service_bounds$M, 0)

  service_bounds <- service_bounds |>
    dplyr::left_join(workload_lookup, by = "service")
  if (base::any(base::is.na(service_bounds$work_rvu))) {
    missing_workload <- service_bounds$service[
      base::is.na(service_bounds$work_rvu)
    ]
    base::stop(
      "Missing work RVU for CMS evidence service(s): ",
      base::paste(missing_workload, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  service_bounds <- service_bounds |>
    dplyr::mutate(
      physician_denominator = .data$T_s - .data$N,
      lower_bound = .data$U / .data$physician_denominator,
      upper_bound = (.data$U + .data$M) /
        .data$physician_denominator,
      observed_cell_share = dplyr::if_else(
        .data$U + .data$O > 0,
        .data$U / (.data$U + .data$O),
        NA_real_
      ),
      capture_share = (.data$U + .data$O + .data$N) / .data$T_s,
      accounting_error = .data$T_s -
        (.data$U + .data$O + .data$N + .data$M)
    ) |>
    dplyr::arrange(.data$cms_tier, .data$service)

  if (base::any(!base::is.finite(service_bounds$physician_denominator)) ||
      base::any(service_bounds$physician_denominator <= 0)) {
    base::stop(
      "CMS physician denominator T-N must be finite and positive.",
      call. = FALSE
    )
  }
  if (base::any(base::abs(service_bounds$accounting_error) > tolerance)) {
    base::stop("CMS suppression accounting failed to close.", call. = FALSE)
  }
  if (base::any(service_bounds$lower_bound < -tolerance) ||
      base::any(service_bounds$upper_bound > 1 + tolerance) ||
      base::any(service_bounds$lower_bound > service_bounds$upper_bound)) {
    base::stop("CMS partial-identification bounds are invalid.", call. = FALSE)
  }

  tier_a_services <- registry |>
    dplyr::filter(.data$cms_tier == "A") |>
    dplyr::distinct(.data$service) |>
    dplyr::pull(.data$service)
  all_services <- base::unique(registry$service)

  aggregate_bounds <- dplyr::bind_rows(
    .cms_aggregate_bounds(
      service_bounds,
      tier_a_services,
      "Tier A (primary, female-specific)"
    ),
    .cms_aggregate_bounds(
      service_bounds,
      all_services,
      "Tier B (secondary, + sex-neutral)"
    )
  )

  diagnostics <- tibble::tibble(
    national_hcpcs_n = dplyr::n_distinct(national$hcpcs),
    retained_provider_rows = base::nrow(provider_rows),
    retained_npi_n = dplyr::n_distinct(provider_rows$npi),
    frozen_roster_npi_n = base::nrow(roster_npi),
    roster_nonphysician_services = roster_nonphysician_services,
    maximum_accounting_error = base::max(
      base::abs(service_bounds$accounting_error)
    )
  )

  provenance <- base::list(
    service_registry_version = base::unique(registry$registry_version),
    provider_service_sha256 = .cms_hash_table(provider_service),
    geography_service_sha256 = .cms_hash_table(geography_service),
    roster_sha256 = .cms_hash_table(roster),
    provider_type_map_sha256 = .cms_hash_table(provider_type_map),
    workload_sha256 = .cms_hash_table(workload)
  )

  base::message(
    "CMS evidence complete: ",
    scales::comma(base::nrow(service_bounds)),
    " service rows; max accounting error = ",
    base::format(diagnostics$maximum_accounting_error, scientific = TRUE),
    "."
  )

  base::list(
    service_bounds = service_bounds,
    aggregate_bounds = aggregate_bounds,
    diagnostics = diagnostics,
    provenance = provenance,
    estimand = base::list(
      parameter = "P(URPS | physician-delivered service)",
      population = "Medicare FFS Part B",
      data_year = 2024L,
      geography = "United States",
      identification = "partial; suppression-robust interval"
    )
  )
}
