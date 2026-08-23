# Canonical service-share evidence registry ---------------------------------

#' Canonical pelvic-floor service registry for provider-share evidence
#'
#' The CMS tier definitions reproduce `docs/PRESPEC_URPS_SHARE.md`. Tier A is
#' the primary female-pelvic-floor-specific basket. Tier B contains the
#' sex-neutral procedural extension and is secondary. Evaluation/management
#' codes are deliberately absent because the public CMS provider/service PUF
#' cannot condition them on pelvic-floor diagnoses.
#'
#' @return A tibble with one row per HCPCS/CPT code.
#' @keywords internal
urogynecology_service_share_registry <- function() {
  registry_version <- "2026-08-22"
  source_id <- "PRESPEC_URPS_SHARE_2026-08-08"
  source_citation <- base::paste(
    "docs/PRESPEC_URPS_SHARE.md; frozen 2026-08-08;",
    "CMS Medicare FFS Part B 2024 service-share code set"
  )

  tier_a <- tibble::tribble(
    ~service, ~routing_service, ~hcpcs,
    "pessary_care", "pessary_fitting", "57160",
    "sling_procedure", "sling_procedure", "57288",
    "sling_procedure", "sling_procedure", "51992",
    "sling_procedure", "sling_procedure", "57287",
    "prolapse_procedure", "prolapse_surgery", "57240",
    "prolapse_procedure", "prolapse_surgery", "57250",
    "prolapse_procedure", "prolapse_surgery", "57260",
    "prolapse_procedure", "prolapse_surgery", "57265",
    "prolapse_procedure", "prolapse_surgery", "57282",
    "prolapse_procedure", "prolapse_surgery", "57283",
    "prolapse_procedure", "prolapse_surgery", "57425",
    "prolapse_procedure", "prolapse_surgery", "57120",
    "prolapse_procedure", "prolapse_surgery", "57268"
  ) |>
    dplyr::mutate(
      cms_tier = "A",
      sex_specific = TRUE,
      calibration_role = "primary",
      evidence_quality = "A"
    )

  tier_b <- tibble::tribble(
    ~service, ~routing_service, ~hcpcs,
    "urodynamics", "urodynamics", "51726",
    "urodynamics", "urodynamics", "51728",
    "urodynamics", "urodynamics", "51729",
    "urodynamics", "urodynamics", "51741",
    "urodynamics", "urodynamics", "51784",
    "cystoscopy", "cystoscopy", "52000",
    "botox_bladder", "botox_injection", "52287",
    "ptns", "ptns_procedure", "64566",
    "bladder_instillation", "bladder_instillation", "51700"
  ) |>
    dplyr::mutate(
      cms_tier = "B",
      sex_specific = FALSE,
      calibration_role = "secondary",
      evidence_quality = "B"
    )

  dplyr::bind_rows(tier_a, tier_b) |>
    dplyr::mutate(
      category = dplyr::case_when(
        service == "pessary_care" ~ "Office Procedure",
        service == "sling_procedure" ~ "Surgical Incontinence",
        service == "prolapse_procedure" ~ "Surgical Prolapse",
        service == "urodynamics" ~ "Urodynamics",
        service == "cystoscopy" ~ "Diagnostic",
        service == "botox_bladder" ~ "Procedural Incontinence",
        service == "ptns" ~ "Neuromodulation",
        TRUE ~ "Office Procedure"
      ),
      code_system = "HCPCS/CPT",
      payer_scope = "Medicare FFS Part B",
      setting_scope = "facility_and_office",
      source_id = source_id,
      source_citation = source_citation,
      evidence_status = "production",
      registry_version = registry_version,
      effective_start = base::as.Date("2024-01-01"),
      effective_end = base::as.Date("2024-12-31")
    ) |>
    dplyr::select(
      .data$service,
      .data$routing_service,
      .data$hcpcs,
      .data$category,
      .data$code_system,
      .data$cms_tier,
      .data$sex_specific,
      .data$payer_scope,
      .data$setting_scope,
      .data$calibration_role,
      .data$evidence_quality,
      .data$source_id,
      .data$source_citation,
      .data$evidence_status,
      .data$registry_version,
      .data$effective_start,
      .data$effective_end
    )
}


#' Canonical provider taxonomy registry for service-share classification
#'
#' Both NUCC branches renamed to Urogynecology and Reconstructive Pelvic
#' Surgery are classified as URPS. The registry intentionally separates the
#' broad provider group used by the routing model from the human-readable
#' provider type.
#'
#' @return A tibble with one row per taxonomy code.
#' @keywords internal
urogynecology_provider_taxonomy_registry <- function() {
  source_citation <- base::paste(
    "NUCC Health Care Provider Taxonomy Code Set;",
    "URPS codes 207VF0040X and 2088F0040X effective 2024-07-01"
  )

  tibble::tribble(
    ~taxonomy_code, ~provider_type, ~provider_group,
    "207VF0040X", "FPMRS physician", "urps",
    "2088F0040X", "URPS physician (urology branch)", "urps",
    "207V00000X", "General OB/GYN", "general_obgyn",
    "208800000X", "Urologist", "general_urology",
    "208C00000X", "Colon and rectal surgeon", "other",
    "363L00000X", "Nurse practitioner", "app",
    "363A00000X", "Physician assistant", "app",
    "364S00000X", "Clinical nurse specialist", "app",
    "367A00000X", "Advanced practice midwife", "app",
    "225100000X", "Physical therapist", "pfpt",
    "207Q00000X", "Family medicine", "primary_care",
    "207R00000X", "Internal medicine", "primary_care"
  ) |>
    dplyr::mutate(
      is_urps_specialist = (.data$provider_group == "urps"),
      source_id = "NUCC_2024-07-01",
      source_citation = source_citation,
      evidence_quality = "A",
      evidence_status = "production",
      registry_version = "2026-08-22"
    )
}


#' Validate the production service-share registry
#'
#' @param registry Registry returned by
#'   `urogynecology_service_share_registry()`.
#' @param strict Whether to reject example, placeholder, or unsourced rows.
#'
#' @return `TRUE`, invisibly.
#' @keywords internal
validate_service_share_registry <- function(registry, strict = TRUE) {
  required <- base::c(
    "service", "routing_service", "hcpcs", "cms_tier",
    "sex_specific", "calibration_role", "evidence_quality",
    "source_id", "source_citation", "evidence_status",
    "registry_version"
  )
  missing <- base::setdiff(required, base::names(registry))
  if (base::length(missing) > 0L) {
    base::stop(
      "Service-share registry is missing: ",
      base::paste(missing, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  duplicate_codes <- registry$hcpcs[base::duplicated(registry$hcpcs)]
  if (base::length(duplicate_codes) > 0L) {
    base::stop(
      "Each HCPCS code must have one canonical owner. Duplicates: ",
      base::paste(base::unique(duplicate_codes), collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  if (base::any(!registry$cms_tier %in% base::c("A", "B"))) {
    base::stop("`cms_tier` must be A or B.", call. = FALSE)
  }
  if (base::any(!registry$calibration_role %in%
      base::c("primary", "secondary"))) {
    base::stop(
      "`calibration_role` must be primary or secondary.",
      call. = FALSE
    )
  }
  if (base::any(base::is.na(registry$evidence_quality)) ||
      base::any(!registry$evidence_quality %in% base::c("A", "B", "C", "sourced", "calibrated", "derived"))) {
    base::stop("Production mode validation failed: invalid evidence_quality", call. = FALSE)
  }

  if (base::isTRUE(strict)) {
    bad_status <- base::grepl(
      "example|placeholder|assumption",
      registry$evidence_status,
      ignore.case = TRUE
    )
    if (base::any(bad_status)) {
      base::stop(
        "Strict service-share registry contains example/placeholder rows.",
        call. = FALSE
      )
    }
    missing_source <- !base::nzchar(registry$source_id) |
      !base::nzchar(registry$source_citation)
    if (base::any(missing_source)) {
      base::stop(
        "Strict registry requires non-empty source_id and source_citation.",
        call. = FALSE
      )
    }
  }

  base::invisible(TRUE)
}


#' Alias for urogynecology_service_share_registry
#' @export
build_urogynecology_service_registry <- urogynecology_service_share_registry

#' Alias for urogynecology_provider_taxonomy_registry
#' @export
build_urogynecology_provider_taxonomy_registry <- urogynecology_provider_taxonomy_registry

#' Alias for validate_service_share_registry
#' @export
validate_service_registry_production <- validate_service_share_registry
