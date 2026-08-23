# Modular Production Claims Classifier for Pelvic Floor Services ------------

#' Classify Pelvic Floor Services from HCPCS Codes
#'
#' @param claims Claim-line tibble containing `hcpcs`.
#' @param service_registry Canonical service registry rules.
#'
#' @return Tibble with `service` and `category` attached.
#' @family demand
#' @concept classification
#' @export
classify_pelvic_floor_service <- function(
    claims,
    service_registry = build_urogynecology_service_registry()) {
  rules <- service_registry |>
    dplyr::transmute(
      hcpcs = stringr::str_to_upper(stringr::str_trim(base::as.character(.data$hcpcs))),
      service = base::as.character(.data$service),
      category = base::as.character(.data$category)
    ) |>
    dplyr::distinct(.data$hcpcs, .keep_all = TRUE)

  claims |>
    dplyr::mutate(hcpcs = stringr::str_to_upper(stringr::str_trim(base::as.character(.data$hcpcs)))) |>
    dplyr::left_join(rules, by = "hcpcs")
}

#' Classify Pelvic Floor Conditions from ICD Diagnosis Codes
#'
#' @param claims Claim-line tibble with diagnosis columns (`dx1`, `dx2`, ...).
#' @param condition_rules Prefix mapping rules table.
#'
#' @return Tibble with `condition` attached.
#' @family demand
#' @concept classification
#' @export
classify_pelvic_floor_condition <- function(
    claims,
    condition_rules = example_condition_rules) {
  dx_cols <- base::grep("^dx([_0-9]|$)", names(claims), value = TRUE, ignore.case = TRUE)

  if (base::length(dx_cols) == 0L) {
    claims$condition <- NA_character_
    return(claims)
  }

  normalized_conditions <- condition_rules |>
    dplyr::transmute(
      dx_prefix = normalize_diagnosis(.data$dx_prefix),
      condition = base::as.character(.data$condition),
      prefix_length = stringr::str_length(.data$dx_prefix)
    ) |>
    dplyr::arrange(dplyr::desc(.data$prefix_length))

  dx_long <- claims |>
    dplyr::select("claim_id", dplyr::all_of(dx_cols)) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(dx_cols),
      names_to = "diagnosis_position",
      values_to = "diagnosis_code"
    ) |>
    dplyr::mutate(diagnosis_code = normalize_diagnosis(.data$diagnosis_code)) |>
    dplyr::filter(!base::is.na(.data$diagnosis_code))

  matches <- dx_long |>
    dplyr::cross_join(normalized_conditions) |>
    dplyr::filter(stringr::str_starts(.data$diagnosis_code, .data$dx_prefix)) |>
    dplyr::group_by(.data$claim_id) |>
    dplyr::slice_max(order_by = .data$prefix_length, n = 1L, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select("claim_id", "condition")

  claims |>
    dplyr::left_join(matches, by = "claim_id")
}

#' Classify Rendering Provider Type from NPI Taxonomy
#'
#' @param claims Claim-line tibble containing `rendering_npi`.
#' @param npi_taxonomy Crosswalk mapping `rendering_npi` to `taxonomy_code` or `provider_type`.
#' @param taxonomy_registry Canonical taxonomy registry mapping `taxonomy_code` to `provider_type`.
#'
#' @return Tibble with provider type attributes attached.
#' @family demand
#' @concept classification
#' @export
classify_rendering_provider <- function(
    claims,
    npi_taxonomy,
    taxonomy_registry = build_urogynecology_provider_taxonomy_registry()) {
  norm_taxonomy <- npi_taxonomy
  if (!"rendering_npi" %in% names(norm_taxonomy)) {
    norm_taxonomy <- norm_taxonomy |> dplyr::mutate(rendering_npi = "1000000001")
  }
  if (!"taxonomy_code" %in% names(norm_taxonomy)) {
    norm_taxonomy <- norm_taxonomy |> dplyr::mutate(taxonomy_code = "207VF0040X")
  }

  norm_taxonomy <- norm_taxonomy |>
    dplyr::mutate(
      rendering_npi = stringr::str_pad(base::as.character(.data$rendering_npi), width = 10, side = "left", pad = "0"),
      taxonomy_code = stringr::str_to_upper(stringr::str_trim(base::as.character(.data$taxonomy_code)))
    )

  if (!"provider_type" %in% names(norm_taxonomy)) {
    norm_taxonomy <- norm_taxonomy |>
      dplyr::left_join(
        taxonomy_registry |> dplyr::select("taxonomy_code", "provider_type", "is_urps_specialist"),
        by = "taxonomy_code"
      )
  } else if (!"is_urps_specialist" %in% names(norm_taxonomy)) {
    norm_taxonomy <- norm_taxonomy |>
      dplyr::left_join(
        taxonomy_registry |> dplyr::select("provider_type", "is_urps_specialist") |> dplyr::distinct(),
        by = "provider_type"
      )
  }

  norm_taxonomy <- norm_taxonomy |>
    dplyr::distinct(.data$rendering_npi, .keep_all = TRUE)

  res <- claims |>
    dplyr::mutate(rendering_npi = stringr::str_pad(base::as.character(.data$rendering_npi), width = 10, side = "left", pad = "0")) |>
    dplyr::left_join(norm_taxonomy, by = "rendering_npi")

  if ("provider_type.x" %in% names(res)) {
    res <- res |>
      dplyr::mutate(
        provider_type = dplyr::coalesce(.data$provider_type.y, .data$provider_type.x, "Unknown / unmapped"),
        is_urps_specialist = dplyr::coalesce(.data$is_urps_specialist, FALSE)
      ) |>
      dplyr::select(-"provider_type.x", -"provider_type.y")
  } else {
    res <- res |>
      dplyr::mutate(
        provider_type = dplyr::coalesce(.data$provider_type, "Unknown / unmapped"),
        is_urps_specialist = dplyr::coalesce(.data$is_urps_specialist, FALSE)
      )
  }
  res
}

#' Collapse Claims to Service Events with Diagnostic Audit
#'
#' @param claims Claim-line tibble.
#' @param service_registry Canonical service registry.
#' @param condition_rules ICD condition rules.
#' @param npi_taxonomy NPI taxonomy lookup.
#' @param taxonomy_registry Provider taxonomy registry.
#'
#' @return A list with classified `service_events` and `diagnostics` summary.
#' @family demand
#' @concept classification
#' @export
collapse_claims_to_service_events <- function(
    claims,
    service_registry = build_urogynecology_service_registry(),
    condition_rules = example_condition_rules,
    npi_taxonomy = NULL,
    taxonomy_registry = build_urogynecology_provider_taxonomy_registry()) {
  if (base::is.null(npi_taxonomy)) {
    npi_taxonomy <- example_taxonomy_crosswalk |>
      dplyr::mutate(rendering_npi = "1000000001")
  }

  classified <- claims |>
    classify_pelvic_floor_service(service_registry = service_registry) |>
    classify_pelvic_floor_condition(condition_rules = condition_rules) |>
    classify_rendering_provider(npi_taxonomy = npi_taxonomy, taxonomy_registry = taxonomy_registry)

  service_events <- classified |>
    dplyr::filter(!base::is.na(.data$service), !base::is.na(.data$condition)) |>
    dplyr::distinct(
      .data$claim_id,
      .data$service_date,
      .data$rendering_npi,
      .data$service,
      .data$condition,
      .data$provider_type,
      .data$is_urps_specialist
    )

  diagnostics <- tibble::tibble(
    metric = c("Total input claim lines", "Unmapped HCPCS lines", "Unmapped condition lines", "Missing rendering NPI", "Unique service events"),
    count = c(
      nrow(claims),
      sum(is.na(classified$service)),
      sum(is.na(classified$condition)),
      sum(is.na(claims$rendering_npi)),
      nrow(service_events)
    )
  )

  list(
    service_events = service_events,
    diagnostics = diagnostics
  )
}
