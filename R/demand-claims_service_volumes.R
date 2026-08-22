# Claims-derived URPS service volumes & provider share estimation ----------

#' Estimate urogynecology service shares from claims
#'
#' Estimates
#' \deqn{\pi_{scgt}=P(G=g\mid S=s,C=c,T=t)}
#' from claim-level rendering NPIs. Sparse cells are stabilized with an
#' empirical Dirichlet prior learned separately within each service.
#'
#' Required columns in `claims` are `claim_id`, `service_date`,
#' `rendering_npi`, `hcpcs`, and one or more diagnosis columns beginning with
#' `dx`. Required columns in `npi_taxonomy` are `rendering_npi` and
#' `taxonomy_code`. Required columns in `taxonomy_crosswalk` are
#' `taxonomy_code` and `provider_type`. Required columns in `service_rules`
#' are `hcpcs` and `service`. Required columns in `condition_rules` are
#' `dx_prefix` and `condition`.
#'
#' @param claims Claim-line tibble.
#' @param npi_taxonomy NPI-to-taxonomy crosswalk. May contain `is_primary`.
#' @param taxonomy_crosswalk Taxonomy-to-provider-type crosswalk.
#' @param service_rules HCPCS-to-service crosswalk.
#' @param condition_rules ICD prefix-to-condition crosswalk.
#' @param practice_affiliation Optional NPI-to-practice crosswalk.
#' @param prior_strength Effective service count in the Dirichlet prior.
#' @param confidence_level Credible interval coverage.
#' @param minimum_cell_services Threshold for a stable-cell flag.
#' @param unknown_provider Label for unmapped rendering NPIs.
#' @param save_directory Optional directory for timestamped CSV and RDS files.
#'
#' @return A named list containing shares, trends, classified claims,
#'   exclusions, and a dynamic summary sentence.
#' @family demand
#' @concept claims
#' @export
estimate_urogynecology_service_share <- function(
    claims,
    npi_taxonomy,
    taxonomy_crosswalk = example_taxonomy_crosswalk,
    service_rules = example_service_rules,
    condition_rules = example_condition_rules,
    practice_affiliation = NULL,
    prior_strength = 20,
    confidence_level = 0.95,
    minimum_cell_services = 30L,
    unknown_provider = "Unknown / unmapped",
    save_directory = NULL) {
  base::message("Starting claims-based service-share estimation.")
  base::message("Input claim lines: ",
                scales::comma(base::nrow(claims)))

  required_claim_columns <- c(
    "claim_id", "service_date", "rendering_npi", "hcpcs"
  )
  missing_claim_columns <- base::setdiff(
    required_claim_columns,
    base::names(claims)
  )
  if (base::length(missing_claim_columns) > 0L) {
    base::stop(
      "Missing claims columns: ",
      base::paste(missing_claim_columns, collapse = ", ")
    )
  }

  diagnosis_columns <- base::grep(
    "^dx([_0-9]|$)",
    base::names(claims),
    value = TRUE,
    ignore.case = TRUE
  )
  if (base::length(diagnosis_columns) == 0L) {
    base::stop("At least one diagnosis column beginning with `dx` is needed.")
  }

  validate_columns(
    npi_taxonomy,
    c("rendering_npi", "taxonomy_code"),
    "npi_taxonomy"
  )
  validate_columns(
    taxonomy_crosswalk,
    c("taxonomy_code", "provider_type"),
    "taxonomy_crosswalk"
  )
  validate_columns(service_rules, c("hcpcs", "service"), "service_rules")
  validate_columns(
    condition_rules,
    c("dx_prefix", "condition"),
    "condition_rules"
  )

  if (!base::is.numeric(prior_strength) || prior_strength <= 0) {
    base::stop("`prior_strength` must be greater than zero.")
  }
  if (!base::is.numeric(confidence_level) ||
      confidence_level <= 0 || confidence_level >= 1) {
    base::stop("`confidence_level` must lie strictly between zero and one.")
  }

  base::message("Normalizing identifiers, dates, HCPCS codes, and diagnoses.")
  normalized_claims <- claims |>
    dplyr::mutate(
      claim_id = base::as.character(.data$claim_id),
      service_date = base::as.Date(.data$service_date),
      rendering_npi = stringr::str_pad(
        base::as.character(.data$rendering_npi),
        width = 10,
        side = "left",
        pad = "0"
      ),
      hcpcs = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$hcpcs))
      ),
      year = base::as.integer(base::format(base::as.Date(.data$service_date), "%Y"))
    )

  if (!"is_primary" %in% base::names(npi_taxonomy)) {
    npi_taxonomy <- npi_taxonomy |>
      dplyr::mutate(is_primary = FALSE)
  }

  normalized_taxonomy <- npi_taxonomy |>
    dplyr::mutate(
      rendering_npi = stringr::str_pad(
        base::as.character(.data$rendering_npi),
        width = 10,
        side = "left",
        pad = "0"
      ),
      taxonomy_code = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$taxonomy_code))
      ),
      is_primary = dplyr::coalesce(
        base::as.logical(.data$is_primary),
        FALSE
      )
    ) |>
    dplyr::arrange(.data$rendering_npi, dplyr::desc(.data$is_primary)) |>
    dplyr::distinct(.data$rendering_npi, .keep_all = TRUE)

  normalized_provider_map <- taxonomy_crosswalk |>
    dplyr::transmute(
      taxonomy_code = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$taxonomy_code))
      ),
      provider_type = base::as.character(.data$provider_type)
    ) |>
    dplyr::distinct(.data$taxonomy_code, .keep_all = TRUE)

  provider_lookup <- normalized_taxonomy |>
    dplyr::left_join(normalized_provider_map, by = "taxonomy_code") |>
    dplyr::mutate(
      provider_type = dplyr::coalesce(
        .data$provider_type,
        unknown_provider
      )
    ) |>
    dplyr::select(
      .data$rendering_npi,
      .data$taxonomy_code,
      .data$provider_type
    )

  normalized_services <- service_rules |>
    dplyr::transmute(
      hcpcs = stringr::str_to_upper(
        stringr::str_trim(base::as.character(.data$hcpcs))
      ),
      service = base::as.character(.data$service)
    ) |>
    dplyr::distinct(.data$hcpcs, .keep_all = TRUE)

  base::message("Assigning each claim line to a pelvic-floor service.")
  service_claims <- normalized_claims |>
    dplyr::left_join(normalized_services, by = "hcpcs")

  base::message("Assigning condition groups from all diagnosis positions.")
  normalized_conditions <- condition_rules |>
    dplyr::transmute(
      dx_prefix = normalize_diagnosis(.data$dx_prefix),
      condition = base::as.character(.data$condition),
      prefix_length = stringr::str_length(.data$dx_prefix)
    ) |>
    dplyr::arrange(dplyr::desc(.data$prefix_length))

  diagnosis_long <- service_claims |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(diagnosis_columns),
      names_to = "diagnosis_position",
      values_to = "diagnosis_code"
    ) |>
    dplyr::mutate(
      diagnosis_code = normalize_diagnosis(.data$diagnosis_code)
    ) |>
    dplyr::filter(!base::is.na(.data$diagnosis_code))

  condition_matches <- diagnosis_long |>
    dplyr::cross_join(normalized_conditions) |>
    dplyr::filter(
      stringr::str_starts(.data$diagnosis_code, .data$dx_prefix)
    ) |>
    dplyr::group_by(.data$claim_id, .data$service, .data$condition) |>
    dplyr::slice_max(
      order_by = .data$prefix_length,
      n = 1L,
      with_ties = FALSE
    ) |>
    dplyr::ungroup()

  base::message("Linking rendering NPI taxonomy to provider type.")
  classified_claims <- condition_matches |>
    dplyr::filter(!base::is.na(.data$service)) |>
    dplyr::left_join(provider_lookup, by = "rendering_npi") |>
    dplyr::mutate(
      provider_type = dplyr::coalesce(
        .data$provider_type,
        unknown_provider
      )
    )

  if (!base::is.null(practice_affiliation)) {
    validate_columns(
      practice_affiliation,
      c("rendering_npi", "practice_id"),
      "practice_affiliation"
    )
    base::message("Adding practice affiliation to classified claims.")
    affiliation_lookup <- practice_affiliation |>
      dplyr::mutate(
        rendering_npi = stringr::str_pad(
          base::as.character(.data$rendering_npi),
          width = 10,
          side = "left",
          pad = "0"
        )
      ) |>
      dplyr::distinct(.data$rendering_npi, .keep_all = TRUE) |>
      dplyr::select(.data$rendering_npi, .data$practice_id)
    classified_claims <- classified_claims |>
      dplyr::left_join(affiliation_lookup, by = "rendering_npi")
  } else {
    classified_claims <- classified_claims |>
      dplyr::mutate(practice_id = NA_character_)
  }

  base::message("Deduplicating to one provider attribution per service event.")
  service_events <- classified_claims |>
    dplyr::distinct(
      .data$claim_id,
      .data$year,
      .data$service,
      .data$condition,
      .data$rendering_npi,
      .data$provider_type,
      .data$practice_id
    )

  event_counts <- service_events |>
    dplyr::count(
      .data$service,
      .data$condition,
      .data$year,
      .data$provider_type,
      name = "service_events"
    )

  cell_totals <- event_counts |>
    dplyr::group_by(.data$service, .data$condition, .data$year) |>
    dplyr::summarise(
      total_service_events = base::sum(.data$service_events),
      .groups = "drop"
    )

  provider_levels <- event_counts |>
    dplyr::distinct(.data$provider_type)
  share_cells <- cell_totals |>
    tidyr::crossing(provider_levels) |>
    dplyr::left_join(
      event_counts,
      by = c("service", "condition", "year", "provider_type")
    ) |>
    dplyr::mutate(service_events = tidyr::replace_na(.data$service_events, 0L))

  base::message("Learning service-specific empirical Dirichlet priors.")
  service_prior_counts <- event_counts |>
    dplyr::group_by(.data$service, .data$provider_type) |>
    dplyr::summarise(
      provider_service_events = base::sum(.data$service_events),
      .groups = "drop"
    )
  service_prior <- event_counts |>
    dplyr::distinct(.data$service) |>
    tidyr::crossing(provider_levels) |>
    dplyr::left_join(
      service_prior_counts,
      by = c("service", "provider_type")
    ) |>
    dplyr::mutate(
      provider_service_events = tidyr::replace_na(
        .data$provider_service_events,
        0L
      )
    ) |>
    dplyr::group_by(.data$service) |>
    dplyr::mutate(
      service_events_all_providers = base::sum(
        .data$provider_service_events
      ),
      prior_probability = (.data$provider_service_events + 0.5) /
        (.data$service_events_all_providers +
           0.5 * dplyr::n())
    ) |>
    dplyr::ungroup()

  alpha_tail <- (1 - confidence_level) / 2
  base::message("Estimating empirical and smoothed provider shares.")
  shares <- share_cells |>
    dplyr::left_join(
      service_prior |>
        dplyr::select(
          .data$service,
          .data$provider_type,
          .data$prior_probability
        ),
      by = c("service", "provider_type")
    ) |>
    dplyr::mutate(
      empirical_share = .data$service_events /
        .data$total_service_events,
      posterior_alpha = .data$service_events +
        prior_strength * .data$prior_probability,
      posterior_alpha_total = .data$total_service_events + prior_strength,
      posterior_share = .data$posterior_alpha /
        .data$posterior_alpha_total,
      posterior_lower = stats::qbeta(
        alpha_tail,
        shape1 = .data$posterior_alpha,
        shape2 = .data$posterior_alpha_total - .data$posterior_alpha
      ),
      posterior_upper = stats::qbeta(
        1 - alpha_tail,
        shape1 = .data$posterior_alpha,
        shape2 = .data$posterior_alpha_total - .data$posterior_alpha
      ),
      stable_cell = .data$total_service_events >= minimum_cell_services
    ) |>
    dplyr::arrange(
      .data$service,
      .data$condition,
      .data$year,
      dplyr::desc(.data$posterior_share)
    )

  base::message("Estimating annual log-odds trends by provider type.")
  trends <- estimate_share_trends(shares)

  exclusions <- tibble::tibble(
    exclusion = c(
      "No service-rule match",
      "No condition-rule match",
      "Missing rendering NPI",
      "Unknown provider taxonomy"
    ),
    claim_lines = c(
      base::sum(base::is.na(service_claims$service)),
      dplyr::n_distinct(service_claims$claim_id) -
        dplyr::n_distinct(condition_matches$claim_id),
      base::sum(base::is.na(normalized_claims$rendering_npi)),
      base::sum(service_events$provider_type == unknown_provider)
    )
  )

  summary_sentence <- build_share_summary(shares, trends)
  base::message(summary_sentence)

  analysis_bundle <- base::list(
    shares = shares,
    trends = trends,
    classified_claims = service_events,
    exclusions = exclusions,
    summary_sentence = summary_sentence,
    estimand = "P(provider type | service, condition, year)"
  )

  if (!base::is.null(save_directory)) {
    base::dir.create(save_directory, recursive = TRUE, showWarnings = FALSE)
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    share_path <- base::file.path(
      save_directory,
      base::paste0("urogynecology_service_shares_", timestamp, ".csv")
    )
    trend_path <- base::file.path(
      save_directory,
      base::paste0("urogynecology_service_share_trends_", timestamp, ".csv")
    )
    bundle_path <- base::file.path(
      save_directory,
      base::paste0("urogynecology_service_share_bundle_", timestamp, ".rds")
    )
    readr::write_csv(shares, share_path)
    readr::write_csv(trends, trend_path)
    base::saveRDS(analysis_bundle, bundle_path)
    base::message("Saved shares to: ", base::normalizePath(share_path))
    base::message("Saved trends to: ", base::normalizePath(trend_path))
    base::message("Saved analysis bundle to: ",
                  base::normalizePath(bundle_path))
  }

  base::message("Completed claims-based service-share estimation.")
  analysis_bundle
}

#' Validate required columns
#' @keywords internal
validate_columns <- function(table, required_columns, table_name) {
  missing_columns <- base::setdiff(required_columns, base::names(table))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Missing columns in `", table_name, "`: ",
      base::paste(missing_columns, collapse = ", ")
    )
  }
  base::invisible(TRUE)
}

#' Normalize ICD diagnosis strings
#' @keywords internal
normalize_diagnosis <- function(code) {
  code |>
    base::as.character() |>
    stringr::str_to_upper() |>
    stringr::str_replace_all("[^A-Z0-9]", "") |>
    dplyr::na_if("")
}

#' Estimate grouped-binomial annual trends
#' @keywords internal
estimate_share_trends <- function(shares) {
  trend_input <- shares |>
    dplyr::filter(.data$total_service_events > 0) |>
    dplyr::group_by(.data$service, .data$condition, .data$provider_type) |>
    dplyr::filter(dplyr::n_distinct(.data$year) >= 2L) |>
    dplyr::group_split()

  if (base::length(trend_input) == 0L) {
    return(tibble::tibble())
  }

  purrr::map_dfr(
    trend_input,
    function(trend_cell) {
      fitted_model <- stats::glm(
        base::cbind(
          trend_cell$service_events,
          trend_cell$total_service_events - trend_cell$service_events
        ) ~ trend_cell$year,
        family = stats::binomial()
      )
      coefficient_table <- base::summary(fitted_model)$coefficients
      annual_log_odds <- coefficient_table[2L, "Estimate"]
      annual_standard_error <- coefficient_table[2L, "Std. Error"]
      annual_p_value <- coefficient_table[2L, "Pr(>|z|)"]
      tibble::tibble(
        service = trend_cell$service[[1L]],
        condition = trend_cell$condition[[1L]],
        provider_type = trend_cell$provider_type[[1L]],
        first_year = base::min(trend_cell$year),
        last_year = base::max(trend_cell$year),
        annual_odds_ratio = base::exp(annual_log_odds),
        annual_odds_ratio_lower = base::exp(
          annual_log_odds - 1.96 * annual_standard_error
        ),
        annual_odds_ratio_upper = base::exp(
          annual_log_odds + 1.96 * annual_standard_error
        ),
        p_value = annual_p_value,
        direction = dplyr::if_else(
          annual_log_odds >= 0,
          "increased",
          "decreased"
        )
      )
    }
  )
}

#' Build a dynamic summary sentence
#' @keywords internal
build_share_summary <- function(shares, trends) {
  first_year <- base::min(shares$year, na.rm = TRUE)
  last_year <- base::max(shares$year, na.rm = TRUE)
  latest_cell <- shares |>
    dplyr::filter(.data$year == last_year) |>
    dplyr::slice_max(
      order_by = .data$total_service_events,
      n = 1L,
      with_ties = FALSE
    )

  if (base::nrow(trends) == 0L) {
    return(base::paste0(
      "From ", first_year, " through ", last_year, ", the largest ",
      "latest-year cell contained ",
      scales::comma(latest_cell$total_service_events[[1L]]),
      " service events; too few annual observations were available for ",
      "a trend test."
    ))
  }

  leading_trend <- trends |>
    dplyr::slice_min(.data$p_value, n = 1L, with_ties = FALSE)
  formatted_p <- scales::pvalue(
    leading_trend$p_value[[1L]],
    accuracy = 0.001,
    add_p = TRUE
  )
  base::paste0(
    "From ", first_year, " through ", last_year, ", the ",
    leading_trend$provider_type[[1L]], " share of ",
    leading_trend$service[[1L]], " for ",
    leading_trend$condition[[1L]], " ",
    leading_trend$direction[[1L]], " (annual OR ",
    base::formatC(
      leading_trend$annual_odds_ratio[[1L]],
      format = "f",
      digits = 2
    ), "; ", formatted_p, "), based on cells containing up to ",
    scales::comma(base::max(shares$total_service_events)),
    " service events."
  )
}

# Example crosswalk skeletons ---------------------------------------------

#' @export
example_service_rules <- tibble::tribble(
  ~hcpcs, ~service,
  "57160", "Pessary fitting",
  "A4562", "Pessary supply",
  "57288", "Midurethral sling",
  "51715", "Urethral bulking",
  "52287", "Bladder chemodenervation",
  "64561", "Sacral neuromodulation test",
  "64581", "Sacral neuromodulation implant"
)

#' @export
example_condition_rules <- tibble::tribble(
  ~dx_prefix, ~condition,
  "N39.3", "Stress urinary incontinence",
  "N39.41", "Urgency urinary incontinence",
  "N39.46", "Mixed urinary incontinence",
  "N81", "Pelvic organ prolapse",
  "N32.81", "Overactive bladder",
  "R33", "Urinary retention",
  "N39.0", "Urinary tract infection"
)

#' @export
example_taxonomy_crosswalk <- tibble::tribble(
  ~taxonomy_code, ~provider_type,
  "207VF0040X", "FPMRS physician",
  "207V00000X", "General OB/GYN",
  "208800000X", "Urologist",
  "208C00000X", "Colorectal surgeon",
  "363L00000X", "Nurse practitioner",
  "363A00000X", "Physician assistant",
  "225100000X", "Physical therapist",
  "207Q00000X", "Family medicine",
  "208D00000X", "General internal medicine"
)


#' Calibrate URPS service volumes from claims data with fallback
#'
#' @param claims Data frame of claims-derived service volumes.
#' @param fallback Data frame of fallback service volumes.
#' @param require_complete Require complete claims coverage.
#' @param mode Mode when fallback is required: "strict" (error) or "relaxed" (warning).
#'
#' @return Service volumes table with overall_status attribute.
#' @family demand
#' @concept claims
#' @export
claims_service_volumes <- function(
    claims,
    fallback = NULL,
    require_complete = FALSE,
    mode = base::c("strict", "relaxed")) {

  mode <- base::match.arg(mode)

  allowed_statuses <- c("calibrated", "uncalibrated_illustrative", "modeled", "survey")
  if ("calibration_status" %in% base::names(claims)) {
    invalid_status <- base::setdiff(claims$calibration_status, allowed_statuses)
    if (base::length(invalid_status) > 0L) {
      base::stop("unknown calibration_status: ", base::paste(invalid_status, collapse = ", "), call. = FALSE)
    }
  } else {
    claims$calibration_status <- "calibrated"
  }

  if (!"source" %in% base::names(claims)) {
    claims$source <- "CADR"
  }

  if (base::is.null(fallback)) {
    out <- claims |>
      dplyr::select(dplyr::any_of(c("year", "service", "volume", "calibration_status", "source")))
    overall <- if (base::all(out$calibration_status == "calibrated")) "calibrated" else "uncalibrated_illustrative"
    base::attr(out, "overall_status") <- overall
    base::return(out)
  }

  fallback_prep <- fallback |>
    dplyr::mutate(
      fallback_source = "illustrative_fallback",
      fallback_status = "uncalibrated_illustrative"
    )

  merged <- fallback_prep |>
    dplyr::left_join(
      claims,
      by = c("service", "year"),
      suffix = c("_fb", "_claims")
    )

  used_fallback <- base::any(base::is.na(merged$volume_claims))

  if (require_complete && used_fallback) {
    msg <- "Basket is not fully claims-calibrated."
    if (mode == "strict") {
      base::stop(msg, call. = FALSE)
    } else {
      base::warning(msg, call. = FALSE)
    }
  }

  out <- merged |>
    dplyr::transmute(
      year = .data$year,
      service = .data$service,
      volume = dplyr::coalesce(.data$volume_claims, .data$volume_fb),
      calibration_status = dplyr::coalesce(.data$calibration_status, .data$fallback_status),
      source = dplyr::coalesce(.data$source, .data$fallback_source)
    )

  overall <- if (!used_fallback && base::all(out$calibration_status == "calibrated")) {
    "calibrated"
  } else {
    "uncalibrated_illustrative"
  }

  base::attr(out, "overall_status") <- overall
  out
}


#' Resolve service volumes using calibrated claims file or fallback
#'
#' @param demand_long Demand table.
#' @param path Path to claims service volume CSV file.
#'
#' @return Table of service volumes.
#' @keywords internal
resolve_service_volumes <- function(demand_long, path = NULL, mode = NULL) {
  fallback <- example_service_volumes(demand_long)
  if (!base::is.null(path) && base::file.exists(path)) {
    claims_csv <- readr::read_csv(path, show_col_types = FALSE)
    claims_service_volumes(claims_csv, fallback = fallback)
  } else {
    fallback
  }
}

