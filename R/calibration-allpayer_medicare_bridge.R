# CHIA all-payer <-> Medicare FFS calibration bridge --------------------------
#
# PURPOSE
#
# Build a common provider-year workload representation from:
#
#   1. Massachusetts CHIA all-payer claims / case-mix data
#   2. Medicare fee-for-service claims
#
# The overlap in Massachusetts estimates:
#
#   all-payer workload / Medicare workload
#
# conditional on provider characteristics and year. The fitted relationship can
# then be applied to Medicare-observed URPS nationally to estimate total
# clinical workload -- the all-payer signal the package otherwise lacks
# (R/supply-medicare_capacity.R produces Medicare-only relative workload and
# says so: "not latent all-payer demand").
#
# IMPORTANT
#
# This is a delivered-workload calibration, not a direct adequacy estimate.
# Claims measure care delivered. They do not observe patients who never entered
# the system. Therefore this module must not promote baseline adequacy to
# "calibrated" without independent access/capacity evidence; every artifact it
# emits carries calibration_status = "measured_input_unvalidated_response".
#
# Base-R note: the package declares neither scales nor lubridate. Thousands-
# formatting and year extraction are done in base R. Parquet/Feather reads go
# through the optional `arrow` package (Suggests) and are guarded.

.bridge_comma <- function(x) base::format(x, big.mark = ",")


#' First matching column from a set of candidates
#'
#' @param source_tbl Data frame.
#' @param candidates Candidate column names.
#' @param required Whether failure to find a column should stop.
#' @param label Human-readable field label.
#'
#' @return Column name or NULL.
#' @keywords internal
detect_bridge_column <- function(
    source_tbl,
    candidates,
    required = TRUE,
    label = "column") {

  base::message(
    "detect_bridge_column(): looking for ",
    label,
    "."
  )

  source_names <- base::names(source_tbl)

  normalized_names <- source_names |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("[^a-z0-9]+", "_") |>
    stringr::str_replace_all("^_|_$", "")

  normalized_candidates <- candidates |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("[^a-z0-9]+", "_") |>
    stringr::str_replace_all("^_|_$", "")

  matched_index <- base::match(
    normalized_candidates,
    normalized_names
  )

  matched_index <- matched_index[!base::is.na(matched_index)]

  if (base::length(matched_index) == 0L) {
    if (isTRUE(required)) {
      base::stop(
        "Could not identify ",
        label,
        ". Candidates were: ",
        base::paste(candidates, collapse = ", "),
        ". Available columns include: ",
        base::paste(
          utils::head(source_names, 40L),
          collapse = ", "
        ),
        call. = FALSE
      )
    }

    base::message(
      "detect_bridge_column(): optional ",
      label,
      " not found."
    )

    return(NULL)
  }

  selected_name <- source_names[matched_index[[1]]]

  base::message(
    "detect_bridge_column(): ",
    label,
    " -> `",
    selected_name,
    "`."
  )

  selected_name
}


#' Read a claims source file
#'
#' Supports CSV, CSV.GZ, TSV, RDS, Parquet, and Feather. Parquet/Feather require
#' the optional `arrow` package.
#'
#' @param path File path.
#'
#' @return Data frame.
#' @family allpayer bridge
#' @concept calibration
#' @export
read_claims_source <- function(path) {

  base::message(
    "read_claims_source(): reading ",
    path
  )

  if (!base::file.exists(path)) {
    base::stop(
      "Claims source does not exist: ",
      path,
      call. = FALSE
    )
  }

  lower_path <- stringr::str_to_lower(path)

  needs_arrow <- stringr::str_detect(lower_path, "\\.parquet$") ||
    stringr::str_detect(lower_path, "\\.feather$")
  if (needs_arrow && !requireNamespace("arrow", quietly = TRUE)) {
    base::stop(
      "Reading '", path, "' needs the 'arrow' package. Install arrow, or ",
      "convert the source to CSV/RDS.",
      call. = FALSE
    )
  }

  source_tbl <- if (
    stringr::str_detect(lower_path, "\\.csv(\\.gz)?$")
  ) {
    readr::read_csv(
      path,
      show_col_types = FALSE,
      progress = FALSE
    )
  } else if (
    stringr::str_detect(lower_path, "\\.tsv(\\.gz)?$")
  ) {
    readr::read_tsv(
      path,
      show_col_types = FALSE,
      progress = FALSE
    )
  } else if (
    stringr::str_detect(lower_path, "\\.rds$")
  ) {
    base::readRDS(path)
  } else if (
    stringr::str_detect(lower_path, "\\.parquet$")
  ) {
    arrow::read_parquet(path)
  } else if (
    stringr::str_detect(lower_path, "\\.feather$")
  ) {
    arrow::read_feather(path)
  } else {
    base::stop(
      "Unsupported claims file extension: ",
      path,
      call. = FALSE
    )
  }

  source_tbl <- tibble::as_tibble(source_tbl)

  base::message(
    "read_claims_source(): loaded ",
    .bridge_comma(base::nrow(source_tbl)),
    " rows x ",
    base::ncol(source_tbl),
    " columns."
  )

  source_tbl
}


#' Normalize NPI values
#'
#' @param npi NPI vector.
#'
#' @return Character NPI.
#' @family allpayer bridge
#' @concept calibration
#' @export
normalize_npi <- function(npi) {

  normalized_npi <- npi |>
    base::as.character() |>
    stringr::str_replace_all("[^0-9]", "") |>
    dplyr::na_if("")

  normalized_npi[
    base::nchar(normalized_npi) != 10L
  ] <- NA_character_

  normalized_npi
}


#' Construct a generic claims workload panel
#'
#' The function deliberately accepts explicit column overrides because CHIA
#' releases and Medicare extracts can use different names.
#'
#' @param source_tbl Raw claims table.
#' @param source_name Source label.
#' @param npi_col Optional NPI column override.
#' @param year_col Optional year column override.
#' @param date_col Optional service/admission date column override.
#' @param hcpcs_col Optional CPT/HCPCS column override.
#' @param patient_col Optional patient identifier override.
#' @param units_col Optional service-unit count override.
#' @param wrvu_col Optional work-RVU override.
#' @param state_col Optional provider-state override.
#' @param age_col Optional provider-age override.
#' @param sex_col Optional provider-sex override.
#' @param keep_state Optional state filter.
#'
#' @return Provider-year workload tibble.
#' @family allpayer bridge
#' @concept calibration
#' @export
build_claims_provider_year <- function(
    source_tbl,
    source_name,
    npi_col = NULL,
    year_col = NULL,
    date_col = NULL,
    hcpcs_col = NULL,
    patient_col = NULL,
    units_col = NULL,
    wrvu_col = NULL,
    state_col = NULL,
    age_col = NULL,
    sex_col = NULL,
    keep_state = NULL) {

  base::message(
    "build_claims_provider_year(): starting ",
    source_name,
    "."
  )

  if (!base::is.data.frame(source_tbl)) {
    base::stop(
      "`source_tbl` must be a data frame.",
      call. = FALSE
    )
  }

  if (is.null(npi_col)) {
    npi_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "npi",
        "provider_npi",
        "rendering_npi",
        "rendering_provider_npi",
        "performing_npi",
        "physician_npi",
        "billing_npi",
        "index_npi"
      ),
      required = TRUE,
      label = "provider NPI"
    )
  }

  if (is.null(year_col)) {
    year_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "year",
        "service_year",
        "claim_year",
        "calendar_year",
        "data_year"
      ),
      required = FALSE,
      label = "service year"
    )
  }

  if (is.null(date_col) && is.null(year_col)) {
    date_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "service_date",
        "date_of_service",
        "claim_date",
        "admission_date",
        "admit_date",
        "index_date",
        "from_date",
        "line_service_date"
      ),
      required = TRUE,
      label = "service date"
    )
  }

  if (is.null(hcpcs_col)) {
    hcpcs_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "hcpcs",
        "hcpcs_code",
        "cpt",
        "cpt_code",
        "procedure_code",
        "procedure"
      ),
      required = FALSE,
      label = "procedure code"
    )
  }

  if (is.null(patient_col)) {
    patient_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "wu_id",
        "patient_id",
        "member_id",
        "beneficiary_id",
        "bene_id",
        "encrypted_member_id"
      ),
      required = FALSE,
      label = "patient identifier"
    )
  }

  if (is.null(units_col)) {
    units_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "line_srvc_cnt",
        "service_count",
        "services",
        "units",
        "unit_count",
        "claim_count"
      ),
      required = FALSE,
      label = "service units"
    )
  }

  if (is.null(wrvu_col)) {
    wrvu_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "wrvu",
        "work_rvu",
        "work_rvus",
        "total_wrvu"
      ),
      required = FALSE,
      label = "work RVU"
    )
  }

  if (is.null(state_col)) {
    state_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "provider_state",
        "state",
        "rndrng_prvdr_state_abrvtn",
        "practice_state"
      ),
      required = FALSE,
      label = "provider state"
    )
  }

  if (is.null(age_col)) {
    age_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "provider_age",
        "physician_age",
        "age_provider"
      ),
      required = FALSE,
      label = "provider age"
    )
  }

  if (is.null(sex_col)) {
    sex_col <- detect_bridge_column(
      source_tbl,
      candidates = c(
        "provider_sex",
        "physician_sex",
        "gender",
        "sex"
      ),
      required = FALSE,
      label = "provider sex"
    )
  }

  base::message(
    "build_claims_provider_year(): standardizing provider identifiers."
  )

  standardized_tbl <- source_tbl |>
    dplyr::mutate(
      bridge_npi = normalize_npi(.data[[npi_col]])
    )

  if (!is.null(year_col)) {
    standardized_tbl <- standardized_tbl |>
      dplyr::mutate(
        bridge_year = base::as.integer(
          .data[[year_col]]
        )
      )
  } else {
    standardized_tbl <- standardized_tbl |>
      dplyr::mutate(
        bridge_year = base::as.integer(
          base::format(
            base::as.Date(.data[[date_col]]),
            "%Y"
          )
        )
      )
  }

  if (!is.null(state_col)) {
    standardized_tbl <- standardized_tbl |>
      dplyr::mutate(
        bridge_state = .data[[state_col]] |>
          base::as.character() |>
          stringr::str_to_upper()
      )
  } else {
    standardized_tbl <- standardized_tbl |>
      dplyr::mutate(
        bridge_state = NA_character_
      )
  }

  if (!is.null(keep_state) && !is.null(state_col)) {
    base::message(
      "build_claims_provider_year(): restricting to state ",
      keep_state,
      "."
    )

    standardized_tbl <- standardized_tbl |>
      dplyr::filter(
        .data$bridge_state ==
          stringr::str_to_upper(keep_state)
      )
  }

  standardized_tbl <- standardized_tbl |>
    dplyr::filter(
      !base::is.na(.data$bridge_npi),
      !base::is.na(.data$bridge_year)
    )

  base::message(
    "build_claims_provider_year(): constructing workload variables."
  )

  standardized_tbl <- standardized_tbl |>
    dplyr::mutate(
      bridge_units = if (!is.null(units_col)) {
        base::as.numeric(.data[[units_col]])
      } else {
        1
      },
      bridge_wrvu = if (!is.null(wrvu_col)) {
        base::as.numeric(.data[[wrvu_col]])
      } else {
        NA_real_
      },
      bridge_hcpcs = if (!is.null(hcpcs_col)) {
        base::as.character(.data[[hcpcs_col]])
      } else {
        NA_character_
      },
      bridge_patient = if (!is.null(patient_col)) {
        base::as.character(.data[[patient_col]])
      } else {
        NA_character_
      },
      bridge_provider_age = if (!is.null(age_col)) {
        base::as.numeric(.data[[age_col]])
      } else {
        NA_real_
      },
      bridge_provider_sex = if (!is.null(sex_col)) {
        base::as.character(.data[[sex_col]])
      } else {
        NA_character_
      }
    ) |>
    dplyr::mutate(
      bridge_units = dplyr::if_else(
        base::is.na(.data$bridge_units) |
          .data$bridge_units <= 0,
        1,
        .data$bridge_units
      )
    )

  base::message(
    "build_claims_provider_year(): aggregating provider-year workload."
  )

  provider_year_tbl <- standardized_tbl |>
    dplyr::group_by(
      .data$bridge_npi,
      .data$bridge_year
    ) |>
    dplyr::summarise(
      source = source_name,
      state = dplyr::first(
        .data$bridge_state[
          !base::is.na(.data$bridge_state)
        ],
        default = NA_character_
      ),
      claim_lines = dplyr::n(),
      service_units = base::sum(
        .data$bridge_units,
        na.rm = TRUE
      ),
      unique_patients = if (
        !is.null(patient_col)
      ) {
        dplyr::n_distinct(
          .data$bridge_patient,
          na.rm = TRUE
        )
      } else {
        NA_integer_
      },
      unique_procedures = if (
        !is.null(hcpcs_col)
      ) {
        dplyr::n_distinct(
          .data$bridge_hcpcs,
          na.rm = TRUE
        )
      } else {
        NA_integer_
      },
      total_wrvu = if (
        !is.null(wrvu_col)
      ) {
        base::sum(
          .data$bridge_wrvu *
            .data$bridge_units,
          na.rm = TRUE
        )
      } else {
        NA_real_
      },
      provider_age = stats::median(
        .data$bridge_provider_age,
        na.rm = TRUE
      ),
      provider_sex = dplyr::first(
        .data$bridge_provider_sex[
          !base::is.na(.data$bridge_provider_sex)
        ],
        default = NA_character_
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      provider_age = dplyr::if_else(
        base::is.nan(.data$provider_age),
        NA_real_,
        .data$provider_age
      )
    ) |>
    dplyr::rename(
      npi = "bridge_npi",
      year = "bridge_year"
    )

  base::message(
    "build_claims_provider_year(): generated ",
    .bridge_comma(base::nrow(provider_year_tbl)),
    " provider-years."
  )

  provider_year_tbl
}


#' Build the Massachusetts CHIA provider-year panel
#'
#' @param chia_tbl Raw CHIA claims/case-mix table.
#' @param ... Column overrides passed to build_claims_provider_year().
#'
#' @return CHIA provider-year workload tibble.
#' @family allpayer bridge
#' @concept calibration
#' @export
build_chia_provider_year <- function(
    chia_tbl,
    ...) {

  base::message(
    "build_chia_provider_year(): preparing CHIA all-payer workload."
  )

  build_claims_provider_year(
    source_tbl = chia_tbl,
    source_name = "CHIA_all_payer",
    keep_state = "MA",
    ...
  )
}


#' Build Medicare provider-year workload
#'
#' @param medicare_tbl Medicare FFS claims table.
#' @param ... Column overrides passed to build_claims_provider_year().
#'
#' @return Medicare provider-year workload tibble.
#' @family allpayer bridge
#' @concept calibration
#' @export
build_medicare_provider_year <- function(
    medicare_tbl,
    ...) {

  base::message(
    "build_medicare_provider_year(): preparing Medicare workload."
  )

  build_claims_provider_year(
    source_tbl = medicare_tbl,
    source_name = "Medicare_FFS",
    ...
  )
}


#' Restrict a claims table to canonical URPS NPIs
#'
#' @param claims_tbl Claims table.
#' @param urps_roster Canonical URPS roster.
#' @param claims_npi_col NPI column in claims. `NULL` auto-detects.
#' @param roster_npi_col NPI column in roster.
#'
#' @return Claims restricted to URPS providers.
#' @family allpayer bridge
#' @concept calibration
#' @export
filter_claims_to_urps <- function(
    claims_tbl,
    urps_roster,
    claims_npi_col = NULL,
    roster_npi_col = "npi") {

  base::message(
    "filter_claims_to_urps(): standardizing URPS roster."
  )

  if (is.null(claims_npi_col)) {
    claims_npi_col <- detect_bridge_column(
      claims_tbl,
      candidates = c(
        "npi",
        "provider_npi",
        "rendering_npi",
        "rendering_provider_npi",
        "performing_npi",
        "physician_npi",
        "billing_npi",
        "index_npi"
      ),
      required = TRUE,
      label = "claims provider NPI"
    )
  }

  roster_tbl <- urps_roster |>
    dplyr::transmute(
      bridge_npi = normalize_npi(
        .data[[roster_npi_col]]
      )
    ) |>
    dplyr::filter(
      !base::is.na(.data$bridge_npi)
    ) |>
    dplyr::distinct(
      .data$bridge_npi
    )

  base::message(
    "filter_claims_to_urps(): canonical NPIs = ",
    .bridge_comma(base::nrow(roster_tbl)),
    "."
  )

  filtered_tbl <- claims_tbl |>
    dplyr::mutate(
      bridge_npi = normalize_npi(
        .data[[claims_npi_col]]
      )
    ) |>
    dplyr::semi_join(
      roster_tbl,
      by = "bridge_npi"
    ) |>
    dplyr::select(-dplyr::any_of("bridge_npi"))

  base::message(
    "filter_claims_to_urps(): retained ",
    .bridge_comma(base::nrow(filtered_tbl)),
    " claims rows."
  )

  filtered_tbl
}
