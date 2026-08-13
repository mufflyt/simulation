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


#' Join CHIA and Medicare provider-year workloads in Massachusetts
#'
#' @param chia_provider_year CHIA provider-year table.
#' @param medicare_provider_year Medicare provider-year table.
#'
#' @return Overlapping provider-year calibration sample.
#' @family allpayer bridge
#' @concept calibration
#' @export
join_chia_medicare_overlap <- function(
    chia_provider_year,
    medicare_provider_year) {

  base::message(
    "join_chia_medicare_overlap(): restricting Medicare to Massachusetts."
  )

  medicare_ma_tbl <- medicare_provider_year |>
    dplyr::filter(
      .data$state == "MA" |
        base::is.na(.data$state)
    ) |>
    dplyr::select(
      "npi",
      "year",
      medicare_claim_lines = "claim_lines",
      medicare_units = "service_units",
      medicare_patients = "unique_patients",
      medicare_wrvu = "total_wrvu",
      medicare_provider_age = "provider_age",
      medicare_provider_sex = "provider_sex"
    )

  base::message(
    "join_chia_medicare_overlap(): joining by NPI + year."
  )

  overlap_tbl <- chia_provider_year |>
    dplyr::select(
      "npi",
      "year",
      chia_claim_lines = "claim_lines",
      chia_units = "service_units",
      chia_patients = "unique_patients",
      chia_wrvu = "total_wrvu",
      chia_provider_age = "provider_age",
      chia_provider_sex = "provider_sex"
    ) |>
    dplyr::inner_join(
      medicare_ma_tbl,
      by = c("npi", "year")
    )

  base::message(
    "join_chia_medicare_overlap(): overlap = ",
    .bridge_comma(base::nrow(overlap_tbl)),
    " provider-years."
  )

  if (base::nrow(overlap_tbl) < 20L) {
    base::warning(
      "Only ",
      base::nrow(overlap_tbl),
      " overlapping provider-years were found. ",
      "Do not use a national bridge until NPI/year linkage is checked.",
      call. = FALSE
    )
  }

  overlap_tbl
}


#' Choose the strongest workload metric shared by CHIA and Medicare
#'
#' Preference: wRVU -> service units -> patients -> claim lines.
#'
#' @param overlap_tbl Joined CHIA/Medicare provider-year table.
#'
#' @return List describing the selected workload pair.
#' @family allpayer bridge
#' @concept calibration
#' @export
select_bridge_workload <- function(overlap_tbl) {

  base::message(
    "select_bridge_workload(): selecting common workload metric."
  )

  candidate_pairs <- list(
    wrvu = c("chia_wrvu", "medicare_wrvu"),
    units = c("chia_units", "medicare_units"),
    patients = c("chia_patients", "medicare_patients"),
    claim_lines = c(
      "chia_claim_lines",
      "medicare_claim_lines"
    )
  )

  for (metric_name in base::names(candidate_pairs)) {
    pair <- candidate_pairs[[metric_name]]

    usable <- overlap_tbl |>
      dplyr::filter(
        base::is.finite(.data[[pair[[1]]]]),
        base::is.finite(.data[[pair[[2]]]]),
        .data[[pair[[1]]]] > 0,
        .data[[pair[[2]]]] > 0
      )

    if (base::nrow(usable) >= 20L) {
      base::message(
        "select_bridge_workload(): selected ",
        metric_name,
        " using ",
        .bridge_comma(base::nrow(usable)),
        " provider-years."
      )

      return(
        list(
          metric = metric_name,
          chia_column = pair[[1]],
          medicare_column = pair[[2]],
          usable = usable
        )
      )
    }
  }

  base::stop(
    paste(
      "No workload metric has at least 20 provider-years with positive",
      "values in both CHIA and Medicare."
    ),
    call. = FALSE
  )
}


#' Fit the CHIA all-payer to Medicare workload bridge
#'
#' Fits a log-log provider-year model:
#'
#' \preformatted{log(all-payer workload) = intercept + beta * log(Medicare
#'   workload) + year effects}
#'
#' Provider age and sex enter when available.
#'
#' @param overlap_tbl Joined overlap sample.
#'
#' @return Bridge-fit object.
#' @family allpayer bridge
#' @concept calibration
#' @export
fit_chia_medicare_bridge <- function(overlap_tbl) {

  base::message(
    "fit_chia_medicare_bridge(): selecting workload."
  )

  workload_spec <- select_bridge_workload(
    overlap_tbl
  )

  bridge_tbl <- workload_spec$usable |>
    dplyr::mutate(
      allpayer_workload =
        .data[[workload_spec$chia_column]],
      medicare_workload =
        .data[[workload_spec$medicare_column]],
      provider_age = dplyr::coalesce(
        .data$chia_provider_age,
        .data$medicare_provider_age
      ),
      provider_sex = dplyr::coalesce(
        .data$chia_provider_sex,
        .data$medicare_provider_sex
      ),
      log_allpayer = base::log(
        .data$allpayer_workload
      ),
      log_medicare = base::log(
        .data$medicare_workload
      ),
      year_factor = base::factor(.data$year)
    )

  has_age <- base::sum(
    !base::is.na(bridge_tbl$provider_age)
  ) >= 20L

  has_sex <- base::length(
    base::unique(
      stats::na.omit(bridge_tbl$provider_sex)
    )
  ) >= 2L

  formula_terms <- c(
    "log_medicare",
    "year_factor"
  )

  if (has_age) {
    formula_terms <- c(
      formula_terms,
      "splines::ns(provider_age, df = 3)"
    )
  }

  if (has_sex) {
    formula_terms <- c(
      formula_terms,
      "provider_sex"
    )
  }

  bridge_formula <- stats::as.formula(
    base::paste(
      "log_allpayer ~",
      base::paste(
        formula_terms,
        collapse = " + "
      )
    )
  )

  base::message(
    "fit_chia_medicare_bridge(): fitting ",
    base::deparse(bridge_formula),
    "."
  )

  bridge_model <- stats::lm(
    formula = bridge_formula,
    data = bridge_tbl
  )

  residual_sd <- stats::sigma(
    bridge_model
  )

  smearing_factor <- base::mean(
    base::exp(
      stats::residuals(bridge_model)
    )
  )

  raw_ratio <- bridge_tbl |>
    dplyr::mutate(
      allpayer_to_medicare =
        .data$allpayer_workload /
        .data$medicare_workload
    )

  ratio_summary <- raw_ratio |>
    dplyr::summarise(
      n_provider_years = dplyr::n(),
      mean_ratio = base::mean(
        .data$allpayer_to_medicare
      ),
      sd_ratio = stats::sd(
        .data$allpayer_to_medicare
      ),
      p25_ratio = stats::quantile(
        .data$allpayer_to_medicare,
        probs = 0.25,
        names = FALSE
      ),
      median_ratio = stats::median(
        .data$allpayer_to_medicare
      ),
      p75_ratio = stats::quantile(
        .data$allpayer_to_medicare,
        probs = 0.75,
        names = FALSE
      )
    )

  base::message(
    "fit_chia_medicare_bridge(): median all-payer/Medicare ratio = ",
    base::sprintf(
      "%.2f",
      ratio_summary$median_ratio
    ),
    "."
  )

  list(
    model = bridge_model,
    workload_metric = workload_spec$metric,
    calibration_sample = bridge_tbl,
    ratio_summary = ratio_summary,
    smearing_factor = smearing_factor,
    residual_sd = residual_sd,
    calibration_status =
      "measured_input_unvalidated_response"
  )
}


#' Apply CHIA/Medicare bridge to national Medicare provider-years
#'
#' @param bridge_fit Output from fit_chia_medicare_bridge().
#' @param medicare_provider_year National Medicare provider-year panel.
#'
#' @return Provider-year table with estimated all-payer workload.
#' @family allpayer bridge
#' @concept calibration
#' @export
predict_allpayer_from_medicare <- function(
    bridge_fit,
    medicare_provider_year) {

  base::message(
    "predict_allpayer_from_medicare(): projecting national workload."
  )

  metric_column <- switch(
    bridge_fit$workload_metric,
    wrvu = "total_wrvu",
    units = "service_units",
    patients = "unique_patients",
    claim_lines = "claim_lines"
  )

  prediction_tbl <- medicare_provider_year |>
    dplyr::filter(
      base::is.finite(.data[[metric_column]]),
      .data[[metric_column]] > 0
    ) |>
    dplyr::mutate(
      medicare_workload =
        .data[[metric_column]],
      log_medicare =
        base::log(.data$medicare_workload),
      year_factor =
        base::factor(.data$year)
    )

  prediction_log <- stats::predict(
    bridge_fit$model,
    newdata = prediction_tbl,
    se.fit = TRUE
  )

  prediction_tbl <- prediction_tbl |>
    dplyr::mutate(
      estimated_allpayer_workload =
        base::exp(
          prediction_log$fit
        ) *
        bridge_fit$smearing_factor,
      estimated_allpayer_low =
        base::exp(
          prediction_log$fit -
            1.96 * prediction_log$se.fit
        ) *
        bridge_fit$smearing_factor,
      estimated_allpayer_high =
        base::exp(
          prediction_log$fit +
            1.96 * prediction_log$se.fit
        ) *
        bridge_fit$smearing_factor,
      allpayer_medicare_multiplier =
        .data$estimated_allpayer_workload /
        .data$medicare_workload,
      calibration_status =
        "measured_input_unvalidated_response"
    )

  base::message(
    "predict_allpayer_from_medicare(): generated ",
    .bridge_comma(base::nrow(prediction_tbl)),
    " national provider-year estimates."
  )

  prediction_tbl
}


#' Estimate empirical provider workload by age
#'
#' This is the claims-derived replacement candidate for the SHAPE of the
#' borrowed HWSM hours/FTE curve. It does not assert that workload equals hours;
#' it estimates the empirical age gradient in delivered all-payer workload and
#' normalizes that gradient to age 45-54.
#'
#' @param provider_year_tbl Projected all-payer provider-year table.
#' @param minimum_provider_years Minimum observations per age group.
#'
#' @return Age-specific workload factors.
#' @family allpayer bridge
#' @concept calibration
#' @export
estimate_workload_age_curve <- function(
    provider_year_tbl,
    minimum_provider_years = 20L) {

  base::message(
    "estimate_workload_age_curve(): estimating age gradient."
  )

  age_curve_tbl <- provider_year_tbl |>
    dplyr::filter(
      base::is.finite(.data$provider_age),
      base::is.finite(
        .data$estimated_allpayer_workload
      ),
      .data$estimated_allpayer_workload > 0
    ) |>
    dplyr::mutate(
      age_group = base::cut(
        .data$provider_age,
        breaks = c(
          0,
          44,
          54,
          64,
          74,
          Inf
        ),
        labels = c(
          "<45",
          "45-54",
          "55-64",
          "65-74",
          "75+"
        ),
        right = TRUE
      )
    ) |>
    dplyr::group_by(
      .data$age_group
    ) |>
    dplyr::summarise(
      n_provider_years = dplyr::n(),
      mean_workload = base::mean(
        .data$estimated_allpayer_workload
      ),
      sd_workload = stats::sd(
        .data$estimated_allpayer_workload
      ),
      p25_workload = stats::quantile(
        .data$estimated_allpayer_workload,
        probs = 0.25,
        names = FALSE
      ),
      median_workload = stats::median(
        .data$estimated_allpayer_workload
      ),
      p75_workload = stats::quantile(
        .data$estimated_allpayer_workload,
        probs = 0.75,
        names = FALSE
      ),
      .groups = "drop"
    ) |>
    dplyr::filter(
      .data$n_provider_years >=
        minimum_provider_years
    )

  reference_workload <- age_curve_tbl |>
    dplyr::filter(
      .data$age_group == "45-54"
    ) |>
    dplyr::pull(
      .data$median_workload
    )

  if (base::length(reference_workload) != 1L) {
    base::stop(
      paste(
        "Could not identify a usable 45-54 reference group.",
        "Do not normalize the age curve."
      ),
      call. = FALSE
    )
  }

  age_curve_tbl <- age_curve_tbl |>
    dplyr::mutate(
      relative_workload =
        .data$median_workload /
        reference_workload
    )

  base::message(
    "estimate_workload_age_curve(): complete."
  )

  age_curve_tbl
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
