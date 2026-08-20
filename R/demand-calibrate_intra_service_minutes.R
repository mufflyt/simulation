# Calibrate CPT-Specific Intra-Service Time Distributions ----
#
# Scientific Hardening Layer: CMS Physician Fee Schedule (PFS) times serve as the anchor.
# Published CPT-specific prospective literature informs variability and updates the mean center
# via evidence-quality and precision weighting when at least 2 eligible studies exist.

#' Calibrate CPT-specific physician intra-service time distributions
#'
#' CMS Physician Fee Schedule times remain the primary anchor. Published
#' procedure-specific literature informs variability and may partially update
#' the center when a study reports an isolated procedure matching one CPT.
#'
#' @param pfs_times A tibble with `cpt` and `intra_service_minutes`.
#' @param literature_times A tibble with study-level operative-time summaries.
#' @param literature_center_weight Maximum weight given to published studies
#'   when updating the CMS center. Must be between zero and one.
#' @param default_cv Coefficient of variation used when literature is absent.
#' @param min_studies Minimum number of eligible studies required to update the
#'   center of the CMS distribution.
#'
#' @return A tibble containing one calibrated distribution per CPT code.
#' @family workload calibration
#' @concept demand
#' @export
calibrate_intra_service_minutes <- function(
    pfs_times,
    literature_times,
    literature_center_weight = 0.35,
    default_cv = 0.25,
    min_studies = 2L) {
  base::message("Starting intra-service time calibration.")
  base::message(
    "Inputs: ",
    scales::comma(base::nrow(pfs_times)),
    " PFS rows and ",
    scales::comma(base::nrow(literature_times)),
    " literature rows."
  )

  required_pfs <- base::c("cpt", "intra_service_minutes")
  required_literature <- base::c(
    "study_id",
    "cpt",
    "n",
    "mean_minutes",
    "sd_minutes",
    "median_minutes",
    "p25_minutes",
    "p75_minutes",
    "isolated_procedure",
    "evidence_weight"
  )

  assert_required_columns(
    table_value = pfs_times,
    required_names = required_pfs,
    table_name = "pfs_times"
  )
  assert_required_columns(
    table_value = literature_times,
    required_names = required_literature,
    table_name = "literature_times"
  )

  if (!base::is.numeric(literature_center_weight) ||
      base::length(literature_center_weight) != 1L ||
      base::is.na(literature_center_weight) ||
      literature_center_weight < 0 ||
      literature_center_weight > 1) {
    base::stop("literature_center_weight must be between zero and one.")
  }

  if (!base::is.numeric(default_cv) ||
      base::length(default_cv) != 1L ||
      base::is.na(default_cv) ||
      default_cv <= 0) {
    base::stop("default_cv must be a positive number.")
  }

  base::message("Validating and deduplicating CMS PFS physician times.")
  pfs_clean <- pfs_times |>
    dplyr::transmute(
      cpt = base::as.character(cpt),
      cms_intra_service_minutes = base::as.numeric(
        intra_service_minutes
      )
    ) |>
    dplyr::filter(
      !base::is.na(cpt),
      cpt != "",
      !base::is.na(cms_intra_service_minutes),
      cms_intra_service_minutes > 0
    ) |>
    dplyr::group_by(cpt) |>
    dplyr::summarise(
      pfs_distinct_times = dplyr::n_distinct(
        cms_intra_service_minutes
      ),
      cms_intra_service_minutes = dplyr::first(
        cms_intra_service_minutes
      ),
      .groups = "drop"
    )

  conflicting_cpt <- pfs_clean |>
    dplyr::filter(pfs_distinct_times > 1L) |>
    base::nrow()

  if (conflicting_cpt > 0L) {
    base::stop(
      "PFS input contains conflicting intra-service times for ",
      conflicting_cpt,
      " CPT codes."
    )
  }

  base::message("Converting literature summaries to log-normal moments.")
  literature_clean <- literature_times |>
    dplyr::transmute(
      study_id = base::as.character(study_id),
      cpt = base::as.character(cpt),
      n = base::as.numeric(n),
      mean_minutes = base::as.numeric(mean_minutes),
      sd_minutes = base::as.numeric(sd_minutes),
      median_minutes = base::as.numeric(median_minutes),
      p25_minutes = base::as.numeric(p25_minutes),
      p75_minutes = base::as.numeric(p75_minutes),
      isolated_procedure = base::as.logical(isolated_procedure),
      evidence_weight = base::as.numeric(evidence_weight)
    ) |>
    dplyr::mutate(
      mean_from_quantiles = base::exp(
        base::log(median_minutes) +
          0.5 * literature_log_sd(
            p25_minutes = p25_minutes,
            p75_minutes = p75_minutes
          )^2
      ),
      sd_from_quantiles = mean_from_quantiles * base::sqrt(
        base::exp(
          literature_log_sd(
            p25_minutes = p25_minutes,
            p75_minutes = p75_minutes
          )^2
        ) - 1
      ),
      study_mean = dplyr::coalesce(
        mean_minutes,
        mean_from_quantiles
      ),
      study_sd = dplyr::coalesce(
        sd_minutes,
        sd_from_quantiles
      )
    ) |>
    dplyr::filter(
      !base::is.na(study_id),
      study_id != "",
      !base::is.na(cpt),
      cpt != "",
      !base::is.na(n),
      n >= 2,
      !base::is.na(study_mean),
      study_mean > 0,
      !base::is.na(study_sd),
      study_sd > 0,
      isolated_procedure,
      !base::is.na(evidence_weight),
      evidence_weight > 0,
      evidence_weight <= 1
    ) |>
    dplyr::inner_join(pfs_clean, by = "cpt") |>
    dplyr::mutate(
      log_ratio = base::log(
        study_mean / cms_intra_service_minutes
      ),
      log_se = study_sd /
        (base::sqrt(n) * study_mean),
      precision_weight = evidence_weight /
        base::pmax(log_se^2, 1e-6),
      study_log_sd = base::sqrt(
        base::log1p((study_sd / study_mean)^2)
      )
    )

  excluded_rows <- base::nrow(literature_times) -
    base::nrow(literature_clean)
  base::message(
    "Eligible literature rows: ",
    scales::comma(base::nrow(literature_clean)),
    "; excluded or unmatched rows: ",
    scales::comma(excluded_rows),
    "."
  )

  base::message("Pooling eligible studies within CPT codes.")
  literature_pool <- literature_clean |>
    dplyr::group_by(cpt) |>
    dplyr::summarise(
      literature_studies = dplyr::n_distinct(study_id),
      literature_patients = base::sum(n, na.rm = TRUE),
      pooled_log_ratio = stats::weighted.mean(
        log_ratio,
        w = precision_weight,
        na.rm = TRUE
      ),
      pooled_log_sd = stats::weighted.mean(
        study_log_sd,
        w = n * evidence_weight,
        na.rm = TRUE
      ),
      mean_evidence_weight = stats::weighted.mean(
        evidence_weight,
        w = n,
        na.rm = TRUE
      ),
      .groups = "drop"
    )

  default_log_sd <- base::sqrt(base::log1p(default_cv^2))

  base::message("Anchoring calibrated distributions to CMS PFS times.")
  calibrated_times <- pfs_clean |>
    dplyr::left_join(literature_pool, by = "cpt") |>
    dplyr::mutate(
      literature_studies = dplyr::coalesce(
        literature_studies,
        0L
      ),
      literature_patients = dplyr::coalesce(
        literature_patients,
        0
      ),
      center_update_allowed = literature_studies >= min_studies,
      applied_literature_weight = dplyr::if_else(
        center_update_allowed,
        literature_center_weight * mean_evidence_weight,
        0
      ),
      applied_literature_weight = dplyr::coalesce(
        applied_literature_weight,
        0
      ),
      calibrated_log_mean = base::log(
        cms_intra_service_minutes
      ) + applied_literature_weight * dplyr::coalesce(
        pooled_log_ratio,
        0
      ),
      calibrated_log_sd = dplyr::coalesce(
        pooled_log_sd,
        default_log_sd
      ),
      calibrated_mean_minutes = base::exp(
        calibrated_log_mean + 0.5 * calibrated_log_sd^2
      ),
      calibrated_sd_minutes = base::sqrt(
        (base::exp(calibrated_log_sd^2) - 1) *
          base::exp(
            2 * calibrated_log_mean + calibrated_log_sd^2
          )
      ),
      calibrated_p25_minutes = stats::qlnorm(
        0.25,
        meanlog = calibrated_log_mean,
        sdlog = calibrated_log_sd
      ),
      calibrated_median_minutes = stats::qlnorm(
        0.50,
        meanlog = calibrated_log_mean,
        sdlog = calibrated_log_sd
      ),
      calibrated_p75_minutes = stats::qlnorm(
        0.75,
        meanlog = calibrated_log_mean,
        sdlog = calibrated_log_sd
      ),
      recommended_time_source = dplyr::case_when(
        center_update_allowed ~ "CMS anchor + literature calibration",
        literature_studies > 0L ~ "CMS anchor + literature variability",
        TRUE ~ "CMS anchor + default variability"
      )
    ) |>
    dplyr::select(-pfs_distinct_times) |>
    dplyr::arrange(cpt)

  base::message(
    "Calibration complete for ",
    scales::comma(base::nrow(calibrated_times)),
    " CPT codes."
  )
  calibrated_times
}


#' Simulate annual physician intra-service workload
#'
#' @param cpt_workload A tibble with `cpt`, `year`, and `case_count`.
#' @param calibrated_times The value returned by
#'   `calibrate_intra_service_minutes()`.
#' @param simulations Number of Monte Carlo iterations.
#' @param seed Reproducible random-number seed.
#'
#' @return A tibble with simulated annual workload by CPT and year.
#' @family workload calibration
#' @concept demand
#' @export
deconstruct_intra_service_workload <- function(
    cpt_workload,
    calibrated_times,
    simulations = 10000L,
    seed = 20260820L) {
  base::message("Starting CPT-level workload deconstruction.")
  base::message(
    "Inputs: ",
    scales::comma(base::nrow(cpt_workload)),
    " workload rows and ",
    scales::comma(simulations),
    " simulations."
  )

  assert_required_columns(
    table_value = cpt_workload,
    required_names = base::c("cpt", "year", "case_count"),
    table_name = "cpt_workload"
  )
  assert_required_columns(
    table_value = calibrated_times,
    required_names = base::c(
      "cpt",
      "calibrated_log_mean",
      "calibrated_log_sd"
    ),
    table_name = "calibrated_times"
  )

  if (simulations < 1L || simulations != base::as.integer(simulations)) {
    base::stop("simulations must be a positive integer.")
  }

  base::message("Joining annual CPT counts to calibrated time distributions.")
  workload_clean <- cpt_workload |>
    dplyr::transmute(
      cpt = base::as.character(cpt),
      year = base::as.integer(year),
      case_count = base::as.numeric(case_count)
    ) |>
    dplyr::filter(
      !base::is.na(cpt),
      !base::is.na(year),
      !base::is.na(case_count),
      case_count >= 0
    ) |>
    dplyr::left_join(calibrated_times, by = "cpt")

  missing_time_cpt <- workload_clean |>
    dplyr::filter(base::is.na(calibrated_log_mean)) |>
    dplyr::distinct(cpt) |>
    dplyr::pull(cpt)

  if (base::length(missing_time_cpt) > 0L) {
    base::stop(
      "No calibrated time exists for CPT code(s): ",
      base::paste(missing_time_cpt, collapse = ", "),
      "."
    )
  }

  base::message("Drawing CPT-specific Monte Carlo workload values.")
  base::set.seed(seed)

  simulation_grid <- workload_clean |>
    tidyr::crossing(simulation = base::seq_len(simulations)) |>
    dplyr::mutate(
      expected_case_minutes = base::exp(
        calibrated_log_mean + 0.5 * calibrated_log_sd^2
      ),
      variance_case_minutes = (
        base::exp(calibrated_log_sd^2) - 1
      ) * base::exp(
        2 * calibrated_log_mean + calibrated_log_sd^2
      ),
      expected_total_minutes = case_count * expected_case_minutes,
      variance_total_minutes = case_count * variance_case_minutes,
      gamma_shape = expected_total_minutes^2 /
        base::pmax(variance_total_minutes, 1e-9),
      gamma_scale = variance_total_minutes /
        base::pmax(expected_total_minutes, 1e-9),
      total_minutes_draw = stats::rgamma(
        dplyr::n(),
        shape = base::pmax(gamma_shape, 1e-9),
        scale = base::pmax(gamma_scale, 1e-9)
      ),
      total_minutes = dplyr::if_else(
        case_count == 0,
        0,
        total_minutes_draw
      ),
      total_hours = total_minutes / 60,
      clinical_fte = total_hours / 2080
    )

  base::message("Summarizing simulated workload distributions.")
  workload_summary <- simulation_grid |>
    dplyr::group_by(year, cpt) |>
    dplyr::summarise(
      case_count = dplyr::first(case_count),
      mean_hours = base::mean(total_hours),
      sd_hours = stats::sd(total_hours),
      p25_hours = stats::quantile(total_hours, 0.25),
      median_hours = stats::median(total_hours),
      p75_hours = stats::quantile(total_hours, 0.75),
      mean_clinical_fte = base::mean(clinical_fte),
      sd_clinical_fte = stats::sd(clinical_fte),
      .groups = "drop"
    ) |>
    dplyr::arrange(year, cpt)

  base::message(
    "Workload deconstruction complete: ",
    scales::comma(base::nrow(workload_summary)),
    " CPT-year estimates."
  )
  workload_summary
}


#' Calculate log-normal dispersion from an interquartile range
#'
#' @param p25_minutes The 25th percentile in minutes.
#' @param p75_minutes The 75th percentile in minutes.
#'
#' @return A numeric vector of log-scale standard deviations.
#' @family workload calibration
#' @concept demand
#' @export
literature_log_sd <- function(p25_minutes, p75_minutes) {
  valid_quantiles <- !base::is.na(p25_minutes) &
    !base::is.na(p75_minutes) &
    p25_minutes > 0 &
    p75_minutes > p25_minutes

  log_sd <- base::rep(NA_real_, base::length(p25_minutes))
  log_sd[valid_quantiles] <- (
    base::log(p75_minutes[valid_quantiles]) -
      base::log(p25_minutes[valid_quantiles])
  ) / (2 * stats::qnorm(0.75))
  log_sd
}


#' Confirm that a table contains required columns
#'
#' @param table_value A data frame or tibble.
#' @param required_names Required column names.
#' @param table_name Name used in error messages.
#'
#' @return The input invisibly.
#' @family workload calibration
#' @concept demand
#' @export
assert_required_columns <- function(
    table_value,
    required_names,
    table_name) {
  if (!base::is.data.frame(table_value)) {
    base::stop(table_name, " must be a data frame or tibble.")
  }

  missing_names <- base::setdiff(required_names, base::names(table_value))
  if (base::length(missing_names) > 0L) {
    base::stop(
      table_name,
      " is missing required column(s): ",
      base::paste(missing_names, collapse = ", "),
      "."
    )
  }
  base::invisible(table_value)
}


#' Example input schemas for the calibration workflow
#'
#' @return A named list containing example tibbles.
#' @family workload calibration
#' @concept demand
#' @export
example_intra_service_inputs <- function() {
  base::message("Creating example CMS, literature, and workload inputs.")

  pfs_example <- tibble::tibble(
    cpt = base::c("57288", "57425"),
    intra_service_minutes = base::c(45, 135)
  )

  literature_example <- tibble::tibble(
    study_id = base::c("Study A", "Study B", "Study C"),
    cpt = base::c("57288", "57288", "57425"),
    n = base::c(120, 85, 150),
    mean_minutes = base::c(48, NA, 165),
    sd_minutes = base::c(15, NA, 42),
    median_minutes = base::c(NA, 46, NA),
    p25_minutes = base::c(NA, 37, NA),
    p75_minutes = base::c(NA, 58, NA),
    isolated_procedure = base::c(TRUE, TRUE, TRUE),
    evidence_weight = base::c(0.90, 0.80, 0.85)
  )

  workload_example <- tibble::tibble(
    cpt = base::c("57288", "57425"),
    year = base::c(2025L, 2025L),
    case_count = base::c(1250, 800)
  )

  base::message("Example inputs created.")
  base::list(
    pfs_times = pfs_example,
    literature_times = literature_example,
    cpt_workload = workload_example
  )
}
