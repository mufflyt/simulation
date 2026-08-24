# CHIA Hospital Realized Surgical Volume & Concentration Mapping ----
#
# Calibration tier: regional_external_validation (Massachusetts FY2004-FY2018)
#
# SEMANTIC CLARIFICATION & SCIENTIFIC HARDENING:
#   1. CHIA measures REALIZED INPATIENT SURGICAL VOLUME, not latent staffing/OR capacity.
#   2. Mode defaults to "observed" and FAILS CLOSED if con is NULL/invalid.
#   3. Synthetic fixtures are generated ONLY via explicit mode="synthetic_fixture" or fixture_chia_hospital_volume().
#   4. Synthetic artifacts are restricted from production directories.

#' Generate Synthetic CHIA Hospital Volume Fixture
#'
#' Explicit helper for testing and offline development. Labeled source_kind = "synthetic".
#'
#' @param min_year Starting fiscal year (default 2004).
#' @param max_year Ending fiscal year (default 2018).
#' @param seed Random seed for reproducibility.
#' @return List containing synthetic `facility_volumes` and `market_summary` with provenance.
#' @family chia inpatient geography
#' @concept geography
#' @export
fixture_chia_hospital_volume <- function(min_year = 2004L, max_year = 2018L, seed = 2026L) {
  set.seed(seed)
  years <- seq.int(min_year, max_year)
  fac_ids <- sprintf("FAC_%03d", 1:45)
  fac_grid <- expand.grid(year = years, facility_id = fac_ids, stringsAsFactors = FALSE) |>
    tibble::as_tibble()

  fac_weights <- stats::setNames(stats::rexp(45, rate = 0.05) + 5, fac_ids)

  raw_fac <- fac_grid |>
    dplyr::mutate(
      base_vol = fac_weights[facility_id],
      time_trend = 1.0 - (year - 2004) * 0.025,
      inpatient_cases = pmax(0L, round(stats::rnorm(dplyr::n(), mean = base_vol * time_trend, sd = base_vol * 0.10))),
      unique_origins = pmax(1L, round(inpatient_cases * stats::runif(dplyr::n(), 0.35, 0.75)))
    )

  fac_analysis <- raw_fac |>
    dplyr::group_by(year) |>
    dplyr::mutate(
      total_state_cases = sum(inpatient_cases, na.rm = TRUE),
      market_share = inpatient_cases / pmax(1, total_state_cases)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      volume_category = dplyr::case_when(
        inpatient_cases < 10 ~ "Low Volume (<10 cases)",
        inpatient_cases <= 50 ~ "Medium Volume (10-50 cases)",
        TRUE                  ~ "High Volume (>50 cases)"
      )
    )

  calc_gini <- function(x) {
    x <- sort(x[is.finite(x) & x >= 0])
    n <- length(x)
    if (n == 0 || sum(x) == 0) return(0)
    2 * sum(seq_len(n) * x) / (n * sum(x)) - (n + 1) / n
  }

  market_summary <- fac_analysis |>
    dplyr::group_by(year) |>
    dplyr::summarize(
      total_state_cases = sum(inpatient_cases, na.rm = TRUE),
      n_active_facilities = dplyr::n_distinct(facility_id[inpatient_cases > 0]),
      gini_concentration = calc_gini(inpatient_cases),
      pct_low_volume_facs = mean(inpatient_cases < 10) * 100,
      pct_high_volume_facs = mean(inpatient_cases > 50) * 100,
      .groups = "drop"
    )

  structure(
    list(facility_volumes = fac_analysis, market_summary = market_summary),
    source_kind = "synthetic",
    source_dataset = "CHIA_HOSPITAL_SURGICAL_VOLUME_SYNTHETIC_FIXTURE",
    source_years = paste0(min_year, "-", max_year),
    calibration_status = "synthetic_fixture",
    input_hash = digest::digest(fac_analysis),
    query_hash = "SYNTHETIC_FIXTURE_NO_SQL",
    config_hash = digest::digest(list(min_year, max_year, seed)),
    created_at = base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

#' Build CHIA hospital realized surgical volume map and concentration metrics
#'
#' Evaluates annual hospital inpatient URPS case volumes, market shares,
#' Gini volume concentration coefficients, and catchment migration across Massachusetts
#' facilities from CHIA discharge data.
#'
#' @param con Database connection to DuckDB CHIA casemix. Required in mode = "observed".
#' @param min_year Starting fiscal year (default 2004).
#' @param max_year Ending fiscal year (default 2018).
#' @param mode Operation mode: "observed" (default, fails if con missing) or "synthetic_fixture".
#' @param save_dir Directory for timestamped capacity map artifacts.
#' @param allow_synthetic_artifact Permit writing synthetic artifacts to save_dir (default FALSE).
#'
#' @return A list containing `facility_volumes` (tibble of per-facility annual metrics),
#'   `market_summary` (state-level Gini and volume category distribution), and artifact paths.
#'
#' @family chia inpatient geography
#' @concept geography
#' @export
build_chia_hospital_surgical_volume_map <- function(
    con = NULL,
    min_year = 2004L,
    max_year = 2018L,
    mode = c("observed", "synthetic_fixture"),
    save_dir = "artifacts/chia_capacity",
    allow_synthetic_artifact = FALSE) {

  mode <- match.arg(mode)

  if (identical(mode, "observed")) {
    if (base::is.null(con) || !DBI::dbIsValid(con)) {
      stop("build_chia_hospital_surgical_volume_map(): mode='observed' requires a valid database connection (con). ",
           "Passing con=NULL in production mode is prohibited. ",
           "Use mode='synthetic_fixture' or fixture_chia_hospital_volume() explicitly for offline tests.", call. = FALSE)
    }

    base::message("Querying DuckDB CHIA facility inpatient discharge volumes...")
    sql_query <- "
      SELECT
        _data_year AS year,
        id_org_site AS facility_id,
        COUNT(*) AS inpatient_cases,
        COUNT(DISTINCT zip5) AS unique_origins
      FROM chia_casemix.v_hdd_discharge_canonical
      WHERE _data_year BETWEEN ? AND ?
        AND procedure_family IS NOT NULL
        AND id_org_site IS NOT NULL
      GROUP BY 1, 2
      ORDER BY 1, 2
    "
    raw_fac <- DBI::dbGetQuery(con, sql_query, params = list(min_year, max_year)) |>
      tibble::as_tibble()

    fac_analysis <- raw_fac |>
      dplyr::group_by(year) |>
      dplyr::mutate(
        total_state_cases = sum(inpatient_cases, na.rm = TRUE),
        market_share = inpatient_cases / pmax(1, total_state_cases)
      ) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        volume_category = dplyr::case_when(
          inpatient_cases < 10 ~ "Low Volume (<10 cases)",
          inpatient_cases <= 50 ~ "Medium Volume (10-50 cases)",
          TRUE                  ~ "High Volume (>50 cases)"
        )
      )

    calc_gini <- function(x) {
      x <- sort(x[is.finite(x) & x >= 0])
      n <- length(x)
      if (n == 0 || sum(x) == 0) return(0)
      2 * sum(seq_len(n) * x) / (n * sum(x)) - (n + 1) / n
    }

    market_summary <- fac_analysis |>
      dplyr::group_by(year) |>
      dplyr::summarize(
        total_state_cases = sum(inpatient_cases, na.rm = TRUE),
        n_active_facilities = dplyr::n_distinct(facility_id[inpatient_cases > 0]),
        gini_concentration = calc_gini(inpatient_cases),
        pct_low_volume_facs = mean(inpatient_cases < 10) * 100,
        pct_high_volume_facs = mean(inpatient_cases > 50) * 100,
        .groups = "drop"
      )

    res <- structure(
      list(facility_volumes = fac_analysis, market_summary = market_summary),
      source_kind = "observed",
      source_dataset = "CHIA_HOSPITAL_SURGICAL_VOLUME_OBSERVED",
      source_years = paste0(min_year, "-", max_year),
      calibration_status = "observed_regional",
      input_hash = digest::digest(fac_analysis),
      query_hash = digest::digest(sql_query),
      config_hash = digest::digest(list(min_year, max_year, mode)),
      created_at = base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S")
    )
  } else {
    res <- fixture_chia_hospital_volume(min_year = min_year, max_year = max_year)
  }

  source_kind <- attr(res, "source_kind") %||% "unknown"

  if (identical(source_kind, "synthetic") && !isTRUE(allow_synthetic_artifact)) {
    save_dir <- tempdir()
    fac_path    <- base::file.path(save_dir, paste0("synthetic_chia_hospital_facility_volumes_", base::format(base::Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))
    market_path <- base::file.path(save_dir, paste0("synthetic_chia_hospital_market_summary_", base::format(base::Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))
    base::message("Synthetic hospital volume generated: writing artifacts to tempdir()")
  } else {
    base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
    fac_path    <- base::file.path(save_dir, paste0("chia_hospital_facility_volumes_", base::format(base::Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))
    market_path <- base::file.path(save_dir, paste0("chia_hospital_market_summary_", base::format(base::Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))
  }

  readr::write_csv(res$facility_volumes, fac_path)
  readr::write_csv(res$market_summary, market_path)

  res$paths <- list(facility_volumes = fac_path, market_summary = market_path)
  res
}

#' Legacy Alias for [build_chia_hospital_surgical_volume_map()]
#'
#' @param ... Passed unchanged to [build_chia_hospital_surgical_volume_map()];
#'   see that function for the arguments and their meanings.
#' @return Whatever [build_chia_hospital_surgical_volume_map()] returns.
#' @export
build_chia_hospital_capacity_map <- function(...) {
  build_chia_hospital_surgical_volume_map(...)
}
