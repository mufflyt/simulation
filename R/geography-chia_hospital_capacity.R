# CHIA Hospital Surgical-Capacity & Volume Mapping ----
#
# Calibration tier: regional_external_validation (Massachusetts FY2004-FY2018)
#
# PURPOSE:
#   Constructs a facility-level hospital surgical-capacity map from CHIA inpatient
#   discharges. Evaluates annual hospital volume, market concentration (Gini),
#   low-volume vs. high-volume facilities, and geographic migration of inpatient
#   pelvic floor surgery.

#' Build CHIA hospital surgical capacity map and market concentration metrics
#'
#' Evaluates annual hospital inpatient URPS case volumes, market shares,
#' Gini concentration coefficients, and catchment migration across Massachusetts
#' facilities from CHIA discharge data.
#'
#' @param con Database connection to DuckDB CHIA casemix, or NULL for synthetic fixture.
#' @param min_year Starting fiscal year (default 2004).
#' @param max_year Ending fiscal year (default 2018).
#' @param save_dir Directory for timestamped capacity map artifacts.
#'
#' @return A list containing `facility_volumes` (tibble of per-facility annual metrics),
#'   `market_summary` (state-level Gini and volume category distribution), and artifact paths.
#'
#' @family chia inpatient geography
#' @concept geography
#' @export
build_chia_hospital_capacity_map <- function(
    con = NULL,
    min_year = 2004L,
    max_year = 2018L,
    save_dir = "artifacts/chia_capacity") {

  base::message("build_chia_hospital_capacity_map(): starting.")

  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  years <- base::seq.int(min_year, max_year)

  if (!base::is.null(con) && DBI::dbIsValid(con)) {
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
  } else {
    base::message("DuckDB connection not supplied. Generating synthetic MA hospital capacity map...")
    set.seed(2026)

    # 45 Massachusetts hospital sites
    fac_ids <- sprintf("FAC_%03d", 1:45)
    fac_grid <- expand.grid(year = years, facility_id = fac_ids, stringsAsFactors = FALSE) |>
      tibble::as_tibble()

    # Pareto-distributed volume (a few high-volume tertiary centers, many low-volume community hospitals)
    fac_weights <- stats::setNames(rexp(45, rate = 0.05) + 5, fac_ids)

    raw_fac <- fac_grid |>
      dplyr::mutate(
        base_vol = fac_weights[facility_id],
        # Trend: inpatient volumes shifting slightly downward over 2004-2018
        time_trend = 1.0 - (year - 2004) * 0.025,
        inpatient_cases = pmax(0L, round(rnorm(n(), mean = base_vol * time_trend, sd = base_vol * 0.10))),
        unique_origins = pmax(1L, round(inpatient_cases * runif(n(), 0.35, 0.75)))
      )
  }

  # Calculate annual state volume, market share, and volume categories
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

  # Compute state-level Gini coefficient of market volume concentration per year
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

  base::message("Hospital Capacity Analysis Completed:")
  base::message("  Active Facilities Evaluated: ", dplyr::n_distinct(fac_analysis$facility_id))
  base::message("  Mean Gini Concentration: ", sprintf("%.3f", mean(market_summary$gini_concentration)))

  fac_path    <- base::file.path(save_dir, paste0("chia_hospital_facility_volumes_", timestamp, ".csv"))
  market_path <- base::file.path(save_dir, paste0("chia_hospital_market_summary_", timestamp, ".csv"))

  readr::write_csv(fac_analysis, fac_path)
  readr::write_csv(market_summary, market_path)

  list(
    facility_volumes = fac_analysis,
    market_summary = market_summary,
    paths = list(facility_volumes = fac_path, market_summary = market_path)
  )
}
