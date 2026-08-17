# All-Payer Inpatient Surgical Utilization Estimand D6 (CHIA Dataset) ----
#
# Calibration tier: observed_regional (Massachusetts, FY2004-FY2018)
# Estimand D6: All-payer inpatient/overnight pelvic reconstructive surgical utilization.
#
# PURPOSE:
#   Provides age-specific, procedure-family-specific annual counts and rates
#   per 100,000 Massachusetts adult females for inpatient pelvic reconstructive
#   surgeries (POP-indication hysterectomy, apical suspension, sacrocolpopexy,
#   colpocleisis, A/P repair, inpatient slings, complex URPS).

#' Build all-payer inpatient surgical utilization series (Estimand D6)
#'
#' Extracts annual counts and population-adjusted rates for inpatient URPS procedure
#' families across FY2004-FY2018 from CHIA inpatient discharge data.
#'
#' @param con Database connection to DuckDB CHIA casemix, or NULL to load/generate mock fixture.
#' @param min_year Starting fiscal year (default 2004).
#' @param max_year Ending fiscal year (default 2018).
#' @param save_dir Directory for timestamped artifact output.
#'
#' @return A tibble with columns: `year`, `age_band`, `procedure_family`,
#'   `inpatient_cases`, `female_population`, `rate_per_100k`.
#'
#' @family chia inpatient surgery
#' @concept demand
#' @export
build_chia_inpatient_urps_series <- function(
    con = NULL,
    min_year = 2004L,
    max_year = 2018L,
    save_dir = "artifacts/chia_inpatient") {

  base::message("build_chia_inpatient_urps_series(): starting.")

  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  procedure_families <- c(
    "pop_hysterectomy",
    "apical_suspension",
    "sacrocolpopexy",
    "colpocleisis",
    "ap_repair",
    "inpatient_sling",
    "complex_urps"
  )

  age_bands <- c("18-49", "50-64", "65-74", "75+")
  years <- base::seq.int(min_year, max_year)

  if (!base::is.null(con) && DBI::dbIsValid(con)) {
    base::message("Querying DuckDB CHIA inpatient discharges...")
    # Executing against live DuckDB connection
    sql_query <- "
      SELECT
        _data_year AS year,
        CASE
          WHEN age < 50 THEN '18-49'
          WHEN age < 65 THEN '50-64'
          WHEN age < 75 THEN '65-74'
          ELSE '75+'
        END AS age_band,
        procedure_family,
        COUNT(*) AS inpatient_cases
      FROM chia_casemix.v_hdd_discharge_canonical
      WHERE _data_year BETWEEN ? AND ?
        AND procedure_family IS NOT NULL
      GROUP BY 1, 2, 3
      ORDER BY 1, 2, 3
    "
    raw_counts <- DBI::dbGetQuery(con, sql_query, params = list(min_year, max_year)) |>
      tibble::as_tibble()
  } else {
    base::message("DuckDB connection not supplied or invalid. Generating synthetic MA CHIA D6 series...")
    set.seed(2026)

    grid <- expand.grid(
      year = years,
      age_band = age_bands,
      procedure_family = procedure_families,
      stringsAsFactors = FALSE
    ) |> tibble::as_tibble()

    raw_counts <- grid |>
      dplyr::mutate(
        base_rate = dplyr::case_when(
          procedure_family == "pop_hysterectomy" ~ 45.0,
          procedure_family == "apical_suspension" ~ 28.0,
          procedure_family == "sacrocolpopexy"    ~ 22.0,
          procedure_family == "colpocleisis"     ~ 12.0,
          procedure_family == "ap_repair"         ~ 35.0,
          procedure_family == "inpatient_sling"   ~ 15.0,
          TRUE                                    ~ 8.0
        ),
        age_multiplier = dplyr::case_when(
          age_band == "18-49" ~ 0.35,
          age_band == "50-64" ~ 1.00,
          age_band == "65-74" ~ 1.85,
          TRUE                ~ 2.10
        ),
        # Model outpatient shift trend over time (inpatient cases decline)
        time_trend = 1.0 - (year - 2004) * 0.035,
        female_population = dplyr::case_when(
          age_band == "18-49" ~ 1450000,
          age_band == "50-64" ~ 720000,
          age_band == "65-74" ~ 380000,
          TRUE                ~ 290000
        ),
        expected_cases = (base_rate * age_multiplier * time_trend * female_population) / 100000,
        inpatient_cases = pmax(0L, round(rnorm(n(), mean = expected_cases, sd = expected_cases * 0.08)))
      ) |>
      dplyr::select(year, age_band, procedure_family, inpatient_cases, female_population)
  }

  res <- raw_counts |>
    dplyr::mutate(
      female_population = dplyr::coalesce(.data$female_population, 500000),
      rate_per_100k = (inpatient_cases / female_population) * 100000
    ) |>
    dplyr::arrange(year, age_band, procedure_family)

  saved_path <- base::file.path(save_dir, paste0("chia_d6_inpatient_series_", timestamp, ".csv"))
  readr::write_csv(res, saved_path)
  base::message("Saved D6 inpatient series artifact: ", saved_path)

  base::attr(res, "saved_path") <- saved_path
  res
}
