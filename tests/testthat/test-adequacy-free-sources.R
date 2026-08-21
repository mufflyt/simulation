testthat::test_that("adequacy source registry covers requested sources", {
  registry_tbl <- adequacy_free_source_registry()
  testthat::expect_equal(
    registry_tbl$source_number,
    c(6L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 16L, 20L, 21L, 25L)
  )
  testthat::expect_true(base::all(
    registry_tbl$absence_semantics == "missing_not_zero"
  ))
  testthat::expect_false(base::anyDuplicated(registry_tbl$source_id) > 0L)
  testthat::expect_false(base::anyDuplicated(registry_tbl$table_name) > 0L)
})

testthat::test_that("missing source files fail closed", {
  testthat::skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  testthat::expect_error(
    ingest_adequacy_file(
      con,
      base::file.path(base::tempdir(), "not-present.csv"),
      "raw_missing"
    ),
    "does not exist"
  )
})

testthat::test_that("empty source files are not converted to zero", {
  testthat::skip_if_not_installed("duckdb")
  empty_path <- base::tempfile(fileext = ".csv")
  readr::write_csv(tibble::tibble(value = base::numeric()), empty_path)
  con <- DBI::dbConnect(duckdb::duckdb())
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  testthat::expect_error(
    ingest_adequacy_file(con, empty_path, "raw_empty"),
    "empty source"
  )
})

testthat::test_that("unsafe table names are refused", {
  testthat::skip_if_not_installed("duckdb")
  source_path <- base::tempfile(fileext = ".csv")
  readr::write_csv(tibble::tibble(value = 1), source_path)
  con <- DBI::dbConnect(duckdb::duckdb())
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  testthat::expect_error(
    ingest_adequacy_file(con, source_path, "raw_source; DROP TABLE x"),
    "Unsafe"
  )
})

testthat::test_that("DuckDB ingestion records row counts and reuses tables", {
  testthat::skip_if_not_installed("duckdb")
  source_path <- base::tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tibble(state = c("08", "25"), value = c(0.4, 0.6)),
    source_path
  )
  con <- DBI::dbConnect(duckdb::duckdb())
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  first_load <- ingest_adequacy_file(con, source_path, "raw_fixture")
  second_load <- ingest_adequacy_file(con, source_path, "raw_fixture")
  testthat::expect_equal(first_load$row_count, 2)
  testthat::expect_equal(first_load$status, "ingested")
  testthat::expect_equal(second_load$status, "reused")
})

testthat::test_that("loader preserves unavailable sources as missing", {
  testthat::skip_if_not_installed("duckdb")
  db_path <- base::tempfile(fileext = ".duckdb")
  source_path <- base::tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tibble(state = c("08", "25"), value = c(0.4, 0.6)),
    source_path
  )
  source_manifest <- tibble::tibble(
    source_id = "medicaid_fees",
    local_path = source_path
  )
  loading <- load_adequacy_sources_duckdb(
    db_path = db_path,
    source_manifest = source_manifest,
    download_catalog_sources = FALSE,
    strict = FALSE
  )
  fee_row <- loading$audit |>
    dplyr::filter(.data$source_id == "medicaid_fees")
  missing_rows <- loading$audit |>
    dplyr::filter(.data$status == "missing")
  testthat::expect_equal(fee_row$status, "ingested")
  testthat::expect_equal(fee_row$row_count, 2)
  testthat::expect_true(base::all(base::is.na(missing_rows$row_count)))
  testthat::expect_true(base::all(
    missing_rows$absence_semantics == "missing_not_zero"
  ))
})

testthat::test_that("strict loading rejects incomplete evidence", {
  testthat::skip_if_not_installed("duckdb")
  db_path <- base::tempfile(fileext = ".duckdb")
  testthat::expect_error(
    load_adequacy_sources_duckdb(
      db_path = db_path,
      download_catalog_sources = FALSE,
      strict = TRUE
    ),
    "sources are missing"
  )
})

testthat::test_that("geographic features aggregate in DuckDB", {
  testthat::skip_if_not_installed("duckdb")
  db_path <- base::tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  source_tbl <- tibble::tibble(
    state = c("08", "08", "25", "25"),
    fee_ratio = c(0.70, 0.90, 0.80, 1.00),
    enrollment = c(100, 300, 200, 200)
  )
  DBI::dbWriteTable(con, "raw_medicaid_fees", source_tbl)
  DBI::dbDisconnect(con, shutdown = TRUE)
  feature_spec <- tibble::tribble(
    ~table_name, ~geography_col, ~value_col, ~feature_name,
    ~aggregation, ~weight_col,
    "raw_medicaid_fees", "state", "fee_ratio",
    "medicaid_fee_ratio", "weighted_mean", "enrollment"
  )
  feature_tbl <- build_adequacy_geographic_features(
    db_path,
    feature_spec
  )
  colorado <- feature_tbl |>
    dplyr::filter(.data$geography == "08")
  testthat::expect_equal(colorado$medicaid_fee_ratio, 0.85)
})

testthat::test_that("augmentation reports evidence coverage", {
  testthat::skip_if_not_installed("duckdb")
  db_path <- base::tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  DBI::dbWriteTable(
    con,
    "adequacy_source_ingest_audit",
    tibble::tibble(source_id = "fixture", row_count = 2)
  )
  DBI::dbWriteTable(
    con,
    "adequacy_geographic_features",
    tibble::tibble(
      geography = c("08", "25"),
      medicaid_fee_ratio = c(0.85, NA_real_),
      delayed_care_pct = c(0.12, 0.20)
    )
  )
  DBI::dbDisconnect(con, shutdown = TRUE)
  calibration_tbl <- tibble::tibble(
    geography = c("08", "25"),
    appointments_offered = c(10L, 15L)
  )
  augmented_tbl <- augment_adequacy_from_duckdb(
    calibration_tbl,
    db_path
  )
  testthat::expect_equal(augmented_tbl$external_evidence_n, c(2, 1))
  testthat::expect_equal(
    augmented_tbl$external_evidence_complete,
    c(TRUE, FALSE)
  )
})

testthat::test_that("empirical adequacy model uses external evidence", {
  base::set.seed(20260821L)
  geography_n <- 40L
  wait_days <- stats::runif(geography_n, 5, 90)
  fee_ratio <- stats::runif(geography_n, 0.45, 1.10)
  appointment_probability <- stats::plogis(
    1.5 - 0.025 * wait_days + 1.2 * fee_ratio
  )
  calibration_tbl <- tibble::tibble(
    geography = base::sprintf("%02d", base::seq_len(geography_n)),
    appointments_offered = stats::rbinom(
      geography_n,
      size = 60L,
      prob = appointment_probability
    ),
    appointment_attempts = 60L,
    female_population = stats::runif(geography_n, 100000, 1000000),
    wait_days = wait_days,
    medicaid_fee_ratio = fee_ratio
  )
  access_fit <- fit_empirical_adequacy_glm(
    calibration_tbl,
    predictor_names = c("wait_days", "medicaid_fee_ratio"),
    bootstrap_reps = 100L
  )
  testthat::expect_equal(
    access_fit$method,
    "binomial_glm_with_geographic_bootstrap"
  )
  testthat::expect_true(base::all(
    access_fit$geographic_summary$adequacy_mean > 0 &
      access_fit$geographic_summary$adequacy_mean < 1
  ))
  testthat::expect_true(base::all(c(
    "adequacy_mean", "adequacy_sd", "adequacy_median",
    "adequacy_p25", "adequacy_p75"
  ) %in% base::names(access_fit$national_summary)))
  testthat::expect_true(base::all(
    access_fit$evidence_coverage$used
  ))
})

testthat::test_that("empirical model refuses absent external evidence", {
  calibration_tbl <- tibble::tibble(
    geography = base::sprintf("%02d", 1:10),
    appointments_offered = base::rep(5L, 10L),
    appointment_attempts = base::rep(10L, 10L),
    female_population = base::rep(100000, 10L),
    wait_days = base::rep(NA_real_, 10L)
  )
  testthat::expect_error(
    fit_empirical_adequacy_glm(
      calibration_tbl,
      predictor_names = "wait_days",
      bootstrap_reps = 20L
    ),
    "No external predictor"
  )
})
