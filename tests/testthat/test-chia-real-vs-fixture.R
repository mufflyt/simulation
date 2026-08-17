# tests/testthat/test-chia-real-vs-fixture.R
# Scientific Hardening Gate: CHIA Fail-Closed & Estimand Boundary Contracts

test_that("build_chia_inpatient_urps_series errors on con=NULL in observed mode", {
  expect_error(
    build_chia_inpatient_urps_series(con = NULL, mode = "observed"),
    "mode='observed' requires a valid database connection"
  )
})

test_that("build_chia_hospital_surgical_volume_map errors on con=NULL in observed mode", {
  expect_error(
    build_chia_hospital_surgical_volume_map(con = NULL, mode = "observed"),
    "mode='observed' requires a valid database connection"
  )
})

test_that("fixture_chia_d6 explicitly labels source_kind = 'synthetic'", {
  fix <- fixture_chia_d6(min_year = 2010L, max_year = 2012L)
  expect_equal(attr(fix, "source_kind"), "synthetic")
  expect_equal(attr(fix, "calibration_status"), "synthetic_fixture")
  expect_true(all(c("year", "age_band", "procedure_family", "inpatient_cases", "female_population", "rate_per_100k") %in% names(fix)))
})

test_that("synthetic artifacts cannot write to production save_dir without allow_synthetic_artifact", {
  temp_save <- file.path(tempdir(), "test_chia_prod_block")
  res <- build_chia_inpatient_urps_series(
    con = NULL,
    mode = "synthetic_fixture",
    save_dir = temp_save,
    allow_synthetic_artifact = FALSE
  )
  saved_path <- attr(res, "saved_path")
  expect_true(grepl("synthetic", basename(saved_path)))
  expect_true(grepl(tempdir(), saved_path, fixed = TRUE))
})

test_that("DuckDB schema-faithful query path executes accurately on mock DuckDB connection", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix;")
  DBI::dbExecute(con, "
    CREATE TABLE chia_casemix.v_hdd_discharge_canonical (
      _data_year INTEGER,
      age INTEGER,
      procedure_family VARCHAR
    );
  ")

  DBI::dbExecute(con, "
    INSERT INTO chia_casemix.v_hdd_discharge_canonical VALUES
    (2015, 45, 'pop_hysterectomy'),
    (2015, 55, 'sacrocolpopexy'),
    (2015, 70, 'apical_suspension'),
    (2015, 80, 'colpocleisis');
  ")

  obs <- build_chia_inpatient_urps_series(con = con, min_year = 2015L, max_year = 2015L, mode = "observed")
  expect_equal(attr(obs, "source_kind"), "observed")
  expect_equal(sum(obs$inpatient_cases), 4)
  expect_true(all(obs$female_population > 0))
  expect_true(nzchar(attr(obs, "population_source")))
  expect_true(nzchar(attr(obs, "population_vintage")))
  expect_true(nzchar(attr(obs, "population_definition")))
  expect_true(nzchar(attr(obs, "population_sha256")))

})

test_that("assert_estimand_boundary forbids using D6 for total surgical volume or national FTE", {
  expect_true(assert_estimand_boundary("regional_external_validation"))
  expect_error(
    assert_estimand_boundary("total_surgical_volume_calibration"),
    "FORBIDDEN"
  )
  expect_error(
    assert_estimand_boundary("national_fte_calibration"),
    "FORBIDDEN"
  )
})

test_that("build_chia_surgical_travel_kernel fails closed on zero routes", {
  empty_routes <- tibble::tibble(drive_minutes = numeric(0))
  expect_error(
    build_chia_surgical_travel_kernel(empty_routes),
    "Zero valid routed pairs provided"
  )
})
