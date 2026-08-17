# tests/testthat/test-hall-of-shame-regressions.R
# Scientific Hardening Section 40: Hall-of-Shame Scientific Regression Suite
#
# Permanent detectors for historical scientific defects to prevent silent reintroduction.

test_that("Hall-of-Shame 1: D6 inpatient surgical volume cannot calibrate D3 total surgery", {
  expect_error(
    assert_estimand_compatible("D6", "total_surgical_demand_calibration"),
    "SEMANTIC FAILURE"
  )
})

test_that("Hall-of-Shame 2: CHIA build_chia_inpatient_urps_series fails closed on con=NULL in observed mode", {
  expect_error(
    build_chia_inpatient_urps_series(con = NULL, mode = "observed"),
    "mode='observed' requires a valid database connection"
  )
})

test_that("Hall-of-Shame 3: Travel kernel fails closed when zero routes exist", {
  empty_routes <- tibble::tibble(drive_minutes = numeric(0))
  expect_error(
    build_chia_surgical_travel_kernel(empty_routes),
    "Zero valid routed pairs provided"
  )
})

test_that("Hall-of-Shame 4: Missing population denominator causes hard fail, not silent 500,000 fallback", {
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

  # Discharge in year 1999 (outside 2004-2018 population denominator range)
  DBI::dbExecute(con, "INSERT INTO chia_casemix.v_hdd_discharge_canonical VALUES (1999, 45, 'pop_hysterectomy');")

  expect_error(
    build_chia_inpatient_urps_series(con = con, min_year = 1999L, max_year = 1999L, mode = "observed"),
    "Year outside documented census population range"
  )
})

