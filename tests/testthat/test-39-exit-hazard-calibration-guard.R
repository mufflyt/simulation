library(testthat)
library(urpssim)

# The failure this guards against.
#
# build_urps_exit_hazard() returns a well-formed 102-row curve on every fallback
# path. A missing CSV, an absent database, a drifted schema -- each yields a
# full-looking result whose only tell is the `source` field, which nothing
# forces a caller to read. The age-band CSV making the calibrated path the
# common case narrows the window but does not close it: strip the CSV and the
# old silent fallbacks are all still there underneath.
#
# require_calibrated = TRUE turns them into errors for callers that depend on
# the hazard actually being fitted.

no_csv <- function(...) {
  build_urps_exit_hazard(cliff_ageband_csv = NULL, verbose = FALSE, ...)
}

test_that("require_calibrated errors when no calibrated source is reachable", {
  expect_error(
    no_csv(cliff_duckdb_path = NULL, require_calibrated = TRUE),
    "no calibrated hazard could be fitted"
  )
})

test_that("the error names the reason, not just the failure", {
  expect_error(
    no_csv(cliff_duckdb_path = NULL, require_calibrated = TRUE),
    "cliff_duckdb_path is NULL"
  )
})

test_that("require_calibrated errors on a nonexistent DuckDB path", {
  expect_error(
    no_csv(cliff_duckdb_path = tempfile(fileext = ".duckdb"),
           require_calibrated = TRUE),
    "does not exist"
  )
})

test_that("require_calibrated errors when the DuckDB has no retirement table", {
  skip_if_not_installed("duckdb")
  tmp <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), tmp)
  DBI::dbExecute(con, "CREATE TABLE junk(x INT)")
  DBI::dbDisconnect(con, shutdown = TRUE)

  expect_error(
    no_csv(cliff_duckdb_path = tmp, require_calibrated = TRUE),
    "No calibrated hazard|no calibrated hazard could be fitted"
  )
})

test_that("default (require_calibrated = FALSE) still falls back silently", {
  h <- suppressMessages(no_csv(cliff_duckdb_path = NULL))
  expect_identical(h$source, "hwsm_weibull_analogy")
  expect_identical(h$n_events, 0L)
  expect_identical(nrow(h$exit_probs), 102L)
  expect_true(all(h$exit_probs$calibration_tier == "derived_by_analogy"))
})

test_that("the age-band CSV path is unaffected by the guard", {
  # The calibrated default must still succeed, and succeed identically, whether
  # or not the caller asks for the guarantee.
  a <- suppressMessages(build_urps_exit_hazard(verbose = FALSE))
  b <- suppressMessages(build_urps_exit_hazard(verbose = FALSE,
                                               require_calibrated = TRUE))
  expect_identical(a$source, "cliff_ageband_empirical")
  expect_identical(b$source, "cliff_ageband_empirical")
  expect_equal(a$exit_probs, b$exit_probs)
  expect_gt(a$n_events, 0L)
})

test_that("require_calibrated defaults to FALSE, so existing callers are unchanged", {
  expect_identical(
    eval(formals(build_urps_exit_hazard)$require_calibrated), FALSE)
})
