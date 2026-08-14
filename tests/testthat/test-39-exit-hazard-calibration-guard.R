library(testthat)
library(urpssim)

# The failure these guard: build_urps_exit_hazard() returns a well-formed
# 102-row curve on every fallback path, so a missing table or a drifted schema
# looks identical to success at the call site. require_calibrated = TRUE turns
# those silent fallbacks into errors for callers that depend on a real fit.

test_that("require_calibrated errors when no cliff DuckDB is supplied", {
  expect_error(
    build_urps_exit_hazard(cliff_duckdb_path = NULL,
                           require_calibrated = TRUE, verbose = FALSE),
    "no calibrated hazard could be fitted"
  )
})

test_that("require_calibrated names the reason in the message", {
  expect_error(
    build_urps_exit_hazard(cliff_duckdb_path = NULL,
                           require_calibrated = TRUE, verbose = FALSE),
    "cliff_duckdb_path is NULL"
  )
})

test_that("require_calibrated errors on a nonexistent cliff path", {
  expect_error(
    build_urps_exit_hazard(cliff_duckdb_path = tempfile(fileext = ".duckdb"),
                           require_calibrated = TRUE, verbose = FALSE),
    "does not exist"
  )
})

test_that("default (require_calibrated = FALSE) still falls back silently", {
  h <- suppressMessages(
    build_urps_exit_hazard(cliff_duckdb_path = NULL, verbose = FALSE)
  )
  expect_identical(h$source, "hwsm_weibull_analogy")
  expect_identical(h$n_events, 0L)
  expect_identical(nrow(h$exit_probs), 102L)
  expect_true(all(h$exit_probs$calibration_tier == "derived_by_analogy"))
})

test_that("urps_cliff_query: cohort is NPPES taxonomy, not the ML predictions", {
  q <- urps_cliff_query()
  expect_true(grepl("207VF0040X", q, fixed = TRUE))
  # The procedure-derived table is billing-biased: retirees are absent by
  # construction, so it must never define the cohort here.
  expect_false(grepl("obgyn_subspecialty_ml_predictions", q, fixed = TRUE))
  # Surrogate 4-5 char keys, not NPIs.
  expect_false(grepl("subspecialty_by_npi", q, fixed = TRUE))
})

test_that("urps_cliff_query: returns the columns the fitter requires", {
  q <- urps_cliff_query()
  expect_true(grepl("AS age", q, fixed = TRUE))
  expect_true(grepl("AS sex", q, fixed = TRUE))
  expect_true(grepl("AS confidence_score", q, fixed = TRUE))
})

test_that("urps_cliff_query: age uses year_of_birth, not a graduation proxy", {
  q <- urps_cliff_query()
  expect_true(grepl("year_of_birth", q, fixed = TRUE))
  expect_false(grepl("dox_graduation_year", q, fixed = TRUE))
})

# --- automatic resolution -----------------------------------------------
#
# The legacy scan only looks in schema 'main'. On the NBER DuckDB that finds
# nothing, so a bare call used to return a full-looking 102-row analogy curve
# with n_events = 0 -- indistinguishable from success. urps_cliff_query() is
# now attempted before giving up. These pin the two halves of that: it must
# fire where the data exists, and must not disturb a database where it doesn't.

nber_db <- "/Volumes/MufflySamsung 1 1/DuckDB/nber_my_duckdb.duckdb"

test_that("auto-resolution leaves an unrelated DuckDB on the fallback path", {
  skip_if_not_installed("duckdb")
  tmp <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), tmp)
  DBI::dbExecute(con, "CREATE TABLE junk(x INT)")
  DBI::dbDisconnect(con, shutdown = TRUE)

  h <- suppressWarnings(suppressMessages(
    build_urps_exit_hazard(cliff_duckdb_path = tmp, verbose = FALSE)
  ))
  expect_identical(h$source, "hwsm_weibull_analogy_no_table")
  expect_identical(h$n_events, 0L)
  expect_true(all(h$exit_probs$calibration_tier == "derived_by_analogy"))
})

test_that("require_calibrated still errors when auto-resolution finds nothing", {
  skip_if_not_installed("duckdb")
  tmp <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), tmp)
  DBI::dbExecute(con, "CREATE TABLE junk(x INT)")
  DBI::dbDisconnect(con, shutdown = TRUE)

  expect_error(
    build_urps_exit_hazard(cliff_duckdb_path = tmp,
                           require_calibrated = TRUE, verbose = FALSE),
    "returned no rows"
  )
})

test_that("a bare call on the NBER DuckDB auto-resolves to a calibrated fit", {
  skip_if_not(file.exists(nber_db), "NBER DuckDB not mounted")
  h <- suppressMessages(
    build_urps_exit_hazard(cliff_duckdb_path = nber_db, verbose = FALSE)
  )
  expect_identical(h$source, "cliff_empirical_gompertz")
  expect_identical(h$cohort_source, "urps_cliff_query_auto")
  expect_gte(h$n_events, 30L)
  expect_true(all(h$exit_probs$calibration_tier == "calibrated"))
})

test_that("auto-resolution and an explicit cliff_query agree exactly", {
  skip_if_not(file.exists(nber_db), "NBER DuckDB not mounted")
  auto <- suppressMessages(
    build_urps_exit_hazard(nber_db, verbose = FALSE))
  expl <- suppressMessages(
    build_urps_exit_hazard(nber_db, cliff_query = urps_cliff_query(), verbose = FALSE))
  expect_equal(auto$exit_probs, expl$exit_probs)
  expect_identical(expl$cohort_source, "cliff_query")
})

test_that("urps_cliff_query: confidence and age bounds are interpolated", {
  q <- urps_cliff_query(min_confidence = 0.75, age_range = c(35, 78))
  expect_true(grepl("0.75", q, fixed = TRUE))
  expect_true(grepl("BETWEEN 35 AND 78", q, fixed = TRUE))
})
