# ARCHIVED tests, moved from tests/testthat/test-urps-migration.R.
# They exercise functions now in inst/archive/ and are NOT run.

test_that("apply_urps_migration returns a data frame with the same columns", {
  agents <- make_agents(30)
  mat    <- urps_migration_matrix(c("NY", "CA", "WV", "MT"))
  set.seed(42)
  out    <- apply_urps_migration(agents, year = 2026, migration_matrix = mat)
  expect_true(is.data.frame(out))
  expect_true(all(names(agents) %in% names(out)))
})

test_that("apply_urps_migration adds n_moves and left_country columns", {
  agents <- make_agents(20)
  mat    <- urps_migration_matrix(c("NY", "CA", "WV", "MT"))
  set.seed(99)
  out    <- apply_urps_migration(agents, year = 2026, migration_matrix = mat)
  expect_true("n_moves" %in% names(out))
  expect_true("left_country" %in% names(out))
})

test_that("agents without state column pass through unchanged", {
  agents <- tibble::tibble(provider_id = "p1", age = 45, entry_year = 2010L)
  mat    <- urps_migration_matrix(c("NY", "WV"))
  out    <- apply_urps_migration(agents, 2026, migration_matrix = mat)
  expect_equal(names(out), names(agents))
})

test_that("apply_urps_migration builds matrix on the fly when NULL", {
  agents <- make_agents(10, states = rep(c("NY", "WV"), 5))
  set.seed(7)
  out <- apply_urps_migration(agents, year = 2026, migration_matrix = NULL)
  expect_true(is.data.frame(out))
  expect_true("n_moves" %in% names(out))
})

