# ARCHIVED tests, moved from tests/testthat/test-workforce-microsimulation.R.
# They exercise functions now in inst/archive/ and are NOT run.

test_that("provider migration moves only some providers and never invents states", {
  agents <- tibble::tibble(
    provider_id = sprintf("P%02d", 1:200),
    entry_year = 2023, age = 40,
    state = rep(c("CO", "NY"), 100)
  )
  shares <- tibble::tibble(geo = c("CO", "NY", "TX"), share = c(0.4, 0.4, 0.2))
  set.seed(11)
  moved <- apply_provider_migration(agents, 2025, shares)

  expect_equal(nrow(moved), nrow(agents))
  expect_true(all(moved$state %in% shares$geo))
  expect_true(all(moved$n_moves %in% c(0L, 1L)))   # one move per provider per year
  # Early-career hazard is 4.5%/yr, so most providers must stay put.
  expect_lt(sum(moved$n_moves), nrow(agents) * 0.2)
  expect_gt(sum(moved$n_moves), 0)
  # A roster with no state column is passed through untouched.
  expect_identical(apply_provider_migration(dplyr::select(agents, -"state"), 2025, shares),
                   dplyr::select(agents, -"state"))
})

