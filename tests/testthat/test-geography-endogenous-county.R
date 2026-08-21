test_that("solve_endogenous_geography solves spatial supply-demand equilibrium", {
  provs <- tibble::tribble(
    ~provider_id, ~current_county_fips, ~clinical_fte,
    "P01", "08001", 1.0,
    "P02", "08005", 0.8
  )

  mkts <- tibble::tribble(
    ~county_fips, ~year, ~population, ~required_fte, ~income_index, ~academic_center, ~county_intercept, ~initial_supply_fte,
    "08001", 2025L, 250000, 2.5, 1.1, 1L, 0.0, 1.0,
    "08005", 2025L, 350000, 3.5, 1.3, 0L, 0.0, 0.8
  )

  choices <- tibble::tribble(
    ~provider_id, ~county_fips, ~training_tie, ~distance_penalty, ~historical_tie,
    "P01", "08001", 1L, 0.0, 1L,
    "P01", "08005", 0L, 25.0, 0L,
    "P02", "08001", 0L, 25.0, 0L,
    "P02", "08005", 1L, 0.0, 1L
  )

  coeffs <- c(
    training_tie = 1.2,
    income = 0.5,
    unmet_demand = 2.0,
    academic_center = 0.8,
    current_county = 1.5,
    historical_tie = 1.0,
    distance_penalty = -0.05,
    rural_incentive = 1.0,
    new_program = 0.5,
    fellowship_slots = 0.3
  )

  res <- solve_endogenous_geography(
    providers = provs,
    markets = mkts,
    choice_set = choices,
    coefficients = coeffs,
    year_value = 2025L,
    tolerance = 1e-4,
    max_iterations = 50L
  )

  expect_named(res, c("county_markets", "destination_probabilities", "diagnostics"))
  expect_equal(nrow(res$county_markets), 2L)
  expect_true(res$diagnostics$converged)

  locs <- draw_provider_locations(res$destination_probabilities, seed = 20260820L)
  expect_equal(nrow(locs), 2L)
})
