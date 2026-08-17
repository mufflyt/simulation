# tests/testthat/test-supply-known-truth-dgp.R
# Scientific Hardening Section 9 P1: Supply Known-Truth DGP Parameter Recovery Tests

test_that("generate_synthetic_supply_world generates valid synthetic world", {
  world <- generate_synthetic_supply_world(n_initial = 1000, years = 2024:2030, seed = 2026)
  expect_equal(attr(world, "source_kind"), "synthetic_dgp")
  expect_equal(nrow(world$annual_counts), 7)
  expect_true(all(world$annual_counts$active_headcount > 500))
})

test_that("evaluate_supply_parameter_recovery evaluates parameter bias and RMSE", {
  world <- generate_synthetic_supply_world(n_initial = 1200, years = 2024:2035, seed = 2026)
  res <- evaluate_supply_parameter_recovery(world)
  expect_true(is.list(res))
  expect_true(is.numeric(res$bias))
  expect_true(is.numeric(res$rmse))
  expect_true(res$recovery_passed)
})
