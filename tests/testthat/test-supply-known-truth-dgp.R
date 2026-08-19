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

test_that("evaluate_supply_parameter_recovery fits the REAL entrant-regime pipeline, not a standalone formula", {
  # Pins the fix: this used to be mean(diff(active_headcount) + 20), an ad hoc
  # placeholder that never called any of the package's actual
  # entrant-estimation code (its own comment said "approximation") and was
  # biased by ~27pp on this same DGP. Asserting the recovered rate is close
  # to what fit_entrant_regime_model()+project_entrant_path() independently
  # produce on the DGP's own entrant series is what would catch a regression
  # back to a disconnected proxy.
  world <- generate_synthetic_supply_world(n_initial = 1200, years = 2024:2035, seed = 2026)
  res <- evaluate_supply_parameter_recovery(world)

  series <- tibble::tibble(year = as.integer(world$annual_counts$year),
                           count = as.numeric(world$annual_counts$n_entrants))
  model <- fit_entrant_regime_model(series, fit_through_year = max(series$year) - 1L,
                                    verbose = FALSE)
  proj <- project_entrant_path(model, years = max(series$year))

  expect_equal(res$recovered_entry_rate, proj$expected[[1]])
})
