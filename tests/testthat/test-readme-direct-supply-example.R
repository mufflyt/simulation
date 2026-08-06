test_that("the documented direct-supply example has quantified uncertainty and a coherent hours scale", {
  agents <- initialize_provider_agents(80, "FPMRS", 2025)
  history <- urps_certification_cohorts()
  history <- history$n_certified[history$cert_year >= 2018]
  spec <- supply_parameter_spec(entrant_series = history, entrant_mean = 55)

  out <- run_supply_microsimulation(
    initial_workforce = agents,
    years = 2025:2027,
    entrants_per_year = 55,
    n_iterations = 3,
    retirement_schedule = urps_empirical_retirement_schedule(),
    param_spec = spec,
    fte_method = "hours",
    hours_intercept = calibrate_hours_intercept(agents$age),
    verbose = FALSE
  )

  expect_true(spec$quantified[["entrant_rate"]])
  expect_equal(out$scenario$entrants_source, "param_spec (drawn per iteration)")
  expect_equal(out$summary$effective_fte_median[[1]],
               out$summary$headcount_median[[1]], tolerance = 1e-8)
})
