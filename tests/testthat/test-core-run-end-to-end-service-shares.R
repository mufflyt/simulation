test_that("legacy service-share engine is exact pre-change runner", {
  args <- list(
    start_year = 2025L,
    end_year = 2025L,
    initial_provider_count = 25L,
    fellowship_entrants = 2L,
    seed = 818L,
    save_outputs = FALSE
  )

  expected <- do.call(.run_end_to_end_simulation_legacy, args)
  observed <- do.call(
    run_end_to_end_simulation,
    c(args, list(service_share_engine = "legacy_matrix"))
  )

  expect_identical(observed, expected)
})


test_that("calibrated runner fails closed without a valid bundle", {
  expect_error(
    run_end_to_end_simulation(
      start_year = 2025L,
      end_year = 2025L,
      initial_provider_count = 25L,
      fellowship_entrants = 2L,
      service_share_engine = "calibrated",
      service_share_bundle = NULL,
      save_outputs = FALSE
    ),
    "service_share_bundle"
  )
})


test_that("calibrated shares reach workload FTE and provider-year allocation", {
  bundle <- service_share_full_routing_fixture()
  result <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    initial_provider_count = 30L,
    fellowship_entrants = 2L,
    service_share_engine = "calibrated",
    service_share_bundle = bundle,
    service_share_draw = 1L,
    seed = 919L,
    save_outputs = FALSE
  )

  expect_equal(result$simulation_config$service_share_engine, "calibrated")
  expect_equal(result$simulation_config$service_share_draw, 1L)
  expect_equal(nrow(result$service_share_diagnostics), 1L)
  expect_gt(result$audit_ledger_tbl$wrvu_total, 0)
  expect_equal(
    result$audit_ledger_tbl$wrvu_total,
    result$service_share_diagnostics$urps_wrvu,
    tolerance = 1e-8
  )
  expect_equal(
    sum(result$service_share_provider_workload$annual_wrvu),
    result$audit_ledger_tbl$wrvu_total,
    tolerance = 1e-8
  )
  expect_equal(
    result$service_share_diagnostics$routed_volume_error,
    0,
    tolerance = 1e-8
  )
  expect_equal(
    result$service_share_diagnostics$app_capacity_multiplier_applied,
    FALSE
  )
})


test_that("calibrated practice economics receives provider-level URPS wRVU", {
  .skip_unless_namcs_pooled_data()
  bundle <- service_share_full_routing_fixture()
  result <- suppressWarnings(run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    initial_provider_count = 20L,
    fellowship_entrants = 1L,
    service_share_engine = "calibrated",
    service_share_bundle = bundle,
    service_share_draw = 1L,
    run_practice_economics = TRUE,
    # simulate_practice_economics() refuses draws < 100 -- a deliberate
    # guard against Monte Carlo intervals too small to be statistically
    # meaningful, not a limit this test should route around.
    practice_economics_draws = 100L,
    seed = 1122L,
    save_outputs = FALSE
  ))

  expect_equal(nrow(result$practice_economics_diagnostics), 1L)
  expect_equal(
    result$practice_economics_diagnostics$input_annual_wrvu,
    result$audit_ledger_tbl$wrvu_total,
    tolerance = 1e-8
  )
  expect_equal(
    result$practice_economics_diagnostics$provider_workload_wrvu,
    sum(result$service_share_provider_workload$annual_wrvu),
    tolerance = 1e-8
  )
})
