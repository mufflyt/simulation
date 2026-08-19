test_that("simulate_policy_scenario calculates valid supply and demand trajectories", {
  res <- simulate_policy_scenario(
    fellowship_delta = 10,
    medicaid_multiplier = 1.1,
    app_delegation_rate = 0.15,
    retirement_shift = 1.0,
    start_year = 2025L,
    end_year = 2050L
  )

  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 26) # 2025 to 2050 inclusive
  expect_named(res, c("year", "supply_fte", "demand_fte", "gap_fte", "deficit_status"))
  expect_true(all(res$supply_fte > 0))
  expect_true(all(res$demand_fte > 0))
})
