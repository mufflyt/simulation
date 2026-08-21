test_that("predict_hrsa_demographic_fte computes age and gender curves correctly", {
  res <- predict_hrsa_demographic_fte(
    age = c(35, 45, 60, 35, 45, 60),
    gender = c("female", "female", "female", "male", "male", "male"),
    return_components = TRUE
  )

  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 6L)
  expect_true(all(c("weekly_hours", "annual_hours", "demographic_fte") %in% names(res)))
  expect_true(all(res$demographic_fte > 0))
})

test_that("apply_hrsa_insurance_demand_multipliers scales baseline demand", {
  base_demand <- tibble::tibble(
    year = 2025:2030,
    age_band = "65-74",
    baseline_demand = 1000,
    insurance = "medicare"
  )
  adj <- apply_hrsa_insurance_demand_multipliers(base_demand)

  expect_s3_class(adj, "tbl_df")
  expect_true("demand_after_insurance" %in% names(adj))
  expect_true(all(adj$demand_after_insurance > 0))
})

test_that("aggregate_hrr_workforce_balance aggregates regional supply and demand", {
  roster <- tibble::tibble(hrr_code = c("HRR01", "HRR01", "HRR02"), fte = c(1.0, 0.8, 1.0))
  demand <- tibble::tibble(hrr_code = c("HRR01", "HRR02", "HRR03"), hrr_name = c("Boston", "Worcester", "Springfield"), demand_fte = c(2.5, 1.0, 3.0))
  hrr_ref <- tibble::tibble(hrr_code = c("HRR01", "HRR02", "HRR03"), hrr_name = c("Boston", "Worcester", "Springfield"), state = c("MA", "MA", "MA"))

  bal <- aggregate_hrr_workforce_balance(roster, demand, hrr_ref, expected_hrr_n = 3L)

  expect_s3_class(bal, "tbl_df")
  expect_equal(nrow(bal), 3L)
  expect_true("shortage_20pct" %in% names(bal))
  # HRR03 has demand 3.0, supply 0.0 -> deficit 100% -> shortage area
  expect_true(bal$shortage_20pct[bal$hrr_code == "HRR03"])
})
