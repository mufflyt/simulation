test_that("insurance_demand_multiplier_spec returns valid tibble", {
  spec <- insurance_demand_multiplier_spec()
  expect_s3_class(spec, "tbl_df")
  expect_equal(nrow(spec), 4L)
  expect_true(all(c("insurance_type", "multiplier", "interpretation", "calibration_status") %in% names(spec)))
})

test_that("normalize_urps_insurance handles aliases", {
  expect_equal(normalize_urps_insurance(c("commercial", "medicare ffs", "chip", "uninsured")), c("commercial", "medicare", "medicaid", "uninsured"))
})

test_that("apply_hrsa_insurance_demand_multipliers fails closed when baseline includes insurance", {
  pop <- tibble::tibble(baseline_demand = c(10, 20), insurance = c("commercial", "medicare"))
  expect_error(
    apply_hrsa_insurance_demand_multipliers(pop, baseline_includes_insurance = TRUE),
    "already represented"
  )
})

test_that("apply_hrsa_insurance_demand_multipliers computes adjusted demand correctly", {
  pop <- tibble::tibble(baseline_demand = c(10, 10, 10, 10), insurance = c("commercial", "medicare", "medicaid", "uninsured"))
  res <- apply_hrsa_insurance_demand_multipliers(pop, baseline_includes_insurance = FALSE)

  expect_s3_class(res, "tbl_df")
  expect_equal(res$demand_after_insurance, c(11.5, 13.5, 7.5, 4.5))
})

test_that("apply_insurance_mix_demand_multiplier computes weighted cell demand", {
  demand <- tibble::tibble(GEOID = "08031000100", baseline_demand = 1000)
  shares <- tibble::tribble(
    ~GEOID, ~insurance, ~insurance_share,
    "08031000100", "commercial", 0.55,
    "08031000100", "medicare", 0.25,
    "08031000100", "medicaid", 0.15,
    "08031000100", "uninsured", 0.05
  )

  res <- apply_insurance_mix_demand_multiplier(demand, shares, by = "GEOID")
  expect_s3_class(res, "tbl_df")
  # 0.55*1.15 + 0.25*1.35 + 0.15*0.75 + 0.05*0.45 = 1.105
  expect_equal(res$effective_insurance_multiplier[1], 1.105)
  expect_equal(res$demand_after_insurance[1], 1105)
})
