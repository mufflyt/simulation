# tests/testthat/test-estimand-semantic-contracts.R
# Scientific Hardening Section 2 Layer 2B: Semantic Correctness & Estimand Contract Tests

test_that("assert_estimand_compatible allows valid uses for D6", {
  expect_true(assert_estimand_compatible("D6", "regional_external_validation"))
  expect_true(assert_estimand_compatible("D6", "inpatient_setting_validation"))
})

test_that("assert_estimand_compatible blocks forbidden D6 substitutions", {
  expect_error(
    assert_estimand_compatible("D6", "total_surgical_demand_calibration"),
    "SEMANTIC FAILURE"
  )
  expect_error(
    assert_estimand_compatible("D6", "national_fte_calibration"),
    "SEMANTIC FAILURE"
  )
  expect_error(
    assert_estimand_compatible("D6", "care_seeking_calibration"),
    "SEMANTIC FAILURE"
  )
  expect_error(
    assert_estimand_compatible("D6", "wait_time_calibration"),
    "SEMANTIC FAILURE"
  )
})

test_that("assert_estimand_compatible allows valid D3 uses and blocks invalid ones", {
  expect_true(assert_estimand_compatible("D3", "total_surgical_demand_calibration"))
  expect_error(
    assert_estimand_compatible("D3", "inpatient_only_validation"),
    "SEMANTIC FAILURE"
  )
})

