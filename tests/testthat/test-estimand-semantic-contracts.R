# tests/testthat/test-estimand-semantic-contracts.R
# Scientific Hardening Section 2 Layer 2B: Semantic Correctness & Estimand Contract Tests

test_that("assert_estimand_compatible allows valid uses for D6", {
  # read_estimand_registry() reads config/estimands.yml, which is excluded
  # from the built package (.Rbuildignore: ^config$) and only reachable from
  # the source tree. Under covr's isolated temp install this path is
  # genuinely absent -- the same class of gap .source_tree_root()'s Meta/
  # discriminator exists to detect.
  skip_if(length(.source_tree_root()) == 0,
          "estimand registry unreachable (source tree absent under R CMD check/covr)")
  expect_true(assert_estimand_compatible("D6", "regional_external_validation"))
  expect_true(assert_estimand_compatible("D6", "inpatient_setting_validation"))
})

test_that("assert_estimand_compatible blocks forbidden D6 substitutions", {
  skip_if(length(.source_tree_root()) == 0,
          "estimand registry unreachable (source tree absent under R CMD check/covr)")
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
  skip_if(length(.source_tree_root()) == 0,
          "estimand registry unreachable (source tree absent under R CMD check/covr)")
  expect_true(assert_estimand_compatible("D3", "total_surgical_demand_calibration"))
  expect_error(
    assert_estimand_compatible("D3", "inpatient_only_validation"),
    "SEMANTIC FAILURE"
  )
})

