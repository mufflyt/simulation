# tests/testthat/test-simulation-cliff-contract.R
# Scientific Hardening Section 12 P1: Cross-Repository Contract Tests

test_that("validate_simulation_cliff_contract validates compliant access surface", {
  df <- tibble::tibble(
    geoid = c("25001000100", "25001000200"),
    spatial_access_score = c(0.85, 1.20),
    provider_count = c(3L, 5L),
    calibration_status = c("observed_valhalla_2sfca", "observed_valhalla_2sfca")
  )

  res <- validate_simulation_cliff_contract(df)
  expect_true(res$valid)
  expect_equal(res$row_count, 2)
  expect_true(nzchar(res$checksum_sha256))
})

test_that("validate_simulation_cliff_contract errors on missing columns", {
  df_bad <- tibble::tibble(
    geoid = c("25001000100"),
    spatial_access_score = c(0.85)
  )
  expect_error(validate_simulation_cliff_contract(df_bad), "missing required schema column")
})
