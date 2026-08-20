test_that("academic coefficient reproduces OR 3.42", {
  coef_spec <- medicaid_acceptance_coefficients()

  expect_equal(
    exp(coef_spec$academic_setting),
    3.42,
    tolerance = 1e-12
  )
})

test_that("HOPD coefficient reproduces OR 2.15", {
  coef_spec <- medicaid_acceptance_coefficients()

  expect_equal(
    exp(coef_spec$hospital_outpatient),
    2.15,
    tolerance = 1e-12
  )
})

test_that("fee ratio increase of 0.20 reproduces OR 1.85", {
  coef_spec <- medicaid_acceptance_coefficients()

  implied_or <- exp(coef_spec$medicaid_fee_ratio * 0.20)

  expect_equal(
    implied_or,
    1.85,
    tolerance = 1e-12
  )

  expect_equal(
    coef_spec$medicaid_fee_ratio,
    log(1.85) / 0.20,
    tolerance = 1e-12
  )
})

test_that("2024 KFF state fee ratios are reproduced", {
  ratio_vector <- lookup_state_medicaid_fee_ratio(c("AK", "NY", "NC", "CA", "CO"))

  expect_equal(
    ratio_vector,
    c(1.30, 0.76, 0.82, 0.67, 0.83)
  )
})

test_that("expected Medicaid FTE cannot exceed clinical FTE", {
  fixture_tbl <- tibble::tibble(
    provider_id = c("A", "B"),
    supply = c(1.0, 0.8),
    academic_setting = c(TRUE, FALSE)
  )

  access_tbl <- filter_supply_by_insurance(
    provider_supply = fixture_tbl,
    insurance = "Medicaid",
    mode = "expected_capacity"
  )

  expect_true(
    all(access_tbl$insurance_accessible_fte <= access_tbl$clinical_fte)
  )
})

test_that("attach_state_medicaid_fee_policy attaches state ratios", {
  prov <- tibble::tibble(provider_id = "P1", state_abbr = "CO", supply = 1.0)
  attached <- attach_state_medicaid_fee_policy(prov, year = 2024L)

  expect_s3_class(attached, "tbl_df")
  expect_equal(attached$medicaid_fee_ratio[1], 0.83)
})
