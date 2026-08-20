test_that("medicaid_medicare_fee_index_table returns state fee ratios", {
  tbl <- medicaid_medicare_fee_index_table()
  expect_s3_class(tbl, "tbl_df")
  expect_true(all(c("state_abbr", "medicaid_fee_ratio", "fee_tier") %in% names(tbl)))
  expect_true(nrow(tbl) > 0)
})

test_that("lookup_state_medicaid_fee_ratio resolves state ratios and default", {
  ratios <- lookup_state_medicaid_fee_ratio(c("AK", "NY", "XX"))
  expect_equal(ratios, c(1.25, 0.48, 0.72))
})

test_that("predict_medicaid_acceptance matches Acosta 2026 empirical benchmarks", {
  # Private office in average fee state (0.72) -> ~24.0%
  p_priv <- predict_medicaid_acceptance(academic_setting = FALSE, hospital_outpatient = FALSE, medicaid_fee_ratio = 0.72)
  expect_true(p_priv > 0.20 && p_priv < 0.30)

  # Academic medical center alone -> ~52.0%
  p_acad <- predict_medicaid_acceptance(academic_setting = TRUE, hospital_outpatient = FALSE, medicaid_fee_ratio = 0.72)
  expect_true(p_acad > 0.45 && p_acad < 0.60)

  # Academic hospital outpatient department (HOD) -> ~70.1%
  p_acad_hod <- predict_medicaid_acceptance(academic_setting = TRUE, hospital_outpatient = TRUE, medicaid_fee_ratio = 0.72)
  expect_true(p_acad_hod > 0.65 && p_acad_hod < 0.80)

  # Academic setting increases acceptance relative to private office
  expect_true(p_acad > p_priv)
})

test_that("filter_supply_by_insurance scales Medicaid supply FTEs", {
  supply <- tibble::tibble(provider_id = "P1", supply = 1.0, academic_setting = TRUE)
  res_comm <- filter_supply_by_insurance(supply, insurance = "Commercial")
  expect_equal(res_comm$supply, 1.0)

  res_med <- filter_supply_by_insurance(supply, insurance = "Medicaid", probabilistic = TRUE)
  expect_true(res_med$supply < 1.0 && res_med$supply > 0.5)
})
