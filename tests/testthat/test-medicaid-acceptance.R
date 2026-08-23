test_that("filter_supply_by_insurance filters Medicaid capacity correctly", {
  prov <- tibble::tibble(
    rendering_npi = c("1", "2"),
    supply = c(1.0, 1.0),
    academic_setting = c(FALSE, TRUE)
  )

  # Commercial gets full capacity
  comm <- filter_supply_by_insurance(prov, insurance = "Commercial")
  expect_equal(comm$insurance_accessible_fte, c(1.0, 1.0))

  # Medicaid scales capacity by acceptance probability
  med <- filter_supply_by_insurance(prov, insurance = "Medicaid", mode = "expected_capacity")
  expect_true(med$insurance_accessible_fte[2] > med$insurance_accessible_fte[1])
  expect_equal(round(sum(med$insurance_accessible_fte), 2), 0.76)
})
