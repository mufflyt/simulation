test_that("predict_medicaid_acceptance returns higher probabilities for academic providers", {
  p_private <- predict_medicaid_acceptance(academic_setting = FALSE, hospital_outpatient = FALSE)
  p_academic <- predict_medicaid_acceptance(academic_setting = TRUE, hospital_outpatient = FALSE)

  expect_true(p_academic > p_private)
  expect_equal(round(p_private, 2), 0.24) # ~24% baseline private
  expect_equal(round(p_academic, 2), 0.52) # ~52% academic
})

test_that("filter_supply_by_insurance filters Medicaid capacity correctly", {
  prov <- tibble::tibble(
    provider_id = c("P1", "P2"),
    supply = c(1.0, 1.0),
    academic_setting = c(FALSE, TRUE)
  )

  # Commercial gets full capacity
  comm <- filter_supply_by_insurance(prov, insurance = "Commercial")
  expect_equal(comm$supply, c(1.0, 1.0))

  # Medicaid scales capacity by acceptance probability
  med <- filter_supply_by_insurance(prov, insurance = "Medicaid", probabilistic = TRUE)
  expect_true(med$supply[2] > med$supply[1])
  expect_equal(round(sum(med$supply), 2), 0.76)
})
