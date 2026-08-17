# tests/testthat/test-source-mutation-engine.R
# Scientific Hardening Section 7 & 8: Source Mutation Testing Suite

test_that("test_scientific_mutation kills top high-priority scientific mutations", {
  res1 <- test_scientific_mutation(1) # D6 to D3 substitution
  expect_true(res1$killed)
  expect_equal(res1$detector_fired, "assert_estimand_compatible")

  res2 <- test_scientific_mutation(2) # CHIA con=NULL
  expect_true(res2$killed)
  expect_equal(res2$detector_fired, "build_chia_inpatient_urps_series")

  res3 <- test_scientific_mutation(3) # Zero routes travel kernel
  expect_true(res3$killed)

  res4 <- test_scientific_mutation(4) # Out of range denominator year
  expect_true(res4$killed)
})
