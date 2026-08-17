# tests/testthat/test-obstetric-lifecourse-dpmm.R
# Unit tests for Obstetric Lifecourse DPMM Engine

test_that("generate_obstetric_lifecourse_cohort generates valid person records", {
  cohort <- generate_obstetric_lifecourse_cohort(n_women = 100L, seed = 123L)
  expect_equal(nrow(cohort), 100L)
  expect_true(all(cohort$vaginal_births <= cohort$parity))
  expect_true(all(cohort$bmi >= 15 & cohort$bmi <= 60))
})

test_that("predict_pelvic_floor_disease_trajectory assigns POP/SUI driven by vaginal births", {
  cohort <- generate_obstetric_lifecourse_cohort(n_women = 200L, seed = 456L)
  res <- predict_pelvic_floor_disease_trajectory(cohort)

  expect_true("pop_state" %in% names(res))
  expect_true("ui_state" %in% names(res))
  expect_true("annual_service_units" %in% names(res))

  # Verify higher POP prevalence in women with 3+ vaginal births vs 0
  pop_high_parity <- mean(res$pop_state[res$vaginal_births >= 3] != "none")
  pop_nulliparous <- mean(res$pop_state[res$vaginal_births == 0] != "none")
  expect_gt(pop_high_parity, pop_nulliparous)
})
