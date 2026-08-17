test_that("apply_wait_time_elasticity adjusts entry rates based on wait days", {
  base_entry <- 0.20
  # At baseline wait days (23.1), multiplier is 1.0
  expect_equal(apply_wait_time_elasticity(base_entry, observed_wait_days = 23.1, baseline_wait_days = 23.1), 0.20)

  # When wait time doubles to 46.2 days, entry rate decreases (elasticity -0.25)
  longer_wait_entry <- apply_wait_time_elasticity(base_entry, observed_wait_days = 46.2, baseline_wait_days = 23.1)
  expect_true(longer_wait_entry < base_entry)
  expect_equal(round(longer_wait_entry, 4), round(0.20 * (2.0^-0.25), 4))
})

test_that("advance_care_engagement accepts observed_wait_days without circularity", {
  res <- advance_care_engagement(
    untreated_eligible = 1000,
    previously_disengaged = 500,
    care_engaged_previous = 2000,
    first_entry_rate = 0.10,
    reentry_rate = 0.05,
    retention_rate = 0.90,
    observed_wait_days = 46.2
  )
  expect_true("care_engaged" %in% names(res))
  expect_true(res$newly_entering_care < (1000 * 0.10 + 500 * 0.05))
})
