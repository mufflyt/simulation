test_that("HRSA surgical hours reproduce published values", {
  female_hours <- predict_hrsa_demographic_hours(
    age = c(
      30,
      40,
      50,
      57,
      62,
      67,
      72,
      80
    ),
    gender = "female",
    specialty_group = "surgery"
  )

  expect_equal(
    female_hours,
    c(
      50.4,
      48.2,
      47.1,
      45.9,
      44.9,
      40.0,
      34.3,
      26.9
    )
  )

  male_hours <- predict_hrsa_demographic_hours(
    age = c(
      30,
      40,
      50,
      57,
      62,
      67,
      72,
      80
    ),
    gender = "male",
    specialty_group = "surgery"
  )

  expect_equal(
    male_hours,
    c(
      56.0,
      50.5,
      49.4,
      51.5,
      50.5,
      45.6,
      40.0,
      32.6
    )
  )
})

test_that("HRSA FTE uses exactly 2,080 annual hours", {
  predicted_fte <- predict_hrsa_demographic_fte(
    age = 67,
    gender = "female",
    specialty_group = "surgery"
  )

  expect_equal(predicted_fte, 1.0)
  expect_equal(HRSA_FTE_HOURS_PER_YEAR, 2080)
  expect_equal(HRSA_FTE_HOURS_PER_WEEK * 52, HRSA_FTE_HOURS_PER_YEAR)
})

test_that("predict_hrsa_demographic_fte return_components produces tibble", {
  res <- predict_hrsa_demographic_fte(
    age = c(32, 40, 50, 60, 67, 77),
    gender = "female",
    specialty_group = "surgery",
    return_components = TRUE
  )

  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 6L)
  expect_true(all(c("age", "gender", "specialty_group", "weekly_hours", "annual_hours", "demographic_fte") %in% names(res)))
})
