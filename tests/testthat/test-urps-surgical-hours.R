test_that("predict_urps_clinical_hours evaluates mid-career surgical intensity correctly", {
  hrs_female_45 <- predict_urps_clinical_hours(45, sex = "female")
  hrs_male_45   <- predict_urps_clinical_hours(45, sex = "male")

  expect_true(hrs_female_45 > 50.0) # Surgical subspecialty mid-career > 50 hrs/wk
  expect_true(hrs_male_45 > hrs_female_45)
  expect_equal(round(hrs_female_45, 1), 55.2)

})

test_that("URPS_SURGICAL_SPECIALTY_BENCHMARKS includes peer surgical subspecialties", {
  bench <- URPS_SURGICAL_SPECIALTY_BENCHMARKS
  expect_true("Urogynecology (URPS)" %in% bench$specialty)
  expect_true("Gynecologic Oncology" %in% bench$specialty)
  expect_true("Urology" %in% bench$specialty)
  expect_equal(bench$wrvu_median[bench$specialty == "Urogynecology (URPS)"], 7850)
})
