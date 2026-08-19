test_that("incident_entry_wilson computes valid score intervals", {
  res <- incident_entry_wilson(successes = 250, trials = 1000, conf_level = 0.95)
  expect_s3_class(res, "tbl_df")
  expect_true(res$q_low >= 0 && res$q_low <= 0.25)
  expect_true(res$q_high >= 0.25 && res$q_high <= 1.0)
})

test_that("estimate_incident_entry_hazard runs on valid synthetic inputs", {
  member_year_tbl <- tibble::tribble(
    ~person_id, ~year, ~female, ~age, ~payer_group,
    "P001", 2023L, TRUE, 52L, "Commercial",
    "P002", 2023L, TRUE, 68L, "Medicare"
  )

  enrollment_tbl <- tidyr::crossing(
    person_id = c("P001", "P002"),
    coverage_year = 2021:2023,
    coverage_month = 1:12
  )

  claims_tbl <- tibble::tribble(
    ~person_id, ~service_year, ~service_month, ~rendering_npi, ~condition, ~is_outpatient_evaluation, ~is_qualifying_urps_encounter,
    "P001", 2023L, 4L, "1234567890", "ui", TRUE, TRUE
  )

  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")

  stock_probability_tbl <- tidyr::crossing(
    condition = c("ui", "pop", "ai"),
    age_band = c("18-44", "45-54", "55-64", "65-74", "75+"),
    year = 2023L,
    payer_group = c("Commercial", "Medicare"),
    eligible_stock_probability = 0.25
  )

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl,
    enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl,
    roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L,
    washout_months = 24L,
    min_cell_n = 1L
  )

  expect_named(res, c("analytic", "public", "diagnostics"))
  expect_s3_class(res$analytic, "tbl_df")
  expect_s3_class(res$public, "tbl_df")
  expect_s3_class(res$diagnostics, "tbl_df")
})
