test_that("simulate_joint_entrant_characteristics samples historical profiles jointly", {
  hist_entrants <- tibble::tibble(
    cohort_year = rep(2015:2024, each = 10),
    age_at_entry = runif(100, 30, 42),
    sex = sample(c("Female", "Male"), 100, replace = TRUE, prob = c(0.8, 0.2)),
    parent_specialty = sample(c("OBGYN", "Urology"), 100, replace = TRUE, prob = c(0.85, 0.15)),
    training_region = sample(c("Northeast", "Midwest", "South", "West"), 100, replace = TRUE),
    fellowship_duration_years = 3.0,
    completed_fellowship = 1L,
    practice_region = sample(c("Northeast", "Midwest", "South", "West"), 100, replace = TRUE),
    initial_practice_setting = sample(c("Academic", "Private"), 100, replace = TRUE),
    initial_clinical_fte = runif(100, 0.6, 1.0),
    academic = sample(c(0L, 1L), 100, replace = TRUE),
    case_mix_office = runif(100, 0.2, 0.5),
    case_mix_surgery = runif(100, 0.5, 0.8)
  )

  counts <- tibble::tibble(
    cohort_year = 2025:2028,
    n_entrants = c(20L, 22L, 25L, 30L)
  )

  res <- simulate_joint_entrant_characteristics(
    historical_entrants = hist_entrants,
    entrant_counts = counts,
    case_mix_cols = c("case_mix_office", "case_mix_surgery"),
    seed = 20260820L
  )

  expect_named(res, c("entrants", "cohort_summary", "saved_path"))
  expect_equal(nrow(res$entrants), 97L)
  expect_true(all(c("initial_clinical_fte", "academic", "case_mix_office") %in% names(res$entrants)))
})
