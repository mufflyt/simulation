test_that("simulate_entrant_characteristics generates valid entrant records", {
  cohort_counts <- tibble::tibble(
    cohort_year = 2025:2028,
    n_entrants = c(50L, 52L, 54L, 55L)
  )

  res <- simulate_entrant_characteristics(
    cohort_counts = cohort_counts,
    count_stage = "certified",
    simulation_draw = 1L,
    seed = 20260820L
  )

  expect_named(res, c("entrants", "case_mix", "cohort_summary", "validation", "summary_sentence"))
  expect_equal(nrow(res$entrants), 211L)
  expect_true(all(res$validation$passed))
})
