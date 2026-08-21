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

test_that("build_empirical_entrant_parameters calibrates empirical parameters and handles strict mode", {
  counts <- tibble::tibble(
    cohort_year = 2025:2028,
    n_entrants = c(20L, 22L, 25L, 30L)
  )

  # Non-strict mode when provider profiles are absent
  res_nonstrict <- build_empirical_entrant_parameters(
    cohort_counts = counts,
    strict = FALSE,
    seed = 123L
  )

  expect_true(is.list(res_nonstrict))
  expect_named(res_nonstrict, c("parameters", "evidence_registry", "acgme_series", "summary_sentence", "saved_path"))
  expect_true("prob_female" %in% names(res_nonstrict$parameters))

  # Strict mode fails closed when age/FTE distributions are absent
  expect_error(
    build_empirical_entrant_parameters(cohort_counts = counts, strict = TRUE),
    "No empirical `age_at_entry` distribution is available"
  )

  # With valid provider profiles
  profiles <- tibble::tibble(
    entry_year = 2020:2024,
    age_at_entry = c(32, 34, 35, 33, 36),
    initial_clinical_fte = c(0.85, 0.90, 0.80, 0.88, 0.92),
    academic = c(1, 0, 1, 0, 1)
  )
  # Duplicate profiles to reach minimum count
  profiles_large <- dplyr::bind_rows(lapply(1:4, function(i) profiles))

  res_strict <- build_empirical_entrant_parameters(
    cohort_counts = counts,
    provider_profiles = profiles_large,
    strict = TRUE,
    seed = 456L
  )
  expect_true(is.list(res_strict))
  expect_true(res_strict$parameters$age_mean[1] > 30)
})
