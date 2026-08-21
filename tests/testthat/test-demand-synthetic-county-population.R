test_that("build_county_synthetic_female_population calibrates margins and simulates trajectories", {
  set.seed(42)
  donors <- tibble::tibble(
    donor_id = sprintf("D%03d", 1:100),
    survey_weight = runif(100, 10, 50),
    age_cat = sample(c("20-49", "50+"), 100, replace = TRUE),
    race = sample(c("White", "Non-White"), 100, replace = TRUE),
    age = runif(100, 20, 80),
    bmi = runif(100, 18, 35),
    parity = sample(0:3, 100, replace = TRUE),
    diabetes = rbinom(100, 1, 0.1),
    sui = rbinom(100, 1, 0.2),
    uui = rbinom(100, 1, 0.15),
    pop_stage = sample(0:3, 100, replace = TRUE),
    fecal_incontinence = rbinom(100, 1, 0.05),
    oab = rbinom(100, 1, 0.18),
    prior_hysterectomy = rbinom(100, 1, 0.12)
  )

  targets <- tibble::tribble(
    ~county_fips, ~year, ~variable, ~level, ~target_n,
    "08001", 2025, "age_cat", "20-49", 6000,
    "08001", 2025, "age_cat", "50+", 4000,
    "08001", 2025, "race", "White", 7000,
    "08001", 2025, "race", "Non-White", 3000
  )

  bundle <- build_county_synthetic_female_population(
    donors = donors,
    county_targets = targets,
    start_year = 2025,
    end_year = 2027,
    persons_per_county = 500L,
    max_iterations = 200L,
    tolerance = 1e-4,
    seed = 123
  )

  expect_named(bundle, c("synthetic_people", "trajectories", "calibration_diagnostics", "county_targets", "metadata"))
  expect_equal(nrow(bundle$synthetic_people), 500)
  expect_true(bundle$calibration_diagnostics$converged[[1]])

  burden <- summarize_county_pfd_burden(bundle$trajectories)
  expect_equal(nrow(burden), 3) # 3 years (2025, 2026, 2027)
  expect_true(all(burden$sui_prevalence >= 0 & burden$sui_prevalence <= 1))
})
