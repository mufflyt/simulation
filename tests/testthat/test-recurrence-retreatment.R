test_that("fit_recurrence_survival_models runs and predicts cause-specific cumulative incidence", {
  set.seed(20260820)
  n <- 80
  claims_cohort <- tibble::tibble(
    beneficiary_id = sprintf("BEN%05d", seq_len(n)),
    index_year = sample(2018:2023, n, replace = TRUE),
    age_at_index = sample(40:80, n, replace = TRUE),
    charlson_index = sample(0:4, n, replace = TRUE),
    diabetes = sample(c(0L, 1L), n, replace = TRUE),
    obesity = sample(c(0L, 1L), n, replace = TRUE),
    tobacco_use = sample(c(0L, 1L), n, replace = TRUE),
    prior_hysterectomy = sample(c(0L, 1L), n, replace = TRUE),
    retreatment_time_days = stats::runif(n, 30, 1800),
    retreatment_event = sample(c(0L, 1L), n, replace = TRUE, prob = c(0.8, 0.2)),
    mesh_complication_time_days = stats::runif(n, 30, 1800),
    mesh_complication_event = sample(c(0L, 1L), n, replace = TRUE, prob = c(0.9, 0.1)),
    reoperation_time_days = stats::runif(n, 30, 1800),
    reoperation_event = sample(c(0L, 1L), n, replace = TRUE, prob = c(0.85, 0.15)),
    death_time_days = stats::runif(n, 30, 1800),
    death_event = sample(c(0L, 1L), n, replace = TRUE, prob = c(0.95, 0.05))
  )

  fitted_models <- fit_recurrence_survival_models(
    claims_cohort = claims_cohort,
    validation_years = 2023L,
    num_trees = 50L
  )

  expect_s3_class(fitted_models, "recurrence_survival_models")

  patient_agents <- claims_cohort |>
    dplyr::select(beneficiary_id, age_at_index, charlson_index, diabetes, obesity, tobacco_use, prior_hysterectomy)

  preds <- predict_patient_recurrence(
    patient_agents = patient_agents,
    fitted_models = fitted_models,
    horizons_years = 1:5
  )

  expect_s3_class(preds, "tbl_df")
  expect_equal(nrow(preds), n * 5L * 3L) # 3 endpoints x 5 horizons x n agents
  expect_named(preds, c("agent_id", "endpoint", "horizon_years", "cumulative_incidence", "annualized_hazard", "estimand"))
  expect_true(all(preds$cumulative_incidence >= 0 & preds$cumulative_incidence <= 1))
})

test_that("fit_recurrence_survival_models enforces fail-closed validation", {
  invalid_cohort <- tibble::tibble(
    beneficiary_id = c("B1", "B2"),
    index_year = c(2021, 2022),
    age_at_index = c(50, 60),
    charlson_index = c(0, 1),
    diabetes = c(0, 1),
    obesity = c(0, 0),
    tobacco_use = c(0, 0),
    prior_hysterectomy = c(0, 1),
    retreatment_time_days = c(-10, 500), # Invalid negative time
    retreatment_event = c(0, 1),
    mesh_complication_time_days = c(100, 500),
    mesh_complication_event = c(0, 0),
    reoperation_time_days = c(100, 500),
    reoperation_event = c(0, 0),
    death_time_days = c(100, 500),
    death_event = c(0, 0)
  )

  expect_error(fit_recurrence_survival_models(invalid_cohort), "Follow-up times must be positive")
})
