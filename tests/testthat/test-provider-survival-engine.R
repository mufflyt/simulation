test_that("fit_provider_survival_hazards fits Cox PH and Weibull AFT models", {
  set.seed(42)
  n <- 50
  roster_history_tbl <- tibble::tibble(
    provider_id = sprintf("P%04d", seq_len(n)),
    years_experience = sample(1:30, n, replace = TRUE),
    event_exit = sample(c(0, 1), n, replace = TRUE, prob = c(0.7, 0.3)),
    pathway = sample(c("ABOG_PLUS_ABU", "ABOG_ONLY", "ABU_ONLY"), n, replace = TRUE),
    practice_setting = sample(c("office", "academic_medical_center", "community_hospital"), n, replace = TRUE),
    malpractice_tier = sample(c("low", "moderate", "high"), n, replace = TRUE)
  )

  engine_cox <- fit_provider_survival_hazards(roster_history_tbl, model_type = "cox_ph")
  expect_s3_class(engine_cox, "urps_provider_survival_engine")
  expect_equal(engine_cox$model_type, "cox_ph")

  engine_weibull <- fit_provider_survival_hazards(roster_history_tbl, model_type = "weibull_aft")
  expect_s3_class(engine_weibull, "urps_provider_survival_engine")
  expect_equal(engine_weibull$model_type, "weibull_aft")

  # Test predictions
  preds <- predict_provider_survival_probability(engine_cox, roster_history_tbl, t_years = 2.0)
  expect_s3_class(preds, "tbl_df")
  expect_equal(nrow(preds), n)
  expect_true(all(preds$exit_probability >= 0 & preds$exit_probability <= 1))
})
