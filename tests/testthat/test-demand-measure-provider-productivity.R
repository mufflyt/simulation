test_that("build_provider_year_productivity_panel builds valid provider-year panel", {
  prov_yr <- tibble::tribble(
    ~provider_id, ~year, ~clinical_fte, ~clinical_hours_week, ~age, ~sex, ~academic, ~rural, ~years_since_fellowship,
    "P1", 2023L, 1.0, 40, 45, "F", "Academic", "Urban", 10,
    "P1", 2024L, 1.0, 40, 46, "F", "Academic", "Urban", 11,
    "P2", 2023L, 0.8, 32, 55, "M", "Private", "Rural", 20,
    "P2", 2024L, 0.8, 32, 56, "M", "Private", "Rural", 21
  )

  services <- tibble::tribble(
    ~provider_id, ~service_date, ~cpt, ~service_type, ~work_rvu, ~actual_operative_minutes,
    "P1", "2023-03-15", "57288", "surgical_procedure", 14.5, 90,
    "P1", "2023-04-10", "57160", "office_procedure", 2.5, NA,
    "P1", "2024-05-12", "57288", "surgical_procedure", 14.5, 95,
    "P2", "2023-06-01", "57160", "office_procedure", 2.5, NA,
    "P2", "2024-07-01", "57160", "office_procedure", 2.5, NA
  )

  panel <- build_provider_year_productivity_panel(
    provider_year = prov_yr,
    services = services
  )

  expect_s3_class(panel, "tbl_df")
  expect_equal(nrow(panel), 4L)
  expect_true(all(c("work_rvus", "wrvu_per_clinical_fte", "wrvu_per_clinical_hour") %in% names(panel)))
})

test_that("fit_provider_productivity_model fits lmer productivity model", {
  base::set.seed(42)
  n <- 40
  prov_ids <- rep(sprintf("P%02d", 1:10), each = 4)
  years <- rep(2021:2024, times = 10)

  panel_mock <- tibble::tibble(
    provider_id = prov_ids,
    year = years,
    clinical_fte = 1.0,
    clinical_hours_week = 40,
    age = runif(n, 35, 65),
    sex = sample(c("F", "M"), n, replace = TRUE),
    academic = sample(c("Academic", "Private"), n, replace = TRUE),
    rural = sample(c("Urban", "Rural"), n, replace = TRUE),
    years_since_fellowship = runif(n, 1, 30),
    app_support_rate = runif(n, 0, 0.3),
    surgical_wrvu_share = runif(n, 0.1, 0.6),
    office_procedure_share = runif(n, 0.1, 0.4),
    new_visit_share = runif(n, 0.1, 0.3),
    wrvu_per_clinical_fte = runif(n, 3000, 8000),
    encounters_per_clinical_fte = runif(n, 1000, 3000),
    wrvu_per_clinical_hour = runif(n, 2, 5)
  )

  fit_res <- fit_provider_productivity_model(
    panel = panel_mock,
    outcome = "wrvu_per_clinical_fte",
    include_year_effect = FALSE
  )

  expect_s3_class(fit_res, "provider_productivity_model")
  expect_s3_class(fit_res$diagnostics, "tbl_df")

  preds <- predict_provider_capacity(fit_res, new_provider_year = panel_mock[1:5, ])
  expect_s3_class(preds, "tbl_df")
  expect_true(all(c("predicted_capacity", "predicted_capacity_low", "predicted_capacity_high") %in% names(preds)))
})
