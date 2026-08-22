# Unit tests for the CHIA Inpatient Subsystem (D6, Rate Fitting, Rolling-Origin Backtest, Travel Kernel)

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) return(p1)
  if (file.exists(p2)) return(p2)
  curr <- getwd()
  for (i in 1:6) {
    c1 <- file.path(curr, ...)
    if (file.exists(c1)) return(c1)
    c2 <- file.path(curr, "00_pkg_src", "urpssim", ...)
    if (file.exists(c2)) return(c2)
    curr <- dirname(curr)
  }
  p1
}

test_that("CHIA inpatient subsystem files exist and parse cleanly", {
  files <- c(
    .repo_path("R", "data-chia_inpatient_surgery.R"),
    .repo_path("R", "calibration-chia_inpatient_surgery.R"),
    .repo_path("R", "validation-chia_inpatient_surgery.R"),
    .repo_path("R", "geography-chia_inpatient_flows.R")
  )
  skip_if_not(file.exists(files[1]), "Source files absent under R CMD check")
  for (f in files) {
    expect_true(file.exists(f), info = paste("File missing:", f))
    parsed <- tryCatch(parse(f), error = function(e) e)
    expect_false(inherits(parsed, "error"), info = paste("Syntax error in:", f))
  }
})

test_that("build_chia_inpatient_urps_series generates Estimand D6 series", {
  d6 <- build_chia_inpatient_urps_series(min_year = 2004L, max_year = 2006L, mode = "synthetic_fixture")
  expect_s3_class(d6, "data.frame")
  expect_gt(nrow(d6), 0L)
  expect_true(all(c("year", "age_band", "procedure_family", "inpatient_cases", "female_population", "rate_per_100k") %in% names(d6)))
  expect_true(all(d6$rate_per_100k >= 0))
})

test_that("fit_inpatient_surgery_rate_model fits Poisson population-offset model", {
  d6 <- build_chia_inpatient_urps_series(min_year = 2004L, max_year = 2006L, mode = "synthetic_fixture")
  fit_res <- fit_inpatient_surgery_rate_model(d6, family = "quasipoisson", include_interaction = FALSE)

  expect_type(fit_res, "list")
  expect_true(all(c("model", "coefficients", "fitted_rates", "dispersion") %in% names(fit_res)))
  expect_s3_class(fit_res$model, "glm")
})

test_that("validate_chia_inpatient_demand runs rolling-origin backtest", {
  d6 <- build_chia_inpatient_urps_series(min_year = 2004L, max_year = 2008L, mode = "synthetic_fixture")
  val_res <- validate_chia_inpatient_demand(d6, start_cutoff = 2006L)

  expect_type(val_res, "list")
  expect_true(all(c("summary", "evaluations", "mape", "bias", "rmse", "calibration_slope") %in% names(val_res)))
  expect_gt(val_res$mape, 0)
})

test_that("build_chia_surgical_travel_kernel computes empirical decay weights", {
  mock_routes <- tibble::tibble(
    origin_zip = c("02115", "01605"),
    destination_zip = c("02114", "01655"),
    drive_minutes = c(15.0, 45.0)
  )

  kernel <- build_chia_surgical_travel_kernel(mock_routes)
  expect_type(kernel, "list")
  expect_true("observed_travel_band_distribution" %in% names(kernel) || "band_shares" %in% names(kernel))
})

test_that("build_chia_hospital_capacity_map evaluates facility volume & Gini concentration", {
  cap_res <- build_chia_hospital_capacity_map(min_year = 2004L, max_year = 2006L, mode = "synthetic_fixture")
  expect_type(cap_res, "list")
  expect_true(all(c("facility_volumes", "market_summary", "paths") %in% names(cap_res)))
  expect_s3_class(cap_res$facility_volumes, "data.frame")
  expect_true(all(c("year", "facility_id", "inpatient_cases", "market_share", "volume_category") %in% names(cap_res$facility_volumes)))
  expect_true("hospital_inpatient" %in% URPS_SETTING_NAMES)
  expect_false("operative" %in% URPS_SETTING_NAMES)
})


