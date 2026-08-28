# Unit tests for SWAN dynamic transition hazard pipeline (a*) and fitted transitions

test_that("estimate_swan_dmdm_hazards.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_swan_dmdm_hazards.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("dmdm_swan_fitted_transitions returns valid fitted transitions object", {
  tr <- dmdm_swan_fitted_transitions()
  expect_type(tr, "list")
  expect_equal(tr$status, "fitted")
  expect_equal(tr$calibration_status, "fitted")

  # Check UI onset hazards (a*)
  ui_onset <- tr$onset$ui
  expect_named(ui_onset, c("a0", "avag", "aage", "absl", "abmi", "ahyst", "ameno", "acom"))
  expect_equal(unname(ui_onset["a0"]), -1.305345, tolerance = 1e-4)
  expect_equal(unname(ui_onset["avag"]), 0.059042, tolerance = 1e-4)
  expect_equal(unname(ui_onset["abmi"]), 0.125193, tolerance = 1e-4)

  # Check remission hazard
  expect_equal(unname(tr$remission["ui"]), 0.135649, tolerance = 1e-4)
})

test_that("assert_calibrated_transitions passes for dmdm_swan_fitted_transitions", {
  tr <- dmdm_swan_fitted_transitions()
  res <- assert_calibrated_transitions(tr, allow_uncalibrated = FALSE, mode = "strict")
  expect_true(res)
})

test_that("simulate_dmdm executes cleanly with dmdm_swan_fitted_transitions", {
  tr <- dmdm_swan_fitted_transitions()

  # Create a small 100-woman synthetic cohort
  set.seed(2026)
  cohort0 <- data.frame(
    age = runif(100, 45, 65),
    cumulative_vaginal_deliveries = rpois(100, 2),
    years_since_last_vaginal_birth = runif(100, 10, 30),
    bmi = rnorm(100, 28, 5),
    hysterectomy = rbinom(100, 1, 0.2),
    menopause_status = rbinom(100, 1, 0.7),
    comorbidity = rbinom(100, 1, 0.3),
    has_ui = rbinom(100, 1, 0.3),
    has_pop = rbinom(100, 1, 0.1),
    has_ai = rbinom(100, 1, 0.05)
  )

  sim <- simulate_dmdm(cohort0, start_year = 2024, end_year = 2028, transitions = tr, seed = 2026)
  expect_s3_class(sim, "data.frame")
  expect_equal(nrow(sim), 5L) # 2024 to 2028 inclusive
  expect_true(all(c("year", "living", "prev_ui", "prev_pop", "prev_ai") %in% names(sim)))
})
