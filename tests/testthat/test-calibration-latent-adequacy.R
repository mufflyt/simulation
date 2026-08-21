# Fast validation tests for latent adequacy calibration

required_packages <- c(
  "dplyr",
  "posterior",
  "purrr",
  "readr",
  "stringr",
  "testthat",
  "tibble",
  "tidyr",
  "tidyselect"
)

make_valid_adequacy_fixture <- function() {
  tibble::tibble(
    geography = c("08001", "08005", "08013", "08031"),
    female_population = c(260000, 360000, 170000, 410000),
    appointments_offered = c(18L, 14L, 9L, 11L),
    appointment_attempts = c(20L, 20L, 15L, 20L),
    wait_days = c(14, 25, 42, 60),
    closed_prop = c(0.05, 0.10, 0.20, 0.30),
    travel_minutes = c(12, 18, 35, 50),
    medicaid_barrier = c(0.10, 0.20, 0.35, 0.50),
    e2sfca = c(1.40, 1.10, 0.80, 0.55),
    absorb_prop = c(0.90, 0.75, 0.60, 0.40),
    unmet_referral_prop = c(0.05, 0.10, 0.25, 0.40),
    operative_delay_days = c(21, 35, 56, 84)
  )
}

simulate_adequacy_fixture <- function(
    geography_n = 80L,
    attempts_per_geography = 80L,
    seed = 20260821L) {
  base::set.seed(seed)
  wait_z <- stats::rnorm(geography_n)
  closed_z <- stats::rnorm(geography_n)
  travel_z <- stats::rnorm(geography_n)
  medicaid_z <- stats::rnorm(geography_n)
  geographic_effect <- stats::rnorm(geography_n, 0, 0.25)
  true_eta <- 0.50 - 0.60 * wait_z - 0.80 * closed_z - 0.40 * travel_z - 0.50 * medicaid_z + geographic_effect
  true_adequacy <- stats::plogis(true_eta)
  population <- base::round(stats::runif(geography_n, min = 20000, max = 1500000))

  draw_indicator <- function(intercept, loading, precision) {
    indicator_mean <- stats::plogis(intercept + loading * true_eta)
    stats::rbeta(geography_n, shape1 = indicator_mean * precision, shape2 = (1 - indicator_mean) * precision)
  }

  simulated_tbl <- tibble::tibble(
    geography = base::sprintf("g%03d", base::seq_len(geography_n)),
    female_population = population,
    appointments_offered = stats::rbinom(geography_n, attempts_per_geography, true_adequacy),
    appointment_attempts = attempts_per_geography,
    wait_days = base::pmax(0, base::expm1(wait_z + 3.5)),
    closed_prop = stats::plogis(closed_z - 1.5),
    travel_minutes = base::pmax(0, base::expm1(travel_z + 3.2)),
    medicaid_barrier = stats::plogis(medicaid_z - 0.5),
    e2sfca = stats::qnorm(draw_indicator(-0.10, 0.90, 30)),
    absorb_prop = draw_indicator(0.10, 1.10, 25),
    unmet_referral_prop = 1 - draw_indicator(0.00, 0.80, 20),
    operative_delay_days = base::pmax(0, base::expm1(stats::qnorm(1 - draw_indicator(-0.20, 0.70, 20)) + 4)),
    true_adequacy = true_adequacy
  )

  national_truth <- stats::weighted.mean(simulated_tbl$true_adequacy, simulated_tbl$female_population)
  list(calibration_tbl = simulated_tbl, national_truth = national_truth)
}

testthat::test_that("valid fixture has the expected contract", {
  calibration_tbl <- make_valid_adequacy_fixture()
  testthat::expect_s3_class(calibration_tbl, "tbl_df")
  testthat::expect_equal(base::nrow(calibration_tbl), 4L)
  testthat::expect_true(base::all(calibration_tbl$appointments_offered <= calibration_tbl$appointment_attempts))
  testthat::expect_true(base::all(calibration_tbl$female_population > 0))
})

testthat::test_that("invalid appointment counts fail closed", {
  calibration_tbl <- make_valid_adequacy_fixture()
  calibration_tbl$appointments_offered[[1]] <- 21L
  testthat::expect_error(
    calibrate_latent_adequacy(calibration_tbl, supply_fte = 1000, save_dir = base::tempdir()),
    "0 <= offered <= attempts"
  )
})

testthat::test_that("zero appointment attempts fail closed", {
  calibration_tbl <- make_valid_adequacy_fixture()
  calibration_tbl$appointment_attempts[[1]] <- 0L
  testthat::expect_error(
    calibrate_latent_adequacy(calibration_tbl, supply_fte = 1000, save_dir = base::tempdir()),
    "attempts > 0"
  )
})

testthat::test_that("invalid population weights fail closed", {
  calibration_tbl <- make_valid_adequacy_fixture()
  calibration_tbl$female_population[[1]] <- 0
  testthat::expect_error(
    calibrate_latent_adequacy(calibration_tbl, supply_fte = 1000, save_dir = base::tempdir()),
    "Population weights"
  )
})

testthat::test_that("duplicate geography identifiers fail closed", {
  calibration_tbl <- make_valid_adequacy_fixture()
  calibration_tbl$geography[[2]] <- calibration_tbl$geography[[1]]
  testthat::expect_error(
    calibrate_latent_adequacy(calibration_tbl, supply_fte = 1000, save_dir = base::tempdir()),
    "exactly one row"
  )
})

testthat::test_that("nonpositive supply fails closed", {
  calibration_tbl <- make_valid_adequacy_fixture()
  testthat::expect_error(
    calibrate_latent_adequacy(calibration_tbl, supply_fte = 0, save_dir = base::tempdir()),
    "positive number"
  )
})

testthat::test_that("synthetic data recover geographic adequacy", {
  simulated <- generate_synthetic_adequacy_data(n_counties = 50L, seed = 20260821L)
  fitted_calibration <- calibrate_latent_adequacy(simulated$county_data)
  eval_res <- evaluate_adequacy_synthetic_recovery(fitted_calibration, simulated$true_parameters)

  testthat::expect_true(eval_res$geographic_correlation >= 0.80)
  testthat::expect_true(eval_res$national_error <= 0.05)
  testthat::expect_true(eval_res$interval_coverage)
  testthat::expect_true(eval_res$pass_status)
})
