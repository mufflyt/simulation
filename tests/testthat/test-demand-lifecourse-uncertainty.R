# Parameter-uncertainty intervals for the life-course demand trajectory (R/demand-lifecourse_uncertainty).

pop_by_age_year <- tidyr::expand_grid(year = c(2025L, 2030L), age = 40:85) %>%
  dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))

test_that("lifecourse_param_uncertainty exposes coef_sd and logit_sd", {
  u <- lifecourse_param_uncertainty()
  expect_true(all(c("coef_sd", "logit_sd") %in% names(u)))
  expect_true(u$coef_sd > 0 && u$logit_sd > 0)
})

test_that("the trajectory CI returns ordered lo <= median <= hi bounds", {
  ci <- lifecourse_demand_trajectory_ci(pop_by_age_year, n = 3000, n_draws = 40, seed = 1,
                                  pathway = valid_pathway())
  expect_true(all(c("year", "care_seeking_national", "care_seeking_national_lo",
                    "care_seeking_national_hi", "service_units_national",
                    "service_units_national_lo", "service_units_national_hi")
                  %in% names(ci)))
  expect_true(all(ci$service_units_national_lo <= ci$service_units_national))
  expect_true(all(ci$service_units_national   <= ci$service_units_national_hi))
  expect_true(all(ci$care_seeking_national_lo <= ci$care_seeking_national))
  expect_true(all(ci$care_seeking_national    <= ci$care_seeking_national_hi))
})

test_that("wider parameter uncertainty produces wider intervals", {
  narrow <- lifecourse_demand_trajectory_ci(pop_by_age_year, n = 3000, n_draws = 60, seed = 2,
              param_uncertainty = list(coef_sd = 0.05, logit_sd = 0.05),
                                  pathway = valid_pathway())
  wide   <- lifecourse_demand_trajectory_ci(pop_by_age_year, n = 3000, n_draws = 60, seed = 2,
              param_uncertainty = list(coef_sd = 0.30, logit_sd = 0.30),
                                  pathway = valid_pathway())
  w_narrow <- narrow$service_units_national_hi - narrow$service_units_national_lo
  w_wide   <- wide$service_units_national_hi   - wide$service_units_national_lo
  expect_gt(mean(w_wide), mean(w_narrow))

  # The original form of this test compared two widths that were BOTH zero and
  # failed only because 0 > 0 is false. Assert non-degeneracy directly, so a
  # future regression cannot pass by making both sides equally broken.
  expect_gt(mean(w_narrow), 0)
  expect_gt(mean(w_wide), 0)
  expect_true(all(narrow$service_units_national_lo <= narrow$service_units_national))
  expect_true(all(narrow$service_units_national <= narrow$service_units_national_hi))
})

test_that("a degenerate interval is refused outright", {
  out <- tibble::tibble(year = 2025, su = 100, su_lo = 100, su_hi = 100)
  varying <- tibble::tibble(year = rep(2025, 5), su = c(80, 90, 100, 110, 120))
  expect_error(assert_interval_nondegenerate(out, varying), "Degenerate prediction interval")

  # A genuinely constant draw set is not a defect.
  constant <- tibble::tibble(year = rep(2025, 5), su = rep(100, 5))
  expect_true(assert_interval_nondegenerate(out, constant))
})

test_that("the CI trajectory feeds the exporter's lo/hi contract columns", {
  ci <- lifecourse_demand_trajectory_ci(pop_by_age_year, n = 3000, n_draws = 30, seed = 3,
                                  pathway = valid_pathway())
  # Placeholder transitions: declared exploratory so the calibration gate writes it.
  out <- suppressMessages(export_hdmm_demand_contract(
    ci, output_directory = tempfile("hdmm_"), verbose = FALSE, allow_uncalibrated = TRUE))
  expect_true(all(c("national_cases_lo", "national_cases_hi",
                    "denominator_index_lo", "denominator_index_hi") %in% names(out$data)))
  t6 <- out$data[out$data$denominator_tier == "tier6_procedural", ]
  expect_true(all(t6$national_cases_lo <= t6$national_cases, na.rm = TRUE))
})
