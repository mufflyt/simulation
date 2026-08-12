# The forecast-calibration layer replaces two fixed assumptions the supply
# back-test flagged: a timeless entrant rate and hazard_cv = 0. These tests pin
# the contract of the four functions on small fabricated fixtures -- an entrant
# series with a level+slope break and a provider-year departure panel with a
# rising age hazard -- and lock the three bugs fixed on import: the data-mask
# collision on `draw`, the base-vs-stats `factor()`, and the single-year
# model.matrix() contrasts error.

sd_entrant_fixture <- function(seed = 1L) {
  set.seed(seed)
  yrs <- 2008:2020
  rate <- 40 + 1.0 * (yrs - 2008)
  rate[yrs >= 2016] <- rate[yrs >= 2016] + 12 + 3 * (yrs[yrs >= 2016] - 2016)
  tibble::tibble(year = yrs, entrants = stats::rpois(length(yrs), rate))
}

sd_departure_fixture <- function(seed = 2L, per_year = 500L) {
  set.seed(seed)
  purrr::map_dfr(2008:2020, function(y) {
    age <- round(stats::runif(per_year, 50, 88))
    p <- 1 - exp(-exp(-6 + 0.08 * (age - 50)))
    tibble::tibble(provider_id = paste0("P", y, "_", seq_len(per_year)),
                   year = y, age = age, departed = stats::rbinom(per_year, 1, p))
  })
}

test_that("calibrate estimates a break, an empirical hazard, and a positive CV", {
  cal <- suppressMessages(calibrate_urps_supply_dynamics(
    sd_entrant_fixture(), sd_departure_fixture(),
    forecast_years = 2021:2025, n_draws = 200L, seed = 42L))

  # The empirical retirement CV is now READ OFF the fitted hazard, never 0.
  expect_true(is.finite(cal$retirement_hazard_cv))
  expect_gt(cal$retirement_hazard_cv, 0)

  expect_true(all(c("entrant_model", "entrant_break_year", "entrant_draws",
                    "departure_model", "departure_hazard_draws",
                    "retirement_hazard_cv", "diagnostics") %in% names(cal)))
  # A break is selected inside the observation window.
  expect_true(cal$entrant_break_year >= 2008 && cal$entrant_break_year <= 2020)
  # Entrant draws span every forecast year and carry per-draw variation.
  expect_setequal(unique(cal$entrant_draws$year), 2021:2025)
  expect_gt(stats::sd(cal$entrant_draws$entrants), 0)
})

test_that("advancing one year removes and adds providers without leaking a draw column", {
  cal <- suppressMessages(calibrate_urps_supply_dynamics(
    sd_entrant_fixture(), sd_departure_fixture(),
    forecast_years = 2021:2025, n_draws = 100L, seed = 42L))
  set.seed(5)
  pop <- tibble::tibble(provider_id = paste0("B", 1:200),
                        age = stats::runif(200, 45, 80))
  adv <- suppressMessages(advance_urps_supply_one_year(
    pop, simulation_year = 2022L, draw = 1L, calibration = cal))

  # THE FIX THIS PINS. The hazard join must not carry its own `draw` column into
  # the roster, or the next year's join fans out. Only provider_id + age remain.
  expect_setequal(names(adv), c("provider_id", "age"))
  # Entrants were added at the entrant age; the population is non-empty.
  expect_gt(nrow(adv), 0)
  expect_true(any(adv$age == 35))
})

test_that("providers below the retirement-model floor never depart (age not floored to 50)", {
  cal <- suppressMessages(calibrate_urps_supply_dynamics(
    sd_entrant_fixture(), sd_departure_fixture(),
    forecast_years = 2021:2025, n_draws = 100L, seed = 42L))
  # Everyone is 40 -> 41 after aging, below the age-50 departure model. The bug
  # floored the hazard-lookup age to 50, charging sub-floor providers the age-50
  # retirement probability; the fix leaves them unmatched (probability 0), so no
  # original provider may be dropped by departure. Deterministic under the fix:
  # departure_probability is exactly 0, so runif() < 0 is always FALSE.
  pop <- tibble::tibble(provider_id = paste0("Y", 1:300), age = rep(40, 300))
  adv <- suppressMessages(advance_urps_supply_one_year(
    pop, simulation_year = 2023L, draw = 4L, calibration = cal))
  expect_true(all(pop$provider_id %in% adv$provider_id))
})

test_that("advance actually filters to the requested draw (data-mask collision)", {
  cal <- suppressMessages(calibrate_urps_supply_dynamics(
    sd_entrant_fixture(), sd_departure_fixture(),
    forecast_years = 2021:2025, n_draws = 100L, seed = 42L))
  # Two different draws generally yield different entrant counts, which is only
  # possible if the internal filter reads the ARGUMENT, not the same-named
  # column (the bug: .data$draw == draw compared the column to itself).
  pop <- tibble::tibble(provider_id = paste0("B", 1:50), age = rep(40, 50))
  n_by_draw <- vapply(1:20, function(d) {
    nrow(suppressMessages(advance_urps_supply_one_year(
      pop, simulation_year = 2023L, draw = d, calibration = cal)))
  }, numeric(1))
  expect_gt(length(unique(n_by_draw)), 1L)
})

test_that("backtest reports bias, coverage, and width together", {
  cal <- suppressMessages(calibrate_urps_supply_dynamics(
    sd_entrant_fixture(), sd_departure_fixture(),
    forecast_years = 2021:2025, n_draws = 100L, seed = 42L))
  pop <- tibble::tibble(provider_id = paste0("B", 1:200),
                        age = stats::runif(200, 45, 80))
  obs <- tibble::tibble(year = 2021:2025,
                        observed_supply = c(200, 205, 212, 220, 231))
  bt <- suppressMessages(backtest_urps_supply_calibration(
    pop, obs, cal, start_year = 2021L, end_year = 2025L,
    n_draws = 40L, seed = 7L))

  expect_true(all(c("mean_percent_error", "coverage_95", "mean_interval_width")
                  %in% names(bt$metrics)))
  expect_true(is.finite(bt$metrics$mean_percent_error))
  expect_true(bt$metrics$coverage_95 >= 0 && bt$metrics$coverage_95 <= 1)
  expect_true(all(c("percent_error", "covered_95", "interval_width")
                  %in% names(bt$forecast_summary)))
  expect_true(nzchar(bt$summary_sentence))
})

test_that("decompose splits the miss into break and model, both regimes scored", {
  ent <- sd_entrant_fixture()
  dep <- sd_departure_fixture()
  pop <- tibble::tibble(provider_id = paste0("B", 1:200),
                        age = stats::runif(200, 45, 80))
  obs <- tibble::tibble(year = 2016:2020,
                        observed_supply = c(200, 204, 209, 215, 223))
  dec <- suppressMessages(decompose_urps_forecast_miss(
    ent, dep, pop, obs, start_year = 2016L, end_year = 2020L,
    n_draws = 40L, seed = 3L))

  expect_setequal(dec$metrics$regime, c("train", "oracle"))
  expect_true(all(c("signed_percent_bias", "slope_bias", "coverage_95",
                    "mean_interval_width") %in% names(dec$metrics)))
  expect_setequal(dec$decomposition$component,
                  c("unforeseeable_regime_break", "deficient_entrant_model"))
  expect_true(all(is.finite(dec$decomposition$slope_error_clinicians)))
  # The two components sum to the leakage-free (train) slope error.
  train_slope <- dec$metrics$slope_bias[dec$metrics$regime == "train"]
  expect_equal(sum(dec$decomposition$slope_error_clinicians), train_slope,
               tolerance = 1e-6)
})
