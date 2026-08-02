# Guards for parameter uncertainty in the supply engine.
#
# The 2020->2023 back-test found the engine's intervals 6.5-8.2x too narrow
# because every coefficient was held fixed across replicates. These tests lock
# in the fix and, just as importantly, lock in the honesty about what still
# cannot be quantified.

test_that("the sampling standard error of a series mean is correct", {
  x <- c(40, 48, 10)
  expect_equal(series_mean_se(x), stats::sd(x) / sqrt(3))
  expect_true(is.na(series_mean_se(5)))       # one observation has no SE
  expect_true(is.na(series_mean_se(numeric(0))))
})

test_that("a spec reports which parameters are quantified and which are not", {
  spec <- supply_parameter_spec(entrant_series = c(40, 48, 10), entrant_mean = 60.7,
                                departures = 28)
  expect_true(spec$quantified[["entrant_rate"]])
  # The published retirement and hours schedules carry no standard errors, so
  # inventing a spread for them would manufacture false confidence.
  expect_false(spec$quantified[["retirement_hazard"]])
  expect_false(spec$quantified[["hours"]])

  with_cv <- supply_parameter_spec(entrant_series = c(40, 48, 10), entrant_mean = 60.7,
                                   hazard_cv = 0.15)
  expect_true(with_cv$quantified[["retirement_hazard"]])
})

test_that("draws are centred on the point estimate and actually vary", {
  spec <- supply_parameter_spec(entrant_series = c(40, 48, 10), entrant_mean = 60.7,
                                departures = 28)
  set.seed(3)
  d <- vapply(1:4000, function(i) draw_supply_parameters(spec)$entrants, numeric(1))
  # Centred on net mean + departures, with the series' own standard error.
  expect_equal(mean(d), mean(c(40, 48, 10)) + 28, tolerance = 0.05 * 60.7)
  expect_equal(stats::sd(d), series_mean_se(c(40, 48, 10)), tolerance = 0.1 * 11.6)
  expect_gt(stats::sd(d), 0)
  expect_true(all(d >= 0))
})

test_that("a hazard CV scales the whole schedule and stays a probability", {
  spec <- supply_parameter_spec(entrant_mean = 55, hazard_cv = 0.20)
  set.seed(5)
  draws <- lapply(1:200, function(i) draw_supply_parameters(spec)$retirement_schedule)
  # Every draw is a rescaling of the base schedule, so the shape is preserved.
  ratios <- vapply(draws, function(s) unname(s[["60"]] / s[["55"]]), numeric(1))
  expect_equal(stats::sd(ratios), 0, tolerance = 1e-9)
  expect_true(all(vapply(draws, function(s) all(s >= 0 & s <= 1), logical(1))))
  # Centred on 1: the multiplier must not bias the hazard level.
  mult <- vapply(draws, function(s) unname(s[["60"]] / RETIREMENT_HAZARD_BY_AGE[["60"]]), numeric(1))
  expect_equal(mean(mult), 1, tolerance = 0.05)
})

test_that("fitted models are drawn through the shared .param_draw()", {
  survey <- data.frame(
    age = rep(c(35, 45, 55, 65), each = 30),
    sex = rep(c("male", "female"), 60),
    clinical_hours = stats::rnorm(120, 40, 5)
  )
  m <- fit_clinical_hours_model(survey, df = 2L)
  spec <- supply_parameter_spec(entrant_mean = 55, hours_model = m)
  expect_true(spec$quantified[["hours"]])
  set.seed(2)
  d <- draw_supply_parameters(spec)
  expect_equal(length(d$hours_coef), length(stats::coef(m)[!is.na(stats::coef(m))]))
  # Two draws must differ, or the coefficients are not being redrawn.
  expect_false(identical(d$hours_coef, draw_supply_parameters(spec)$hours_coef))
})

test_that("running with fixed parameters is refused in strict mode", {
  expect_error(assert_parameter_uncertainty(NULL, mode = "strict"), "6.5-8.2x too narrow")
  expect_false(assert_parameter_uncertainty(NULL, mode = "relaxed"))
  spec <- supply_parameter_spec(entrant_series = c(40, 48, 10), entrant_mean = 60)
  expect_true(assert_parameter_uncertainty(spec, mode = "strict"))
})

test_that("drawing parameters widens the interval without moving the median", {
  skip_if_not_installed("mufflyaccess")
  set.seed(11)
  a <- agents_from_certification_cohorts(2023L)
  spec <- entrant_spec_from_series(a)

  fx <- run_supply_microsimulation(a, 2023:2030, spec$entrant_mean, "FPMRS",
                                   n_iterations = 150, verbose = FALSE)
  dr <- run_supply_microsimulation(a, 2023:2030, spec$entrant_mean, "FPMRS",
                                   n_iterations = 150, param_spec = spec, verbose = FALSE)
  f <- fx$summary[fx$summary$year == 2030, ]
  d <- dr$summary[dr$summary$year == 2030, ]

  # The point estimate must be essentially unchanged: this adds uncertainty, it
  # does not recalibrate.
  expect_equal(d$headcount_median, f$headcount_median, tolerance = 0.03)
  # The interval must widen materially.
  expect_gt((d$headcount_hi - d$headcount_lo) / (f$headcount_hi - f$headcount_lo), 2)

  expect_match(dr$scenario$parameter_uncertainty, "entrant_rate")
  expect_match(fx$scenario$parameter_uncertainty, "none")
})

test_that("the back-test spec uses pre-cutoff data only", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  seed_microsimulation(1)
  c0 <- backtest_cohort_at(2020L)
  spec <- entrant_spec_from_series(c0, from_year = 2018L, through_year = 2020L)
  expect_silent(assert_no_leakage(2020L))
  # Exactly the three pre-cutoff years, no more.
  expect_equal(length(spec$entrant_series), 3L)
  expect_equal(sort(spec$entrant_series), sort(c(40, 48, 10)))
})
