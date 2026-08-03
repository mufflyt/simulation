illustrative_series <- function() {
  data.frame(
    year = 2012:2023,
    certification_count = c(
      33L, 36L, 41L, 39L, 45L, 44L, 40L, 48L, 10L, 81L, 54L, 72L
    )
  )
}

test_that("forecasting into the training window is refused as leakage", {
  baseline_fit <- fit_certification_baseline(
    illustrative_series(), 2020L, "loglinear", verbose = FALSE
  )
  expect_error(
    forecast_certification(baseline_fit, 2019:2021, 1.0, 0.95, FALSE),
    "this is leakage"
  )
  expect_error(
    forecast_certification(baseline_fit, 2020L, 1.0, 0.95, FALSE),
    "this is leakage"
  )
})

test_that("dispersion is estimated, not assumed to be one", {
  baseline_fit <- fit_certification_baseline(
    illustrative_series(), 2020L, "loglinear", verbose = FALSE
  )
  expect_gt(baseline_fit$dispersion, 1)
})

test_that("inflation widens intervals monotonically", {
  baseline_fit <- fit_certification_baseline(
    illustrative_series(), 2020L, "loglinear", verbose = FALSE
  )
  narrow <- forecast_certification(baseline_fit, 2021:2023, 1.0, 0.95, FALSE)
  wide   <- forecast_certification(baseline_fit, 2021:2023, 3.7, 0.95, FALSE)
  expect_true(all(
    (wide$upper_bound - wide$lower_bound) >=
      (narrow$upper_bound - narrow$lower_bound)
  ))
  expect_equal(wide$expected_count, narrow$expected_count)
})

test_that("widening does not move the point estimate", {
  baseline_fit <- fit_certification_baseline(
    illustrative_series(), 2020L, "loglinear", verbose = FALSE
  )
  expected_by_inflation <- vapply(c(1.0, 1.5, 3.7, 8.0), function(f) {
    forecast_certification(baseline_fit, 2023L, f, 0.95, FALSE)$expected_count
  }, numeric(1))
  expect_equal(length(unique(round(expected_by_inflation, 10))), 1L)
})

test_that("coverage is monotone non-decreasing in inflation", {
  coverage_by_inflation <- vapply(c(1.0, 2.0, 4.0, 8.0), function(f) {
    rolling_origin_coverage(
      illustrative_series(), 6L, 3L, f, 0.95, verbose = FALSE
    )$empirical_coverage
  }, numeric(1))
  expect_false(is.unsorted(coverage_by_inflation))
})

test_that("lower bounds never go negative", {
  wide_coverage <- rolling_origin_coverage(
    illustrative_series(), 6L, 3L, 9.0, 0.95, verbose = FALSE
  )
  expect_true(all(wide_coverage$fold_results$lower_bound >= 0))
})

test_that("solved inflation is the smallest that reaches nominal", {
  solved <- solve_interval_inflation(
    illustrative_series(), 6L, 3L, 0.95, 10, verbose = FALSE
  )
  expect_identical(solved$tier, "solved")
  if (solved$reached_nominal) {
    coverage_just_below <- rolling_origin_coverage(
      illustrative_series(), 6L, 3L,
      max(solved$solved_inflation - 0.05, 1), 0.95, verbose = FALSE
    )$empirical_coverage
    expect_lt(coverage_just_below, 0.95)
  } else {
    expect_lt(solved$attained_coverage, 0.95)
  }
})

test_that("a low grid ceiling reports failure rather than the ceiling", {
  capped <- solve_interval_inflation(
    illustrative_series(), 6L, 3L, 0.95, 1.2, verbose = FALSE
  )
  expect_false(capped$reached_nominal)
  expect_lte(capped$solved_inflation, 1.2)
})

test_that("the gate refuses too few folds before scoring coverage", {
  two_fold_report <- rolling_origin_coverage(
    illustrative_series(), 10L, 3L, 1.0, 0.95, verbose = FALSE
  )
  expect_lt(two_fold_report$n_folds, 3L)
  expect_error(
    assert_interval_coverage_publishable(two_fold_report, 99, 3L, FALSE),
    "one realization of the forecast problem"
  )
})

test_that("HALL_OF_SHAME: COVID break is not absorbed into the trend", {
  shame_fixture <- read.csv(
    testthat::test_path("fixtures", "covid_certification_break_hall_of_shame.csv")
  )
  baseline_fit <- fit_certification_baseline(
    shame_fixture[, c("year", "certification_count")],
    fit_through_year = 2020L, trend_family = "loglinear", verbose = FALSE
  )
  break_forecast <- forecast_certification(
    baseline_fit, 2021:2023, 1.0, 0.95, FALSE
  )
  observed_break <- shame_fixture$certification_count[
    shame_fixture$year %in% 2021:2023
  ]
  # Uninflated intervals fitted through 2020 must NOT cover the post-break window.
  expect_false(all(
    observed_break >= break_forecast$lower_bound &
      observed_break <= break_forecast$upper_bound
  ))
})

test_that("gapped and duplicated series fail loudly", {
  gapped_series <- illustrative_series()[-5, ]
  expect_error(
    rolling_origin_coverage(gapped_series, 6L, 3L, 1.0, 0.95, FALSE),
    "has gaps"
  )
  duplicated_series <- rbind(illustrative_series(), illustrative_series()[1, ])
  expect_error(
    rolling_origin_coverage(duplicated_series, 6L, 3L, 1.0, 0.95, FALSE),
    "duplicated"
  )
})

test_that("intercept trend family fits and forecasts without error", {
  baseline_fit <- fit_certification_baseline(
    illustrative_series(), 2020L, "intercept", verbose = FALSE
  )
  fc <- forecast_certification(baseline_fit, 2021:2023, 1.0, 0.95, FALSE)
  expect_equal(nrow(fc), 3L)
  expect_true(all(fc$expected_count > 0))
})

test_that("assert_interval_coverage_publishable passes when ratio is below ceiling", {
  good_report <- rolling_origin_coverage(
    illustrative_series(), 6L, 3L, 8.0, 0.95, verbose = FALSE
  )
  expect_true(assert_interval_coverage_publishable(good_report, 99, 3L, FALSE))
})
