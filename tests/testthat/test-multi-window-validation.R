# Multi-window validation and out-of-sample intervals (R/34).

test_that("windows are scored on both predictors and the error direction flips", {
  skip_if_not_installed("mufflyaccess")
  cert <- backtest_multi_window(cutoffs = 2016:2020, predictor = "certification")
  expect_gte(nrow(cert), 4)
  expect_true(all(cert$target_year - cert$cutoff_year == 3))
  # THE FINDING. "Every arm under-predicts" is a property of the 2020 window,
  # not of the engine: across cutoffs the sign of the error changes. A test that
  # only ever saw one window would report a structural bias that is not there.
  expect_true(any(cert$percent_error > 0))
  expect_true(any(cert$percent_error < 0))
})

test_that("the NRMP predictor is more stable across windows than the certification flow", {
  skip_if_not_installed("mufflyaccess")
  w <- 2017:2020
  cert <- backtest_multi_window(cutoffs = w, predictor = "certification")
  nrmp <- backtest_multi_window(cutoffs = w, predictor = "nrmp")
  expect_equal(nrow(cert), nrow(nrmp))
  # Lower spread of out-of-sample error is the criterion that matters for a
  # predictor, and it is not the same criterion as covering one endpoint.
  expect_lt(stats::sd(nrmp$percent_error), stats::sd(cert$percent_error))
  expect_lt(mean(abs(nrmp$percent_error)), mean(abs(cert$percent_error)))
})

test_that("the NRMP predictor never uses a report published after its cutoff", {
  skip_if_not_installed("mufflyaccess")
  w <- backtest_multi_window(cutoffs = 2017:2020, predictor = "nrmp")
  s <- nrmp_entrant_series()
  for (i in seq_len(nrow(w))) {
    avail <- s$positions_filled[s$available_by_year <= w$cutoff_year[i]]
    expect_equal(w$entrant_rate[i], mean(avail))
  }
  # The 2025 report must never reach any of these windows.
  expect_false(any(w$entrant_rate > 65))
})

test_that("the out-of-sample interval excludes the window it is scoring", {
  skip_if_not_installed("mufflyaccess")
  o <- backtest_oos_interval(target_cutoff = 2020L, cutoffs = 2017:2020)
  # THE ANTI-TUNING CONTRACT. If the scored window entered the error estimate,
  # the interval would be fitted to the endpoint it is judged against.
  expect_equal(o$n_train, 3L)
  expect_length(o$train_errors_pct, 3L)
  w <- backtest_multi_window(cutoffs = 2017:2020, predictor = "nrmp")
  expect_false(w$percent_error[w$cutoff_year == 2020] %in% o$train_errors_pct)
})

test_that("the out-of-sample interval is vastly wider than the arm's own", {
  skip_if_not_installed("mufflyaccess")
  o <- backtest_oos_interval()
  # Arm 5 reports width 8. Out-of-sample predictor error is an order of
  # magnitude larger, which is the whole finding of the calibration audit: the
  # arm quantifies parameter error and omits model error.
  expect_gt(o$upper - o$lower, 100)
  # t rather than normal, because the spread rests on three windows.
  expect_gt(o$t_quantile, 3)
})

test_that("two training windows are the minimum, and one is refused", {
  skip_if_not_installed("mufflyaccess")
  expect_error(backtest_oos_interval(target_cutoff = 2020L, cutoffs = c(2019L, 2020L)),
               "fewer than two training windows")
})

test_that("the capacity anchor is reported as unresolved, with its requirements", {
  s <- capacity_status()
  expect_false(s$resolved)
  expect_match(s$current_source, "physical-therapy")
  expect_match(s$why_unresolved, "assumes the question")
  req <- urps_capacity_survey_requirements()
  expect_true(all(c("clinical_fte", "annual_visits", "annual_procedures",
                    "operative_volume", "new_patient_capacity", "panel_size",
                    "wait_time") %in% req$variable))
  expect_true(all(nzchar(req$why_needed)))
})
