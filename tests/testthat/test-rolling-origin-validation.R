# Rolling-origin vs leave-one-out validation (R/validation-backtest).
#
# The single most important property here is that rolling origin cannot see the
# future. If that breaks, the validation silently becomes the leaky comparator
# and reports better numbers than the model earns.

test_that("rolling origin trains only on outcomes that had already occurred", {
  skip_if_not_installed("mufflyaccess")
  r <- backtest_rolling_origin(cutoffs = 2013:2020, predictor = "nrmp", min_train = 2L)
  w <- backtest_multi_window(cutoffs = 2013:2020, predictor = "nrmp")

  for (i in seq_len(nrow(r))) {
    targets <- as.integer(strsplit(r$train_targets[i], ",")[[1]])
    # THE LEAKAGE CONTRACT. A training window's error is not observable until
    # its own target year, so every training target must be at or before the
    # origin of the forecast being bounded.
    expect_true(all(targets <= r$origin[i]),
                info = sprintf("origin %d trained on targets %s",
                               r$origin[i], r$train_targets[i]))
    expect_equal(length(targets), r$n_train[i])
  }
})

test_that("leave-one-out does leak, and the leak is counted rather than hidden", {
  skip_if_not_installed("mufflyaccess")
  l <- backtest_loo_validation(cutoffs = 2013:2020, predictor = "nrmp")
  # LOO is retained ONLY as the comparator that shows what rolling origin
  # corrects. If it ever stopped leaking, the comparison would be pointless and
  # this test should be the thing that notices.
  expect_true(all(l$n_train_future > 0))
  expect_equal(nrow(l), 8L)
})

test_that("the honest method is less accurate and far wider than the leaky one", {
  skip_if_not_installed("mufflyaccess")
  l <- backtest_loo_validation(cutoffs = 2013:2020, predictor = "nrmp")
  r <- backtest_rolling_origin(cutoffs = 2013:2020, predictor = "nrmp", min_train = 2L)
  # THE HEADLINE. LOO's apparent skill is future information, not calibration.
  expect_lt(stats::median(l$abs_pct_error), stats::median(r$abs_pct_error))
  expect_lt(mean(l$width), mean(r$width))
  # Rolling origin scores fewer origins because early ones have no usable
  # history -- excluded, not scored on an unestimable spread.
  expect_lt(nrow(r), nrow(l))
})

test_that("origins without enough history are excluded, not imputed", {
  skip_if_not_installed("mufflyaccess")
  r2 <- backtest_rolling_origin(cutoffs = 2013:2020, min_train = 2L)
  r4 <- backtest_rolling_origin(cutoffs = 2013:2020, min_train = 4L)
  expect_lt(nrow(r4), nrow(r2))
  expect_true(all(r4$n_train >= 4))
  # Demanding more history than exists must error rather than quietly return
  # nothing usable.
  expect_error(backtest_rolling_origin(cutoffs = 2013:2020, min_train = 99L),
               "no origin has")
})

test_that("results are deterministic: no RNG is involved", {
  skip_if_not_installed("mufflyaccess")
  set.seed(1); a <- backtest_rolling_origin(cutoffs = 2013:2020, min_train = 2L)
  set.seed(999); b <- backtest_rolling_origin(cutoffs = 2013:2020, min_train = 2L)
  expect_equal(a, b)
  set.seed(1); c1 <- backtest_multi_window(cutoffs = 2013:2020, predictor = "nrmp")
  set.seed(42); c2 <- backtest_multi_window(cutoffs = 2013:2020, predictor = "nrmp")
  expect_equal(c1, c2)
})

test_that("an unscorable window is dropped without breaking the run", {
  skip_if_not_installed("mufflyaccess")
  # 2024 has no contract count, so the 2021 cutoff cannot be scored. It must
  # disappear from the result rather than error or arrive as NA.
  w <- backtest_multi_window(cutoffs = 2019:2021, predictor = "nrmp")
  expect_false(2021 %in% w$cutoff_year)
  expect_true(all(is.finite(w$observed)))
  expect_gt(nrow(w), 0)
})

test_that("misses cluster by calendar period", {
  skip_if_not_installed("mufflyaccess")
  w <- backtest_multi_window(cutoffs = 2013:2020, predictor = "nrmp")
  early <- w$percent_error[w$cutoff_year <= 2014]
  late <- w$percent_error[w$cutoff_year >= 2015]
  # The establishment era is a different regime: the certification backlog was
  # still clearing, so the stock grew far faster than fellowship intake implies.
  expect_lt(min(early), -10)
  expect_true(all(abs(late) < 6))
})
