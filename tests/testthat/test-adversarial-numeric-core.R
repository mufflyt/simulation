# Adversarial tests for the pure numeric core.
#
# Companion to test-adversarial-guards.R. Each case is an ATTACK on a scoring or
# reporting function: an input that is the wrong shape, degenerate, or built to
# make the function return a plausible-but-wrong number instead of refusing. A
# function that computes coverage from an inverted interval, or a WIS from a
# quantile vector with no median, is not merely imprecise -- it reports a
# quantity that is not the one it claims. These pin the refusal.
#
# Behaviours below were confirmed by running a base-R transcription of each body
# (package deps unavailable in the authoring environment); the assertions are
# expect_error / expect_true on the guard, not on a fragile message.

# ---- weighted_interval_score --------------------------------------------------

test_that("WIS refuses a quantile forecast with no median", {
  # Without the 0.5 level there is no point term and the 'central interval'
  # decomposition is undefined; the function must refuse, not silently pick a
  # nearby quantile as the median.
  expect_error(
    weighted_interval_score(10, c(9, 11), c(0.25, 0.75)),
    "median"
  )
})

test_that("WIS refuses quantile levels outside the open (0, 1) interval", {
  expect_error(weighted_interval_score(10, c(9, 10, 11), c(0, 0.5, 1)))
  expect_error(weighted_interval_score(10, c(9, 10, 11), c(-0.1, 0.5, 0.9)))
})

test_that("WIS refuses a level/column count mismatch", {
  # Three columns, two levels: silently recycling would score the wrong pairs.
  expect_error(weighted_interval_score(10, matrix(c(9, 10, 11), nrow = 1),
                                       c(0.25, 0.5)))
})

test_that("WIS refuses an observation vector that neither matches nor is scalar", {
  q <- matrix(c(9, 10, 11, 0, 10, 20), nrow = 2, byrow = TRUE)
  # length(y) is 3 but there are 2 rows: rep_len would partial-recycle and score
  # row 1 against y[1], row 2 against y[2], silently dropping y[3].
  expect_error(weighted_interval_score(c(10, 10, 10), q, c(0.25, 0.5, 0.75)))
})

# ---- forecast_scorecard: the inverted-interval attack -----------------------

test_that("forecast_scorecard refuses an inverted interval instead of scoring it", {
  # THE ATTACK. lower > upper gives coverage 0 and a negative mean_width; the
  # zero coverage then reads as 'the model never covers the truth' when the
  # bounds are merely swapped. The guard converts a silent-wrong-number into a
  # loud refusal.
  d <- data.frame(observed = 100, predicted = 100, lower = 110, upper = 90)
  expect_error(forecast_scorecard(d), "invert|>")
})

test_that("forecast_scorecard requires the observed and point columns", {
  expect_error(forecast_scorecard(data.frame(observed = 1)))          # no point
  expect_error(forecast_scorecard(data.frame(predicted = 1)))         # no observed
})

test_that("a single non-finite row does not null every scorecard metric", {
  # ok <- is.finite(y) & is.finite(yhat) must drop the bad row, not poison the
  # aggregate: one NA observation should still leave a finite RMSE from the rest.
  d <- data.frame(observed = c(100, NA), predicted = c(100, 50),
                  lower = c(90, 40), upper = c(110, 60))
  sc <- forecast_scorecard(d)
  expect_true(is.finite(sc$rmse))
})

# ---- forecast_probabilities / workforce_gap_probabilities -------------------

test_that("forecast_probabilities refuses when every draw is NA", {
  # na.rm drops the NAs, leaving nothing; computing a mean of length-0 is a
  # silent NaN, so the function stops instead.
  expect_error(forecast_probabilities(c(NA_real_, NA_real_)), "non-NA")
})

test_that("forecast_probabilities refuses a degenerate prob_level", {
  expect_error(forecast_probabilities(1:10, prob_level = 0))
  expect_error(forecast_probabilities(1:10, prob_level = 1))
})

test_that("forecast_probabilities counts dropped NAs rather than hiding them", {
  # The report must say how many draws were unusable; a silent drop understates
  # the failure rate of the underlying simulation.
  fp <- forecast_probabilities(c(1:9, NA))
  expect_equal(fp$summary$n, 9L)
  expect_equal(fp$summary$n_na, 1L)
})

test_that("workforce_gap_probabilities refuses an unknown metric column", {
  # A PSA result lacking the requested metric must be rejected, not silently
  # coerced -- otherwise 'gap_pct' vs 'gap_fte' confusion is invisible.
  expect_error(
    workforce_gap_probabilities(list(draws = data.frame(other = 1:10)),
                                metric = "gap_pct")
  )
})

# ---- calculate_proportion_ci: the zero / NA denominator ---------------------

test_that("calculate_proportion_ci degrades safely on a zero or NA denominator", {
  # x / 0 is a silent NaN and the Wilson formula divides by n; the function must
  # short-circuit to a labelled NA result rather than returning NaN bounds.
  z0 <- calculate_proportion_ci(1, 0)
  expect_true(is.na(z0$proportion))
  expect_match(z0$note, "Zero denominator")
  zna <- calculate_proportion_ci(1, NA)
  expect_true(is.na(zna$proportion))
})

# ---- haversine_km: antipodal numerical stability ----------------------------

test_that("haversine_km stays real at antipodal points", {
  # Floating point can push the argument of asin() just past 1 at antipodes,
  # which would return NaN; the pmin(1, .) clamp keeps it real and equal to the
  # half-circumference. Identity distance is exactly 0.
  expect_equal(haversine_km(40, -105, 40, -105), 0)
  expect_false(is.nan(haversine_km(0, 0, 0, 180)))
  expect_equal(haversine_km(0, 0, 0, 180), pi * 6371.0088, tolerance = 1e-6)
})
