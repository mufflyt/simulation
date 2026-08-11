# Boundary value analysis for the pure numeric core.
#
# Companion to test-boundary-values.R, extending BVA to the exported numeric
# functions a downstream user calls directly: the forecast scores, the
# probability reporter, and the small statistical helpers. Every expected value
# below was established by RUNNING the function (a base-R transcription of each
# body, deps unavailable in the authoring environment), not by reading its
# arithmetic -- the point of BVA is to catch the operator that does not say what
# the author meant, and reading it back proves nothing.
#
# Each block probes the boundary itself: the value at, just below, and just above
# a threshold, and the degenerate case (zero width, single point, constant
# series) where an off-by-one or a divide-by-zero hides.

# ---- weighted_interval_score: the sharpness/coverage boundary ---------------

test_that("WIS charges width even when the point forecast is exact", {
  # obs == median, so the 0.5 * |y - med| term is 0, but the interval still has
  # width and the interval score charges it: a perfect point inside a wide band
  # is NOT a perfect forecast. (1200,1300,1400) at (.25,.5,.75): alpha = 0.5,
  # interval score = width 200, weight alpha/2 = 0.25 -> 50, /(1 + 0.5) = 33.33.
  expect_equal(
    weighted_interval_score(1300, c(1200, 1300, 1400), c(0.25, 0.5, 0.75)),
    100 / 3
  )
})

test_that("WIS is monotone in sharpness among intervals that all cover", {
  # Same observation, same coverage (both contain 10), narrower band must score
  # lower. This is the property the score exists to enforce.
  sharp <- weighted_interval_score(10, c(9, 10, 11), c(0.25, 0.5, 0.75))
  wide  <- weighted_interval_score(10, c(0, 10, 20), c(0.25, 0.5, 0.75))
  expect_equal(sharp, 1 / 3)
  expect_equal(wide, 10 / 3)
  expect_lt(sharp, wide)
})

test_that("WIS with only the median reduces to the absolute error", {
  # m = 1, quantile_levels = 0.5: no interval terms, acc = 0.5 * |y - med|,
  # divided by (0 + 0.5) -> exactly |y - med|. The degenerate lower boundary.
  expect_equal(weighted_interval_score(10, matrix(12, nrow = 1), 0.5), 2)
  expect_equal(weighted_interval_score(10, matrix(10, nrow = 1), 0.5), 0)
})

test_that("WIS recycles a length-1 observation across matrix rows", {
  q <- matrix(c(9, 10, 11, 0, 10, 20), nrow = 2, byrow = TRUE)
  expect_equal(weighted_interval_score(10, q, c(0.25, 0.5, 0.75)),
               c(1 / 3, 10 / 3))
})

# ---- forecast_probabilities: interval mass and exceedance boundaries --------

test_that("prediction-interval quantiles sit at the exact tail mass", {
  fp <- forecast_probabilities(1:100, prob_level = 0.90)
  expect_equal(fp$summary$n, 100L)
  expect_equal(fp$summary$median, 50.5)
  # 90% central -> 5th and 95th percentiles (type-7): 5.95 and 95.05.
  expect_equal(fp$summary$pi_lo, 5.95)
  expect_equal(fp$summary$pi_hi, 95.05)
})

test_that("exceedance probability is a strict inequality at the threshold", {
  # P(x > t) with x = 1:100. At t = 50, values 51..100 exceed -> 0.50 exactly;
  # the boundary value 50 itself is NOT counted (strict >), which is the whole
  # question a >= vs > confusion would get wrong.
  fp <- forecast_probabilities(1:100, exceed = c(0, 50, 100), direction = "above")
  expect_equal(fp$exceedance$probability, c(1, 0.5, 0))
  # 'below' is the mirror: P(x < 50) counts 1..49 -> 0.49.
  fb <- forecast_probabilities(1:100, exceed = 50, direction = "below")
  expect_equal(fb$exceedance$probability, 0.49)
})

# ---- series_mean_se: the n < 2 boundary ------------------------------------

test_that("series_mean_se needs at least two finite points", {
  expect_true(is.na(series_mean_se(5)))         # n = 1 -> NA, not 0 or an error
  expect_true(is.na(series_mean_se(c(NA, NA)))) # no finite points -> NA
  expect_equal(series_mean_se(c(50, 50, 50)), 0)  # zero variance -> 0, defined
  expect_equal(series_mean_se(c(50, 57, 53, 59, 59)), sd(c(50, 57, 53, 59, 59)) / sqrt(5))
  # NAs are dropped, not propagated: the finite subset drives the answer.
  expect_equal(series_mean_se(c(1, 2, 3, NA)), series_mean_se(c(1, 2, 3)))
})

# ---- calculate_proportion_ci: the Wilson interval clamps at 0 and 1 ---------

test_that("the Wilson interval is clamped to [0, 1] at the p = 0 and p = 1 edges", {
  z0 <- calculate_proportion_ci(0, 50)
  expect_equal(z0$proportion, 0)
  expect_equal(z0$lower_ci, 0)          # clamp, never negative
  expect_gt(z0$upper_ci, 0)             # Wilson keeps a non-zero upper at p = 0

  z1 <- calculate_proportion_ci(50, 50)
  expect_equal(z1$proportion, 1)
  expect_equal(z1$upper_ci, 1)          # clamp, never above 1
  expect_lt(z1$lower_ci, 1)

  zmid <- calculate_proportion_ci(12, 50)
  expect_equal(zmid$proportion, 0.24)
  expect_true(zmid$lower_ci < 0.24 && zmid$upper_ci > 0.24)
})

# ---- career_state_of: the age-band boundaries flip at 45 and 60 exactly -----

test_that("career state flips exactly at the mid/late onset ages", {
  # 44 -> early, 45 -> mid (>= boundary), 59 -> mid, 60 -> late (>= boundary).
  expect_equal(as.character(career_state_of(c(44, 45, 59, 60))),
               c("early_career", "mid_career", "mid_career", "late_career"))
  # Not yet entered practice is 'fellow' regardless of age; retired overrides.
  expect_equal(as.character(career_state_of(30, entered = FALSE)), "fellow")
  expect_equal(as.character(career_state_of(50, retired = TRUE)), "retired")
})
