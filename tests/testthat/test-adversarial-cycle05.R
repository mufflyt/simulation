# Adversarial cycle 05 -- the aging recurrence, forecast probabilities, and
# cumulative-vs-flow series.
#
# Cycle 04 left a bug class open: cumulative counters read as if they were
# per-period rates. Tests 7 and 8 discharge it on the one function that takes a
# cumulative series and a flow series as separate arguments.
#
# The sweep also turned up the THIRD occurrence of a class this ledger has now
# seen in three modules: a probability-valued input accepted outside [0, 1].
# Cycle 03 found it as negative provider counts, cycle 04 as sum-to-one
# validators without a range check, and here it produces negative PREVALENCE.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the aging recurrence handles a one-age and a zero-age grid", {
  # `for (i in 2:length(incidence))` counts DOWN at length 1, so a single-age
  # grid grew the prevalence vector to length 2 and then died inside R with
  # "replacement has length zero" -- an index failure wearing the costume of an
  # input error.
  one <- prevalence_from_incidence(0.01, remission = 0.02, p0 = 0.15)
  expect_length(one, 1L)
  expect_equal(one, 0.15)          # nothing to age into: prevalence is p0

  expect_length(prevalence_from_incidence(numeric(0), remission = 0.02), 0L)

  # Two ages is the smallest grid where the recurrence does any work, and it
  # must take exactly one step.
  two <- prevalence_from_incidence(c(0.10, 0.10), remission = 0, p0 = 0)
  expect_equal(two, c(0, 0.10))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: every argument to the recurrence is closed on the unit interval", {
  ages <- rep(0.02, 5)
  expect_silent(prevalence_from_incidence(rep(0, 5), remission = 0, p0 = 0))
  expect_silent(prevalence_from_incidence(rep(1, 5), remission = 1, p0 = 1))
  expect_error(prevalence_from_incidence(ages, remission = 1 + 1e-9), "in \\[0, 1\\]")
  expect_error(prevalence_from_incidence(ages, remission = -1e-9), "in \\[0, 1\\]")
  expect_error(prevalence_from_incidence(c(ages, 1 + 1e-9), remission = 0), "in \\[0, 1\\]")
  expect_error(prevalence_from_incidence(ages, remission = 0, p0 = 1.5), "in \\[0, 1\\]")
  expect_error(prevalence_from_incidence(ages, remission = c(0.1, 0.2)), "single probability")
  expect_error(prevalence_from_incidence(c(0.1, NA), remission = 0), "finite")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the prediction-interval mass is open at 0 and 1", {
  x <- stats::qnorm(seq(0.001, 0.999, length.out = 999), 1000, 50)
  expect_error(forecast_probabilities(x, prob_level = 0))
  expect_error(forecast_probabilities(x, prob_level = 1))
  narrow <- forecast_probabilities(x, prob_level = 1e-6)$summary
  wide <- forecast_probabilities(x, prob_level = 1 - 1e-6)$summary
  # A vanishing mass collapses to the median; a near-total mass spans the draws.
  expect_lt(narrow$pi_hi - narrow$pi_lo, wide$pi_hi - wide$pi_lo)
  expect_equal(narrow$pi_lo, narrow$pi_hi, tolerance = 1e-3)
  expect_gte(wide$pi_lo, min(x))
  expect_lte(wide$pi_hi, max(x))

  # Exceedance is a STRICT inequality, so a threshold sitting exactly on draws
  # excludes them. With draws -2,-1,0,1,2 and a "P(any shortage)" threshold of
  # 0, the zero draw is not a shortage.
  e <- forecast_probabilities(c(-2, -1, 0, 1, 2), exceed = 0, direction = "above")$exceedance
  expect_equal(e$probability, 0.4)
  eb <- forecast_probabilities(c(-2, -1, 0, 1, 2), exceed = 0, direction = "below")$exceedance
  expect_equal(eb$probability, 0.4)
  # The two directions plus the ties account for every draw exactly once.
  expect_equal(e$probability + eb$probability + mean(c(-2, -1, 0, 1, 2) == 0), 1)
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the recurrence obeys the DisMod identity in both degenerate directions", {
  # dP/da = (1-P)i - Pr. With no onset, prevalence decays geometrically by
  # (1-r); with no remission it is non-decreasing. If either fails, the sign of
  # the remission term is wrong and every fitted incidence absorbs the error.
  decay <- prevalence_from_incidence(rep(0, 6), remission = 0.1, p0 = 0.3)
  expect_equal(decay, 0.3 * (1 - 0.1)^(0:5))
  expect_true(all(diff(decay) < 0))

  grow <- prevalence_from_incidence(rep(0.05, 20), remission = 0, p0 = 0)
  expect_true(all(diff(grow) > 0))
  expect_true(all(grow >= 0 & grow <= 1))

  # Total remission empties the prevalent pool each year, so only the (1-P)
  # susceptible fraction can acquire: p[i] = (1 - p[i-1]) * incidence[i-1].
  # It is NOT simply last year's incidence -- one step of history survives
  # through the susceptible pool, and that is the term a sign error would drop.
  inc <- c(0.1, 0.2, 0.3, 0.4)
  memoryless <- prevalence_from_incidence(inc, remission = 1, p0 = 0.9)
  expect_equal(memoryless, c(0.9, (1 - 0.9) * 0.1, (1 - 0.01) * 0.2, (1 - 0.198) * 0.3))

  # With constant incidence that recurrence has the fixed point p* = i/(1+i),
  # which is where a long grid must settle under full remission.
  i_const <- 0.1
  settled <- prevalence_from_incidence(rep(i_const, 200), remission = 1, p0 = 0)
  expect_equal(settled[200], i_const / (1 + i_const), tolerance = 1e-9)

  # Certain onset with no remission absorbs everyone after one step.
  expect_equal(prevalence_from_incidence(rep(1, 4), remission = 0), c(0, 1, 1, 1))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: prevalence_from_onset is the same function, not a copy that can drift", {
  # Two exported names for one estimand is a standing invitation for one to be
  # patched and the other not.
  expect_identical(prevalence_from_onset, prevalence_from_incidence)
  inc <- seq(0.001, 0.05, length.out = 30)
  expect_identical(prevalence_from_onset(inc, 0.03, p0 = 0.02),
                   prevalence_from_incidence(inc, 0.03, p0 = 0.02))
  # The alias inherits the guards rather than routing around them.
  expect_error(prevalence_from_onset(inc, remission = 2), "in \\[0, 1\\]")
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: exceedance probabilities are monotone in the threshold and count NAs before dropping", {
  set.seed(505)
  draws <- stats::rnorm(2000, mean = 8, sd = 4)
  thresholds <- c(0, 5, 10, 15, 20)
  fp <- forecast_probabilities(draws, exceed = thresholds, direction = "above")
  # P(x > t) cannot rise as t rises. A non-monotone exceedance table means the
  # thresholds and the probabilities were paired in different orders.
  expect_false(is.unsorted(rev(fp$exceedance$probability)))
  expect_equal(fp$exceedance$threshold, thresholds)

  below <- forecast_probabilities(draws, exceed = thresholds, direction = "below")
  expect_false(is.unsorted(below$exceedance$probability))
  # Above and below partition the draws at every threshold (no ties here).
  expect_equal(fp$exceedance$probability + below$exceedance$probability,
               rep(1, length(thresholds)))

  # n is the count actually summarised and n_na the count discarded; reporting
  # n as the pre-drop length would understate the interval's own uncertainty.
  with_na <- forecast_probabilities(c(draws[1:100], rep(NA_real_, 7)))
  expect_equal(with_na$summary$n, 100L)
  expect_equal(with_na$summary$n_na, 7L)
  expect_error(forecast_probabilities(rep(NA_real_, 5)), "no non-NA draws")
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the prediction interval brackets the median and widens with its mass", {
  set.seed(506)
  draws <- stats::rnorm(5000, 1200, 90)
  levels <- c(0.5, 0.8, 0.95, 0.99)
  widths <- vapply(levels, function(l) {
    s <- forecast_probabilities(draws, prob_level = l)$summary
    expect_lte(s$pi_lo, s$median)
    expect_gte(s$pi_hi, s$median)
    s$pi_hi - s$pi_lo
  }, numeric(1))
  expect_false(is.unsorted(widths))
  # The reported level must be the one asked for, not the default.
  expect_equal(forecast_probabilities(draws, prob_level = 0.8)$summary$prob_level, 0.8)
  # A symmetric sample puts the mean and median together; a large gap would mean
  # one of the two is being computed on a different vector.
  s <- forecast_probabilities(draws)$summary
  expect_equal(s$mean, s$median, tolerance = 0.02 * stats::sd(draws))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a cumulative stock and an annual flow are not interchangeable", {
  # The carried-forward class. entrant_regime_rolling_validation() takes BOTH:
  # `series` is an annual entrant flow and `cumulative_series` is a stock, and
  # it forecasts by adding a summed flow to a stock base. Handing it the flow
  # in the stock's place must not quietly produce a plausible answer.
  set.seed(507)
  flow <- data.frame(year = 2005:2020, count = as.integer(rpois(16, 55)))
  stock <- data.frame(year = 2005:2020, n_active = cumsum(flow$count) + 400)

  ok <- entrant_regime_rolling_validation(flow, stock, horizon = 3L, n_draws = 200L,
                                          verbose = FALSE)
  expect_true(nrow(ok$folds) > 0L)
  # The base of each fold is the STOCK at the cutoff, so predictions must be on
  # the stock's scale, not the flow's.
  expect_true(all(ok$folds$predicted_median > max(flow$count)))
  expect_true(all(ok$folds$observed > max(flow$count)))

  # Passing the flow as the stock produces predictions on the wrong scale --
  # they must not silently coincide with the correct ones.
  wrong <- entrant_regime_rolling_validation(flow, data.frame(year = flow$year,
                                                              n_active = flow$count),
                                             horizon = 3L, n_draws = 200L, verbose = FALSE)
  expect_false(isTRUE(all.equal(wrong$folds$observed, ok$folds$observed)))
  # A stock is non-decreasing when nobody leaves; a flow is not, and that is
  # the property that tells them apart.
  expect_false(is.unsorted(stock$n_active))
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a duplicated year in the stock series is skipped, not recycled", {
  # base and obs are looked up by year. A duplicated year makes them length 2,
  # which would recycle through `base + rowSums(paths)` and corrupt every
  # quantile while still returning a tidy-looking fold.
  set.seed(508)
  flow <- data.frame(year = 2005:2020, count = as.integer(rpois(16, 55)))
  stock <- data.frame(year = 2005:2020, n_active = cumsum(flow$count) + 400)
  clean <- entrant_regime_rolling_validation(flow, stock, horizon = 3L, n_draws = 200L,
                                             verbose = FALSE)

  dup <- rbind(stock, stock[stock$year == 2015L, ])
  dirty <- entrant_regime_rolling_validation(flow, dup, horizon = 3L, n_draws = 200L,
                                             verbose = FALSE)
  # Folds touching 2015 as base or target drop out; none is silently corrupted.
  expect_lt(nrow(dirty$folds), nrow(clean$folds))
  expect_false(any(dirty$folds$cutoff_year == 2015L))
  expect_false(any(dirty$folds$target_year == 2015L))
  expect_true(all(is.finite(dirty$folds$predicted_median)))
  expect_equal(length(dirty$folds$pi95_lower), nrow(dirty$folds))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: no admissible input drives prevalence outside [0, 1]", {
  # The recurrence is unbounded by construction, so the guard is the only thing
  # keeping it a probability. Fuzz the whole admissible corner of the space.
  set.seed(509)
  grid <- expand.grid(r = c(0, 0.01, 0.3, 0.99, 1), p0 = c(0, 0.5, 1))
  for (k in seq_len(nrow(grid))) {
    inc <- stats::runif(40)            # any admissible incidence path
    p <- prevalence_from_incidence(inc, remission = grid$r[k], p0 = grid$p0[k])
    expect_true(all(p >= 0 & p <= 1),
                info = sprintf("remission=%g p0=%g gave prevalence in [%g, %g]",
                               grid$r[k], grid$p0[k], min(p), max(p)))
    expect_length(p, 40L)
  }

  # And the values that USED to escape are refused rather than clamped: clamping
  # would hide a mis-specified remission behind a plausible-looking curve.
  expect_error(prevalence_from_incidence(rep(0, 5), remission = 1.5, p0 = 0.4))
  expect_error(prevalence_from_incidence(rep(1.5, 5), remission = 0))
  expect_error(prevalence_from_incidence(rep(-0.2, 5), remission = 0, p0 = 0.5))
})
