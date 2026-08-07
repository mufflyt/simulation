# Decision-relevant probability statements from Monte Carlo draws
# (R/validation-forecast_probabilities.R). A microsimulation should report a distribution
# and the probabilities a reader decides on, not a single number.

test_that("forecast_probabilities returns median, PI, and exceedance probabilities", {
  x <- c(rep(0, 25), rep(10, 75))            # 75% of draws at 10, 25% at 0
  fp <- forecast_probabilities(x, prob_level = 0.90, exceed = c(0, 5), direction = "above",
                               label = "gap_pct")
  expect_equal(fp$summary$label, "gap_pct")
  expect_equal(fp$summary$n, 100)
  expect_equal(fp$summary$median, 10)
  expect_equal(fp$exceedance$probability[fp$exceedance$threshold == 5], 0.75)
  # P(x > 0) counts strictly-positive draws only (the 75 tens)
  expect_equal(fp$exceedance$probability[fp$exceedance$threshold == 0], 0.75)
  expect_true(grepl("75.0%", fp$exceedance$statement[fp$exceedance$threshold == 5]))
})

test_that("NA draws (failed PSA evaluations) are counted, not silently dropped", {
  x <- c(1, 2, 3, NA, NA)
  fp <- forecast_probabilities(x, exceed = 0)
  expect_equal(fp$summary$n, 3)
  expect_equal(fp$summary$n_na, 2)
  expect_equal(fp$summary$mean, 2)
})

test_that("direction = 'below' flips the tail (e.g. P(access declines))", {
  x <- seq(-5, 4, by = 1)                    # 10 draws, 5 negative
  fp <- forecast_probabilities(x, exceed = 0, direction = "below")
  expect_equal(fp$exceedance$probability, 0.5)
  expect_true(grepl("<", fp$exceedance$statement))
})

test_that("workforce_gap_probabilities phrases shortage statements off PSA draws", {
  set.seed(3)
  draws <- data.frame(gap_pct = rnorm(2000, 8, 4), gap_fte = rnorm(2000, 120, 60))
  psa <- list(draws = draws, output_names = c("gap_pct", "gap_fte"))
  wg <- workforce_gap_probabilities(psa, metric = "gap_pct", shortage_thresholds = c(0, 5, 10))
  expect_equal(nrow(wg$probabilities), 3)
  expect_true(grepl("any shortage", wg$probabilities$statement[1]))
  expect_true(grepl("exceeds 10%", wg$probabilities$statement[3]))
  # probabilities are monotone non-increasing in the threshold
  expect_true(all(diff(wg$probabilities$probability) <= 0))
  # matches a direct computation
  expect_equal(wg$probabilities$probability[2], mean(draws$gap_pct > 5))
})

test_that("workforce_gap_probabilities accepts a raw vector and errors on a bad object", {
  wg <- workforce_gap_probabilities(rnorm(500, 5, 2), shortage_thresholds = 0)
  expect_equal(nrow(wg$probabilities), 1)
  expect_error(workforce_gap_probabilities(list(nope = 1), metric = "gap_pct"),
               "numeric vector of gap draws")
})
