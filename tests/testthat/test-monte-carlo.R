# Monte Carlo adequacy of the reported bands (R/validation-monte_carlo.R).
#
# THE DEFECT THESE PIN. effective_fte_lo/hi were reported as 2.5%/97.5%
# quantiles regardless of iteration count, and iteration counts across this
# repository run 2, 3, 5, 25, 40. Below 40 those bounds are the sample minimum
# and maximum, which UNDERSTATE the spread -- so the failure mode is a band that
# looks tighter the less work you did, which is the wrong direction for a
# reader's confidence to move.

test_that("the iteration floor is the count at which the tails can be populated", {
  # One draw must be able to fall beyond each tail: n >= 2 / (1 - ci).
  expect_equal(mc_min_iterations(0.95), 40L)
  expect_equal(mc_min_iterations(0.80), 10L)
  expect_equal(mc_min_iterations(0.50), 4L)
  # Wider intervals are harder, not easier.
  expect_gt(mc_min_iterations(0.99), mc_min_iterations(0.95))
  expect_error(mc_min_iterations(1), "ci")
  expect_error(mc_min_iterations(0), "ci")
})

test_that("Monte Carlo error falls as 1/sqrt(n), and the median's exceeds the mean's", {
  set.seed(1)
  small <- monte_carlo_se(stats::rnorm(100, 1000, 50))
  large <- monte_carlo_se(stats::rnorm(10000, 1000, 50))
  # Ten times the draws, roughly a third the error.
  expect_lt(large$mcse_mean, small$mcse_mean / 2)
  # The median is a less efficient estimator of centre than the mean, so its
  # simulation error is larger -- by sqrt(pi/2) asymptotically.
  expect_gt(small$mcse_median, small$mcse_mean)
  expect_equal(small$mcse_median / small$mcse_mean, sqrt(pi / 2), tolerance = 1e-9)

  # Degenerate inputs return NA rather than a confident zero.
  expect_true(is.na(monte_carlo_se(5)$mcse_mean))
  expect_true(is.na(monte_carlo_se(numeric(0))$mcse_median))
})

test_that("diagnostics say whether a band describes the workforce or the simulator", {
  set.seed(2)
  draws <- stats::rnorm(1000, 1500, 100)
  d <- monte_carlo_diagnostics(draws, ci = 0.95)
  expect_true(d$bounds_are_quantiles)
  expect_equal(d$min_iterations_for_ci, 40L)
  # With 1,000 draws the simulation contributes a small fraction of the band.
  expect_lt(d$noise_share, 0.1)

  # With few draws it does not: same distribution, far noisier summary.
  set.seed(3)
  thin <- monte_carlo_diagnostics(stats::rnorm(8, 1500, 100), ci = 0.95)
  expect_false(thin$bounds_are_quantiles)
  expect_gt(thin$noise_share, d$noise_share)

  # A degenerate band reports NA, not Inf: no spread is not a catastrophe.
  flat <- monte_carlo_diagnostics(rep(1500, 50), ci = 0.95)
  expect_true(is.na(flat$noise_share))
})

test_that("an interval the iteration count cannot support is refused", {
  expect_true(assert_monte_carlo_adequate(40, ci = 0.95, mode = "strict"))
  expect_true(assert_monte_carlo_adequate(1000, ci = 0.95, mode = "strict"))

  expect_error(assert_monte_carlo_adequate(3, ci = 0.95, mode = "strict"),
               "at least 40")
  expect_false(suppressMessages(
    assert_monte_carlo_adequate(3, ci = 0.95, mode = "relaxed")))
  # The message must name the direction of the error. A band that is too NARROW
  # reads as precision, which is why silence here would be worse than useless.
  expect_message(assert_monte_carlo_adequate(3, ci = 0.95, mode = "relaxed"),
                 "NARROWER")

  # A narrower interval is satisfiable at a count that fails for 95%.
  expect_true(assert_monte_carlo_adequate(12, ci = 0.80, mode = "strict"))
  expect_error(assert_monte_carlo_adequate(12, ci = 0.95, mode = "strict"), "at least 40")
})

test_that("the supply panel carries its simulation error next to its band", {
  agents <- data.frame(
    provider_id = sprintf("P%03d", 1:40), subspecialty = "FPMRS",
    sex = rep(c("female", "male"), 20), age = seq(35, 68, length.out = 40),
    entry_year = 2015L, retirement_year = NA_real_, origin_cohort = "baseline"
  )
  ic <- calibrate_hours_intercept(agents$age, agents$sex)
  s <- suppressMessages(run_supply_microsimulation(
    agents, 2025:2027, 20, "FPMRS", n_iterations = 40,
    hours_intercept = ic, verbose = FALSE))$summary

  # In the same row as the bound it qualifies -- not in metadata a reader of a
  # saved panel would never open.
  expect_true(all(c("effective_fte_mcse", "headcount_mcse",
                    "n_iterations", "bounds_are_quantiles") %in% names(s)))
  expect_true(all(s$bounds_are_quantiles))
  expect_equal(unique(s$n_iterations), 40)
  expect_true(all(is.finite(s$effective_fte_mcse)))

  # The base year is DETERMINISTIC -- every iteration starts from the same
  # cohort, so its band has zero width and zero simulation error. Pinned rather
  # than filtered away: a non-zero spread in the first year would mean the
  # starting cohort had become random, which is a defect worth catching here.
  base_row <- s[s$year == min(s$year), ]
  # unname(): the bounds come from quantile(), which tags them "2.5%"/"97.5%",
  # and the name survives the subtraction.
  expect_equal(unname(base_row$effective_fte_hi - base_row$effective_fte_lo), 0)
  expect_equal(unname(base_row$effective_fte_mcse), 0)

  # Everywhere the band has width, simulation error must be small beside it or
  # the band is reporting the simulator rather than the workforce.
  proj <- s[s$year > min(s$year), ]
  width <- proj$effective_fte_hi - proj$effective_fte_lo
  expect_true(all(width > 0))
  expect_true(all(proj$effective_fte_mcse < width))
})
