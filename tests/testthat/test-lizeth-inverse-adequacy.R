# Lizeth inverse adequacy calibration.
#
# The forward map (clear_access) is exercised elsewhere; here we pin the inverse
# machinery: the weighted quantile, the mean-preserving lognormal adequacy
# distribution, deterministic catchment scores, the wait-target extractor, and
# the fit/bootstrap wrappers. Values for the dependency-free pieces are hand
# derived; the fit-level tests assert self-consistency (loss ~ 0 at the truth)
# and structure rather than brittle exact recovery, since two parameters against
# three quantiles are only weakly identified.

test_that("urps_weighted_quantile matches hand-computed step quantiles", {
  q <- suppressMessages(
    urps_weighted_quantile(x = 1:4, w = c(1, 1, 1, 1), probs = c(0.25, 0.5, 0.75))
  )
  # cumulative weights 0.25/0.5/0.75/1 -> first >= p picks 1, 2, 3.
  expect_equal(unname(q), c(1, 2, 3))
  expect_named(q, c("p25", "p50", "p75"))
})

test_that("urps_weighted_quantile responds to weights and guards inputs", {
  # Mass shifted onto the large value pulls the median up.
  q <- suppressMessages(
    urps_weighted_quantile(x = c(1, 2, 100), w = c(1, 1, 50), probs = 0.5)
  )
  expect_equal(unname(q), 100)
  expect_error(suppressMessages(urps_weighted_quantile(1:3, c(0, 0, 0))),
               "positive weight")
  expect_error(suppressMessages(urps_weighted_quantile(1:3, c(1, 1, 1), probs = 1.5)),
               "between 0 and 1")
})

test_that("urps_adequacy_distribution preserves the weighted mean", {
  z <- c(-2, -1, 0, 1, 2)
  w <- c(1, 2, 3, 4, 5)
  d <- urps_adequacy_distribution(mean_adequacy = 1.3, log_sd = 0.4, z = z, weights = w)
  expect_equal(stats::weighted.mean(d, w), 1.3)
  # log_sd 0 collapses to a constant; positive log_sd is monotone in z.
  expect_equal(urps_adequacy_distribution(1.1, 0, z, w), rep(1.1, length(z)))
  expect_true(all(diff(d) > 0))
})

test_that("urps_adequacy_distribution rejects invalid inputs", {
  z <- c(-1, 0, 1)
  expect_error(urps_adequacy_distribution(0, 0.2, z), "mean_adequacy")
  expect_error(urps_adequacy_distribution(1, -0.1, z), "log_sd")
  expect_error(urps_adequacy_distribution(1, 0.2, z, weights = c(1, 1)), "equal length")
})

test_that("urps_catchment_scores are standardized and rank-preserving", {
  ct <- data.frame(demand_workload = rep(100, 40))
  s <- suppressMessages(urps_catchment_scores(ct))
  expect_length(s, 40L)
  expect_equal(mean(s), 0, tolerance = 1e-8)
  expect_equal(stats::sd(s), 1, tolerance = 1e-8)
  # With an adequacy column, the score rank must equal the adequacy rank.
  ct2 <- data.frame(demand_workload = rep(100, 5), adq = c(5, 3, 1, 4, 2))
  s2 <- suppressMessages(urps_catchment_scores(ct2, adequacy_col = "adq"))
  expect_equal(rank(s2), rank(ct2$adq))
  expect_error(suppressMessages(urps_catchment_scores(data.frame(demand_workload = 1:2))),
               "three catchments")
})

test_that("lizeth_wait_targets extracts ordered quantiles and filters", {
  calls <- tibble::tibble(
    wait_business_days = c(1:60, -5, NA_real_, 10),
    appointment_obtained = c(rep(TRUE, 60), TRUE, TRUE, FALSE)
  )
  targets <- suppressMessages(lizeth_wait_targets(calls))
  expect_named(targets, c("p25", "p50", "p75"))
  expect_true(targets[["p25"]] <= targets[["p50"]])
  expect_true(targets[["p50"]] <= targets[["p75"]])
  # The negative wait, the NA, and the non-obtained call are excluded, leaving
  # exactly 1:60, whose type-7 median is 30.5.
  expect_equal(targets[["p50"]], stats::median(1:60))
  expect_error(
    suppressMessages(lizeth_wait_targets(tibble::tibble(
      wait_business_days = 1:10, appointment_obtained = rep(TRUE, 10)))),
    "Fewer than 20"
  )
  expect_error(
    suppressMessages(lizeth_wait_targets(tibble::tibble(x = 1))),
    "Missing Lizeth"
  )
})

test_that("forward_lizeth_adequacy returns named quantiles and is monotone in adequacy", {
  ct <- tibble::tibble(demand_workload = rep(100, 40))
  z <- suppressMessages(urps_catchment_scores(ct))
  low <- suppressMessages(
    forward_lizeth_adequacy(ct, mean_adequacy = 1.2, log_sd = 0.3, z = z, wait_scale = 30)
  )
  high <- suppressMessages(
    forward_lizeth_adequacy(ct, mean_adequacy = 2.5, log_sd = 0.3, z = z, wait_scale = 30)
  )
  expect_named(low$quantiles, c("p25", "p50", "p75"))
  expect_true("wait_time" %in% names(low$catchments))
  # More capacity per unit demand -> shorter waits.
  expect_lt(high$quantiles[["p50"]], low$quantiles[["p50"]])
})

test_that("lizeth_adequacy_loss is ~0 at the generating parameters", {
  ct <- tibble::tibble(demand_workload = rep(100, 40))
  z <- suppressMessages(urps_catchment_scores(ct))
  truth <- suppressMessages(
    forward_lizeth_adequacy(ct, mean_adequacy = 1.3, log_sd = 0.25, z = z, wait_scale = 30)
  )
  targets <- truth$quantiles
  loss <- suppressMessages(lizeth_adequacy_loss(
    parameters = c(log(1.3), log(0.25)),
    catchments = ct, targets = targets, z = z, wait_scale = 30
  ))
  expect_lt(loss, 1e-8)
})

test_that("fit_lizeth_inverse_adequacy runs, stays in bounds, and labels itself", {
  ct <- tibble::tibble(demand_workload = rep(100, 40))
  calls <- tibble::tibble(
    wait_business_days = rep(1:60, 2),
    appointment_obtained = TRUE,
    npi = rep(sprintf("n%02d", 1:24), 5)
  )
  fit <- suppressMessages(
    fit_lizeth_inverse_adequacy(ct, calls, wait_scale = 30)
  )
  expect_true(fit$mean_adequacy >= 0.30 && fit$mean_adequacy <= 2.00)
  expect_identical(fit$calibration_status, "fitted_to_lizeth_wait_distribution")
  expect_equal(nrow(fit$comparison), 3L)
  expect_length(fit$fitted_quantiles, 3L)
  expect_equal(fit$ratio_to_reference, fit$mean_adequacy / fit$reference_adequacy)
})

test_that("bootstrap_lizeth_inverse_adequacy returns an interval and needs npi", {
  ct <- tibble::tibble(demand_workload = rep(100, 40))
  calls <- tibble::tibble(
    wait_business_days = rep(1:60, 2),
    appointment_obtained = TRUE,
    npi = rep(sprintf("n%02d", 1:24), 5)
  )
  boot <- suppressMessages(
    bootstrap_lizeth_inverse_adequacy(ct, calls, wait_scale = 30, n_boot = 4L, seed = 1L)
  )
  expect_true(all(c("estimate", "p2_5", "median", "p97_5") %in% names(boot$interval)))
  expect_true(boot$probability_below_reference >= 0 &&
                boot$probability_below_reference <= 1)
  expect_error(
    suppressMessages(bootstrap_lizeth_inverse_adequacy(
      ct, tibble::tibble(wait_days = 1:30, appointment_obtained = TRUE),
      wait_scale = 30, n_boot = 2L)),
    "npi"
  )
})
