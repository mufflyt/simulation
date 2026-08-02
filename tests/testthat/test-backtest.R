# Guards for the historical back-test.
#
# The point of a back-test is destroyed by leakage, so these tests are about the
# integrity of the procedure rather than the accuracy of the result. A back-test
# that quietly saw the validation window would look excellent and mean nothing.

# ---- Leakage ---------------------------------------------------------------

test_that("no post-cutoff record enters model fitting", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  seed_microsimulation(1)

  cohort <- backtest_cohort_at(2020L)
  est <- backtest_entrant_estimate(2020L, agents = cohort)

  # Every audited read stopped at the cutoff.
  expect_silent(assert_no_leakage(2020L))

  # The cohort itself contains no post-cutoff certification year.
  expect_lte(max(cohort$cert_year), 2020L)
  expect_lte(max(cohort$entry_year), 2020L)

  # The entrant estimate used only years inside the pre-cutoff window.
  expect_lte(max(as.integer(names(est$yearly))), 2020L)
  expect_equal(unname(est$window), c(2018L, 2020L))
})

test_that("the leakage assertion actually fires when the cutoff is exceeded", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  invisible(.series_through(2023L, what = "deliberate leak"))
  expect_error(assert_no_leakage(2020L), "LEAKAGE")
})

test_that("an unaudited run cannot pass the leakage check", {
  # Silence must not read as success: with no audited reads the assertion fails.
  reset_leakage_audit()
  expect_error(assert_no_leakage(2020L), "no audited contract reads")
})

test_that("the entrant estimate cannot be computed from the leaking window", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  seed_microsimulation(1)
  cohort <- backtest_cohort_at(2020L)

  pre <- backtest_entrant_estimate(2020L, agents = cohort)
  # The main model's estimator uses 2018-2023 and would leak the whole
  # validation window; it must give a materially different answer.
  leaky <- observed_entrant_rate(from_year = 2018L)
  expect_gt(leaky$mean_net_growth, pre$net_growth * 1.4)
})

# ---- Target contract -------------------------------------------------------

test_that("the validated target is 1306 with every dimension recorded", {
  skip_if_not_installed("mufflyaccess")
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_equal(t$value, 1306L)
  expect_equal(t$geography, "national")
  expect_equal(t$board_pathway, "ABOG_PLUS_ABU")
  expect_equal(t$measure, "board_certified_active")
  expect_equal(t$contract_version, "3.0.0")
  expect_match(t$basis, "subspecialty")
  expect_true(nzchar(t$rationale))
})

test_that("the retired 1332/1329 values are identified and rejected", {
  skip_if_not_installed("mufflyaccess")
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_setequal(t$retired_values_rejected, c(1332, 1329))
  expect_false(t$value %in% t$retired_values_rejected)

  cand <- backtest_target_candidates()
  # Every candidate in the project is accounted for, with the dimension that
  # distinguishes it from the chosen target.
  expect_true(all(c(1306L, 1303L, 1332L, 1329L, 1027L, 1339L) %in% cand$value))
  expect_equal(cand$basis[cand$value == 1332L], "primary board cert year")
  expect_equal(cand$status[cand$value == 1332L], "retired")
})

test_that("a mismatched target is an ERROR, not a warning", {
  skip_if_not_installed("mufflyaccess")
  # Wrong pathway: ABOG-only excludes urology and returns 1,027. That count is
  # internally consistent, which is exactly why it is dangerous -- stating the
  # expected target turns a silent substitution into an error.
  expect_error(
    validate_backtest_target(board_pathway = "ABOG", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH"
  )
  # Wrong geography: CONUS returns 1,303.
  expect_error(
    validate_backtest_target(geography = "conus", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH"
  )
  # Wrong measure: roster_snapshot is a headcount, not an active count.
  expect_error(
    validate_backtest_target(measure = "roster_snapshot", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH|roster_snapshot|not available|measure"
  )
  # The correct combination passes, and states what it matched.
  ok <- validate_backtest_target(acknowledge_no_attrition = TRUE, expected_value = 1306L)
  expect_equal(ok$value, 1306L)

  # None of these may warn-and-continue.
  expect_no_warning(try(
    validate_backtest_target(board_pathway = "ABOG", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    silent = TRUE))
})

test_that("the attrition definition mismatch fails closed by default", {
  skip_if_not_installed("mufflyaccess")
  # The observed series applies no attrition; the model does. Proceeding
  # requires an explicit acknowledgement.
  expect_error(validate_backtest_target(), "NO ATTRITION")
  expect_silent(validate_backtest_target(acknowledge_no_attrition = TRUE))
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_false(t$observed_series_applies_attrition)
})

# ---- Reproducibility -------------------------------------------------------

test_that("fixed seeds reproduce identical summaries", {
  skip_if_not_installed("mufflyaccess")
  a <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 99L)
  b <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 99L)
  expect_identical(a$iterations, b$iterations)

  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  expect_identical(score_backtest_arm(a, obs, "x"), score_backtest_arm(b, obs, "x"))

  # A different seed must give a different draw, or the seeding is inert.
  d <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 100L)
  expect_false(identical(a$iterations$headcount, d$iterations$headcount))
})

# ---- Metric correctness ----------------------------------------------------

test_that("interval calculations match the empirical quantiles", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 200L, seed = 7L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  pred <- arm$iterations$headcount[arm$iterations$year == 2023]
  expect_equal(s$predicted_median, stats::median(pred))
  expect_equal(s$predicted_mean, mean(pred))
  expect_equal(s$pi95_lower, unname(stats::quantile(pred, 0.025)))
  expect_equal(s$pi95_upper, unname(stats::quantile(pred, 0.975)))
  expect_equal(s$pi80_lower, unname(stats::quantile(pred, 0.10)))
  expect_equal(s$pi80_upper, unname(stats::quantile(pred, 0.90)))

  # The 80% interval must sit inside the 95% interval.
  expect_gte(s$pi80_lower, s$pi95_lower)
  expect_lte(s$pi80_upper, s$pi95_upper)

  # Coverage flags must agree with the intervals they report.
  expect_equal(s$within_80, s$observed >= s$pi80_lower && s$observed <= s$pi80_upper)
  expect_equal(s$within_95, s$observed >= s$pi95_lower && s$observed <= s$pi95_upper)

  # Monte Carlo standard error is sd / sqrt(n).
  expect_equal(s$mc_standard_error, stats::sd(pred) / sqrt(length(pred)))
})

test_that("errors and annual changes are computed consistently", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 100L, seed = 11L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  expect_equal(s$absolute_error, s$predicted_median - s$observed)
  expect_equal(s$percent_error, 100 * s$absolute_error / s$observed)
  expect_equal(s$observed_annual_change, (1306 - 1099) / 3)
  expect_equal(s$predicted_annual_change, (s$predicted_median - 1099) / 3)
})

# ---- Units -----------------------------------------------------------------

test_that("observed and projected quantities are the same unit", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 50L, seed = 5L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  # Both sides are HEADCOUNT, never clinical FTE. Scoring a headcount series
  # against an FTE projection would be a unit error of roughly 10%.
  expect_true("headcount" %in% names(arm$iterations))
  expect_false(any(grepl("fte", names(arm$iterations), ignore.case = TRUE)))
  expect_equal(s$observed, 1306)
  expect_gt(s$predicted_median, 500)
  expect_lt(s$predicted_median, 3000)

  # The baseline the projection starts from is the observed baseline count.
  expect_equal(s$baseline_count, 1099)
  start <- arm$iterations$headcount[arm$iterations$year == 2020]
  expect_equal(unique(start), 1099)
})

test_that("the no-attrition arm removes departures entirely", {
  skip_if_not_installed("mufflyaccess")
  with_att <- run_backtest_arm("derived", 60, n_iterations = 40L, seed = 3L,
                               apply_attrition = TRUE)
  no_att <- run_backtest_arm("derived", 60, n_iterations = 40L, seed = 3L,
                             apply_attrition = FALSE)
  m <- function(x) stats::median(x$iterations$headcount[x$iterations$year == 2023])
  expect_gt(m(no_att), m(with_att))

  # Without attrition the trajectory is exactly baseline + entrants per year.
  expect_equal(m(no_att), 1099 + 3 * 60, tolerance = 2)
})
