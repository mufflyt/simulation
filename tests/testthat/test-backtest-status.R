# Back-test status stamping (R/38-backtest_status.R).
#
# The engine's only external validation FAILED coverage. The point of this
# module is that the failure travels with the numbers, so these tests check the
# stamping as much as the arithmetic.

test_that("the recorded status reports the coverage failure it actually has", {
  # Coverage moved 0/8 -> 2/8 when the entrant rate began being DRAWN per
  # iteration (run_backtest() accepted a param_spec and passed none, so the old
  # intervals were 0-40 providers wide). These expectations track a regenerated
  # artifact, not a hand-edited target: "the frozen record reproduces the live
  # artifact" below re-derives them from artifacts/ and fails if they drift.
  s <- backtest_status()
  expect_s3_class(s, "urps_backtest_status")
  expect_false(s$validated)
  expect_equal(s$n_arms, 8L)
  expect_equal(s$coverage_95, 0.25)
  expect_equal(s$coverage_80, 0.25)
  expect_lt(s$coverage_95, s$coverage_required)
  # Every arm under-predicted: a level problem, not scatter around the truth.
  expect_true(s$all_same_direction)
  expect_lt(s$worst_percent_error, -12)
  expect_match(s$source, "backtest_2020_to_2023_summary\\.csv")
})

test_that("status is derived from the arms, so passing coverage flips the verdict", {
  # Not asserted: feed the same function a hypothetical passing run.
  passing <- tibble::tibble(
    within_80 = c(TRUE, TRUE, TRUE, FALSE),
    within_95 = c(TRUE, TRUE, TRUE, TRUE),
    percent_error = c(1.2, -0.8, 2.1, -3.0)
  )
  s <- backtest_status_from_summary(passing)
  expect_true(s$validated)
  expect_equal(s$coverage_95, 1)
  expect_false(s$all_same_direction)
  expect_match(interval_label(s), "forecast interval")
  expect_true(assert_forecast_intervals_validated(s, mode = "strict"))

  # One arm short of the bar is still not validated.
  borderline <- tibble::tibble(within_95 = c(TRUE, TRUE, FALSE, FALSE),
                               percent_error = c(1, 1, 1, 1))
  expect_false(backtest_status_from_summary(borderline)$validated)
})

test_that("interval language is refused while coverage fails", {
  s <- backtest_status()
  expect_match(interval_label(s), "NOT a validated forecast interval")
  expect_match(interval_label(s), "6 of 8")
  expect_error(assert_forecast_intervals_validated(s, mode = "strict"),
               "not validated")
  expect_message(assert_forecast_intervals_validated(s, mode = "relaxed"),
                 "Monte Carlo range")
})

test_that("an empty or malformed scoring table is refused", {
  expect_error(backtest_status_from_summary(tibble::tibble(within_95 = logical(0),
                                                           percent_error = numeric(0))),
               "no scored arms")
  expect_error(backtest_status_from_summary(tibble::tibble(a = 1)), "missing column")
})

test_that("a supply run carries its validation status and interval label", {
  set.seed(4)
  agents <- initialize_provider_agents(50, "FPMRS", 2025)
  agents$sex <- "female"
  ic <- calibrate_hours_intercept(agents$age, agents$sex)
  r <- run_supply_microsimulation(agents, 2025:2027, 5, "FPMRS", n_iterations = 3,
                                  hours_intercept = ic, allow_fixed_parameters = TRUE,
                                  verbose = FALSE)
  # effective_fte_lo/hi are in the summary; the caveat must be too.
  expect_true(all(c("effective_fte_lo", "effective_fte_hi") %in% names(r$summary)))
  expect_s3_class(r$scenario$backtest, "urps_backtest_status")
  expect_false(r$scenario$backtest$validated)
  expect_match(r$scenario$interval_label, "NOT a validated forecast interval")
})

test_that("the stamp survives on an object and reads back", {
  x <- data.frame(year = 2025, supply_headcount = 1300)
  expect_null(stamped_backtest_status(x))
  y <- stamp_backtest_status(x)
  expect_s3_class(stamped_backtest_status(y), "urps_backtest_status")
  expect_false(stamped_backtest_status(y)$validated)
  # Stamping must not disturb the data itself -- the projection contract is
  # validated on a fixed 13-column schema.
  expect_equal(as.data.frame(y)[, names(x), drop = FALSE], x)
  expect_identical(names(y), names(x))
})

test_that("the definition-matched subset is reported without loosening the bar", {
  # Half the arms score an attrition-applied projection against a series that
  # removes nobody. Reporting them together averages two different estimands.
  s <- tibble::tibble(
    arm = c("1. Derived [no-attrition]", "1. Derived", "2. Derived [no-attrition]", "2. Derived"),
    percent_error = c(-3, -9, -8, -15),
    within_80 = c(TRUE, FALSE, FALSE, FALSE),
    within_95 = c(TRUE, FALSE, FALSE, FALSE)
  )
  st <- backtest_status_from_summary(s)
  expect_equal(st$n_definition_matched, 2)
  expect_equal(st$coverage_95_definition_matched, 0.5)
  # `validated` is still computed over ALL arms: the subset must not be a
  # back door to a pass.
  expect_equal(st$coverage_95, 0.25)
  expect_false(st$validated)
})

test_that("a subset that would pass cannot validate the engine on its own", {
  s <- tibble::tibble(
    arm = c("A [no-attrition]", "B [no-attrition]", "C", "D"),
    percent_error = c(-1, -2, -14, -15),
    within_80 = c(TRUE, TRUE, FALSE, FALSE),
    within_95 = c(TRUE, TRUE, FALSE, FALSE)
  )
  st <- backtest_status_from_summary(s)
  expect_equal(st$coverage_95_definition_matched, 1)   # subset is perfect
  expect_false(st$validated)                            # and it still fails
})

test_that("the status records that one target year cannot estimate coverage", {
  st <- backtest_status()
  expect_false(st$coverage_is_estimable)
  expect_match(st$coverage_caveat, "not independent trials")
  expect_output(print(st), "not a coverage estimate")
})

test_that("the frozen record reproduces the live artifact", {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  path <- file.path(root[1], "artifacts", "backtest_2020_to_2023_summary.csv")
  skip_if_not(file.exists(path))
  live <- backtest_status_from_summary(utils::read.csv(path, stringsAsFactors = FALSE))
  frozen <- backtest_status()
  # If these drift, the transcribed record in this file is stale and every
  # projection is carrying a status that no artifact supports.
  expect_equal(frozen$coverage_95, live$coverage_95)
  expect_equal(frozen$worst_percent_error, live$worst_percent_error, tolerance = 1e-6)
  expect_equal(frozen$n_arms, live$n_arms)
})
