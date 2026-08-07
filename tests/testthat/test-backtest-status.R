# Back-test status stamping (R/validation-backtest_status.R).
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
  expect_equal(s$n_arms, 10L)
  expect_equal(s$coverage_95, 0.20)
  expect_equal(s$coverage_80, 0.20)
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
  expect_match(interval_label(s), "8 of 10")
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

# ---- Record-vs-artifact drift ----------------------------------------------
#
# BACKTEST_RECORD_2020_2023 is a hand transcription of the scored artifact.
# Re-scoring rewrites the artifact and leaves the constant untouched, so the
# package can report a validation result no artifact supports. That has already
# happened once: extending the NRMP series moved arm 5 from -2.53% to -4.36%.

test_that("the frozen record matches the live artifact ARM BY ARM", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  live <- utils::read.csv(path, stringsAsFactors = FALSE)
  rec <- BACKTEST_RECORD_2020_2023

  # The old check compared three AGGREGATE fields. Two different records can
  # agree on coverage, worst error and arm count while differing arm by arm.
  expect_equal(nrow(rec), nrow(live))
  expect_equal(rec$percent_error, live$percent_error, tolerance = 1e-6)
  expect_equal(as.logical(rec$within_80), as.logical(live$within_80))
  expect_equal(as.logical(rec$within_95), as.logical(live$within_95))

  # ...and the aggregates the status stamp actually reports.
  frozen <- backtest_status()
  live_status <- backtest_status_from_summary(live)
  expect_equal(frozen$coverage_95, live_status$coverage_95)
  expect_equal(frozen$coverage_80, live_status$coverage_80)
  expect_equal(frozen$worst_percent_error, live_status$worst_percent_error,
               tolerance = 1e-6)
  expect_equal(frozen$n_arms, live_status$n_arms)
})

test_that("the recorded checksum identifies the artifact it was taken from", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  skip_if_not_installed("openssl")
  v <- verify_backtest_record(path)
  expect_true(v$checked)
  expect_true(v$checksum_matches)
  expect_equal(v$observed_sha256, BACKTEST_RECORD_SHA256)
  expect_equal(nchar(BACKTEST_RECORD_SHA256), 64L)
})

test_that("verification detects every kind of drift it exists to catch", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  base <- utils::read.csv(path, stringsAsFactors = FALSE)
  write_tmp <- function(d) { f <- tempfile(fileext = ".csv")
                             utils::write.csv(d, f, row.names = FALSE); f }

  # A re-scored percentage.
  d <- base; d$percent_error[5] <- d$percent_error[5] - 1.5
  v <- verify_backtest_record(write_tmp(d))
  expect_false(v$current)
  expect_true("percent_error" %in% v$mismatches$field)
  expect_true(any(grepl("Synthetic", v$mismatches$arm)))

  # A flipped coverage flag -- the field that decides `validated`.
  d <- base; d$within_95[3] <- !d$within_95[3]
  v <- verify_backtest_record(write_tmp(d))
  expect_false(v$current)
  expect_true("within_95" %in% v$mismatches$field)

  # An added or dropped arm.
  v <- verify_backtest_record(write_tmp(base[-1, ]))
  expect_false(v$current)
  expect_equal(v$mismatches$field, "n_arms")

  # Any change at all breaks the checksum, even one the row check tolerates.
  d <- base; d$mc_standard_error[1] <- d$mc_standard_error[1] + 1
  v <- verify_backtest_record(write_tmp(d))
  expect_false(v$checksum_matches)
})

test_that("a drift below tolerance is not reported as drift", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  d <- utils::read.csv(path, stringsAsFactors = FALSE)
  d$percent_error[2] <- d$percent_error[2] + 1e-9
  f <- tempfile(fileext = ".csv"); utils::write.csv(d, f, row.names = FALSE)
  # Six decimal places are transcribed; a 1e-9 difference is float noise, not a
  # re-score. Reporting it would train people to ignore the gate.
  expect_true(verify_backtest_record(f)$current)
})

test_that("the gate fails closed in strict mode and warns in relaxed", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  d <- utils::read.csv(path, stringsAsFactors = FALSE)
  d$percent_error[1] <- d$percent_error[1] - 5
  f <- tempfile(fileext = ".csv"); utils::write.csv(d, f, row.names = FALSE)

  expect_error(assert_backtest_record_current(mode = "strict", path = f),
               "no artifact supports")
  expect_message(assert_backtest_record_current(mode = "relaxed", path = f),
                 "no artifact supports")
  expect_false(suppressMessages(
    assert_backtest_record_current(mode = "relaxed", path = f)))

  # The message must name the fix, or the gate just annoys people.
  msg <- tryCatch(assert_backtest_record_current(mode = "strict", path = f),
                  error = conditionMessage)
  expect_match(msg, "emit_backtest_record")
  expect_match(msg, "BACKTEST_RECORD_SHA256")
  expect_match(msg, "SAME commit")
})

test_that("a missing artifact is unverifiable, not a failure", {
  # artifacts/ is .Rbuildignore'd, so absence is the normal case in an installed
  # build. Erroring there would make the package unusable where it ships.
  v <- verify_backtest_record(file.path(tempdir(), "definitely-absent.csv"))
  expect_false(v$checked)
  expect_true(is.na(v$current))
  expect_true(assert_backtest_record_current(mode = "strict",
                                             path = file.path(tempdir(), "nope.csv")))
})

test_that("the checksum is a bare character, not a classed object", {
  path <- backtest_artifact_path()
  skip_if(is.null(path), "frozen back-test record not reachable (artifacts/ is not shipped)")
  # WHY THIS IS PINNED. The first implementation used openssl::sha256(), which
  # returns a CLASSED object whose class survives as.character(). The result
  # printed as plain hex and compared TRUE under `==` but FALSE under
  # identical(), so an attribute rather than a digest decided whether the record
  # looked stale. digest is already a declared Import and returns bare text.
  h <- digest::digest(file = path, algo = "sha256")
  expect_identical(class(h), "character")
  expect_null(attributes(h))
  expect_equal(nchar(h), 64L)
  expect_identical(h, BACKTEST_RECORD_SHA256)
})
