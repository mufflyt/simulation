# Preregistered rolling-origin evaluation (R/validation-preregistration.R).
#
# The governance layer that removes the "designed after the miss" contamination
# for FUTURE origins: freeze + hash the spec, then refuse to evaluate unless the
# live spec still matches. Tests the hash's order-independence + value-sensitivity,
# the immutability of a written record, the anti-contamination guard, and the
# leakage-freeness of the rolling-origin scoring.

spec_a <- list(form = "count ~ nrmp", horizon = 3L, predictor = "nrmp",
               origins = 2013:2020, metric = "mape")
spec_a_reordered <- list(metric = "mape", origins = 2013:2020, predictor = "nrmp",
                         horizon = 3L, form = "count ~ nrmp")
spec_b <- modifyList(spec_a, list(horizon = 2L))   # a genuine change

series <- data.frame(year = 2005:2020, count = cumsum(c(1000, rep(30, 15))))
persistence <- function(train, target_time) train$count[which.max(train$year)]

test_that("the spec hash is order-independent but value-sensitive", {
  expect_identical(preregister_spec(spec_a, tempfile(), frozen_at = "2026-08-05")$spec_hash,
                   preregister_spec(spec_a_reordered, tempfile(), frozen_at = "2026-08-05")$spec_hash)
  expect_false(identical(
    preregister_spec(spec_a, tempfile(), frozen_at = "2026-08-05")$spec_hash,
    preregister_spec(spec_b, tempfile(), frozen_at = "2026-08-05")$spec_hash))
})

test_that("frozen_at is required", {
  expect_error(preregister_spec(spec_a, tempfile()), "frozen_at")
})

test_that("a preregistration record is written, readable, and immutable", {
  p <- tempfile(fileext = ".txt")
  rec <- preregister_spec(spec_a, p, frozen_at = "2026-08-05", notes = "frozen pre-2024 vintage")
  expect_true(file.exists(p))
  expect_match(rec$spec_hash, "^[0-9a-f]{64}$")
  # re-registering the SAME spec is idempotent
  expect_silent(preregister_spec(spec_a, p, frozen_at = "2026-08-05"))
  # a DIFFERENT spec at the same path is refused (that is the contamination)
  expect_error(preregister_spec(spec_b, p, frozen_at = "2026-08-05"),
               "DIFFERENT spec")
  # ... unless explicitly forced (to correct a pre-data mistake)
  expect_silent(preregister_spec(spec_b, p, frozen_at = "2026-08-05", force = TRUE))
})

test_that("assert_spec_matches_prereg passes on a match and refuses a changed spec", {
  p <- tempfile(fileext = ".txt")
  preregister_spec(spec_a, p, frozen_at = "2026-08-05")
  expect_true(assert_spec_matches_prereg(spec_a, p))              # accepts a path
  pr <- preregister_spec(spec_a, tempfile(), frozen_at = "2026-08-05")
  expect_true(assert_spec_matches_prereg(spec_a_reordered, pr))   # accepts a record; order-free
  expect_error(assert_spec_matches_prereg(spec_b, pr),
               "does not match the preregistration")
})

test_that("rolling-origin scoring is leakage-free and one-step-ahead by default", {
  res <- rolling_origin_evaluation(series, "year", "count", origins = 2012:2019,
                                   horizon = 1L, fit_predict = persistence)
  expect_equal(res$summary$n, 8L)
  expect_true(all(res$by_origin$target_time == res$by_origin$origin + 1L))
  expect_true(res$summary$all_targets_future)                    # never scores its own past
  expect_false(res$summary$preregistered)                       # no prereg supplied here
})

test_that("a future outlier cannot change an earlier origin's prediction (no leakage)", {
  spiked <- series; spiked$count[spiked$year == 2020] <- 1e7
  a <- rolling_origin_evaluation(series, "year", "count", origins = 2015,
                                 horizon = 1L, fit_predict = persistence)
  b <- rolling_origin_evaluation(spiked, "year", "count", origins = 2015,
                                 horizon = 1L, fit_predict = persistence)
  expect_identical(a$by_origin$predicted, b$by_origin$predicted)
})

test_that("the evaluator is gated on the preregistration", {
  pr <- preregister_spec(spec_a, tempfile(), frozen_at = "2026-08-05")
  # matching spec runs and stamps the hash
  ok <- rolling_origin_evaluation(series, "year", "count", origins = 2015, horizon = 1L,
                                  fit_predict = persistence, prereg = pr, spec = spec_a)
  expect_true(ok$summary$preregistered)
  expect_identical(ok$summary$spec_hash, pr$spec_hash)
  # an altered spec is blocked before any scoring
  expect_error(rolling_origin_evaluation(series, "year", "count", origins = 2015,
    horizon = 1L, fit_predict = persistence, prereg = pr, spec = spec_b),
    "does not match")
  # prereg without a spec is refused
  expect_error(rolling_origin_evaluation(series, "year", "count", origins = 2015,
    horizon = 1L, fit_predict = persistence, prereg = pr),
    "`spec` is required")
})

test_that("rolling_origin_evaluation errors when no origin has both train and target", {
  expect_error(
    rolling_origin_evaluation(series, "year", "count", origins = 2100, horizon = 1L,
                              fit_predict = persistence),
    "no \\(origin, target\\)")
})
