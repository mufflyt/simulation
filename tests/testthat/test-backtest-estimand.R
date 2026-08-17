# The back-test estimand contract. These tests exist because the largest single
# component of the back-test's bias was a CATEGORY ERROR -- an attrited
# prediction scored against a cumulative observation -- and that class of defect
# is invisible to every ordinary test: both sides are internally consistent, and
# the comparison between them is what is wrong.

.bt <- function() utils::read.csv("../../artifacts/backtest_2020_to_2023_summary.csv",
                                  stringsAsFactors = FALSE)

test_that("the contract states what each side measures and why", {
  k <- backtest_estimand_contract()
  expect_false(k$observed$applies_attrition)
  expect_equal(k$observed$canonical_2023, 1306)
  # The reason the observation has no attrition must be recorded as
  # UNAVAILABLE, not as an oversight -- otherwise someone will "fix" it by
  # trying to subtract retirements that do not exist in the data.
  expect_match(k$observed$why_no_attrition, "n_retired = 0", fixed = TRUE)
  expect_match(k$observed$arithmetic, "1339", fixed = TRUE)
  # and the attrited arm must be labelled unvalidatable, NOT failing
  expect_false(k$predicted$attrited$comparable_to_observed)
  expect_identical(k$predicted$attrited$status, "unvalidatable")
  expect_match(k$predicted$attrited$why, "never as failing", fixed = TRUE)
})

test_that("strict mode refuses to score a mismatched arm", {
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_summary.csv"))
  expect_error(
    suppressMessages(assert_backtest_estimand_match(.bt(), mode = "strict")),
    "category error")
})

test_that("relaxed mode classifies rather than refuses", {
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_summary.csv"))
  r <- suppressMessages(assert_backtest_estimand_match(.bt(), mode = "relaxed"))
  expect_true(all(r$estimand[r$apply_attrition] == "active_net_of_attrition"))
  expect_true(all(r$estimand[!r$apply_attrition] == "cumulative_board_certified"))
  expect_true(all(r$status[r$apply_attrition] == "unvalidatable"))
  expect_true(all(r$status[!r$apply_attrition] == "validated_comparison"))
})

test_that("a summary with no declared estimand cannot be scored at all", {
  # An arm that does not say what it measures must not be silently compared.
  bad <- data.frame(arm = "x", percent_error = -5, within_95 = TRUE)
  expect_error(assert_backtest_estimand_match(bad), "missing")
})

test_that("headline metrics report matched AND all-arms, never one instead of the other", {
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_summary.csv"))
  h <- backtest_headline_metrics(.bt())
  # Both must be present. Reporting only the matched figure would be an
  # improvement obtained by discarding arms, which is exactly what the
  # back-test ratchet forbids.
  expect_true(all(c("coverage95_all", "coverage95_matched",
                    "mean_error_all", "mean_error_matched") %in% names(h)))
  expect_equal(h$n_arms_all, 10L)
  expect_equal(h$n_arms_matched, 5L)
  expect_equal(h$coverage95_all, 0.2)
  expect_equal(h$coverage95_matched, 0.4)
  # the reconciliation's headline number
  expect_gt(h$bias_from_estimand_mismatch_pp, 5)
  expect_lt(h$bias_from_estimand_mismatch_pp, 7)
})

test_that("matched arms are still not good enough, and the record says so", {
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_summary.csv"))
  h <- backtest_headline_metrics(.bt())
  # Fixing the estimand mismatch is NECESSARY BUT NOT SUFFICIENT. If this ever
  # starts passing, the reconciliation succeeded on its own and the remaining
  # entrant-rate work can be reconsidered -- but it must not be assumed.
  expect_lt(h$coverage95_matched, 0.80)
  expect_lt(h$mean_error_matched, 0)
})
