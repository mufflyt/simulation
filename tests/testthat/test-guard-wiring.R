# WIRED-GUARD TESTS.
#
# Four guards were defined, tested and exported while NOTHING in R/ or
# scripts/ called them. Their unit tests passed the whole time, which is
# exactly why unit tests cannot establish that a guard is live: a test calling
# a function is what a test IS. That is the defect class recorded at the top of
# test-export-wiring.R, and these four were the current instance of it.
#
# So each guard gets three tests, and the third is the one that matters:
#
#   1. the valid path PROCEEDS      -- the guard is not simply always-fatal
#   2. the invalid path REFUSES     -- the invariant is actually enforced
#   3. the PUBLIC entry point refuses on the same invalid input, so the guard
#      cannot be bypassed by calling the function anyone would actually call
#
# Without (3) a guard can be wired into a helper that production never reaches
# and every one of these tests still passes.

# ---------------------------------------------------------------------------
# assert_incident_not_prevalent -- wired into pathway_service_volumes()
# ---------------------------------------------------------------------------

test_that("the incident/prevalent invariant holds when new consults are below the cohort", {
  # per_entering 0.4 on the only new_consultation row => 400 < 1000.
  pw <- condition_service_pathway()
  pw <- pw[pw$condition == "pop", , drop = FALSE]
  pw$per_entering[pw$service == "new_consultation"] <- 0.4
  expect_true(assert_incident_not_prevalent(c(pop = 1000), pw, strict = TRUE))
})

test_that("the incident/prevalent invariant REFUSES when every prevalent patient is counted as new", {
  pw <- condition_service_pathway()
  pw <- pw[pw$condition == "pop", , drop = FALSE]
  pw$per_entering[pw$service == "new_consultation"] <- 1.0
  expect_error(assert_incident_not_prevalent(c(pop = 1000), pw, strict = TRUE),
               "counted as a NEW patient annually")
})

test_that("pathway_service_volumes() -- the PUBLIC path -- cannot bypass the invariant", {
  # THE POINT OF THIS FILE. The guard must be unreachable-around, not merely
  # present. This is also the state the SHIPPED table is in: ratio exactly 1.00.
  expect_error(pathway_service_volumes(treated = c(pop = 1000), year = 2025L),
               "counted as a NEW patient annually")
})

# ---------------------------------------------------------------------------
# assert_backtest_estimand_match -- wired into run_backtest(), forced strict
# ---------------------------------------------------------------------------

.bt_summary <- function(pred_attrition, obs_attrition = FALSE) {
  tibble::tibble(arm = c("a", "b"),
                 apply_attrition = pred_attrition,
                 observed_applies_attrition = obs_attrition)
}

test_that("a back-test summary whose arms match the observed estimand proceeds", {
  out <- assert_backtest_estimand_match(.bt_summary(c(FALSE, FALSE)), mode = "strict")
  expect_true(all(out$comparable))
  expect_equal(unique(out$estimand), "cumulative_board_certified")
})

test_that("a back-test summary comparing different estimands REFUSES under strict", {
  expect_error(
    assert_backtest_estimand_match(.bt_summary(c(TRUE, FALSE)), mode = "strict"),
    "category error")
})

test_that("the estimand refusal lives at PUBLICATION, not inside run_backtest()", {
  # CORRECTED WIRING. This guard was first forced strict inside run_backtest(),
  # which was the wrong abstraction level: that function runs
  # `for (att in c(TRUE, FALSE))` by design and labels the FALSE arms
  # "definition-matched". Producing both estimands IS the experiment, so
  # refusing there made the runner permanently unusable.
  #
  # The uninterpretable act is CLAIMING an attrition arm as validated. Pinned as
  # source in both directions, because the failure mode is an argument being
  # added or removed and no runtime assertion can see that.
  # Source-level pins need the SOURCE TREE, which an installed package does not
  # ship -- see .source_tree_root() in helper-setup.R. Skip rather than fail
  # under R CMD check; the behavioural tests below cover the same contract and
  # run everywhere.
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository sources not present (installed-package context)")
  run_src <- readLines(file.path(root[1], "R", "validation-backtest_run.R"), warn = FALSE)
  call_line <- grep("assert_backtest_estimand_match\\(", run_src, value = TRUE)
  expect_length(call_line, 1L)
  # classify, do not refuse: no strict mode forced in the runner
  expect_false(grepl('mode\\s*=\\s*"strict"', call_line))

  val_src <- readLines(file.path(root[1], "R", "calibration-validation.R"), warn = FALSE)
  pub_line <- grep("assert_backtest_estimand_match\\(", val_src, value = TRUE)
  expect_length(pub_line, 1L)
  expect_match(pub_line, 'mode\\s*=\\s*"strict"')
})

test_that("publication REFUSES a result carrying mismatched back-test arms", {
  # The behavioural counterpart to the source pin above.
  res <- list(backtest = list(summary = .bt_summary(c(TRUE, FALSE))))
  rep <- suppressMessages(publishable_run_report(res, artifact_path = NA_character_,
                                                 require_artifact = FALSE))
  row <- rep[rep$check == "backtest_estimand_match", ]
  expect_equal(nrow(row), 1L)
  expect_false(row$passed[[1]])
  expect_match(row$detail[[1]], "unlike estimands")
})

test_that("publication accepts a result whose arms all match", {
  res <- list(backtest = list(summary = .bt_summary(c(FALSE, FALSE))))
  rep <- suppressMessages(publishable_run_report(res, artifact_path = NA_character_,
                                                 require_artifact = FALSE))
  row <- rep[rep$check == "backtest_estimand_match", ]
  expect_equal(nrow(row), 1L)
  expect_true(row$passed[[1]])
})

# ---------------------------------------------------------------------------
# assert_care_flow_gates -- wired into advance_care_engagement()
# ---------------------------------------------------------------------------

test_that("advance_care_engagement() returns flows that satisfy their own gates", {
  f <- advance_care_engagement(untreated_eligible = 1000,
                               previously_disengaged = 100,
                               care_engaged_previous = 500,
                               first_entry_rate = 0.10,
                               reentry_rate = 0.05,
                               retention_rate = 0.80)
  g <- assert_care_flow_gates(f)
  expect_true(all(g$passed))
  # care_engaged is an OUTPUT: the identity is what makes it non-circular.
  expect_equal(f$newly_entering_care + f$continuing_care, f$care_engaged)
})

test_that("assert_care_flow_gates fails a flows object whose identity is broken", {
  f <- advance_care_engagement(1000, 100, 500, 0.10, 0.05, 0.80)
  f$care_engaged <- f$care_engaged * 1.5      # break the accounting identity
  g <- assert_care_flow_gates(f)
  expect_false(all(g$passed))
})

# ---------------------------------------------------------------------------
# assert_care_engagement_gates -- wired into care_engagement_visits()
# ---------------------------------------------------------------------------

test_that("care_engagement_visits() proceeds on a well-formed split", {
  sp <- split_care_engagement(care_engaged = 1000, incident_share = 0.25)
  v <- suppressMessages(care_engagement_visits(
    sp, first_year_followup_rate = 1.2, annual_followup_rate = 0.5))
  expect_equal(nrow(v), 3L)
  expect_true(all(is.finite(v$volume)))
})

test_that("care_engagement_visits() REFUSES a split whose decomposition does not add up", {
  sp <- split_care_engagement(care_engaged = 1000, incident_share = 0.25)
  sp$continuing_care <- sp$continuing_care * 2   # identity now violated
  expect_error(
    suppressMessages(care_engagement_visits(
      sp, first_year_followup_rate = 1.2, annual_followup_rate = 0.5)),
    "Care-engagement decomposition failed")
})

test_that("the unsourced-parameter gate reports without being fatal", {
  sp <- split_care_engagement(care_engaged = 1000, incident_share = 0.25)
  expect_message(
    care_engagement_visits(sp, first_year_followup_rate = 1.2,
                           annual_followup_rate = 0.5),
    "remain unsourced")
})

test_that("an empty cohort is outside the invariant's domain, not a violation", {
  # REGRESSION. The first wiring refused an empty world: `0 < 0` is FALSE and
  # the ratio printed as NaN, so a degenerate-but-correct case produced an
  # uninterpretable refusal. Found by the property-based worlds in
  # .github/scripts/adversarial/metamorphic.R, not by a hand-written test --
  # which is the argument for generating boundaries rather than imagining them.
  expect_true(assert_incident_not_prevalent(c(pop = 0), strict = TRUE))
  expect_no_error(pathway_service_volumes(treated = c(pop = 0), year = 2025L))
})
