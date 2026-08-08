# Coverage language (R/validation-coverage_language.R).
#
# The claim these protect: the 2020->2023 back-test scores 10 arms against ONE
# target (2023 = 1,306). Those are ten alternative specifications, not ten
# independent forecast occasions, so 2/10 is a containment count and not an
# estimate of interval coverage. The arithmetic is right and the interpretation
# is wrong, which is why a reader can miss it and a guard cannot.

test_that("coverage is not estimable for the single-target back-test", {
  s <- backtest_status()
  expect_false(coverage_is_estimable(s))
  expect_equal(s$n_arms, 10L)
  # 2 of 10 -- kept as a check that the record itself has not drifted.
  expect_equal(round(s$coverage_95 * s$n_arms), 2)
})

test_that("the licensed sentence describes containment, never a rate", {
  s <- backtest_status()
  txt <- containment_statement(s)
  expect_match(txt, "2 of 10 model configurations")
  expect_match(txt, "single observed target")
  expect_match(txt, "not an estimate of interval coverage")
  # It must survive its own guard.
  expect_silent(assert_no_coverage_rate_claim(txt, s, mode = "strict"))
})

test_that("coverage-rate claims are refused", {
  s <- backtest_status()
  for (bad in c("Our intervals achieved 20% coverage.",
                "coverage was 20%",
                "empirical coverage of 0.2",
                "coverage failed the 80% threshold",
                "observed coverage 0.20 across the arms")) {
    expect_error(assert_no_coverage_rate_claim(bad, s, mode = "strict"),
                 "coverage RATE", info = bad)
  }
})

test_that("restricting to the definition-matched arms does not launder the claim", {
  # The denominator is not the problem. 2/5 = 40% has the identical defect:
  # there is still exactly one realised target.
  s <- backtest_status()
  expect_error(
    assert_no_coverage_rate_claim(
      "Among definition-matched arms the coverage rate was 40%.", s, mode = "strict"),
    "coverage RATE")
  expect_error(
    assert_no_coverage_rate_claim("2 of 5 arms, 40% coverage", s, mode = "strict"),
    "coverage RATE")
})

test_that("legitimate uses of the word coverage remain sayable", {
  # A guard that forbade the word outright would make the correction itself
  # unwriteable, and would be muted within a week.
  s <- backtest_status()
  for (ok in c("Coverage is not estimable from a single target.",
               "2 of 10 model configurations contained the observed 2023 value.",
               "Establishing interval coverage requires repeated targets.",
               "The interval score ranks the configurations on one observation.")) {
    expect_silent(assert_no_coverage_rate_claim(ok, s, mode = "strict"))
  }
})

test_that("interval_label never emits a rate while coverage is not estimable", {
  # The function whose job is keeping language honest was itself capable of
  # smuggling the claim in through its validated branch.
  s <- backtest_status()
  expect_silent(assert_no_coverage_rate_claim(interval_label(s), s, mode = "strict"))

  pretend_validated <- s
  pretend_validated$validated <- TRUE
  lab <- interval_label(pretend_validated)
  expect_match(lab, "model configurations")
  expect_match(lab, "not\\s+estimable")
  expect_silent(assert_no_coverage_rate_claim(lab, s, mode = "strict"))
})

test_that("the guard stands down when coverage IS estimable", {
  # Multi-target analyses -- the rolling-origin work, scored over four distinct
  # origins -- are entitled to report coverage. The guard must not blanket-ban
  # it, or it would flag correct statements in RESULTS_INTERVAL_CALIBRATION.md.
  s <- backtest_status()
  s$coverage_is_estimable <- TRUE
  expect_true(coverage_is_estimable(s))
  expect_silent(assert_no_coverage_rate_claim("coverage was 100% across four origins",
                                              s, mode = "strict"))
})

test_that("relaxed mode warns rather than erroring", {
  s <- backtest_status()
  expect_false(suppressWarnings(suppressMessages(
    assert_no_coverage_rate_claim("coverage was 20%", s, mode = "relaxed"))))
})

test_that("negated mentions pass; the same phrase as a claim does not", {
  # Found by the manuscript tripping its own guard on
  # "a containment count, not a 20% coverage rate" -- which is exactly the
  # sentence this module exists to produce. A guard that rejects its own
  # disclaimer gets muted.
  s <- backtest_status()
  for (ok in c("a containment count, not a 20% coverage rate",
               "this is never a coverage rate",
               "rather than an empirical coverage figure",
               "these are containment counts, not observed coverage")) {
    expect_silent(assert_no_coverage_rate_claim(ok, s, mode = "strict"))
  }
  for (bad in c("the coverage rate was 40%",
                "observed coverage 0.20 across the arms",
                "empirical coverage of 0.2")) {
    expect_error(assert_no_coverage_rate_claim(bad, s, mode = "strict"), "coverage RATE")
  }
})

test_that("an earlier negation does not launder a later claim", {
  # The look-back window is deliberately short. A disclaimer in one sentence
  # must not license an assertion two sentences later.
  s <- backtest_status()
  expect_error(
    assert_no_coverage_rate_claim(
      paste("This is not a coverage estimate.",
            "We report substantial detail on the design and the arms involved.",
            "The coverage rate was 40%."), s, mode = "strict"),
    "coverage RATE")
})

test_that("the manuscript narrative passes the guard", {
  # The document is generated prose; it should be held to the rule it states.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(!length(root))
  f <- file.path(root[1], "docs", "VALIDATION_PAPER.md")
  skip_if(!file.exists(f))
  expect_silent(assert_no_coverage_rate_claim(readLines(f, warn = FALSE),
                                              backtest_status(), mode = "strict"))
})
