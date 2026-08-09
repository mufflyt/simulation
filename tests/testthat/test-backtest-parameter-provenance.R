# The parameter-side leakage audit.
#
# The data-side guard (`assert_no_leakage()`) passed for months while a 2025
# career-change estimate sat in a back-test advertising a 2020 forecast origin,
# because the parameter never arrived through a series read. These tests pin the
# gap shut.

test_that("every published parameter in the primary back-test predates the cutoff", {
  expect_silent(assert_backtest_parameters_precede_cutoff(2020L))
  used <- assert_backtest_parameters_precede_cutoff(2020L)
  pub <- used[used$basis == "published", ]
  expect_gt(nrow(pub), 0L)
  expect_true(all(pub$available_by <= 2020L))
})

test_that("the 2025 career-change hazard is recorded and excluded from the primary path", {
  row <- BACKTEST_PARAMETER_PROVENANCE[
    BACKTEST_PARAMETER_PROVENANCE$parameter == "CAREER_CHANGE_HAZARD_UNDER_50", ]
  expect_equal(nrow(row), 1L)
  expect_equal(row$available_by, 2025L)
  expect_false(row$in_primary_backtest)
  expect_match(row$source, "2025")
})

test_that("the retirement schedule is retained because its sources predate the cutoff", {
  row <- BACKTEST_PARAMETER_PROVENANCE[
    BACKTEST_PARAMETER_PROVENANCE$parameter == "RETIREMENT_HAZARD_BY_AGE", ]
  expect_true(row$in_primary_backtest)
  expect_lte(row$available_by, 2020L)
})

test_that("a post-cutoff parameter in the primary path is a hard error", {
  leaky <- BACKTEST_PARAMETER_PROVENANCE
  leaky$in_primary_backtest[
    leaky$parameter == "CAREER_CHANGE_HAZARD_UNDER_50"] <- TRUE
  expect_error(
    assert_backtest_parameters_precede_cutoff(2020L, provenance = leaky),
    "PARAMETER LEAKAGE")
  # and it must say that a sensitivity analysis is not the remedy
  expect_error(
    assert_backtest_parameters_precede_cutoff(2020L, provenance = leaky),
    "sensitivity analysis does not repair")
})

test_that("a published parameter with no availability year cannot be audited silently", {
  undated <- BACKTEST_PARAMETER_PROVENANCE
  undated$available_by[undated$parameter == "MICROSIM_TERMINAL_AGE"] <- NA_integer_
  expect_error(
    assert_backtest_parameters_precede_cutoff(2020L, provenance = undated),
    "cannot be audited")
})

test_that("an NA in_primary_backtest flag does not silently skip a parameter", {
  # A bare logical subset would drop the NA row and skip its audit, which is the
  # failure mode this guard exists to prevent.
  ambiguous <- BACKTEST_PARAMETER_PROVENANCE
  ambiguous$in_primary_backtest[
    ambiguous$parameter == "CAREER_CHANGE_HAZARD_UNDER_50"] <- NA
  expect_silent(assert_backtest_parameters_precede_cutoff(2020L,
                                                          provenance = ambiguous))
  used <- assert_backtest_parameters_precede_cutoff(2020L, provenance = ambiguous)
  expect_false("CAREER_CHANGE_HAZARD_UNDER_50" %in% used$parameter)
})

test_that("the primary back-test omits the career-change process by default", {
  expect_equal(BACKTEST_CAREER_CHANGE_HAZARD, 0)
  expect_equal(formals(run_backtest_arm)$career_change_hazard,
               as.name("BACKTEST_CAREER_CHANGE_HAZARD"))
  expect_equal(formals(run_backtest)$career_change_hazard,
               as.name("BACKTEST_CAREER_CHANGE_HAZARD"))
})

test_that("the production model still applies the 2025 career-change hazard", {
  # Omission is scoped to the historical back-test. A projection made today may
  # legitimately use evidence published in 2025, so the production default must
  # NOT have been changed to zero.
  expect_equal(CAREER_CHANGE_HAZARD_UNDER_50, 0.0142)
  expect_gt(CAREER_CHANGE_HAZARD_UNDER_50, 0)
})

test_that("the career-change hazard reaches the engine and moves the result", {
  # Guards against the parameter being accepted but ignored, which would make
  # the sensitivity analysis silently identical to the primary analysis.
  omitted <- run_backtest_arm("derived", 55, n_iterations = 40L,
                              apply_attrition = TRUE, seed = 4242L,
                              career_change_hazard = 0)
  applied <- run_backtest_arm("derived", 55, n_iterations = 40L,
                              apply_attrition = TRUE, seed = 4242L,
                              career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50)
  m <- function(x) stats::median(x$iterations$headcount[x$iterations$year == 2023])
  expect_gt(m(omitted), m(applied))
  expect_equal(omitted$settings$career_change_hazard, 0)
  expect_equal(applied$settings$career_change_hazard, CAREER_CHANGE_HAZARD_UNDER_50)
})

test_that("no-attrition arms are unaffected by the career-change setting", {
  # Both exit hazards are already zero there, so the two specifications must
  # agree exactly. This is what makes the sensitivity analysis interpretable:
  # any difference it shows comes from the career-change process alone.
  a <- run_backtest_arm("derived", 55, n_iterations = 40L, apply_attrition = FALSE,
                        seed = 99L, career_change_hazard = 0)
  b <- run_backtest_arm("derived", 55, n_iterations = 40L, apply_attrition = FALSE,
                        seed = 99L, career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50)
  expect_identical(a$iterations, b$iterations)
})
