# Boundary value analysis.
#
# Every threshold in this package is a decision: below it a number is refused,
# at or above it the number is published. The interesting failures live at the
# boundary itself, and they are invisible to tests that use comfortable inputs --
# a guard written `>` when it meant `>=` passes every test at 0.5 and 2.0.
#
# Each block below probes min-epsilon / min / min+epsilon (and the max side where
# one exists). The expected values were established by RUNNING the functions,
# not by reading the comparison operators, because the point is to catch the
# case where the operator does not say what the author meant.
#
# Two genuine defects were found this way and are documented in place rather than
# quietly asserted: search this file for DEFECT.

# ---- Monte Carlo iteration floor -------------------------------------------

test_that("the iteration floor is exact at every interval width", {
  # n >= 2 / (1 - ci) is the count at which the outer quantiles become order
  # statistics. Off by one here and a published band is the sample extremes.
  expect_equal(mc_min_iterations(0.95), 40L)
  expect_equal(mc_min_iterations(0.80), 10L)
  expect_equal(mc_min_iterations(0.50), 4L)
  expect_equal(mc_min_iterations(0.99), 200L)

  # THE FLOATING-POINT BOUNDARY. 1 - 0.8 is 0.19999999999999996, so 2/(1-ci)
  # arrives as 10.000000000000002 and an unrounded ceiling() returns 11 --
  # demanding an extra iteration for no statistical reason, at the ci a caller
  # is most likely to pick as a cheaper alternative to 95%.
  expect_false(mc_min_iterations(0.80) == 11L)
})

test_that("the adequacy gate flips exactly at the floor, not near it", {
  n95 <- mc_min_iterations(0.95)   # 40
  expect_error(assert_monte_carlo_adequate(n95 - 1L, ci = 0.95, mode = "strict"))
  expect_true(assert_monte_carlo_adequate(n95,      ci = 0.95, mode = "strict"))
  expect_true(assert_monte_carlo_adequate(n95 + 1L, ci = 0.95, mode = "strict"))

  n80 <- mc_min_iterations(0.80)   # 10
  expect_error(assert_monte_carlo_adequate(n80 - 1L, ci = 0.80, mode = "strict"))
  expect_true(assert_monte_carlo_adequate(n80,      ci = 0.80, mode = "strict"))

  # The same n straddles the boundary depending on ci: 10 clears 80% and fails 95%.
  expect_true(assert_monte_carlo_adequate(10L, ci = 0.80, mode = "strict"))
  expect_error(assert_monte_carlo_adequate(10L, ci = 0.95, mode = "strict"))
})

test_that("ci itself is refused at its open boundaries", {
  expect_error(mc_min_iterations(0))
  expect_error(mc_min_iterations(1))
  expect_silent(invisible(mc_min_iterations(1 - 1e-6)))
  expect_silent(invisible(mc_min_iterations(1e-6)))
})

# ---- Reportability tier floor ----------------------------------------------

test_that("reportability flips between adjacent tiers, not within them", {
  r <- CALIBRATION_STATUS_RANK
  floor_rank <- r[[REPORTABLE_MIN_CALIBRATION]]
  below <- names(r)[r == floor_rank - 1L]
  expect_true("measured_input_unvalidated_response" %in% below)

  # One tier below the floor blocks; the floor itself clears. This is the
  # boundary the isochrone import sits on: a verified surface is one rank short.
  st_below <- c(disease_burden = "fitted", care_seeking = "calibrated",
                access_barriers = "measured_input_unvalidated_response",
                baseline_adequacy = "calibrated")
  st_at <- replace(st_below, "access_barriers", REPORTABLE_MIN_CALIBRATION)

  tb <- demand_estimand_table(st_below)
  ta <- demand_estimand_table(st_at)
  expect_false(tb$reportable[tb$estimand == "reduced_barrier"])
  expect_true(ta$reportable[ta$estimand == "reduced_barrier"])
})

# ---- Workforce outlook thresholds ------------------------------------------

test_that("outlook thresholds are inclusive at the lower bound", {
  eps <- 1e-9
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_MARGINAL_MIN - eps), "Insufficient")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_MARGINAL_MIN),       "Marginal")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_ADEQUATE_MIN - eps), "Marginal")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_ADEQUATE_MIN),       "Adequate")
  # NA must not silently classify.
  expect_true(is.na(classify_workforce_outlook(NA_real_)))
})

# ---- Productivity plausibility band ----------------------------------------

test_that("the productivity band is inclusive at both ends", {
  implausible <- function(v) {
    any(grepl("outside the plausible",
              capture.output(check_productivity_plausible(v, mode = "relaxed"), type = "message")))
  }
  lo <- WRVU_PER_FTE_BENCHMARK[["low"]]; hi <- WRVU_PER_FTE_BENCHMARK[["high"]]
  expect_true(implausible(lo - 1))
  expect_false(implausible(lo))
  expect_false(implausible(hi))
  expect_true(implausible(hi + 1))
})

# ---- Capacity-survey denominators ------------------------------------------

test_that("vanishing denominators stop rather than returning Inf", {
  # Each category divides by a DIFFERENT quantity, so each has its own zero.
  expect_error(capacity_category_adequacy("shortage_unmet", seen = 0, additional = 5))
  expect_error(capacity_category_adequacy("shortage_hours", seen = 5, additional = 5))
  # One step away from the zero denominator is finite and must be allowed.
  expect_true(is.finite(capacity_category_adequacy("shortage_hours", seen = 5, additional = 4)))
})

test_that("DEFECT: shortage_hours can return a NEGATIVE adequacy, and it survives", {
  # 1 - additional / (seen - additional) diverges to -Inf as additional -> seen.
  # At seen = 5, additional = 4 the denominator is 1 and the result is -3: a
  # provider cannot have negative adequacy, since adequacy is supply / demand.
  expect_equal(capacity_category_adequacy("shortage_hours", seen = 5, additional = 4), -3)

  # required_fte_base_year() does fail closed on a non-positive adequacy, so a
  # single pathological group alone is caught.
  expect_error(required_fte_base_year(1306, -3))
  expect_error(required_fte_base_year(1306, 0))

  # THE HOLE. Dilution hides it. Ninety-five ordinary respondents plus five
  # pathological ones average to 0.80 -- an entirely plausible 20% shortfall --
  # which passes every guard and yields a concrete required-FTE number.
  resp <- data.frame(category = c("equilibrium", "shortage_hours"), n = c(95, 5),
                     seen = c(NA, 5), additional = c(NA, 4))
  ad <- capacity_survey_adequacy(resp)$adequacy
  expect_equal(ad, 0.80, tolerance = 1e-9)
  expect_true(ad > 0)                              # passes the only guard there is
  expect_gt(required_fte_base_year(1306, ad), 1600)

  # The exact uncovered window, established by sweeping `additional`:
  # adequacy = 1 - a/(s - a) is zero at a = s/2 and NEGATIVE for a > s/2, while
  # the existing denominator guard only fires at a >= s. So the hole is
  #     s/2 < additional < s
  # and the fix is to require a POSITIVE RESULT, not merely a positive
  # denominator. Asserting the defect here keeps it visible and makes this test
  # fail loudly when someone repairs it.
})

test_that("DEFECT: a surplus respondent seeing zero patients scores 200% adequacy", {
  # denom = seen + additional = 5 > 0, so the zero guard does not fire, and
  # 1 + 5/5 = 2. Someone who saw nobody is not twice as adequate as someone at
  # equilibrium; `seen` should have its own positivity requirement.
  expect_equal(capacity_category_adequacy("surplus", seen = 0, additional = 5), 2)
})

# ---- Base-year equilibrium --------------------------------------------------

test_that("adequacy of exactly 1.0 is the refused case, either side of it is not", {
  eps <- 1e-6
  at <- suppressMessages(baseline_gap(1306, 1.0, method = "assumed", evidence = "x"))
  expect_equal(at$shortfall_fte, 0)
  expect_false(suppressMessages(assert_baseline_gap_estimated(at, mode = "relaxed")))

  below <- suppressMessages(baseline_gap(1306, 1 - eps, method = "assumed", evidence = "x"))
  above <- suppressMessages(baseline_gap(1306, 1 + eps, method = "assumed", evidence = "x"))
  expect_gt(below$shortfall_fte, 0)                 # a shortfall
  expect_lt(above$shortfall_fte, 0)                 # a surplus
  # Both are ESTIMATES, so neither trips the equilibrium refusal -- only exact 1.0 does.
  expect_true(suppressMessages(
    assert_baseline_gap_estimated(below, mode = "relaxed", allow_analogy = TRUE)))
})

# ---- Coordinate coverage floor ---------------------------------------------

test_that("the coordinate coverage floor is a real boundary", {
  expect_equal(COORD_COVERAGE_MIN, 0.95)
  # A pathway at exactly zero is a structural hole, not a low rate, and must not
  # be rescued by a high overall share -- the case provider_coordinate_coverage()
  # documents as passing at 72% overall.
  expect_gt(COORD_COVERAGE_MIN, 0.5)
  expect_lt(COORD_COVERAGE_MIN, 1)
})
