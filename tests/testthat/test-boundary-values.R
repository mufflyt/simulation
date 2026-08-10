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
# Two genuine defects were found this way and have since been FIXED in
# capacity_category_adequacy(); the blocks below now pin the corrected boundaries
# so a regression re-opens them loudly.

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
  # Comfortably inside the valid region is finite and allowed.
  expect_true(is.finite(capacity_category_adequacy("shortage_hours", seen = 5, additional = 2)))
})

test_that("shortage_hours refuses the whole negative-adequacy window, not just a >= seen", {
  # adequacy = 1 - a/(s - a) is zero at a = s/2 and negative beyond it, while a
  # denominator-only guard fires at a >= s. The window s/2 < a < s once produced
  # a NEGATIVE adequacy -- at s = 5, a = 4 it returned -3 -- so the guard is on
  # the RESULT now. Boundary swept rather than reasoned about:
  s_seen <- 5
  expect_equal(capacity_category_adequacy("shortage_hours", s_seen, 2.0), 1 - 2 / 3)
  expect_equal(capacity_category_adequacy("shortage_hours", s_seen, 2.4),
               1 - 2.4 / 2.6, tolerance = 1e-9)
  expect_error(capacity_category_adequacy("shortage_hours", s_seen, 2.5))   # exactly 0
  expect_error(capacity_category_adequacy("shortage_hours", s_seen, 2.6))   # first negative
  expect_error(capacity_category_adequacy("shortage_hours", s_seen, 4.0))   # was -3
  expect_error(capacity_category_adequacy("shortage_hours", s_seen, 5.0))   # denominator 0

  # The message must name the condition, because a survey analyst reading it
  # needs to know which response to re-check, not merely that something failed.
  expect_error(capacity_category_adequacy("shortage_hours", s_seen, 4.0),
               "seen / 2", fixed = TRUE)
})

test_that("a pathological response can no longer be diluted into a plausible mean", {
  # THE FAILURE THIS CLOSES. 95 ordinary respondents plus 5 pathological ones
  # used to average to 0.80 -- an ordinary-looking 20% shortfall -- and pass
  # every remaining guard, yielding required FTE 1,632.5 from an input holding a
  # -3. The weighted mean cannot launder the group any more, because the group
  # never produces a number to average.
  resp <- data.frame(category = c("equilibrium", "shortage_hours"), n = c(95, 5),
                     seen = c(NA, 5), additional = c(NA, 4))
  expect_error(capacity_survey_adequacy(resp))
})

test_that("every non-equilibrium category needs a positive `seen`", {
  # surplus divides by seen + additional, so seen = 0 passed the denominator
  # check and returned 1 + 5/5 = 2 -- someone who saw nobody scoring twice the
  # adequacy of someone at equilibrium.
  expect_error(capacity_category_adequacy("surplus", seen = 0, additional = 5))
  expect_error(capacity_category_adequacy("shortage_unmet", seen = 0, additional = 5))
  expect_error(capacity_category_adequacy("shortage_hours", seen = 0, additional = 5))
  # equilibrium takes no seen at all and must stay unaffected.
  expect_equal(capacity_category_adequacy("equilibrium"), 1.0)
  # One patient is enough; the requirement is positivity, not a magnitude.
  expect_true(is.finite(capacity_category_adequacy("surplus", seen = 1, additional = 1)))
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

# ---- Retirement age window --------------------------------------------------

test_that("the hazard switches regime exactly at the retirement and terminal ages", {
  h <- function(a) implied_annual_departure_rate(a, "female")
  # Below RETIREMENT_MIN_AGE only the career-change hazard applies; at it, the
  # retirement schedule takes over. An off-by-one here silently ages the whole
  # workforce out a year early or late, compounded over a 25-year horizon.
  expect_lt(h(RETIREMENT_MIN_AGE - 1L), h(RETIREMENT_MIN_AGE))
  expect_equal(h(RETIREMENT_MIN_AGE), h(RETIREMENT_MIN_AGE + 1L))

  # The terminal age is an absorbing boundary: everyone leaves, and nothing
  # beyond it can be less than certain.
  expect_lt(h(MICROSIM_TERMINAL_AGE - 1L), 1)
  expect_equal(h(MICROSIM_TERMINAL_AGE), 1)
  expect_equal(h(MICROSIM_TERMINAL_AGE + 5L), 1)
})

# ---- Entry-to-certification conversion --------------------------------------

test_that("a conversion above 1 is refused, at the tolerance and not before", {
  # More people cannot reach an outcome than entered, so ratio > 1 is
  # misalignment rather than a high estimate. The guard carries a 1e-8 tolerance
  # so exact-1 arithmetic survives floating point.
  expect_silent(invisible(.assert_possible_conversion(0.857, "x")))
  expect_silent(invisible(.assert_possible_conversion(1, "x")))
  expect_silent(invisible(.assert_possible_conversion(1 + 1e-8, "x")))
  expect_error(.assert_possible_conversion(1 + 2e-8, "x"))
  expect_error(.assert_possible_conversion(1.05, "x"))

  # The message must send the reader to the LAG first: a uniform lag against
  # pathway-specific fellowship lengths produced exactly 1.050.
  expect_error(.assert_possible_conversion(1.05, "x"), "lag")
  # The escape hatch exists but must be explicit.
  expect_silent(invisible(.assert_possible_conversion(1.05, "x", allow_implausible = TRUE)))
})

test_that("the observed conversions sit strictly inside the possible region", {
  skip_if_not_installed("mufflyaccess")
  for (src in c("acgme", "nrmp")) {
    r <- entrant_to_cert_ratio(source = src)$ratio
    expect_gt(r, 0)
    expect_lte(r, 1 + 1e-8)   # would have thrown otherwise; asserted for the record
  }
})

# ---- FTE restatement --------------------------------------------------------

test_that("restating FTE refuses a zero hours basis but not a tiny one", {
  expect_error(restate_fte(1000, 0, 37.2))
  expect_equal(restate_fte(1000, 37.2, 37.2), 1000)

  # BOUNDARY HOLE, recorded rather than fixed here. A vanishingly small but
  # positive `from_hours` passes the positivity check and silently collapses the
  # count to zero. Same shape as adequacy = 1e-9 yielding a required FTE in the
  # trillions: `> 0` is a weaker precondition than "physically possible", and
  # neither function carries a plausibility band on its result.
  tiny <- restate_fte(1000, 1e-9, 37.2)
  expect_gt(tiny, 0)          # strictly positive, so no guard fires
  expect_lt(tiny, 1e-6)       # and physically meaningless: 1,000 FTE -> 2.7e-8
})

# ---- Monte Carlo degenerate bands -------------------------------------------

test_that("a degenerate band reports NA rather than Inf or a false zero", {
  # Every draw identical: no spread, so noise_share is undefined, not infinite.
  flat <- monte_carlo_diagnostics(rep(100, 50), ci = 0.95)
  expect_equal(flat$half_width, 0)
  expect_true(is.na(flat$noise_share))

  # NEARLY degenerate is the sharper case: 49 identical values and one outlier
  # still gives a zero-width 95% band, because both outer quantiles land on the
  # mode. A ratio guarded only against exact zero would divide by it here.
  near <- monte_carlo_diagnostics(c(rep(100, 49), 101), ci = 0.95)
  expect_equal(near$half_width, 0)
  expect_true(is.na(near$noise_share))

  # With genuine spread the ratio is finite and positive.
  set.seed(1)
  real <- monte_carlo_diagnostics(stats::rnorm(200, 100, 10), ci = 0.95)
  expect_gt(real$half_width, 0)
  expect_true(is.finite(real$noise_share))
})

# ---- Hours-curve gradient scale --------------------------------------------

test_that("a zero gradient scale is exactly flat, and any positive scale is not", {
  ag <- data.frame(age = c(40, 50, 60), sex = "female")
  flat <- fte_curve_gradient_leverage(ag, 25L, gradient_scale = 0)$drift_pct
  expect_equal(flat, 0, tolerance = 1e-9)

  # The published gradient must move it materially, or the leverage claim in the
  # README is empty.
  published <- fte_curve_gradient_leverage(ag, 25L, gradient_scale = 1)$drift_pct
  expect_lt(published, -5)

  # Monotone in the scale: more gradient, more drift, no crossing.
  d <- fte_curve_gradient_leverage(ag, 25L, gradient_scale = c(0, 0.5, 1, 1.5))$drift_pct
  expect_true(all(diff(d) < 0))
})
