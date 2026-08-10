# Adversarial tests: attempts to make the model claim more than it knows.
#
# Every case here was written as an ATTACK first and became a test only after it
# succeeded. Four did. The pattern in all four is the same: a guard checked the
# SHAPE of its input and never the substance, so an object that looked right
# walked through a gate protecting a load-bearing quantity.
#
# Semantic tests follow the adversarial ones: they assert that quantities mean
# what their names say, and that the things which should cancel do while the
# things which should not, don't.

# ---- Attack 1: satisfy the interval gate with no uncertainty ----------------

test_that("a constant entrant series cannot pass as quantified uncertainty", {
  # THE ATTACK. supply_parameter_spec(entrant_series = rep(50, 6)) has sd 0, so
  # se is 0, and `is.finite(0)` is TRUE. The spec claimed the entrant rate was
  # DRAWN while every replicate used the same number -- the too-narrow-interval
  # defect the module exists to prevent, passing the module's own gate.
  flat <- supply_parameter_spec(entrant_series = rep(50, 6), entrant_mean = 50)
  expect_equal(flat$entrant_se, 0)
  expect_false(flat$quantified[["entrant_rate"]])
  expect_error(assert_parameter_uncertainty(flat, mode = "strict"),
               "individual stochasticity ONLY")

  # A series with real spread still qualifies, so the fix is not a blanket
  # refusal.
  varied <- supply_parameter_spec(entrant_series = c(40, 48, 10, 59), entrant_mean = 40)
  expect_gt(varied$entrant_se, 0)
  expect_true(varied$quantified[["entrant_rate"]])
  expect_true(assert_parameter_uncertainty(varied, mode = "strict"))
})

test_that("a near-constant series widens the interval proportionally, not categorically", {
  # Quantified/unquantified is a cliff; the DRAW should be continuous. A series
  # with tiny spread must give a tiny interval, not a wide one, or the gate
  # would just be trading a false negative for a false positive.
  tight <- supply_parameter_spec(entrant_series = c(50, 50, 50, 51), entrant_mean = 50)
  wide <- supply_parameter_spec(entrant_series = c(20, 50, 80, 51), entrant_mean = 50)
  expect_true(tight$quantified[["entrant_rate"]])
  expect_lt(tight$entrant_se, wide$entrant_se / 10)
})

# ---- Attack 2: a calibration that carries no information -------------------

test_that("calibration scalars are inspected, not merely present", {
  mk <- function(x) tibble::tibble(category = "ambulatory_visits", scalar = x)
  # THE ATTACK. The guard checked only that a `scalar` COLUMN existed. A scalar
  # multiplies demand: non-finite propagates NA through every total, zero erases
  # demand, negative inverts it. All four passed and the run called itself
  # calibrated.
  for (bad in list(NA_real_, Inf, -Inf, 0, -1)) {
    expect_error(assert_demand_calibrated(mk(bad), mode = "strict"),
                 "finite and strictly positive",
                 info = paste("scalar =", format(bad)))
  }
  # A real scalar still passes. The NAMCS fit is 0.467.
  expect_true(assert_demand_calibrated(mk(0.467), mode = "strict"))
  # And one bad row among good ones is still refused.
  expect_error(assert_demand_calibrated(
    tibble::tibble(category = c("a", "b"), scalar = c(0.5, NA_real_)), mode = "strict"),
    "finite and strictly positive")
})

# ---- Attack 3: forge the base-year anchor ----------------------------------

test_that("a hand-built object cannot impersonate a base-year gap", {
  # THE ATTACK, and the most serious of the four. The base-year anchor is the
  # only input that can still change the SIGN of the projected gap. Its gate
  # checked `inherits(gap, "urps_baseline_gap")` and the adequacy value, so a
  # bare structure() with the right class attribute walked through.
  forged <- structure(list(required_fte = 1, method = "made_up"),
                      class = "urps_baseline_gap")
  expect_error(assert_baseline_gap_estimated(forged, "strict", allow_analogy = TRUE),
               "missing required field")

  # Present but nonsensical values are refused too: required_fte is the LEVEL
  # every projected year is scaled from.
  for (bad in list(0, -5, NA_real_, Inf)) {
    g <- structure(list(required_fte = bad, adequacy = 0.9, method = "capacity_survey",
                        calibration_status = "calibrated"),
                   class = "urps_baseline_gap")
    expect_error(assert_baseline_gap_estimated(g, "strict", allow_analogy = TRUE),
                 "required_fte", info = paste("required_fte =", format(bad)))
  }
  g <- structure(list(required_fte = 1377, adequacy = NA_real_, method = "capacity_survey",
                      calibration_status = "calibrated"), class = "urps_baseline_gap")
  expect_error(assert_baseline_gap_estimated(g, "strict", allow_analogy = TRUE), "adequacy")
})

test_that("a real gap object still passes, so the guard is not a blanket refusal", {
  skip_if_not_installed("mufflyaccess")
  # A "calibrated" tier passes outright. The structural checks added above must
  # not have turned the gate into a blanket refusal.
  real <- baseline_gap(base_supply_fte = 1306, adequacy = 0.948,
                       method = "capacity_survey", calibration_status = "calibrated",
                       evidence = "adversarial-test fixture")
  expect_true(assert_baseline_gap_estimated(real, "strict", allow_analogy = TRUE))

  # An illustrative tier is well-FORMED but not publishable, so it warns rather
  # than passing silently -- a different refusal from the structural one, and
  # the distinction is the point.
  illus <- baseline_gap(base_supply_fte = 1306, adequacy = 0.948,
                        method = "capacity_survey",
                        calibration_status = "uncalibrated_illustrative",
                        evidence = "adversarial-test fixture")
  expect_message(assert_baseline_gap_estimated(illus, "relaxed", allow_analogy = TRUE),
                 "uncalibrated_illustrative")
  expect_error(assert_baseline_gap_estimated(illus, "strict", allow_analogy = TRUE),
               "uncalibrated_illustrative")
})

# ---- Attack 4: an all-NA scoring table -------------------------------------

test_that("an unscorable back-test cannot report itself validated", {
  # Coverage over all-NA flags is NaN. NaN >= 0.8 is NA, and isTRUE(NA) is
  # FALSE, so this fails safe -- but only by accident of the comparison, so it
  # is pinned rather than assumed.
  s <- backtest_status_from_summary(tibble::tibble(
    within_95 = c(NA, NA), within_80 = c(NA, NA), percent_error = c(1, 2)))
  expect_false(s$validated)
  expect_true(is.nan(s$coverage_95) || is.na(s$coverage_95))
  expect_match(interval_label(s), "NOT a validated forecast interval")
})

# ---- Semantic: units and identities ----------------------------------------

test_that("effective FTE never exceeds headcount when no provider works over 1.0", {
  skip_if_not_installed("mufflyaccess")
  a <- tibble::tibble(provider_id = sprintf("P%03d", 1:80), subspecialty = "FPMRS",
                      sex = rep(c("female", "male"), 40),
                      age = seq(35, 70, length.out = 80),
                      entry_year = 2015L, retirement_year = NA_real_,
                      origin_cohort = "roster", clinical_fte = 1)
  ic <- calibrate_hours_intercept(a$age, a$sex)
  sim <- simulate_provider_career_once(a, 2025:2032, entrants_per_year = 6,
                                       fte_method = "hours", hours_intercept = ic)
  # FTE is a fraction of a person, so headcount and FTE must not be confused --
  # the units error the model is most exposed to, since it reports both and
  # compares FTE against an FTE anchor.
  #
  # But `<= headcount` exactly is STRICTER than this model's contract, and this
  # assertion was passing on luck: it has no set.seed(), so it inherited the RNG
  # stream left by whichever tests ran before it. calibrate_hours_intercept()
  # documents that it makes the BASE year track headcount and that "all
  # subsequent movement comes from the changing age and sex composition", and
  # the engine itself allows FTE_PER_HEAD_TOLERANCE (1.02) of slack. Measured on
  # this cohort: 8 of 40 seeds exceed 1.0, worst ratio 1.00607 -- inside the
  # engine's own tolerance.
  #
  # Seeded, and asserted against the contract that actually exists.
  set.seed(2026)
  sim <- simulate_provider_career_once(a, 2025:2032, entrants_per_year = 6,
                                       fte_method = "hours", hours_intercept = ic)
  ratio <- sim$panel$effective_fte / sim$panel$headcount
  expect_equal(ratio[1], 1, tolerance = 1e-6)          # base year tracks headcount
  expect_true(all(ratio <= FTE_PER_HEAD_TOLERANCE))    # drift stays inside the guard
  expect_true(all(ratio > 0.5))                        # and is not an inverted unit
  expect_true(all(sim$panel$headcount > 0))
  expect_true(all(is.finite(sim$panel$effective_fte)))
})

test_that("entrants are an ANNUAL rate, not a horizon total", {
  a <- tibble::tibble(provider_id = sprintf("P%03d", 1:40), subspecialty = "FPMRS",
                      sex = "female", age = seq(40, 60, length.out = 40),
                      entry_year = 2015L, retirement_year = NA_real_,
                      origin_cohort = "roster", clinical_fte = 1)
  set.seed(11)
  short <- simulate_provider_career_once(a, 2025:2027, 10, fte_method = "hours")
  set.seed(11)
  long <- simulate_provider_career_once(a, 2025:2031, 10, fte_method = "hours")
  # Doubling the horizon must roughly double cumulative intake. If entrants were
  # a total, both runs would end at the same headcount.
  gained_short <- max(short$panel$headcount) - 40
  gained_long <- max(long$panel$headcount) - 40
  expect_gt(gained_long, gained_short * 1.8)
})

test_that("the certification series is a FLOW whose cumulative sum is the stock", {
  skip_if_not_installed("mufflyaccess")
  coh <- urps_certification_cohorts()
  # Stock vs flow is the confusion that produced the entrant double-count: the
  # flow was named net growth, and departures were added to it a second time.
  for (y in c(2018, 2020, 2023)) {
    expect_equal(sum(coh$n_certified[coh$cert_year <= y]),
                 mufflyaccess::urps_count(y, geography = "national",
                                          include_urology = TRUE),
                 info = paste("year", y))
  }
})

# ---- Semantic: what cancels and what does not ------------------------------

test_that("required FTE is invariant to delegation but NOT to the anchor", {
  v <- tidyr::expand_grid(
    year = c(2025, 2050),
    service = c("new_consultation", "return_visit", "sling_procedure",
                "urodynamics", "pessary_care", "cystoscopy", "botox_bladder",
                "ptns", "bladder_instillation", "prolapse_procedure",
                "postoperative_care")) |>
    dplyr::mutate(volume = rep(c(3e6, 7e6, 3e5, 9e5, 1e6, 5e5, 1e5, 3e5, 2e5,
                                 2e5, 1e6), 2) * rep(c(1, 1.15), each = 11))

  # Delegation CANCELS: productivity is solved against the anchor, so the ratio
  # is invariant. A provenance problem, not a results problem.
  s <- delegation_capacity_sensitivity(v)
  expect_equal(diff(range(s$required_fte_target)), 0, tolerance = 1e-8)

  # The ANCHOR does not cancel: it is the level everything is scaled from, and
  # required FTE must move with it proportionally. If this ever became invariant
  # too, the model would have no demand side at all.
  base <- delegation_capacity_sensitivity(v, anchor_fte = 1000)$required_fte_target[1]
  dbl <- delegation_capacity_sensitivity(v, anchor_fte = 2000)$required_fte_target[1]
  expect_equal(dbl / base, 2, tolerance = 1e-6)
})

test_that("more entrants means more supply, and earlier retirement means less", {
  skip_if_not_installed("mufflyaccess")
  a <- tibble::tibble(provider_id = sprintf("P%03d", 1:120), subspecialty = "FPMRS",
                      sex = rep(c("female", "male"), 60),
                      age = seq(38, 70, length.out = 120),
                      entry_year = 2010L, retirement_year = NA_real_,
                      origin_cohort = "roster", clinical_fte = 1)
  fin <- function(e, sched) {
    set.seed(5)
    simulate_provider_career_once(a, 2025:2040, e, retirement_schedule = sched,
                                  fte_method = "hours")$panel$headcount[16]
  }
  base_sched <- RETIREMENT_HAZARD_BY_AGE
  # Directional sanity. These are the two levers the whole supply story rests
  # on; if either sign flipped, every scenario comparison would be backwards.
  expect_gt(fin(20, base_sched), fin(5, base_sched))
  expect_lt(fin(10, pmin(base_sched * 2, 1)), fin(10, base_sched))
})
