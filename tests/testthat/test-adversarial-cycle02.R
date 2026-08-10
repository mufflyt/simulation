# Adversarial cycle 02 -- the recycling sweep, scenario propagation, calibration
# targets, leakage audit.
#
# Cycle 01 left a bug class open: "silent recycling where alignment matters."
# This cycle discharges it. rep_len() was reached in nine places where the
# recycled vector is a PER-PROVIDER covariate, and unlike base arithmetic
# recycling it emits no warning on a non-multiple length -- so a length-3 sex
# vector over four providers assigned provider 4 provider 1's sex and changed
# the answer with no diagnostic. Tests 4 and 5 pin the refusal.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: calibration scalars flag strictly outside [1/max, max], not at it", {
  # The flag decides whether a mismatch is reported as a calibration offset or
  # as a structural error. An off-by-one-boundary here silently reclassifies it.
  at_bounds <- fit_calibration_scalars(
    predicted = data.frame(category = c("hi", "lo"), predicted = c(1, 3)),
    observed  = data.frame(category = c("hi", "lo"), observed  = c(3, 1)),
    max_scalar = 3)
  expect_equal(sort(at_bounds$scalar), c(1 / 3, 3))
  expect_false(any(at_bounds$flagged),
               info = "a scalar exactly at the bound was flagged; the test is > and <")

  outside <- suppressWarnings(fit_calibration_scalars(
    predicted = data.frame(category = c("hi", "lo"), predicted = c(1, 3 + 1e-9)),
    observed  = data.frame(category = c("hi", "lo"), observed  = c(3 + 1e-9, 1)),
    max_scalar = 3))
  expect_true(all(outside$flagged))

  # A zero prediction has no defined scalar. Inf would propagate through
  # apply_calibration_scalars() and blow up every downstream count.
  zero <- fit_calibration_scalars(
    predicted = data.frame(category = "z", predicted = 0),
    observed  = data.frame(category = "z", observed  = 500),
    max_scalar = 3)
  expect_true(is.na(zero$scalar))
  expect_false(zero$flagged)
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: proportionality detection is closed at the tolerance", {
  # Two estimands that are exact rescalings of each other have rank correlation
  # 1 by construction. The tolerance decides what counts as "exact", so both
  # sides of it must behave.
  # a/b has ratios (2, 2k), so the spread is 2(k-1) against a tolerance of
  # tol * max|r| = tol * 2k. k = 1 + tol sits exactly on the boundary.
  mk <- function(k) {
    data.frame(year = c(2025L, 2026L),
               estimand = rep(c("a", "b"), each = 2),
               demand_cases = c(100, 100 * k, 50, 50))
  }
  tol <- 1e-8
  # Exactly at tol * max|r|: caught, because the comparison is <=.
  expect_equal(nrow(detect_proportional_estimands(mk(1 + tol), tol = tol)), 1L)
  # A thousandth of a tolerance beyond it: not proportional.
  expect_equal(nrow(detect_proportional_estimands(mk(1 + 1.001 * tol), tol = tol)), 0L)

  # A single year cannot establish proportionality: one point always lies on
  # some line through the origin. Requires >= 2 finite pairs.
  one_year <- data.frame(year = 2025L, estimand = c("a", "b"),
                         demand_cases = c(100, 50))
  expect_equal(nrow(detect_proportional_estimands(one_year)), 0L)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the leakage cutoff is inclusive of the cutoff year and nothing beyond", {
  on.exit(reset_leakage_audit(), add = TRUE)

  reset_leakage_audit()
  .backtest_audit$max_year <- 2020
  .backtest_audit$reads <- "series: <= 2020"
  expect_silent(assert_no_leakage(through_year = 2020L))   # at the cutoff: legal

  .backtest_audit$max_year <- 2021
  .backtest_audit$reads <- "series: <= 2021"
  expect_error(assert_no_leakage(through_year = 2020L), "LEAKAGE")
  # One year past the cutoff is the case a fencepost error produces, so the
  # message must name both years rather than say "leakage" and stop.
  expect_error(assert_no_leakage(through_year = 2020L), "2021")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: supply_p_active refuses a partial covariate instead of misaligning it", {
  # THE DEFECT. rep_len(years_certified, 4) on c(2, 17, 32) gave provider 4 a
  # years_certified of 2 -- a 75-year-old scored as freshly certified. It
  # returned a number, no warning, and every aggregate looked plausible.
  expect_error(supply_p_active(c(35, 50, 65, 75), "female", c(2, 17, 32)),
               "length 3 but must be length 1 or 4")
  expect_error(supply_p_active(c(35, 50, 65, 75), c("female", "male"), 10),
               "length 2 but must be length 1 or 4")

  # Scalar recycling is the documented contract and must still work, and must
  # agree elementwise with the explicit full-length form.
  scalar <- supply_p_active(c(35, 50, 65, 75), "female", 10)
  spelled <- supply_p_active(c(35, 50, 65, 75), rep("female", 4), rep(10, 4))
  expect_equal(scalar, spelled)
  expect_length(scalar, 4L)
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: every per-provider covariate in the lifecycle family refuses partial recycling", {
  # The sweep. One site fixed and eight left is not a fix -- it just moves the
  # silent misalignment to whichever function the next caller reaches for.
  age4 <- c(38, 52, 66, 71)
  partial_sex <- c("female", "male")

  expect_error(departure_hazard(age4, sex = partial_sex), "length 2 but must be length 1 or 4")
  expect_error(predict_clinical_hours(age4, sex = partial_sex), "length 2 but must be length 1 or 4")
  expect_error(participation_fte(age4, sex = partial_sex), "length 2 but must be length 1 or 4")
  expect_error(participation_p_no_patient_care(age4, sex = partial_sex),
               "length 2 but must be length 1 or 4")
  expect_error(career_departure_by_state(age4, sex = partial_sex),
               "length 2 but must be length 1 or 4")
  expect_error(career_state_of(c(38, 52, 66), retired = c(TRUE, FALSE)),
               "length 2 but must be length 1 or 3")
  expect_error(hwsm_reference_hours(age4, sex = partial_sex), "length 2 but must be length 1 or 4")

  # And the scalar contract survives everywhere.
  expect_length(departure_hazard(age4, sex = "female"), 4L)
  expect_length(participation_fte(age4, sex = "male"), 4L)
  expect_equal(as.character(career_state_of(c(38, 52, 66))),
               c("early_career", "mid_career", "late_career"))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: a category with no fitted scalar is refused, not silently left unscaled", {
  # TWO DEFECTS, one root: the function coalesced a missing scalar to 1, so an
  # uncalibrated category came back in a table labelled calibrated.
  #
  # (a) A category the anchor does not cover. min_match_rate only WARNS in
  #     relaxed mode, so the coalesce was reachable in ordinary use.
  values  <- data.frame(category = c("a", "b"), predicted = c(100, 200))
  scalars <- data.frame(category = "a", scalar = 2)
  expect_error(suppressWarnings(apply_calibration_scalars(values, scalars)),
               "no calibration scalar for category b")

  # (b) A matched category whose scalar is NA -- what fit_calibration_scalars()
  #     emits when predicted == 0. The key matches, so the join guard is silent.
  na_scalars <- data.frame(category = c("a", "b"), scalar = c(2, NA_real_))
  expect_error(apply_calibration_scalars(values, na_scalars),
               "no calibration scalar for category b")

  full <- data.frame(category = c("a", "b"), scalar = c(2, 0.5))
  out <- apply_calibration_scalars(values, full)
  expect_equal(out$predicted, c(200, 100))

  # Re-applying to an already-calibrated table did nothing at all: the join
  # suffixed the column to .x/.y, out$calibration_scalar became NULL, and every
  # value was multiplied by 1 while the result still carried calibration columns.
  expect_error(
    apply_calibration_scalars(out, data.frame(category = c("a", "b"), scalar = c(0.5, 2))),
    "already carries a `calibration_scalar`")

  # Dropped deliberately, scaling is multiplicative and invertible.
  inv <- apply_calibration_scalars(out[, c("category", "predicted")],
                                   data.frame(category = c("a", "b"), scalar = c(0.5, 2)))
  expect_equal(inv$predicted, values$predicted)
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: a fractional retirement shift reaches the model at its declared size", {
  # as.integer() truncated toward zero, so a registry declaring a half-year
  # shift arrived as 0 and a -1.5 year shift arrived as -1. The scenario was
  # then quieter (or, on the negative side, differently sized) than the
  # registry it came from -- scenario propagation failing silently.
  reg <- local_supply_scenario_registry()
  reg$half_year_later <- utils::modifyList(reg$status_quo,
                                           list(label = "Half a year later",
                                                retirement_shift_years = 0.5))
  reg$eighteen_months_earlier <- utils::modifyList(reg$status_quo,
                                                   list(label = "18 months earlier",
                                                        retirement_shift_years = -1.5))
  expect_silent(validate_scenario_registry(reg, kind = "supply"))

  base <- supply_p_active(65, "male", 32, scenario_id = "status_quo", registry = reg)
  half <- supply_p_active(65, "male", 32, scenario_id = "half_year_later", registry = reg)
  early <- supply_p_active(65, "male", 32, scenario_id = "eighteen_months_earlier", registry = reg)

  expect_gt(half, base)     # retiring later => more likely active at 65
  expect_lt(early, base)

  # A half-year shift must land strictly between no shift and a full year --
  # truncation put it exactly on top of `base`.
  full <- supply_p_active(65, "male", 32,
                          registry = utils::modifyList(
                            reg, list(one = utils::modifyList(reg$status_quo,
                                                              list(retirement_shift_years = 1)))),
                          scenario_id = "one")
  expect_gt(half, base)
  expect_lt(half, full)
  # -1.5 must be strictly stronger than -1, which truncation collapsed together.
  minus_one <- supply_p_active(65, "male", 32,
                               registry = utils::modifyList(
                                 reg, list(m1 = utils::modifyList(reg$status_quo,
                                                                  list(retirement_shift_years = -1)))),
                               scenario_id = "m1")
  expect_lt(early, minus_one)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: an unknown scenario id is never silently treated as baseline", {
  # A typo'd scenario id that quietly resolves to a zero shift makes every
  # scenario comparison look like "the lever does nothing" -- indistinguishable
  # from a genuine null result. It must be audible.
  reg <- local_supply_scenario_registry()
  expect_warning(supply_p_active(65, "male", 32, scenario_id = "retire_2yr_latter",
                                 registry = reg),
                 "unknown scenario_id")
  quiet <- suppressWarnings(supply_p_active(65, "male", 32,
                                            scenario_id = "retire_2yr_latter", registry = reg))
  expect_equal(quiet, supply_p_active(65, "male", 32, registry = reg))

  # A known id must NOT warn, or the warning carries no information.
  expect_silent(supply_p_active(65, "male", 32, scenario_id = "retire_2yr_later",
                                registry = reg))
  # And the registry's own levers must actually differ from each other.
  ids <- c("status_quo", "retire_2yr_later", "retire_2yr_earlier")
  vals <- vapply(ids, function(i) supply_p_active(65, "male", 32, scenario_id = i,
                                                  registry = reg), numeric(1))
  expect_equal(length(unique(round(vals, 8))), 3L,
               info = "two retirement scenarios produced the same P(active)")
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the leakage audit is not silently vacuous after a reset", {
  # The audit is stateful, which cuts both ways. A run that resets and then
  # never reaches .series_through() has proved nothing -- and "no recorded
  # violation" must not be reported as "no leakage".
  on.exit(reset_leakage_audit(), add = TRUE)
  reset_leakage_audit()
  expect_error(assert_no_leakage(through_year = 2020L), "no audited contract reads")

  # A stale read from a PREVIOUS run must still trip the guard rather than be
  # forgiven for being old -- the audit records the maximum, not the latest.
  .backtest_audit$max_year <- 2023
  .backtest_audit$reads <- "stale run: <= 2023"
  .backtest_audit$max_year <- max(.backtest_audit$max_year, 2019)
  expect_error(assert_no_leakage(through_year = 2020L), "LEAKAGE")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: proportional estimands are refused in strict mode, not merely noted", {
  # Three estimands built by rescaling one series agree perfectly, and that
  # agreement is arithmetic, not evidence. Reporting it as concordance is the
  # exact overclaim this guard exists to stop.
  long <- data.frame(
    year = rep(2025:2030, times = 3),
    estimand = rep(c("realized_care", "reduced_barrier", "adequate_need"), each = 6),
    demand_cases = c(seq(1000, 1500, length.out = 6),
                     seq(1000, 1500, length.out = 6) * 1.3,
                     seq(1000, 1500, length.out = 6) * 1.8))
  prop <- detect_proportional_estimands(long)
  expect_equal(nrow(prop), 3L)     # all three pairs
  expect_error(assert_estimands_independent(long, mode = "strict"),
               "proportional rescalings")

  # Give one estimand its own age profile and the pairs involving it dissolve.
  long$demand_cases[13:18] <- seq(1200, 3000, length.out = 6)
  expect_equal(nrow(detect_proportional_estimands(long)), 1L)
  expect_error(assert_estimands_independent(long, mode = "strict"),
               "proportional rescalings")
})
