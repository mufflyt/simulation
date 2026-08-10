# Adversarial cycle 10 -- FTE versus headcount, done against the contract that
# actually exists.
#
# The cycle 09 full-suite run failed test-adversarial-guards.R:140
# ("effective FTE never exceeds headcount"). Bisected to cycle 03, which touched
# no FTE code at all -- the test has no set.seed(), so cycle 03's new guards
# shifted RNG consumption in an earlier test in the same file and the new stream
# surfaced something that was always reachable.
#
# Two documents in this repository disagree, and resolving that is this cycle:
#
#   supply-provider_microsimulation.R:  "More FTE than people ... is
#     dimensionally impossible under an hours-threshold FTE definition, so
#     strict mode refuses it outright."
#   calibrate_hours_intercept() docs:   "...so base-year FTE tracks headcount
#     and ALL SUBSEQUENT MOVEMENT comes from the changing age and sex
#     composition."
#
# The second is what the model does; the first is what one guard claims. And
# that guard runs on the BASE COHORT ONLY, so it passes by construction and
# never sees the drift.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc10_cohort <- function(n = 80, lo = 35, hi = 70) {
  data.frame(
    provider_id = sprintf("P%03d", seq_len(n)), subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(lo, hi, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "roster", stringsAsFactors = FALSE
  )
}
cyc10_run <- function(a, years, entrants, ...) {
  simulate_provider_career_once(
    a, years, entrants, fte_method = "hours",
    hours_intercept = calibrate_hours_intercept(a$age, a$sex), ...)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the base year tracks headcount exactly, whatever the cohort", {
  # This is the ONE year the calibration guarantees, and it guarantees it
  # exactly -- mean hours are solved to equal the FTE threshold. If the base
  # year is off, the intercept and the threshold disagree and every projected
  # year inherits it.
  for (spec in list(c(35, 70), c(60, 75), c(40, 45), c(50, 50))) {
    a <- cyc10_cohort(lo = spec[1], hi = spec[2])
    set.seed(10)
    p <- cyc10_run(a, 2025:2027, 5)$panel
    expect_equal(p$effective_fte[1] / p$headcount[1], 1, tolerance = 1e-8,
                 info = sprintf("base ages %g-%g", spec[1], spec[2]))
  }
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the two dimensional guards read one shared tolerance", {
  # They used to be one literal 1.02 in one place. A second check written with
  # its own copy is how two guards start disagreeing about what they permit.
  expect_true(is.numeric(FTE_PER_HEAD_TOLERANCE))
  expect_equal(FTE_PER_HEAD_TOLERANCE, 1.02)
  expect_gt(FTE_PER_HEAD_TOLERANCE, 1)      # some slack, or composition trips it
  expect_lt(FTE_PER_HEAD_TOLERANCE, 1.1)    # not so much that it permits nonsense
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a single-provider cohort is the degenerate FTE case", {
  # With one provider the ratio is that provider's own FTE, so the base-year
  # calibration must put it at exactly 1.0 -- the smallest case where "mean
  # hours equal the threshold" and "this person works full time" coincide.
  a <- cyc10_cohort(n = 1, lo = 52, hi = 52)
  set.seed(11)
  p <- cyc10_run(a, 2025L, 0)$panel
  expect_equal(p$headcount, 1L)
  expect_equal(p$effective_fte / p$headcount, 1, tolerance = 1e-8)

  # And an FTE is never negative, whatever the age.
  for (age in c(18, 34, 50, 80, 100)) {
    expect_gte(provider_clinical_fte(age, "female", method = "hours",
                                     hours_intercept = 40), 0)
  }
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: the hours-to-FTE map is exactly hours divided by the threshold", {
  # FTE is a ratio, not a curve of its own. If this is not exact, some other
  # scaling has crept in between the hours schedule and the reported supply.
  ic <- 40
  for (age in c(34, 45, 60, 72)) for (sx in c("female", "male")) {
    h <- hwsm_reference_hours(age, sx, intercept = ic)
    expect_equal(provider_clinical_fte(age, sx, method = "hours", hours_intercept = ic),
                 h / URPS_FTE_CLINICAL_HOURS_PER_WEEK, tolerance = 1e-12,
                 info = sprintf("age %g %s", age, sx))
  }
  # A provider working exactly the threshold is exactly 1.0 FTE, which is the
  # definition the whole module rests on.
  expect_equal(URPS_FTE_CLINICAL_HOURS_PER_WEEK / URPS_FTE_CLINICAL_HOURS_PER_WEEK, 1)
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: FTE per head drifts with composition, and in the direction composition implies", {
  # THE CONTRACT, stated as a test. An OLDER base cohort calibrates the
  # intercept upward, so entrants at MICROSIM_ENTRY_AGE sit above the base mean
  # and the ratio rises as they accumulate. A YOUNGER base cohort does the
  # reverse. Neither is an error; asserting `<= 1` was.
  old_base <- cyc10_cohort(lo = 60, hi = 75)
  set.seed(12)
  p_old <- cyc10_run(old_base, 2025:2045, 25)$panel
  r_old <- p_old$effective_fte / p_old$headcount
  expect_equal(r_old[1], 1, tolerance = 1e-8)
  expect_gt(max(r_old), 1)            # drifts UP as young entrants accumulate
  expect_gt(max(r_old), r_old[1])

  young_base <- cyc10_cohort(lo = 34, hi = 40)
  set.seed(12)
  p_young <- cyc10_run(young_base, 2025:2045, 25)$panel
  r_young <- p_young$effective_fte / p_young$headcount
  expect_equal(r_young[1], 1, tolerance = 1e-8)
  # The two cohorts must not drift the same way, or composition is not driving it.
  expect_false(isTRUE(all.equal(max(r_old), max(r_young), tolerance = 1e-3)))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: headcount is a count and FTE is not, and neither is derived from the other", {
  # The units error this whole area exists to prevent: reporting one as the
  # other. Headcount must be a whole number; FTE must not be forced to one.
  a <- cyc10_cohort()
  set.seed(13)
  p <- cyc10_run(a, 2025:2032, 6)$panel
  expect_true(is.integer(p$headcount) || all(p$headcount == trunc(p$headcount)))
  expect_false(all(p$effective_fte == trunc(p$effective_fte)))
  expect_true(all(p$effective_fte > 0))

  # Zero entrants and no exits freeze headcount but NOT FTE -- the cohort still
  # ages, and hours change with age. If FTE also froze, it is being read off
  # headcount rather than computed.
  set.seed(13)
  q <- cyc10_run(a, 2025:2032, 0,
                 retirement_schedule = setNames(rep(0, 100), 1:100),
                 career_change_hazard = 0)$panel
  expect_true(all(q$headcount == nrow(a)))
  expect_false(isTRUE(all.equal(q$effective_fte[1], q$effective_fte[8])))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: calibration is a property of the cohort it was solved on", {
  # calibrate_hours_intercept() is not a constant. Solving it on cohort A and
  # applying it to cohort B is exactly how the base year stops tracking
  # headcount -- worth pinning, because the engine takes the intercept as an
  # argument and cannot tell where it came from.
  a <- cyc10_cohort(lo = 35, hi = 70)
  b <- cyc10_cohort(lo = 60, hi = 75)
  ic_a <- calibrate_hours_intercept(a$age, a$sex)
  ic_b <- calibrate_hours_intercept(b$age, b$sex)
  expect_false(isTRUE(all.equal(ic_a, ic_b)))

  # Applied to its own cohort, the mean is exactly the threshold.
  expect_equal(mean(hwsm_reference_hours(a$age, a$sex, intercept = ic_a)),
               URPS_FTE_CLINICAL_HOURS_PER_WEEK, tolerance = 1e-8)
  # Applied to the other cohort, it is not -- and that is the mismatch the
  # base-year check is there to catch.
  expect_false(isTRUE(all.equal(
    mean(hwsm_reference_hours(b$age, b$sex, intercept = ic_a)),
    URPS_FTE_CLINICAL_HOURS_PER_WEEK, tolerance = 1e-3)))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: the dimensional guard covers the projection, not just the base cohort", {
  # THE DEFECT. The guard computed mean FTE over `base_agents` only. Because
  # calibrate_hours_intercept() makes that mean exactly 1.0 by construction, it
  # passed every time and never looked again. Measured with an older base
  # cohort (ages 60-75, 25 entrants/yr): base-year ratio 1.0000, ratio 1.0783 by
  # 2041 -- a 7.8% breach of the guard's own 1.02 tolerance, in a strict-mode
  # run, with nothing firing.
  skip_if_not_installed("mufflyaccess")
  a <- cyc10_cohort(lo = 60, hi = 75)
  ic <- calibrate_hours_intercept(a$age, a$sex)
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA_character_)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE") else
            Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)

  Sys.setenv(REPRODUCIBILITY_MODE = "relaxed")
  expect_message(
    run_supply_microsimulation(initial_workforce = a, years = 2025:2045,
                               entrants_per_year = 25, n_iterations = 3,
                               fte_method = "hours", hours_intercept = ic,
                               allow_fixed_parameters = TRUE, verbose = FALSE),
    "exceeds headcount in the projection")

  Sys.setenv(REPRODUCIBILITY_MODE = "strict")
  expect_error(
    suppressMessages(run_supply_microsimulation(
      initial_workforce = a, years = 2025:2045, entrants_per_year = 25,
      n_iterations = 3, fte_method = "hours", hours_intercept = ic,
      allow_fixed_parameters = TRUE, verbose = FALSE)),
    "exceeds headcount in the projection")
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a well-composed projection does not trip the new guard", {
  # A guard that fires on ordinary runs gets switched off. The default-shaped
  # cohort must stay silent, or the previous test proves nothing about
  # specificity.
  skip_if_not_installed("mufflyaccess")
  a <- cyc10_cohort(lo = 35, hi = 70)
  ic <- calibrate_hours_intercept(a$age, a$sex)
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA_character_)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE") else
            Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)
  Sys.setenv(REPRODUCIBILITY_MODE = "strict")

  res <- suppressMessages(run_supply_microsimulation(
    initial_workforce = a, years = 2025:2035, entrants_per_year = 6,
    n_iterations = 3, fte_method = "hours", hours_intercept = ic,
    allow_fixed_parameters = TRUE, verbose = FALSE))
  ratio <- res$summary$effective_fte_median / res$summary$headcount_median
  expect_true(all(ratio <= FTE_PER_HEAD_TOLERANCE))
  expect_equal(ratio[1], 1, tolerance = 1e-3)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the FTE/headcount ratio does not depend on the ambient RNG stream", {
  # The reason the original failure was invisible for so long: the test that
  # caught it had no set.seed(), so its verdict depended on which tests ran
  # before it. A property that is true of the model must hold under any stream.
  a <- cyc10_cohort()
  ic <- calibrate_hours_intercept(a$age, a$sex)
  ratios <- vapply(1:25, function(s) {
    set.seed(s)
    p <- simulate_provider_career_once(a, 2025:2032, entrants_per_year = 6,
                                       fte_method = "hours", hours_intercept = ic)$panel
    max(p$effective_fte / p$headcount)
  }, numeric(1))
  # Every stream must satisfy the contract; none may satisfy an accidental
  # stricter one, or the contract is being read off one lucky seed.
  expect_true(all(ratios <= FTE_PER_HEAD_TOLERANCE),
              info = sprintf("worst ratio across 25 seeds was %.5f", max(ratios)))
  # And at least one stream must exceed 1.0, or the original `<= headcount`
  # assertion would in fact hold on this cohort and there would be nothing here
  # to correct.
  expect_gt(max(ratios), 1)
  expect_true(all(ratios > 0.9))
})
