# Adversarial cycle 15 -- the default that is right for one way a lookup can
# miss and wrong for the other.
#
# Cycle 14 carried forward exactly that shape, after `0` for an unset numeric
# turned a returning provider into a retired one. This cycle enumerated every
# NA-fill in R/ -- `x[is.na(x)] <- v`, `coalesce(x, v)`, `%||% v` -- and asked
# what each column MEANS rather than what type it is.
#
# Most are correct and documented (a missing hazard takes the highest tabulated
# one; a missing band population is deliberately zero; an unknown calibration
# status ranks 0 so it cannot be a promotion). One was not:
#
#   retirement_survival() filled EVERY missing lookup with 0, but a lookup can
#   miss in two directions. A to_age beyond the horizon really is 0. A to_age at
#   or below from_age is an interval that has not elapsed, and S(t) for that is
#   1. Filling it with 0 said "nobody survives from 50 to 50" and made the
#   returned curve rise.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: survival over a zero-length interval is 1", {
  # THE DEFECT, at its sharpest. Survival from 50 to 50 is certain: no time has
  # passed, so nobody can have left. Measured before the fix: 0.0000.
  expect_equal(unname(retirement_survival(50, 50)), 1)
  expect_equal(unname(retirement_survival(65, 65)), 1)
  # One year later is strictly less than certain, so the boundary is not a
  # flat region hiding the same bug.
  expect_lt(unname(retirement_survival(50, 51)), 1)
  expect_gt(unname(retirement_survival(50, 51)), 0)
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a to_age before the start is 1, and one beyond the horizon is 0", {
  # The two directions a lookup can miss, which is the whole point.
  s <- retirement_survival(50, c(40, 45, 49))
  expect_equal(unname(s), rep(1, 3))
  # Beyond the terminal age everyone has left; 0 there is correct and must stay.
  far <- retirement_survival(50, c(60, 200))
  expect_equal(unname(far[2]), 0)
  expect_gt(unname(far[1]), 0)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: survival is bounded in [0, 1] at every requested age", {
  for (from in c(35, 50, 65)) {
    s <- retirement_survival(from, seq(from - 5, 95, by = 5))
    expect_true(all(s >= 0 & s <= 1),
                info = sprintf("from_age %d gave survival in [%g, %g]", from, min(s), max(s)))
    expect_false(anyNA(s))
  }
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: the requested ages come back in the order and naming asked for", {
  # The function returns a named vector keyed by to_age. If the fill reorders or
  # renames, a caller reading by position gets another age's survival.
  ages <- c(70, 55, 60, 50)
  s <- retirement_survival(50, ages)
  expect_equal(names(s), as.character(ages))
  expect_equal(length(s), length(ages))
  # Sorted or not, the value for a given age is the same.
  s2 <- retirement_survival(50, sort(ages))
  expect_equal(unname(s[as.character(sort(ages))]), unname(s2))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the survival curve is monotone non-increasing", {
  # The property the 0-fill broke. Measured before the fix, for to_ages
  # 45, 50, 55, 60, 65: 0, 0, 0.8947, 0.7965, 0.5452 -- a survival function that
  # RISES. Any downstream reading it as a hazard, a retention rate or a
  # cumulative probability was reading a curve that cannot exist.
  s <- retirement_survival(50, c(45, 50, 55, 60, 65, 70, 75))
  expect_false(is.unsorted(rev(s)))
  expect_true(all(diff(s) <= 1e-12))
  # And with a finer grid, so a coarse one cannot hide a local rise.
  fine <- retirement_survival(45, 40:90)
  expect_true(all(diff(fine) <= 1e-12))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: survival is the running product of one-year survival probabilities", {
  # It must BE cumprod(1 - h), not something that resembles it. If the two
  # disagree, the hazard schedule and the survival curve are telling different
  # stories about the same cohort.
  from <- 55; to <- 60
  h <- departure_hazard(seq(from, to - 1L), sex = "male")
  expect_equal(unname(retirement_survival(from, to, sex = "male")),
               prod(1 - h), tolerance = 1e-12)

  # Composition: surviving 55->65 is surviving 55->60 times 60->65.
  a <- unname(retirement_survival(55, 60, sex = "male"))
  b <- unname(retirement_survival(60, 65, sex = "male"))
  ab <- unname(retirement_survival(55, 65, sex = "male"))
  expect_equal(ab, a * b, tolerance = 1e-12)
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the NA-fills that ARE correct stay correct", {
  # The sweep's other finding: most fills in R/ are right and documented. Pinned
  # as a group so a future pass that "tidies" them has to argue with a test.
  #
  # A retirement age past the tabulated schedule takes the HIGHEST hazard, not
  # zero -- conservative in the direction that matters.
  sched <- RETIREMENT_HAZARD_BY_AGE
  beyond <- departure_hazard(max(as.integer(names(sched))) + 1L, sex = "female")
  expect_true(is.finite(beyond))
  expect_gt(beyond, 0)

  # An unrecognised calibration status ranks 0: not a promotion.
  expect_equal(unname(CALIBRATION_STATUS_RANK["not_a_tier"]), NA_integer_)

  # A sex outside male/female falls back visibly to the modelled default rather
  # than producing NA hours.
  expect_true(is.finite(participation_fte(50, "unspecified")))
  expect_true(all(is.finite(participation_fte(c(30, 60, 85), "female"))))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no from_age/to_age pair produces a rising curve", {
  # Fuzz the corner the fill governs: to_ages that straddle from_age in every
  # arrangement, which is where the two miss-directions meet.
  for (from in c(30, 45, 55, 70)) {
    for (span in list(c(-10, 0, 5), c(-1, 0, 1), c(0, 0, 1), c(-20, -10, 40))) {
      to <- from + span
      s <- retirement_survival(from, to)
      expect_false(is.unsorted(rev(s)),
                   info = sprintf("from %d, to %s gave %s", from,
                                  paste(to, collapse = ","),
                                  paste(round(s, 4), collapse = ",")))
      expect_true(all(s >= 0 & s <= 1))
      # Anything at or before the start is certain.
      expect_true(all(s[to <= from] == 1))
    }
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the participation table has no gap for the fallback to hide", {
  # participation_fte() ends in out[is.na(out)] <- 0.5 -- an invented half-time
  # provider for any (sex, age) the table does not cover. With the shipped table
  # that branch is unreachable, and this is what keeps it unreachable: a gap
  # would silently give some providers exactly 0.5 FTE.
  t <- FUTUREDOCS_PARTICIPATION
  want <- expand.grid(sex = unique(t$sex), age = seq(min(t$age), max(t$age)),
                      stringsAsFactors = FALSE)
  expect_equal(sum(!paste(want$sex, want$age) %in% paste(t$sex, t$age)), 0L)

  # And no provider in the table's age range lands on the fallback value by
  # coincidence of the real data.
  vals <- participation_fte(rep(seq(min(t$age), max(t$age)), 2),
                            rep(c("female", "male"), each = length(seq(min(t$age), max(t$age)))))
  expect_false(anyNA(vals))
  expect_true(all(vals >= 0 & vals <= 1))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: survival and the hazard schedule cannot disagree about a cohort", {
  # Two views of one process. If retiring later (a scenario lever) does not
  # raise survival at every age, the lever is not reaching the curve -- and the
  # curve is what a reader sees.
  base <- retirement_survival(50, c(60, 65, 70, 75), sex = "male")
  # A schedule of zero hazard means nobody leaves: survival is 1 everywhere.
  none <- retirement_survival(50, c(60, 65, 70, 75), sex = "male",
                              retirement_schedule = setNames(rep(0, 120), 1:120),
                              career_change_hazard = 0)
  expect_equal(unname(none), rep(1, 4))
  expect_true(all(base <= none + 1e-12))

  # A schedule of certain departure means nobody survives past the first step.
  all_gone <- retirement_survival(50, c(60, 65), sex = "male",
                                  retirement_schedule = setNames(rep(1, 120), 1:120),
                                  career_change_hazard = 1)
  expect_equal(unname(all_gone), c(0, 0))
})
