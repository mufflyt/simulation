# Known-truth tests for the recurrence cohort convolution.
#
# The formulation being replaced was recurrences_t = procedures_t x hazard,
# which uses THIS YEAR's operations as the entire risk set. Several properties
# below are impossible under that formulation -- they are the tests that would
# have caught it, and they are written so they cannot be satisfied by collapsing
# a historical stock back into a contemporaneous flow.

# The decisive fixture: one cohort, then two years of nothing.
.rc_cohorts <- function() {
  tibble::tibble(year = c(2019L, 2020L, 2021L), n = c(100, 0, 0))
}
.rc_g <- c(0.10, 0.05, 0.02)   # first recurrence in year 1 / 2 / 3 after treatment

test_that("REFERENCE CALCULATION matches by hand", {
  # 2020 = 100 x 0.10          = 10
  # 2021 = 100 x 0.05 +   0    =  5
  # 2022 = 100 x 0.02 + 0 + 0  =  2
  r <- recurrence_from_cohorts(.rc_cohorts(), .rc_g,
                               years = c(2020L, 2021L, 2022L),
                               require_history = FALSE)
  expect_equal(r$recurrences, c(10, 5, 2))
})

test_that("ZERO PROCEDURES THIS YEAR STILL PRODUCES RECURRENCES", {
  # THE KILLER TEST. 2021 and 2022 have no new procedures at all, yet prior
  # cohorts remain at risk. The old formulation returns 0 here by construction.
  r <- recurrence_from_cohorts(.rc_cohorts(), .rc_g,
                               years = c(2021L, 2022L), require_history = FALSE)
  expect_true(all(r$recurrences > 0))
})

test_that("no history means no recurrence", {
  r <- recurrence_from_cohorts(tibble::tibble(year = 2019L, n = 0), .rc_g,
                               years = 2020L, require_history = FALSE)
  expect_equal(r$recurrences, 0)
})

test_that("doubling every prior cohort doubles recurrences", {
  base <- recurrence_from_cohorts(.rc_cohorts(), .rc_g, years = 2021L,
                                  require_history = FALSE)
  dbl  <- recurrence_from_cohorts(transform(.rc_cohorts(), n = n * 2), .rc_g,
                                  years = 2021L, require_history = FALSE)
  expect_equal(dbl$recurrences, 2 * base$recurrences)
})

test_that("deleting a historical cohort cannot INCREASE recurrences", {
  full <- tibble::tibble(year = 2018:2021, n = c(50, 100, 80, 0))
  less <- tibble::tibble(year = 2018:2021, n = c(50,   0, 80, 0))
  a <- recurrence_from_cohorts(full, .rc_g, years = 2022L, require_history = FALSE)
  b <- recurrence_from_cohorts(less, .rc_g, years = 2022L, require_history = FALSE)
  expect_lte(b$recurrences, a$recurrences)
})

test_that("recurrence never precedes its index treatment", {
  r <- recurrence_from_cohorts(.rc_cohorts(), .rc_g,
                               years = c(2018L, 2019L), require_history = FALSE)
  expect_equal(r$recurrences, c(0, 0))
})

test_that("current-year procedures cannot alter recurrences from older cohorts", {
  # The property the old formulation inverts completely: there, this year's
  # procedure count IS the recurrence driver.
  a <- tibble::tibble(year = 2019:2021, n = c(100, 0,    0))
  b <- tibble::tibble(year = 2019:2021, n = c(100, 0, 9999))
  ra <- recurrence_from_cohorts(a, .rc_g, years = 2021L, require_history = FALSE)
  rb <- recurrence_from_cohorts(b, .rc_g, years = 2021L, require_history = FALSE)
  expect_equal(ra$recurrences, rb$recurrences)
})

test_that("shifting the whole history one year shifts output one year", {
  base    <- recurrence_from_cohorts(.rc_cohorts(), .rc_g, years = 2020:2022,
                                     require_history = FALSE)
  shifted <- recurrence_from_cohorts(transform(.rc_cohorts(), year = year + 1L),
                                     .rc_g, years = 2021:2023,
                                     require_history = FALSE)
  expect_equal(base$recurrences, shifted$recurrences)
})

test_that("a cohort ages forward, never back to year one", {
  # 100 treated in 2019 contributes g1 in 2020 and g2 in 2021 -- not g1 twice.
  r <- recurrence_from_cohorts(.rc_cohorts(), .rc_g, years = c(2020L, 2021L),
                               require_history = FALSE)
  expect_equal(r$recurrences[1], 100 * .rc_g[1])
  expect_equal(r$recurrences[2], 100 * .rc_g[2])
  expect_true(r$recurrences[2] != r$recurrences[1])
})

test_that("cumulative first recurrence cannot exceed the treated cohort", {
  expect_error(recurrence_from_cohorts(.rc_cohorts(), c(0.6, 0.6)),
               "cannot exceed the treated cohort")
})

test_that("negative cohorts and negative g are refused", {
  expect_error(recurrence_from_cohorts(tibble::tibble(year = 2019L, n = -5), .rc_g),
               "non-negative")
  expect_error(recurrence_from_cohorts(.rc_cohorts(), c(0.1, -0.2)), "non-negative")
})

# ---------------------------------------------------------------------------
# Converters: risk / hazard / cumulative incidence are NOT interchangeable
# ---------------------------------------------------------------------------

test_that("cumulative incidence differences to event probabilities", {
  expect_equal(recurrence_g_from_cumulative(c(0.10, 0.15, 0.17)),
               c(0.10, 0.05, 0.02))
})

test_that("conditional hazards convert via the survival function", {
  # g_k = S_k h_k, S_k = prod_{j<k}(1 - h_j)
  h <- c(0.10, 0.10, 0.10)
  g <- recurrence_g_from_hazards(h)
  expect_equal(g, c(0.10, 0.09, 0.081))
  # and the two readings genuinely differ -- this is the confusion that turned
  # a multi-year cumulative curve into an "annual hazard"
  expect_false(isTRUE(all.equal(g, recurrence_g_from_cumulative(h))))
})

test_that("a decreasing 'cumulative' curve is refused as mis-supplied", {
  expect_error(recurrence_g_from_cumulative(c(0.10, 0.05)), "non-decreasing")
})

test_that("hazards at or above 1 are refused", {
  expect_error(recurrence_g_from_hazards(c(0.5, 1.0)), "\\[0, 1\\)")
})

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

test_that("insufficient pre-baseline history is REFUSED, not zero-filled", {
  # Zero-filling prior cohorts suppresses early-forecast recurrence, which is
  # the defect this whole module replaces.
  only_recent <- tibble::tibble(year = 2025L, n = 100)
  expect_error(recurrence_from_cohorts(only_recent, .rc_g, years = 2026L),
               "preceding treatment cohorts")
})

test_that("burn-in is available but must be asked for explicitly", {
  only_recent <- tibble::tibble(year = 2025L, n = 100)
  r <- recurrence_from_cohorts(only_recent, .rc_g, years = 2026L,
                               require_history = FALSE)
  expect_equal(r$recurrences, 10)
  expect_equal(r$cohorts_contributing, 1)   # reported, so the shortfall is visible
})

test_that("the recurrence parameters remain unresolved", {
  # 0.12 is documented as an annual hazard but justified by SUPeR/E-CARE
  # cumulative retreatment curves. Until that is settled the convolution must
  # not be fed with it.
  expect_equal(recurrence_parameter_status(), "unresolved_requires_source")
})
