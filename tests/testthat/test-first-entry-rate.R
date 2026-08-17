# annual_first_urps_entry_rate() -- the estimator for the settled first-entry
# estimand (docs/PATHWAY_STATE_TRANSITION_AUDIT.md §8).
#
# The estimand is a POPULATION-LEVEL RATE, not a conditional hazard, so the
# tests below pin the properties that distinguish the two -- above all that the
# numerator and denominator survive separately, because a rate alone hides
# denominator-transport problems.

.fe_num <- function(n = c(120, 300)) {
  tibble::tibble(condition = c("pop", "pop"), age_band = c("45-54", "55-64"),
                 year = c(2023L, 2023L), n = n)
}
.fe_den <- function(n = c(12000, 20000)) {
  tibble::tibble(condition = c("pop", "pop"), age_band = c("45-54", "55-64"),
                 year = c(2023L, 2023L), n = n)
}
.fe <- function(num = .fe_num(), den = .fe_den(), ...) {
  annual_first_urps_entry_rate(num, den,
                               numerator_source = "MA APCD CY2024 (test fixture)",
                               denominator_source = "model prevalence (test fixture)",
                               ...)
}

test_that("numerator, denominator and rate are all retained separately", {
  # THE POINT. Collapsing to a rate hides the case where an MA-APCD numerator
  # is divided by a national denominator -- a category error a single number
  # makes invisible.
  r <- .fe()
  expect_true(all(c("entrants_n", "eligible_prevalent_n", "rate") %in% names(r)))
  expect_equal(r$entrants_n, c(120, 300))
  expect_equal(r$eligible_prevalent_n, c(12000, 20000))
  expect_equal(r$rate, c(0.01, 0.015))
})

test_that("uncertainty accompanies every estimate and brackets it", {
  r <- .fe()
  expect_true(all(r$rate_lo < r$rate))
  expect_true(all(r$rate_hi > r$rate))
  expect_true(all(r$rate_lo >= 0 & r$rate_hi <= 1))
  # smaller denominator => wider interval, the property that makes thin strata
  # visible instead of confidently wrong
  wide <- .fe(.fe_num(c(12, 30)), .fe_den(c(1200, 2000)))
  expect_gt(wide$rate_hi[1] - wide$rate_lo[1], r$rate_hi[1] - r$rate_lo[1])
})

test_that("provenance is mandatory for BOTH sides", {
  # An estimate whose sources are unrecorded cannot be audited, and the two
  # sides routinely come from different populations.
  expect_error(annual_first_urps_entry_rate(.fe_num(), .fe_den()),
               "numerator_source and denominator_source are required")
  expect_error(
    annual_first_urps_entry_rate(.fe_num(), .fe_den(), numerator_source = "x"),
    "required")
  r <- .fe()
  expect_true(all(nzchar(r$numerator_source)))
  expect_true(all(nzchar(r$denominator_source)))
})

test_that("the denominator definition travels with the estimate", {
  # So nobody downstream reconstructs a lifetime never-entered denominator.
  r <- .fe()
  expect_equal(unique(r$estimand), "annual_first_urps_entry_rate")
  expect_match(unique(r$denominator_definition), "regardless of prior care history")
})

test_that("more entrants than eligible women is REFUSED", {
  # The stock-as-flow signature. Most often a numerator counting visits rather
  # than unique women, or a transported denominator.
  expect_error(.fe(.fe_num(c(13000, 300)), .fe_den()),
               "MORE first-time entrants than eligible prevalent women")
})

test_that("a stratum with no denominator is dropped loudly, not silently NA", {
  num <- rbind(.fe_num(), tibble::tibble(condition = "pop", age_band = "75+",
                                         year = 2023L, n = 50))
  # .msg_warn() emits a message(), per this repo's convention -- not warning().
  expect_message(r <- .fe(num, .fe_den()), "no matching denominator")
  expect_equal(nrow(r), 2L)
  expect_false(any(is.na(r$rate)))
})

test_that("no stratum at all is an error, not an empty tibble", {
  other <- tibble::tibble(condition = "ui", age_band = "18-44",
                          year = 1999L, n = 10)
  expect_error(.fe(.fe_num(), other), "No condition/age/year stratum")
})

test_that("malformed inputs name the missing column", {
  bad <- .fe_num()[, c("condition", "n")]
  expect_error(.fe(bad, .fe_den()), "missing required column")
})

test_that("the rate is NOT a conditional hazard and does not deplete", {
  # Two identical years must give the SAME rate. Under estimand A previously
  # entered women stay in the denominator, so nothing accumulates across years
  # inside this function -- depletion is embedded in the measured rate, and a
  # depletion correction applied here would double-count it.
  num <- rbind(.fe_num(), transform(.fe_num(), year = 2024L))
  den <- rbind(.fe_den(), transform(.fe_den(), year = 2024L))
  r <- .fe(num, den)
  y23 <- r$rate[r$year == 2023L]
  y24 <- r$rate[r$year == 2024L]
  expect_equal(y23, y24)
})

test_that("the shipped status is unresolved, and stays so until sourced", {
  # The canonical pathway refuses while this holds. If this ever returns a
  # calibrated tier without an APCD-derived estimate committed with provenance,
  # something has been quietly invented.
  expect_equal(first_entry_rate_status(), "unresolved_requires_source")
})
