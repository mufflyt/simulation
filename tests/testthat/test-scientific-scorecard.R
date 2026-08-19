test_that("generate_scientific_scorecard reports 9 distinct states", {
  card <- generate_scientific_scorecard()
  expect_equal(length(card), 9)
  expect_setequal(names(card),
                  c("SOFTWARE", "REPRODUCIBILITY", "SEMANTICS", "ADVERSARIAL",
                    "SOURCE_MUTATION", "KNOWN_TRUTH_RECOVERY", "UNCERTAINTY",
                    "CROSS_REPO_CONTRACTS", "CANONICAL_READINESS"))
  expect_equal(card$UNCERTAINTY, "GREEN")
})

test_that("REPRODUCIBILITY is always NOT_ELIGIBLE: no in-process equivalent exists", {
  # It is a from-scratch renv::restore() plus system-library install in a
  # throwaway library (~90 min in CI) -- not something an R function can run
  # inline. Reporting anything else here would be exactly the "checks an
  # unrelated proxy" bug this scorecard was fixed to stop making.
  card <- generate_scientific_scorecard()
  expect_equal(card$REPRODUCIBILITY, "NOT_ELIGIBLE")
})

test_that("SOFTWARE and ADVERSARIAL are NOT_ELIGIBLE by default, not silently GREEN", {
  # Both are CI-scale (a full 2000+-test suite run, and a mutation/metamorphic
  # battery) and are gated behind deep = TRUE so the scorecard stays fast by
  # default. NOT_ELIGIBLE, not a guessed GREEN, is the honest default state.
  card <- generate_scientific_scorecard(deep = FALSE)
  expect_equal(card$SOFTWARE, "NOT_ELIGIBLE")
  expect_equal(card$ADVERSARIAL, "NOT_ELIGIBLE")
})

test_that("the four cheap in-process audits agree with the scorecard's derived states", {
  # Pinned against the real audit_*() functions rather than a hardcoded value,
  # so this tracks whatever they actually report instead of assuming GREEN.
  skip_on_cran()
  card <- generate_scientific_scorecard()

  state_of <- function(audit) {
    if (!isTRUE(audit$available)) "NOT_ELIGIBLE" else if (isTRUE(audit$passed)) "GREEN" else "RED"
  }

  expect_equal(card$SEMANTICS, state_of(audit_semantics()))
  expect_equal(card$SOURCE_MUTATION, state_of(audit_source_mutation()))
  expect_equal(card$KNOWN_TRUTH_RECOVERY, state_of(audit_known_truth_recovery()))
  expect_equal(card$CROSS_REPO_CONTRACTS, state_of(audit_cross_repo_contracts()))
})

test_that("CANONICAL_READINESS reflects the REAL canonical-readiness gate, not a proxy", {
  # This is the gate the bug (fixed 2026-08-18) papered over: the scorecard
  # previously derived CANONICAL_READINESS from an unrelated calibration flag
  # that was always "calibrated", so it reported GREEN even while
  # .github/scripts/assert-canonical-science.R refused the canonical
  # pathway. Pinning against audit_canonical_readiness() directly means this
  # test tracks the real gate's status rather than assuming a fixed value --
  # it passes whether the gate is currently red (known blocker) or green
  # (the science shipped), and would only fail if the two diverge again.
  skip_on_cran()
  audit <- audit_canonical_readiness()
  skip_if(!isTRUE(audit$available), "canonical readiness gate not present (no source tree)")

  card <- generate_scientific_scorecard()
  expected <- if (identical(audit$status, 0L)) "GREEN" else "RED"
  expect_equal(card$CANONICAL_READINESS, expected)
})

