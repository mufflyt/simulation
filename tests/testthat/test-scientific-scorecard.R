test_that("generate_scientific_scorecard reports 9 distinct states, all GREEN when calibrated", {
  card <- generate_scientific_scorecard()
  expect_equal(length(card), 9)
  expect_equal(card$SOFTWARE, "GREEN")
  expect_equal(card$SEMANTICS, "GREEN")
  expect_equal(card$UNCERTAINTY, "GREEN")
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

