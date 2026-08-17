# tests/testthat/test-scientific-scorecard.R
# Scientific Hardening Section 42: Scorecard Tests

test_that("generate_scientific_scorecard reports 9 distinct states", {
  card <- generate_scientific_scorecard()
  expect_equal(length(card), 9)
  expect_equal(card$SOFTWARE, "GREEN")
  expect_equal(card$SEMANTICS, "GREEN")
  expect_equal(card$CANONICAL_READINESS, "RED") # Intentionally RED
})
