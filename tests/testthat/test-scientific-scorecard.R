test_that("generate_scientific_scorecard reports 9 distinct states, all GREEN when calibrated", {
  card <- generate_scientific_scorecard()
  expect_equal(length(card), 9)
  expect_equal(card$SOFTWARE, "GREEN")
  expect_equal(card$SEMANTICS, "GREEN")
  expect_equal(card$UNCERTAINTY, "GREEN")
  expect_equal(card$CANONICAL_READINESS, "GREEN")
})

