# ARCHIVED tests, moved from tests/testthat/test-demand-and-validation.R.
# They exercise functions now in inst/archive/ and are NOT run.

test_that("the two placement rules give different answers when demand has moved", {
  roster <- tibble::tibble(state = c(rep("A", 90), rep("B", 10)))
  h <- historical_placement_shares(roster, "state")
  o <- opportunity_placement_shares(
    tibble::tibble(geo = c("A", "B"), demand_growth_fte = c(10, 90)),
    tibble::tibble(geo = c("A", "B"), retirements_fte = c(5, 5))
  )
  expect_gt(o$share[o$geo == "B"], h$share[h$geo == "B"])
  blend <- blend_placement_shares(h, o, weight = 0.5)
  expect_equal(sum(blend$share), 1, tolerance = 1e-12)
  expect_gt(blend$share[blend$geo == "B"], h$share[h$geo == "B"])
})

