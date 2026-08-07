# Telemedicine geographic-reach uplift (geography-telemedicine_reach.R). Models
# telehealth extending specialist reach into rural areas as an uplift to a
# nonmetro catchment's accessible capacity. Output-preserving unless invoked.

cat3 <- function() {
  tibble::tibble(
    catchment = c("rural1", "rural2", "city1"),
    metro = c("NonMetro", "NonMetro", "Metro"),
    demand_workload = c(100, 80, 200),
    accessible_capacity = c(80, 80, 250)
  )
}

test_that("the uplift raises nonmetro capacity and leaves metro untouched", {
  out <- telemedicine_reach(cat3(), nonmetro_uplift = 0.25)
  expect_equal(out$accessible_capacity[out$catchment == "rural1"], 80 * 1.25)
  expect_equal(out$accessible_capacity[out$catchment == "rural2"], 80 * 1.25)
  expect_equal(out$accessible_capacity[out$catchment == "city1"], 250)   # metro unchanged
  expect_equal(out$telemedicine_reach_applied, c(TRUE, TRUE, FALSE))
})

test_that("an uplift of 0 (or no metro column) is capacity-preserving", {
  base <- cat3()
  z <- telemedicine_reach(base, nonmetro_uplift = 0)
  expect_equal(z$accessible_capacity, base$accessible_capacity)
  expect_false(any(z$telemedicine_reach_applied))
  # No metro column -> nothing to flag, capacity untouched.
  no_metro <- base[, c("catchment", "demand_workload", "accessible_capacity")]
  z2 <- telemedicine_reach(no_metro, nonmetro_uplift = 0.5)
  expect_equal(z2$accessible_capacity, no_metro$accessible_capacity)
  expect_false(any(z2$telemedicine_reach_applied))
})

test_that("under reach a nonmetro catchment clears more of its demand", {
  base <- cat3()
  before <- clear_access(base)
  after  <- clear_access(telemedicine_reach(base, nonmetro_uplift = 0.30))
  r1 <- function(x) x[x$catchment == "rural1", ]
  # rural1: demand 100 vs capacity 80 -> unmet 20; with +30% capacity (104) it clears.
  expect_equal(r1(before)$unmet_demand, 20)
  expect_equal(r1(after)$unmet_demand, 0)
  expect_lt(r1(after)$utilization, r1(before)$utilization)
  # The metro catchment's outcome is identical before/after.
  m <- function(x) x[x$catchment == "city1", ]
  expect_equal(m(after)$unmet_demand, m(before)$unmet_demand)
})

test_that("a negative uplift is rejected", {
  expect_error(telemedicine_reach(cat3(), nonmetro_uplift = -0.1))
  expect_error(telemedicine_reach(cat3()[, "demand_workload"]), "accessible_capacity")
})
