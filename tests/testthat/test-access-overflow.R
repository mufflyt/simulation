# Phase 2 spatial overflow (geography-access_clearing.R). The load-bearing
# property is demand conservation: overflow moves demand across space, it never
# creates or destroys it. See docs/ACCESS_CLEARING_SPEC.md sections 5 and 10.

sc <- function() {
  # A shortage (100 vs 60), B and C surplus; A reaches B (cheap) then C (dear).
  list(
    catch = tibble::tibble(
      catchment = c("A", "B", "C"),
      demand_workload = c(100, 20, 10),
      accessible_capacity = c(60, 50, 40),
      median_travel_time = c(10, 8, 12)
    ),
    nb = tibble::tibble(from = c("A", "A"), to = c("B", "C"),
                        travel_penalty = c(5, 20))
  )
}

test_that("overflow conserves demand: served + unmet = demand, spilled balances", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb)
  cons <- overflow_conservation(res)
  expect_true(cons$conserved)
  expect_equal(max(abs(cons$per_catchment_resid)), 0)
  expect_equal(cons$transfer_resid, 0)
  expect_equal(cons$system_resid, 0)
  # served + unmet_before nothing to do with it; the system identity is the one:
  expect_equal(sum(res$served) + sum(res$unmet_demand),
               sum(res$demand_workload_pre_overflow))
})

test_that("excess spills nearest-first and fills spare capacity", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb)
  a <- res[res$catchment == "A", ]
  b <- res[res$catchment == "B", ]
  cc <- res[res$catchment == "C", ]
  # A sheds all 40 of its excess: 30 into the nearer B, the last 10 into C.
  expect_equal(a$spilled_out, 40)
  expect_equal(b$spilled_in, 30)
  expect_equal(cc$spilled_in, 10)
  expect_equal(a$unmet_demand, 0)
  # A stays saturated (served_local == capacity), so its wait is censored.
  expect_equal(a$served_local, 60)
  expect_true(a$wait_censored)
  # C absorbed only part of its slack: utilization rises to 0.5, wait finite.
  expect_equal(cc$utilization, 0.5)
  expect_false(cc$wait_censored)
})

test_that("demand beyond reachable spare capacity is censored as unmet", {
  # Only B is reachable, with spare 10; A's other 30 has nowhere to go.
  catch <- tibble::tibble(catchment = c("A", "B"), demand_workload = c(100, 40),
                          accessible_capacity = c(60, 50))
  nb <- tibble::tibble(from = "A", to = "B", travel_penalty = 5)
  res <- overflow_access(catch, nb)
  expect_equal(res$spilled_out[res$catchment == "A"], 10)
  expect_equal(res$unmet_demand[res$catchment == "A"], 30)
  expect_true(overflow_conservation(res)$conserved)
})

test_that("max_travel_penalty censors demand that cannot reach a neighbour", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb, max_travel_penalty = 10)  # drops the C edge (20)
  expect_equal(res$spilled_in[res$catchment == "C"], 0)
  expect_equal(res$spilled_out[res$catchment == "A"], 30)  # only B reachable
  expect_equal(res$unmet_demand[res$catchment == "A"], 10)
  expect_true(overflow_conservation(res)$conserved)
})

test_that("no reachable spare capacity reduces to Phase-1 clearing", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb[0, ])       # empty edge list -> nothing moves
  cl <- clear_access(s$catch)
  expect_equal(res$served, cl$served)
  expect_equal(res$unmet_demand, cl$unmet_demand)
  expect_equal(sum(res$spilled_out), 0)
})

test_that("overflow attributes report the spilled share and mean added travel", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb)
  ov <- attr(res, "overflow")
  expect_equal(ov$spilled_total, 40)
  expect_equal(ov$spilled_share, 40 / 130)
  # 30 units travel 10+5, 10 units travel 10+20 -> (30*15 + 10*30)/40 = 18.75
  expect_equal(ov$overflow_travel_time, 18.75)
})

test_that("a national roll-up of the overflow result still conserves demand", {
  s <- sc()
  res <- overflow_access(s$catch, s$nb)
  nat <- access_outcomes_national(res)
  unmet <- nat$value[nat$estimand == "A5"]
  unmet_frac <- nat$value[nat$estimand == "A5b"]
  expect_equal(unmet, sum(res$unmet_demand))
  # effective demand sums to the pre-overflow total (transfers cancel).
  expect_equal(sum(res$demand_workload), sum(res$demand_workload_pre_overflow))
})

test_that("bad inputs are rejected loudly, not silently mishandled", {
  s <- sc()
  expect_error(overflow_access(s$catch[, "demand_workload"], s$nb), "catchment")
  expect_error(overflow_access(s$catch, s$nb[, "from"]), "from")
  # self-edge
  expect_error(overflow_access(s$catch,
                               tibble::tibble(from = "A", to = "A", travel_penalty = 1)),
               "own overflow neighbour")
  # id not present
  expect_error(overflow_access(s$catch,
                               tibble::tibble(from = "A", to = "Z", travel_penalty = 1)),
               "catchment. id")
  # negative penalty
  expect_error(overflow_access(s$catch,
                               tibble::tibble(from = "A", to = "B", travel_penalty = -1)),
               "travel_penalty")
  # duplicate catchment id
  dup <- rbind(s$catch, s$catch[1, ])
  expect_error(overflow_access(dup, s$nb), "unique")
})

test_that("NA-demand catchments do not participate and stay NA", {
  catch <- tibble::tibble(catchment = c("A", "B", "E"),
                          demand_workload = c(100, 20, NA),
                          accessible_capacity = c(60, 50, NA))
  nb <- tibble::tibble(from = "A", to = c("B", "E"), travel_penalty = c(5, 1))
  res <- overflow_access(catch, nb)
  e <- res[res$catchment == "E", ]
  expect_true(is.na(e$served))
  expect_equal(e$spilled_in, 0)              # cannot absorb: no known capacity
  expect_true(overflow_conservation(res)$conserved)
})

test_that("catchment_neighbors_from_coords links true nearest neighbours", {
  coords <- tibble::tibble(catchment = c("A", "B", "C"),
                           lat = c(40.0, 40.1, 41.0), lon = c(-75.0, -75.0, -75.0))
  edges <- catchment_neighbors_from_coords(coords, k = 1, penalty_per_km = 0.5)
  expect_equal(nrow(edges), 3L)
  expect_true(all(edges$travel_penalty >= 0))
  # A's nearest is B (~11 km), not C (~111 km).
  expect_equal(edges$to[edges$from == "A"], "B")
  # and the edge list feeds straight back into overflow_access().
  catch <- tibble::tibble(catchment = c("A", "B", "C"),
                          demand_workload = c(100, 5, 5),
                          accessible_capacity = c(60, 60, 60))
  expect_true(overflow_conservation(overflow_access(catch, edges))$conserved)
})

test_that("doubling every catchment's capacity leaves no demand to spill", {
  s <- sc()
  big <- s$catch
  big$accessible_capacity <- big$accessible_capacity * 3   # A: 180 > 100
  res <- overflow_access(big, s$nb)
  expect_equal(sum(res$spilled_out), 0)
  expect_equal(sum(res$unmet_demand), 0)
})
