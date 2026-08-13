# Adequacy -> access response bridge.
#
# The bridge is a faithful composition: demand = required_fte_base_year() and
# capacity = base_supply_fte, so rho = demand/capacity = 1/adequacy exactly.
# These tests pin that identity, the honest saturation behaviour for
# adequacy < 1, and the input guards. Expected access values are hand-derived
# from clear_access()'s documented queue: wait = wait_scale*rho/(1-rho),
# p_appointment = 1 - exp(-W/wait).

test_that("adequacy_access_load encodes rho = 1/adequacy in FTE currency", {
  load_tbl <- suppressMessages(
    adequacy_access_load(adequacy = 1.25, base_supply_fte = 1000)
  )
  expect_equal(load_tbl$accessible_capacity, 1000)
  # required = supply / adequacy = 1000 / 1.25 = 800
  expect_equal(load_tbl$demand_workload, 800)
  # rho = demand / capacity = 0.8 = 1 / 1.25
  expect_equal(load_tbl$demand_workload / load_tbl$accessible_capacity, 1 / 1.25)
  expect_equal(nrow(load_tbl), 1L)
  expect_identical(load_tbl$catchment, "national")
})

test_that("adequacy_access_load stamps the response assumption and refuses to resolve", {
  load_tbl <- suppressMessages(
    adequacy_access_load(adequacy = 2, base_supply_fte = 500, status = "assumed_illustrative")
  )
  ra <- attr(load_tbl, "response_assumption")
  expect_false(ra$identifies_capacity_adequacy)
  expect_identical(ra$status, "assumed_illustrative")
  expect_match(ra$mapping, "1/adequacy")
})

test_that("adequacy_access_load recycles and preserves catchment-level heterogeneity", {
  load_tbl <- suppressMessages(
    adequacy_access_load(
      adequacy = c(0.9, 1.5, 3.0),
      base_supply_fte = 600,
      catchment = c("A", "B", "C")
    )
  )
  expect_equal(nrow(load_tbl), 3L)
  expect_equal(load_tbl$accessible_capacity, rep(600, 3))
  expect_equal(load_tbl$demand_workload, 600 / c(0.9, 1.5, 3.0))
  expect_identical(load_tbl$catchment, c("A", "B", "C"))
})

test_that("adequacy_access_load flags saturation without erroring", {
  # .msg_warn() routes through message(), so this surfaces as a message and the
  # call still returns a table -- saturation is reported, not fatal.
  msgs <- testthat::capture_messages(
    load_tbl <- adequacy_access_load(adequacy = 0.8, base_supply_fte = 100)
  )
  expect_true(any(grepl("saturated", msgs, ignore.case = TRUE)))
  expect_s3_class(load_tbl, "tbl_df")
  expect_equal(load_tbl$demand_workload, 100 / 0.8)
})

test_that("adequacy_access_load rejects invalid inputs", {
  expect_error(suppressMessages(adequacy_access_load(0, 100)), "adequacy")
  expect_error(suppressMessages(adequacy_access_load(-1, 100)), "adequacy")
  expect_error(suppressMessages(adequacy_access_load(NA_real_, 100)), "adequacy")
  expect_error(suppressMessages(adequacy_access_load(1, 0)), "base_supply_fte")
  expect_error(suppressMessages(adequacy_access_load(1, 100, insurance_fraction = 1.5)),
               "insurance_fraction")
  expect_error(
    suppressMessages(adequacy_access_load(c(1, 2), base_supply_fte = c(1, 2, 3))),
    "length 1 or"
  )
})

test_that("simulate_access_for_adequacy: surplus gives the documented finite wait", {
  # adequacy 2 -> rho 0.5 -> wait = 30 * 0.5/0.5 = 30 days;
  # p_appointment = 1 - exp(-30/30) = 1 - exp(-1) ~ 0.6321.
  out <- suppressMessages(
    simulate_access_for_adequacy(adequacy = 2, base_supply_fte = 1000, wait_scale = 30)
  )
  a1 <- out$value[out$estimand == "A1"]
  a2 <- out$value[out$estimand == "A2"]
  expect_equal(a1, 30, tolerance = 1e-6)
  expect_equal(a2, 1 - exp(-1), tolerance = 1e-6)
  expect_true(all(out$calibration_status == "assumed_illustrative"))
})

test_that("simulate_access_for_adequacy: a below-one national cell saturates honestly", {
  # adequacy 0.9 -> rho 1.111 -> saturated: wait censored, appointment prob 0,
  # unmet = 1000/0.9 - 1000 ~ 111.1.
  out <- suppressMessages(
    simulate_access_for_adequacy(adequacy = 0.9, base_supply_fte = 1000, wait_scale = 30)
  )
  censored_share <- out$value[out$estimand == "A1b"]
  unmet <- out$value[out$estimand == "A5"]
  expect_equal(censored_share, 1, tolerance = 1e-6)
  expect_equal(unmet, 1000 / 0.9 - 1000, tolerance = 1e-4)
})
