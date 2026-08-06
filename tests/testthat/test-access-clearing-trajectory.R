# Guards for the Phase-4 dynamic multi-year clearing:
#   R/geography-access_clearing.R  clear_access_trajectory()
#   R/reporting-access_outcomes.R  access_outcomes_trajectory()

panel_df <- function() tibble::tibble(
  year                  = rep(2020:2022, each = 2),
  catchment             = rep(c("X", "Y"), times = 3),
  demand_workload       = c(150, 50, 150, 50, 150, 50),   # X chronically short
  accessible_capacity   = c(100, 100, 100, 100, 100, 100),
  accessible_population   = rep(c(3000, 1000), 3),
  accessible_fte        = rep(c(30, 10), 3),
  insurance_fraction    = 1
)

# ---- clear_access_trajectory(): semantic -----------------------------------

test_that("with no backlog, each year equals independent clearing", {
  p <- panel_df()
  tr <- clear_access_trajectory(p, carry_backlog = FALSE, wait_scale = 30)
  ref <- clear_access(p[p$year == 2020, ], wait_scale = 30)
  expect_equal(tr$unmet_demand[tr$year == 2020], ref$unmet_demand)
  expect_true(all(tr$backlog_in == 0))
  expect_equal(tr$unmet_demand[tr$catchment == "X"], c(50, 50, 50))  # no accumulation
})

test_that("backlog carry-forward compounds a persistent shortfall", {
  tr <- clear_access_trajectory(panel_df(), carry_backlog = TRUE,
                                backlog_fraction = 1, wait_scale = 30)
  # X: y1 unmet 50 -> y2 demand 200 unmet 100 -> y3 demand 250 unmet 150
  expect_equal(tr$unmet_demand[tr$catchment == "X"], c(50, 100, 150))
  expect_equal(tr$backlog_in[tr$catchment == "X" & tr$year == 2022], 100)
  expect_equal(tr$demand_workload[tr$catchment == "X" & tr$year == 2022], 250)
  expect_equal(tr$demand_workload_base[tr$catchment == "X"], c(150, 150, 150))
  expect_true(all(tr$unmet_demand[tr$catchment == "Y"] == 0))          # Y always has slack
})

test_that("backlog_fraction scales how much unmet demand persists", {
  tr <- clear_access_trajectory(panel_df(), carry_backlog = TRUE,
                                backlog_fraction = 0.5, wait_scale = 30)
  # y2 X demand 150 + 0.5*50 = 175 -> unmet 75
  expect_equal(tr$unmet_demand[tr$catchment == "X" & tr$year == 2021], 75)
})

test_that("clear_access_trajectory rejects bad inputs", {
  expect_error(clear_access_trajectory(tibble::tibble(demand_workload = 1, accessible_capacity = 1)))  # no year
  expect_error(clear_access_trajectory(panel_df()[c("year", "demand_workload", "accessible_capacity")],
                                       carry_backlog = TRUE))                                          # no catchment id
  expect_error(clear_access_trajectory(panel_df(), backlog_fraction = 1.5))
  expect_error(clear_access_trajectory(panel_df(), backlog_fraction = -0.1))
})

# ---- access_outcomes_trajectory(): the A1-A5 time series -------------------

test_that("national outcomes are produced per year and rise under backlog", {
  ts <- access_outcomes_trajectory(
    clear_access_trajectory(panel_df(), carry_backlog = TRUE, wait_scale = 30))
  expect_setequal(unique(ts$year), 2020:2022)
  u <- ts$value[ts$label == "unmet_demand"]
  expect_true(all(diff(u) > 0))                        # shortfall compounds nationally
  expect_true(all(c("A1", "A2", "A3", "A4", "A5") %in% ts$estimand))
})

test_that("access_outcomes_trajectory needs a year column", {
  expect_error(access_outcomes_trajectory(tibble::tibble(demand_workload = 1)))
})
