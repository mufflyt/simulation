# Severity-stratified clearing (geography-access_severity.R) and the publish
# guard (reporting-access_outcomes.R). The load-bearing property is PRIORITY:
# capacity is shared but urgent demand books slots before routine demand, so a
# flood of routine volume can never starve urgent care.

sev_panel <- function() {
  # One catchment, capacity 100 shared; urgent 60, routine 60 (total 120 > cap).
  tibble::tibble(
    catchment = c("A", "A"),
    severity = c("urgent", "routine"),
    demand_workload = c(60, 60),
    accessible_capacity = c(100, 100)
  )
}

test_that("capacity is consumed in priority order (urgent before routine)", {
  res <- clear_access_by_severity(sev_panel(),
                                  severity_windows = c(urgent = 7, routine = 30))
  u <- res[res$severity == "urgent", ]
  r <- res[res$severity == "routine", ]
  # Urgent takes 60 of 100; routine gets the remaining 40 and is left 20 short.
  expect_equal(u$served, 60)
  expect_equal(u$unmet_demand, 0)
  expect_equal(r$served, 40)
  expect_equal(r$unmet_demand, 20)
  # Total served == shared capacity; nothing double-booked.
  expect_equal(u$served + r$served, 100)
  # Each class carries its own appointment window.
  expect_equal(u$appointment_window, 7)
  expect_equal(r$appointment_window, 30)
})

test_that("routine volume cannot starve urgent care", {
  panel <- tibble::tibble(
    catchment = c("A", "A"), severity = c("urgent", "routine"),
    demand_workload = c(30, 1000),          # a flood of routine demand
    accessible_capacity = c(100, 100)
  )
  res <- clear_access_by_severity(panel, severity_windows = c(urgent = 7, routine = 30))
  u <- res[res$severity == "urgent", ]
  # Urgent is fully served regardless of the routine flood.
  expect_equal(u$served, 30)
  expect_equal(u$unmet_demand, 0)
  expect_false(isTRUE(u$wait_censored))
})

test_that("an explicit priority overrides the window-derived default", {
  panel <- sev_panel()
  # Force routine first: now routine books 60, urgent gets the remaining 40.
  res <- clear_access_by_severity(panel, severity_windows = c(urgent = 7, routine = 30),
                                  priority = c("routine", "urgent"))
  expect_equal(res$served[res$severity == "routine"], 60)
  expect_equal(res$served[res$severity == "urgent"], 40)
})

test_that("bad inputs are rejected", {
  good <- sev_panel()
  # capacity not constant within a catchment
  bad_cap <- good; bad_cap$accessible_capacity <- c(100, 80)
  expect_error(clear_access_by_severity(bad_cap, c(urgent = 7, routine = 30)),
               "constant within a catchment")
  # a severity not named in the windows
  expect_error(clear_access_by_severity(good, c(urgent = 7)),
               "must be")
  # non-positive window
  expect_error(clear_access_by_severity(good, c(urgent = 7, routine = 0)),
               "positive appointment windows")
})

test_that("per-severity roll-up reads each class's A-series", {
  res <- clear_access_by_severity(sev_panel(), c(urgent = 7, routine = 30))
  nat <- access_outcomes_by_severity(res)
  expect_true(all(c("urgent", "routine") %in% nat$severity))
  # Routine unmet fraction: 20 of 60.
  ruf <- nat$value[nat$severity == "routine" & nat$estimand == "A5b"]
  expect_equal(ruf, 20 / 60)
  # Urgent has no unmet.
  uuf <- nat$value[nat$severity == "urgent" & nat$estimand == "A5b"]
  expect_equal(uuf, 0)
})

# ---- Item 7: the publish guard --------------------------------------------

test_that("publish_access_outcomes refuses an un-calibrated roll-up", {
  cx <- tibble::tibble(catchment = c("A", "B"),
                       demand_workload = c(90, 40),
                       accessible_capacity = c(100, 100))
  cleared <- clear_access(cx)                 # status = "assumed_illustrative"
  expect_error(publish_access_outcomes(cleared), "assumed/illustrative")
  # An explicitly-labeled draft passes with require_calibrated = FALSE.
  nat <- publish_access_outcomes(cleared, require_calibrated = FALSE)
  expect_true(all(c("A1", "A5") %in% nat$estimand))
  # A calibrated clearing publishes.
  calibrated <- clear_access(cx, status = "calibrated")
  expect_true(all(c("A1", "A5") %in% publish_access_outcomes(calibrated)$estimand))
})
