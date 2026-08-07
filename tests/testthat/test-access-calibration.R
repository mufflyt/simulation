# Calibration mechanisms for the access layer (validation-access.R):
# fit_wait_scale() solves the wait-mapping constant against an observed wait, and
# set_access_target() populates an otherwise-unpopulated target (a cited wait, or
# an assumed panel-size benchmark) so validate_access_outcomes() can score it.

catch <- function() {
  tibble::tibble(
    catchment = c("A", "B", "C"),
    demand_workload = c(90, 40, 30),
    accessible_capacity = c(100, 100, 100),   # all rho < 1 -> finite waits
    accessible_population = c(5000, 4000, 3000),
    accessible_fte = c(2, 2, 2)
  )
}

test_that("fit_wait_scale makes the national A1 wait equal the observed wait", {
  cx <- catch()
  fit <- fit_wait_scale(cx, observed_wait = 21)
  expect_gt(fit$wait_scale, 0)
  expect_equal(fit$n_catchments_used, 3L)
  expect_equal(fit$censored_demand_share, 0)
  # Clearing at the fitted scale reproduces the observed national wait exactly.
  nat <- access_outcomes_national(clear_access(cx, wait_scale = fit$wait_scale))
  expect_equal(nat$value[nat$estimand == "A1"], 21)
})

test_that("fit_wait_scale refuses when every catchment is saturated", {
  saturated <- tibble::tibble(catchment = c("A", "B"),
                              demand_workload = c(120, 130),
                              accessible_capacity = c(100, 100))  # rho > 1 everywhere
  expect_error(fit_wait_scale(saturated, observed_wait = 21), "saturated")
})

test_that("fit_wait_scale rejects a non-positive observed wait", {
  expect_error(fit_wait_scale(catch(), observed_wait = 0))
  expect_error(fit_wait_scale(catch(), observed_wait = -5))
})

test_that("set_access_target populates a target and stamps its status", {
  t0 <- access_validation_targets()
  expect_true(all(is.na(t0$observed)))                 # ships unpopulated
  t1 <- set_access_target(t0, "panel_size", observed = 2500, status = "assumed")
  expect_equal(t1$observed[t1$target == "panel_size"], 2500)
  expect_equal(t1$status[t1$target == "panel_size"], "assumed")
  # other targets untouched
  expect_true(is.na(t1$observed[t1$target == "wait_time"]))
  expect_error(set_access_target(t0, "not_a_target", 1), "unknown target")
})

test_that("validate scores an assumed panel benchmark and labels it assumed", {
  cx <- catch()                                        # panel = 12000 pop / 6 fte = 2000
  nat <- access_outcomes_national(cx |> clear_access())
  expect_equal(nat$value[nat$estimand == "A3"], 2000)
  # A benchmark within tolerance -> pass, but flagged target_status == "assumed".
  targ <- set_access_target(observed = 2100, target = "panel_size",
                            status = "assumed", rel_tol = 0.10)
  v <- validate_access_outcomes(nat, targ)
  row <- v[v$target == "panel_size", ]
  expect_equal(row$status, "pass")
  expect_equal(row$target_status, "assumed")
  # wait_time stays unpopulated -> no_target, never a silent pass.
  expect_equal(v$status[v$target == "wait_time"], "no_target")
})

test_that("fit + set round-trips: a fitted wait passes its own target", {
  cx <- catch()
  fit <- fit_wait_scale(cx, observed_wait = 21)
  nat <- access_outcomes_national(clear_access(cx, wait_scale = fit$wait_scale))
  targ <- set_access_target(target = "wait_time", observed = 21, status = "calibrated")
  v <- validate_access_outcomes(nat, targ)
  row <- v[v$target == "wait_time", ]
  expect_equal(row$status, "pass")
  expect_equal(row$target_status, "calibrated")
})
