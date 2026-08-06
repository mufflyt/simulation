# Guards for the access-clearing layer (Phase 1):
#   R/geography-access_clearing.R  clear_access()
#   R/reporting-access_outcomes.R  access_outcomes_national(), assert_access_outcomes_labeled()
#   R/validation-access.R          access_validation_targets(), validate_access_outcomes()

cat_df <- function() tibble::tibble(
  catchment             = c("A", "B", "C"),
  demand_workload       = c(50, 100, 200),
  accessible_capacity   = c(100, 100, 100),
  accessible_population  = c(1000, 2000, 3000),
  accessible_fte        = c(10, 20, 30),
  insurance_fraction    = c(1, 1, 1)
)

# ---- clear_access(): semantic invariants -----------------------------------

test_that("utilization is demand/capacity and unmet is the shortfall", {
  r <- clear_access(cat_df(), wait_scale = 30)
  expect_equal(r$utilization, c(0.5, 1, 1))           # capped at 1
  expect_equal(r$unmet_demand, c(0, 0, 100))
  expect_equal(r$served, c(50, 100, 100))
})

test_that("wait rises with utilization and censors past rho = 1, never NaN/negative", {
  r <- clear_access(tibble::tibble(demand_workload = c(10, 50, 90, 99, 150),
                                   accessible_capacity = rep(100, 5)), wait_scale = 30)
  fin <- r$wait_time[1:4]
  expect_true(all(diff(fin) > 0))                     # strictly increasing in rho < 1
  expect_true(all(fin >= 0))
  expect_true(r$wait_censored[5])                     # rho = 1.5 -> censored
  expect_true(is.infinite(r$wait_time[5]))            # default ceiling Inf
  expect_false(any(is.nan(r$wait_time)))
})

test_that("p_appointment is in [0,1], falls with rho, and is gated by insurance", {
  r <- clear_access(tibble::tibble(demand_workload = c(10, 50, 90),
                                   accessible_capacity = rep(100, 3)), wait_scale = 30)
  expect_true(all(r$p_appointment >= 0 & r$p_appointment <= 1))
  expect_true(all(diff(r$p_appointment) < 0))
  full <- clear_access(tibble::tibble(demand_workload = 50, accessible_capacity = 100,
                                      insurance_fraction = 1), wait_scale = 30)
  half <- clear_access(tibble::tibble(demand_workload = 50, accessible_capacity = 100,
                                      insurance_fraction = 0.5), wait_scale = 30)
  expect_equal(half$p_appointment, 0.5 * full$p_appointment, tolerance = 1e-9)
})

test_that("panel size is population per FTE; doubling capacity halves utilization", {
  r <- clear_access(cat_df(), wait_scale = 30)
  expect_equal(r$panel_size, c(100, 100, 100))        # 1000/10, 2000/20, 3000/30
  a <- clear_access(tibble::tibble(demand_workload = 80, accessible_capacity = 100))
  b <- clear_access(tibble::tibble(demand_workload = 80, accessible_capacity = 200))
  expect_equal(b$utilization, a$utilization / 2, tolerance = 1e-9)
})

test_that("zero-capacity catchment is fully unmet and gets no appointment", {
  r <- clear_access(tibble::tibble(demand_workload = 100, accessible_capacity = 0))
  expect_equal(r$unmet_demand, 100)
  expect_true(r$wait_censored)
  expect_equal(r$p_appointment, 0)
})

test_that("an empty (all-NA) catchment yields NA outcomes, not an error", {
  r <- clear_access(tibble::tibble(demand_workload = NA_real_, accessible_capacity = NA_real_))
  expect_true(is.na(r$utilization) && is.na(r$wait_time) && is.na(r$p_appointment))
  expect_false(inherits(r, "try-error"))
})

# ---- clear_access(): adversarial guards ------------------------------------

test_that("clear_access rejects bad inputs loudly", {
  expect_error(clear_access(tibble::tibble(x = 1)))                                   # missing cols
  expect_error(clear_access(tibble::tibble(demand_workload = -1, accessible_capacity = 1)))
  expect_error(clear_access(tibble::tibble(demand_workload = 1, accessible_capacity = -1)))
  expect_error(clear_access(tibble::tibble(demand_workload = 1, accessible_capacity = 1,
                                           insurance_fraction = 1.5)))
  expect_error(clear_access(tibble::tibble(demand_workload = 1, accessible_capacity = 1), wait_scale = -1))
  expect_error(clear_access(tibble::tibble(demand_workload = 1, accessible_capacity = 1), appointment_window = 0))
})

# ---- access_outcomes_national() + guard ------------------------------------

test_that("national roll-up aggregates by the right base", {
  nat <- access_outcomes_national(clear_access(cat_df(), wait_scale = 30))
  val <- function(l) nat$value[nat$label == l]
  expect_equal(val("utilization"), 250 / 300, tolerance = 1e-9)   # served/capacity
  expect_equal(val("unmet_demand"), 100)
  expect_equal(val("unmet_fraction"), 100 / 350, tolerance = 1e-9)
  expect_equal(val("wait_censored_share"), 300 / 350, tolerance = 1e-9)  # B(rho=1)+C(rho=2)
  expect_equal(val("wait_time"), 30, tolerance = 1e-9)            # finite waits only (A)
  expect_equal(val("panel_size"), 100, tolerance = 1e-9)         # 6000/60
})

test_that("the label guard refuses unlabeled or (optionally) uncalibrated outcomes", {
  nat <- access_outcomes_national(clear_access(cat_df()))
  expect_invisible(assert_access_outcomes_labeled(nat))
  expect_error(assert_access_outcomes_labeled(nat, require_calibrated = TRUE))  # still assumed
  expect_error(assert_access_outcomes_labeled(tibble::tibble(value = 1)))       # no status col
  bad <- clear_access(cat_df()); bad$calibration_status <- NA_character_
  expect_error(assert_access_outcomes_labeled(bad))
})

# ---- validation targets -----------------------------------------------------

test_that("validation is silent-proof: unpopulated targets never pass", {
  nat <- access_outcomes_national(clear_access(cat_df(), wait_scale = 30))
  expect_true(all(validate_access_outcomes(nat)$status == "no_target"))
  t <- access_validation_targets(); t$observed <- c(30, 100)
  expect_true(all(validate_access_outcomes(nat, t)$status == "pass"))
  t$observed <- c(10, 100)                                        # wait off target
  expect_equal(validate_access_outcomes(nat, t)$status[1], "fail")
})
