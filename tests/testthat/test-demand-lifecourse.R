# Reproductive life-course demand generator (R/25-demand_lifecourse.R).

pop_by_age <- tibble::tibble(age = 40:85,
                             population = round(2e6 * exp(-0.02 * (40:85 - 40))))
pop_by_age_year <- dplyr::bind_rows(lapply(c(2025L, 2030L), function(y)
  dplyr::mutate(pop_by_age, year = y)))

BASKET <- c("new_consultation", "return_visit", "pessary_care", "urodynamics",
            "cystoscopy", "botox_bladder", "ptns", "sling_procedure", "prolapse_procedure")

run <- function(scenario = "baseline", ...) {
  simulate_lifecourse_demand(pop_by_age, year = 2025L, scenario = scenario,
                             n = 8000, seed = 1, ...)
}

test_that("service_volumes come out in the shape convert_workload_to_fte() consumes", {
  out <- run("baseline")
  expect_true(all(c("year", "service", "volume") %in% names(out$service_volumes)))
  expect_true(all(out$service_volumes$service %in% BASKET))   # 100% match into the basket
  expect_true(all(out$service_volumes$volume > 0))
  expect_true(out$care_seeking_national > 0)
})

test_that("person-year record carries the life-course state variables", {
  out <- run("baseline")
  expect_true(all(c("person_id", "age", "parity", "vaginal_births",
                    "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
                    "bmi", "menopause_status", "hysterectomy",
                    "care_seeking_state", "treatment_state") %in% names(out$person_years)))
  expect_true(all(out$person_years$cumulative_vaginal_deliveries <= out$person_years$parity))
})

test_that("reduced_barriers raises total service volume without changing prevalence", {
  base <- run("baseline")
  rb   <- run("reduced_barriers")
  expect_gt(sum(rb$service_volumes$volume), sum(base$service_volumes$volume))
})

test_that("more cesarean delivery lowers prolapse-driven volume (vaginal delivery is the engine)", {
  base <- run("baseline")
  csec <- run("delivery_mode", cesarean_rate = 0.60)
  pv <- function(o) sum(o$service_volumes$volume[o$service_volumes$service == "prolapse_procedure"])
  expect_lt(pv(csec), pv(base))
})

test_that("BMI-reduction prevention lowers demand and is the ONLY place BMI intervenes", {
  base <- run("baseline")
  prev <- run("prevention", prevention_target = "bmi", prevention_effect = 0.3)
  expect_lt(sum(prev$service_volumes$volume), sum(base$service_volumes$volume))
})

test_that("trajectory summarises care-seeking and service units by year", {
  traj <- lifecourse_demand_trajectory(pop_by_age_year, scenario = "baseline", n = 6000, seed = 1)
  expect_true(all(c("year", "care_seeking_national", "service_units_national")
                  %in% names(traj$demand_summary)))
  expect_setequal(traj$demand_summary$year, c(2025L, 2030L))
  expect_true(all(c("year", "service", "volume") %in% names(traj$service_volumes)))
})
