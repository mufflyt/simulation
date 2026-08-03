library(testthat)
library(urpssim)

test_that("initialize_urps_agents: all Active at init", {
  a <- initialize_urps_agents(verbose = FALSE)
  expect_true(all(a$status == "Active"))
})

test_that("initialize_urps_agents: n near contract total", {
  a <- initialize_urps_agents(verbose = FALSE)
  expect_gte(nrow(a), 1100L)
  expect_lte(nrow(a), 1500L)
})

test_that("initialize_urps_agents: max age <= 70", {
  a <- initialize_urps_agents(max_age = 70L, verbose = FALSE)
  expect_true(all(a$age <= 70L))
})

test_that("initialize_urps_agents: no duplicate agent_id", {
  a <- initialize_urps_agents(verbose = FALSE)
  expect_equal(length(unique(a$agent_id)), nrow(a))
})

test_that("initialize_urps_agents: required columns present", {
  a <- initialize_urps_agents(verbose = FALSE)
  expect_true(all(c("agent_id", "npi", "age", "sex", "pathway",
                     "census_division", "clinical_fte", "status",
                     "simulation_year") %in% colnames(a)))
})

test_that("advance_urps_agents: simulation_year increments by 1", {
  a  <- initialize_urps_agents(verbose = FALSE)
  h  <- build_urps_exit_hazard(verbose = FALSE)
  a2 <- advance_urps_agents(a, h$exit_probs, year_seed = 42L, verbose = FALSE)
  expect_equal(max(a2$simulation_year), max(a$simulation_year) + 1L)
})

test_that("advance_urps_agents: total rows conserved with no new fellows", {
  a  <- initialize_urps_agents(verbose = FALSE)
  h  <- build_urps_exit_hazard(verbose = FALSE)
  a2 <- advance_urps_agents(a, h$exit_probs, new_entrants = 0L,
                              year_seed = 42L, verbose = FALSE)
  expect_equal(nrow(a2), nrow(a))
})

test_that("advance_urps_agents: retired agents never return to Active", {
  a  <- initialize_urps_agents(verbose = FALSE)
  h  <- build_urps_exit_hazard(verbose = FALSE)
  a2 <- advance_urps_agents(a, h$exit_probs, year_seed = 42L, verbose = FALSE)
  retired_ids <- a2$agent_id[a2$status == "Retired"]
  if (length(retired_ids) > 0) {
    a3 <- advance_urps_agents(a2, h$exit_probs, year_seed = 43L, verbose = FALSE)
    expect_true(all(a3$status[a3$agent_id %in% retired_ids] == "Retired"))
  } else {
    skip("No retirements at this seed — increase cohort size")
  }
})

test_that("build_urps_exit_hazard fallback: prob_exit in [0,1]", {
  h <- build_urps_exit_hazard(cliff_duckdb_path = NULL, verbose = FALSE)
  expect_true(all(h$exit_probs$prob_exit >= 0))
  expect_true(all(h$exit_probs$prob_exit <= 1))
})

test_that("build_urps_exit_hazard fallback: hazard_cv is positive (Weibull spread)", {
  h <- build_urps_exit_hazard(cliff_duckdb_path = NULL, verbose = FALSE)
  expect_gt(h$hazard_cv, 0)
  expect_lte(h$hazard_cv, 1)
})

test_that("apply_hrsa_surgical_fte: clinical_fte in [0,1] for active", {
  a  <- initialize_urps_agents(verbose = FALSE)
  af <- apply_hrsa_surgical_fte(a, verbose = FALSE)
  active_fte <- af$clinical_fte[af$status == "Active"]
  expect_true(all(active_fte >= 0, na.rm = TRUE))
  expect_true(all(active_fte <= 1, na.rm = TRUE))
})

test_that("apply_hrsa_surgical_fte: retired get NA clinical_fte", {
  a  <- initialize_urps_agents(verbose = FALSE)
  h  <- build_urps_exit_hazard(verbose = FALSE)
  a2 <- advance_urps_agents(a, h$exit_probs, year_seed = 42L, verbose = FALSE)
  af <- apply_hrsa_surgical_fte(a2, verbose = FALSE)
  retired_fte <- af$clinical_fte[af$status == "Retired"]
  expect_true(all(is.na(retired_fte)))
})

test_that("HALL_OF_SHAME: unregistered scenario_id rejected", {
  a <- initialize_urps_agents(verbose = FALSE)
  h <- build_urps_exit_hazard(verbose = FALSE)
  expect_error(
    advance_urps_agents(a, h$exit_probs,
                        scenario_id = "NOT_A_REAL_SCENARIO_XYZ"),
    regexp = "not registered"
  )
})
