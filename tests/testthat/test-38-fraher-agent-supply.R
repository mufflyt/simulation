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


