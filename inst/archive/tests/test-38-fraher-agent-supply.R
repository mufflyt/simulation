# ARCHIVED tests, moved from tests/testthat/test-38-fraher-agent-supply.R.
# They exercise functions now in inst/archive/ and are NOT run.

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

