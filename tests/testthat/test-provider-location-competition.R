test_that("flag_urps_hospital_feasibility enforces OR and blood bank constraints", {
  hospital_tbl <- tibble::tribble(
    ~hospital_id, ~market_id, ~year, ~lon, ~lat, ~has_operating_room, ~has_blood_bank, ~hospital_active,
    "H001", "M001", 2025L, -104.9, 39.7, TRUE, TRUE, TRUE,   # Feasible
    "H002", "M002", 2025L, -104.8, 39.6, TRUE, FALSE, TRUE,  # No blood bank -> INFEASIBLE
    "H003", "M003", 2025L, -104.7, 39.5, FALSE, TRUE, TRUE,  # No OR -> INFEASIBLE
    "H004", "M004", 2025L, -104.6, 39.4, TRUE, TRUE, FALSE   # Inactive -> INFEASIBLE
  )

  res <- flag_urps_hospital_feasibility(hospital_tbl)
  expect_s3_class(res, "tbl_df")
  expect_equal(res$location_feasible, c(TRUE, FALSE, FALSE, FALSE))

  feasible_set <- feasible_provider_location_set(res, year = 2025L)
  expect_equal(nrow(feasible_set), 1L)
  expect_equal(feasible_set$hospital_id, "H001")
})

test_that("solve_provider_entry_equilibrium solves fixed point over feasible hospital sites", {
  set.seed(42)
  hospital_market_tbl <- tibble::tribble(
    ~year, ~market_id, ~hospital_id, ~state, ~lon, ~lat, ~has_operating_room, ~has_blood_bank, ~hospital_active, ~unmet_demand_30, ~commercial_share, ~medicaid_share, ~hospital_system_id, ~hospital_system_score, ~competing_provider_fte_30,
    2025L, "M001", "H001", "CO", -104.9, 39.7, TRUE, TRUE, TRUE, 500, 0.6, 0.2, "SYS1", 1.2, 2.0,
    2025L, "M002", "H002", "CO", -104.8, 39.6, TRUE, TRUE, TRUE, 300, 0.5, 0.3, "SYS2", 0.8, 1.0,
    2025L, "M003", "H003", "CO", -104.7, 39.5, FALSE, TRUE, TRUE, 1000, 0.9, 0.1, "SYS3", 2.0, 0.0  # High demand but NO OR!
  )

  feasible_tbl <- flag_urps_hospital_feasibility(hospital_market_tbl)
  feasible_sites <- feasible_provider_location_set(feasible_tbl, year = 2025L)

  # H003 must NOT be in feasible_sites!
  expect_equal(nrow(feasible_sites), 2L)
  expect_false("M003" %in% feasible_sites$market_id)

  # Build synthetic choice model with multiple choices
  choice_tbl <- tibble::tribble(
    ~choice_id, ~provider_id, ~event_type, ~chosen, ~log_unmet_demand_30, ~payer_mix_log_ratio, ~hospital_system_score, ~log_competition_30,
    "C1", "P1", "entrant", 1L, log1p(500), log(0.6/0.2), 1.2, log1p(2),
    "C1", "P1", "entrant", 0L, log1p(300), log(0.5/0.3), 0.8, log1p(1),
    "C2", "P2", "entrant", 0L, log1p(500), log(0.6/0.2), 1.2, log1p(2),
    "C2", "P2", "entrant", 1L, log1p(300), log(0.5/0.3), 0.8, log1p(1)
  )

  choice_model <- fit_provider_location_choice_model(choice_tbl)
  expect_s3_class(choice_model, "urps_provider_location_choice")

  eq_res <- solve_provider_entry_equilibrium(
    choice_model = choice_model,
    market_year_tbl = feasible_tbl,
    n_entrants = 10L
  )

  expect_true(eq_res$converged)
  expect_equal(nrow(eq_res$probabilities), 2L) # Only 2 feasible sites!
  expect_false("M003" %in% eq_res$probabilities$market_id)
})
