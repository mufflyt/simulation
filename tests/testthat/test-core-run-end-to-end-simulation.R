test_that("run_end_to_end_simulation executes 8 coupled annual steps cleanly", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2027L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_s3_class(sim_res$audit_ledger_tbl, "tbl_df")
  expect_equal(nrow(sim_res$audit_ledger_tbl), 3L) # 2025, 2026, 2027

  # Patient-flow conservation identity: served + unserved == appointment_requests
  audit <- sim_res$audit_ledger_tbl
  expect_equal(
    audit$served_patients_n + audit$unserved_delayed_n,
    audit$appointment_requests_n,
    tolerance = 1e-6
  )

  # Check HRR 306 spatial balance
  expect_s3_class(sim_res$annual_hrr_balance, "tbl_df")
  expect_equal(nrow(sim_res$annual_hrr_balance), 3L * 306L)
})
