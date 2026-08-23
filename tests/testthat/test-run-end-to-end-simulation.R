test_that("run_end_to_end_simulation executes all modules cleanly", {
  res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2030L,
    n_agents = 50L,
    save_outputs = FALSE
  )

  expect_type(res, "list")
  expect_true("audit_ledger_tbl" %in% names(res))
  expect_true("final_provider_cohort" %in% names(res))
  expect_true("engine_diagnostics" %in% names(res))
  expect_true("simulation_config" %in% names(res))

  expect_s3_class(res$audit_ledger_tbl, "tbl_df")
  expect_s3_class(res$final_provider_cohort, "tbl_df")
  expect_s3_class(res$engine_diagnostics, "tbl_df")
})
