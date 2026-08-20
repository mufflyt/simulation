test_that("run_end_to_end_simulation executes all 6 modules cleanly", {
  res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2030L,
    n_agents = 50L,
    save_outputs = FALSE
  )

  expect_type(res, "list")
  expect_named(res, c(
    "entry_hazard",
    "retreatment_predictions",
    "feasible_hospitals",
    "spatial_equilibrium",
    "provider_survival_model",
    "workload_decomposition",
    "policy_simulation"
  ))

  expect_s3_class(res$retreatment_predictions, "tbl_df")
  expect_s3_class(res$feasible_hospitals, "tbl_df")
  expect_s3_class(res$workload_decomposition$capacity_summary, "tbl_df")
})
