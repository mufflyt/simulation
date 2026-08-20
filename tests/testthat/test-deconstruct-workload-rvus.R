test_that("deconstruct_workload_rvus deconstructs CPT workload and calculates APP capacity", {
  cpt_volume <- tibble::tribble(
    ~year, ~hcpcs, ~case_volume,
    2024L, "57288", 100,
    2024L, "57283", 50
  )

  pfs_reference <- tibble::tribble(
    ~year, ~hcpcs, ~work_rvu, ~global_days, ~pre_op_pct, ~intra_op_pct, ~post_op_pct, ~pre_service_minutes, ~intra_service_minutes, ~post_service_minutes,
    2024L, "57288", 14.50, "090", 0.10, 0.70, 0.20, 45, 90, 60,
    2024L, "57283", 16.20, "090", 0.10, 0.70, 0.20, 50, 105, 65
  )

  res <- deconstruct_workload_rvus(
    cpt_volume = cpt_volume,
    pfs_reference = pfs_reference
  )

  expect_type(res, "list")
  expect_named(res, c("components", "workload_summary", "capacity_summary", "summary_sentence", "saved_files", "interpretation"))
  expect_s3_class(res$components, "tbl_df")
  expect_s3_class(res$capacity_summary, "tbl_df")

  # Verify intra-service app_share is 0 (surgeon intra-op work preserved)
  intra_rows <- res$components |> dplyr::filter(phase == "intra_service")
  expect_equal(unique(intra_rows$app_share), 0)
  expect_equal(unique(intra_rows$surgeon_minutes_freed), 0)
})

test_that("deconstruct_workload_rvus enforces fail-closed guardrails", {
  cpt_volume <- tibble::tribble(
    ~year, ~hcpcs, ~case_volume,
    2024L, "57288", 100
  )

  pfs_reference <- tibble::tribble(
    ~year, ~hcpcs, ~work_rvu, ~global_days, ~pre_op_pct, ~intra_op_pct, ~post_op_pct, ~pre_service_minutes, ~intra_service_minutes, ~post_service_minutes,
    2024L, "57288", 14.50, "090", 0.10, 0.70, 0.20, 45, 90, 60
  )

  invalid_policy <- tibble::tribble(
    ~phase, ~app_share, ~surgeon_rework_share,
    "initial_intake", 0.80, 0.10,
    "pre_service", 0.50, 0.15,
    "intra_service", 0.50, 0.00, # INVALID: Intra-service delegation > 0!
    "post_service", 0.90, 0.10
  )

  expect_error(
    deconstruct_workload_rvus(cpt_volume, pfs_reference, delegation_policy = invalid_policy),
    "Primary-surgeon intra-service time cannot be delegated to an APP"
  )
})
