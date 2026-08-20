test_that("calibrate_intra_service_minutes calculates log-normal moments correctly", {
  inputs <- example_intra_service_inputs()

  res <- calibrate_intra_service_minutes(
    pfs_times = inputs$pfs_times,
    literature_times = inputs$literature_times,
    literature_center_weight = 0.35,
    min_studies = 2L
  )

  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 2L)
  expect_true("calibrated_mean_minutes" %in% names(res))
  expect_true("recommended_time_source" %in% names(res))

  # 57288 has 2 studies -> center update allowed
  row_57288 <- res |> dplyr::filter(cpt == "57288")
  expect_true(row_57288$center_update_allowed)
  expect_equal(row_57288$recommended_time_source, "CMS anchor + literature calibration")
})

test_that("deconstruct_intra_service_workload simulates annual workload distributions", {
  inputs <- example_intra_service_inputs()

  calib <- calibrate_intra_service_minutes(
    pfs_times = inputs$pfs_times,
    literature_times = inputs$literature_times
  )

  summary_res <- deconstruct_intra_service_workload(
    cpt_workload = inputs$cpt_workload,
    calibrated_times = calib,
    simulations = 500L,
    seed = 20260820L
  )

  expect_s3_class(summary_res, "tbl_df")
  expect_equal(nrow(summary_res), 2L)
  expect_true(all(c("mean_hours", "median_hours", "mean_clinical_fte") %in% names(summary_res)))
  expect_true(all(summary_res$mean_hours > 0))
})
