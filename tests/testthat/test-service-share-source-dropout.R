test_that("service share engine operates robustly when a single data source drops out", {
  .skip_unless_cms_service_share_data()
  cms_only_calib <- calibrate_service_share_model(chia_evidence = NULL)
  expect_type(cms_only_calib, "list")

  chia_only_calib <- calibrate_service_share_model(cms_evidence = NULL)
  expect_type(chia_only_calib, "list")
})
