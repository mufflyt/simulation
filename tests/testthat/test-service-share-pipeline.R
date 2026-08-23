test_that("service share pipeline runs end to end cleanly", {
  service_registry <- build_urogynecology_service_registry()
  taxonomy_registry <- build_urogynecology_provider_taxonomy_registry()

  cms_evidence <- build_cms_service_share_evidence(service_registry = service_registry, taxonomy_registry = taxonomy_registry)
  chia_evidence <- build_chia_service_share_evidence(service_registry = service_registry, taxonomy_registry = taxonomy_registry)

  calib <- calibrate_service_share_model(cms_evidence = cms_evidence, chia_evidence = chia_evidence)
  bundle <- build_service_share_calibration_bundle(calibration_model = calib, n_draws = 5)

  expect_type(bundle, "list")
  expect_gt(nrow(bundle$share_draws), 0)
  expect_s3_class(bundle$calibration, "tbl_df")
  expect_true("optimal_alpha_strength" %in% names(bundle$calibration))
})
