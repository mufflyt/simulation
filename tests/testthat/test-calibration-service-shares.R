test_that("calibrate_service_share_model fits optimal prior strengths", {
  calib <- calibrate_service_share_model()
  expect_type(calib, "list")
  expect_true(all(c("calibrated_priors", "cms_evidence", "chia_evidence", "calibration_status") %in% names(calib)))

  priors <- calib$calibrated_priors
  expect_s3_class(priors, "tbl_df")
  expect_true(all(priors$optimal_alpha_strength > 0))
})

test_that("combine_service_share_evidence synthesizes CMS and CHIA evidence", {
  synth <- combine_service_share_evidence()
  expect_s3_class(synth, "tbl_df")
  expect_true(all(c("service", "L_lower_bound", "H_upper_bound", "midpoint_share", "disagreement_penalty") %in% names(synth)))
})

test_that("build_service_share_calibration_bundle creates auditable provenance bundle", {
  bundle <- build_service_share_calibration_bundle(n_draws = 10)
  expect_type(bundle, "list")
  expect_true(all(c("share_draws", "summary", "cms_fit", "chia_fit", "calibration", "evidence_registry", "input_hashes", "created_at") %in% names(bundle)))
  expect_gt(nrow(bundle$share_draws), 0)
})
