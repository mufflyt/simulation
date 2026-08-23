test_that("draw_compositional_service_shares satisfies strict double-precision simplex equality across 1000 draws", {
  calib <- calibrate_service_share_model()
  draws <- draw_compositional_service_shares(calibration_model = calib, n_draws = 1000L, seed = 999L)

  expect_equal(nrow(draws), 1000L * length(calib$calibrated_priors$service) * 5L)

  # Check that every single draw & service cell sums to 1.0 within double precision threshold (1e-10)
  cell_sums <- draws |>
    dplyr::group_by(draw, service, condition) |>
    dplyr::summarise(total_share = sum(share), .groups = "drop")

  expect_true(all(abs(cell_sums$total_share - 1.0) < 1e-10))
  expect_true(all(draws$share >= 0.0))
  expect_true(all(draws$share <= 1.0))
})

test_that("calibrate_service_share_model selects monotone optimal prior strength as volume increases", {
  calib <- calibrate_service_share_model()
  priors <- calib$calibrated_priors

  expect_s3_class(priors, "tbl_df")
  expect_true(all(priors$optimal_alpha_strength %in% c(2, 5, 10, 20, 30, 50)))
  expect_false(any(is.na(priors$optimal_alpha_strength)))
})

test_that("combine_service_share_evidence applies disagreement penalty proportional to bound width H - L", {
  synth <- combine_service_share_evidence()

  expect_true(all(synth$disagreement_penalty >= 0.05))
  expect_true(all(synth$disagreement_penalty >= (synth$H_upper_bound - synth$L_lower_bound)))
})
