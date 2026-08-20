test_that("build_urps_prior_specification returns 10 validated parameters", {
  spec <- build_urps_prior_specification()

  expect_s3_class(spec, "tbl_df")
  expect_equal(nrow(spec), 10L)

  validated <- validate_urps_prior_specification(spec)
  expect_s3_class(validated, "tbl_df")
})

test_that("draw_urps_prior_parameters samples bounded prior vectors", {
  spec <- build_urps_prior_specification()
  draws <- draw_urps_prior_parameters(prior_tbl = spec, n_draws = 100L, seed = 42L)

  expect_s3_class(draws, "tbl_df")
  expect_equal(nrow(draws), 100L)
  expect_true("draw_id" %in% names(draws))
  expect_equal(ncol(draws), 11L) # draw_id + 10 parameters
})
