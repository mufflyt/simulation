test_that("draw_compositional_service_shares enforces simplex constraints sum(share) == 1", {
  draws <- draw_compositional_service_shares(n_draws = 20)

  expect_s3_class(draws, "tbl_df")
  expect_true(all(c("draw", "service", "condition", "provider_type", "share") %in% names(draws)))

  # Verify share >= 0 and share <= 1
  expect_true(all(draws$share >= 0))
  expect_true(all(draws$share <= 1))

  # Verify sum(share) == 1 for every draw and service cell
  cell_sums <- draws |>
    dplyr::group_by(draw, service, condition) |>
    dplyr::summarise(total_share = sum(share), .groups = "drop")

  expect_true(all(abs(cell_sums$total_share - 1.0) < 1e-6))
})
