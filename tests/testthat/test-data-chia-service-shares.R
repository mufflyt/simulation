test_that("build_chia_service_share_evidence calculates valid setting and payer shares", {
  res <- build_chia_service_share_evidence()

  expect_type(res, "list")
  expect_true(all(c("setting_shares", "payer_shares", "specialty_composition") %in% names(res)))

  setting_shares <- res$setting_shares
  expect_s3_class(setting_shares, "tbl_df")

  # Setting shares sum to 1 within each service
  sums <- setting_shares |>
    dplyr::group_by(service) |>
    dplyr::summarise(total = sum(setting_share), .groups = "drop")

  expect_true(all(abs(sums$total - 1.0) < 1e-6))

  # Payer shares sum to 1 within each service
  payer_sums <- res$payer_shares |>
    dplyr::group_by(service) |>
    dplyr::summarise(total = sum(payer_share), .groups = "drop")

  expect_true(all(abs(payer_sums$total - 1.0) < 1e-6))
})
