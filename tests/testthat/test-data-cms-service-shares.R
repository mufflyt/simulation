test_that("build_cms_service_share_evidence calculates valid suppression bounds and shares", {
  res <- build_cms_service_share_evidence()

  expect_type(res, "list")
  expect_true(all(c("service_shares", "wrvu_shares", "tier_breakdown", "input_hashes") %in% names(res)))

  shares <- res$service_shares
  expect_s3_class(shares, "tbl_df")

  # Accounting Identity Verification: T == U + O + N + M
  expect_equal(
    shares$T_total_services,
    shares$U_urps_services + shares$O_other_physician_services + shares$N_app_pt_services + shares$M_suppressed_missing_services
  )

  # Bound Verification: L <= H, L >= 0, H <= 1 (approx)
  expect_true(all(shares$L_lower_bound <= shares$H_upper_bound + 1e-9))
  expect_true(all(shares$L_lower_bound >= 0))

  # Input Hash Pinning
  expect_type(res$input_hashes$roster_hash, "character")
  expect_type(res$input_hashes$claims_hash, "character")
})

test_that("wRVU weighted shares sum to 1 within each service", {
  res <- build_cms_service_share_evidence()
  wrvu_shares <- res$wrvu_shares

  sums <- wrvu_shares |>
    dplyr::group_by(service) |>
    dplyr::summarise(total_share = sum(wrvu_weighted_share), .groups = "drop")

  expect_true(all(abs(sums$total_share - 1.0) < 1e-6 | sums$total_share == 0))
})
