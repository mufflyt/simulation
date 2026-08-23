test_that("build_cms_service_share_evidence holds mathematical bounds L <= H, L >= 0, H <= 1 across noisy inputs", {
  # Build noisy synthetic claim lines with extreme suppression & high volumes
  service_reg <- build_urogynecology_service_registry()
  tax_reg <- build_urogynecology_provider_taxonomy_registry()

  noisy_claims <- tibble::tibble(
    claim_id = paste0("NOISY_", 1:500),
    rendering_npi = sample(c("1000000001", "1000000002", "9999999999", "8888888888"), 500, replace = TRUE),
    taxonomy_code = sample(tax_reg$taxonomy_code, 500, replace = TRUE),
    hcpcs = sample(service_reg$hcpcs, 500, replace = TRUE),
    billed_services = sample(c(0, 1, 10, 100, 1000), 500, replace = TRUE),
    suppressed_services = sample(c(0, 5, 20, 50), 500, replace = TRUE)
  )

  res <- build_cms_service_share_evidence(claims_data = noisy_claims, service_registry = service_reg, taxonomy_registry = tax_reg)
  shares <- res$service_shares

  # 1. Suppression accounting identity: T = U + O + N + M
  expect_equal(
    shares$T_total_services,
    shares$U_urps_services + shares$O_other_physician_services + shares$N_app_pt_services + shares$M_suppressed_missing_services
  )

  # 2. Lower bound <= Upper bound
  expect_true(all(shares$L_lower_bound <= shares$H_upper_bound + 1e-9))

  # 3. Bounds non-negativity
  expect_true(all(shares$L_lower_bound >= 0.0))
  expect_true(all(shares$H_upper_bound >= 0.0))

  # 4. Midpoint share between L and H
  expect_true(all(shares$midpoint_share >= shares$L_lower_bound - 1e-9))
  expect_true(all(shares$midpoint_share <= shares$H_upper_bound + 1e-9))
})

test_that("build_cms_service_share_evidence handles empty claims input gracefully without crashing", {
  service_reg <- build_urogynecology_service_registry()
  tax_reg <- build_urogynecology_provider_taxonomy_registry()

  empty_claims <- tibble::tibble(
    claim_id = character(0),
    rendering_npi = character(0),
    taxonomy_code = character(0),
    hcpcs = character(0),
    billed_services = numeric(0),
    suppressed_services = numeric(0)
  )

  res <- build_cms_service_share_evidence(claims_data = empty_claims, service_registry = service_reg, taxonomy_registry = tax_reg)
  expect_type(res, "list")
  expect_s3_class(res$service_shares, "tbl_df")
})
