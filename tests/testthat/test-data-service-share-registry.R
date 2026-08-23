test_that("build_urogynecology_service_registry builds valid schema and rules", {
  reg <- build_urogynecology_service_registry()
  expect_s3_class(reg, "tbl_df")
  expect_true(all(c(
    "hcpcs", "service", "category", "evidence_quality",
    "effective_start", "effective_end", "setting_scope"
  ) %in% names(reg)))
  expect_gt(nrow(reg), 10)
  expect_true(all(reg$evidence_quality %in% c("A", "B")))
})

test_that("build_urogynecology_provider_taxonomy_registry builds valid taxonomy crosswalk", {
  tax_reg <- build_urogynecology_provider_taxonomy_registry()
  expect_s3_class(tax_reg, "tbl_df")
  expect_true(all(c("taxonomy_code", "provider_type", "provider_group", "is_urps_specialist") %in% names(tax_reg)))
  expect_true("207VF0040X" %in% tax_reg$taxonomy_code)
  expect_true(any(tax_reg$is_urps_specialist))
})

test_that("validate_service_registry_production throws error on unsourced rules in production", {
  valid_reg <- build_urogynecology_service_registry()
  expect_silent(validate_service_registry_production(valid_reg))

  invalid_reg <- valid_reg
  invalid_reg$evidence_quality[1] <- "example"
  expect_error(validate_service_registry_production(invalid_reg), "Production mode validation failed")
})
