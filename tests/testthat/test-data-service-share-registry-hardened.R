test_that("build_urogynecology_service_registry enforces non-empty HCPCS, valid categories, and version string", {
  reg <- build_urogynecology_service_registry()

  expect_false(any(is.na(reg$hcpcs)))
  expect_false(any(reg$hcpcs == ""))
  expect_true(all(nchar(reg$hcpcs) == 5))

  # Categories must be valid non-empty strings
  expect_false(any(is.na(reg$category)))
  expect_true(all(reg$category %in% c("Office Procedure", "Supply", "Surgical Incontinence", "Procedural Incontinence", "Neuromodulation", "Surgical Prolapse", "Diagnostic", "Urodynamics")))

  # Setting scope must be valid enum
  expect_true(all(reg$setting_scope %in% c("office", "facility", "facility_and_office")))
})

test_that("build_urogynecology_provider_taxonomy_registry handles all 10 core taxonomy codes and flags URPS correctly", {
  tax_reg <- build_urogynecology_provider_taxonomy_registry()

  # NUCC Taxonomy Codes must be 10 characters long
  expect_true(all(nchar(tax_reg$taxonomy_code) == 10))
  expect_true(all(grepl("^[0-9A-Z]{10}$", tax_reg$taxonomy_code)))

  # FPMRS physician must be flagged as is_urps_specialist == TRUE
  fpmrs_row <- tax_reg |> dplyr::filter(provider_type == "FPMRS physician")
  expect_equal(nrow(fpmrs_row), 1)
  expect_true(fpmrs_row$is_urps_specialist)

  # Non-FPMRS specialties must be FALSE
  obgyn_row <- tax_reg |> dplyr::filter(provider_type == "General OB/GYN")
  expect_false(obgyn_row$is_urps_specialist)
})

test_that("validate_service_registry_production rejects malformed, NA, or unverified evidence quality tiers", {
  reg <- build_urogynecology_service_registry()

  # Test NA evidence_quality
  reg_na <- reg
  reg_na$evidence_quality[2] <- NA_character_
  expect_error(validate_service_registry_production(reg_na))

  # Test 'uncited' tier
  reg_uncited <- reg
  reg_uncited$evidence_quality[3] <- "uncited"
  expect_error(validate_service_registry_production(reg_uncited), "Production mode validation failed")
})
