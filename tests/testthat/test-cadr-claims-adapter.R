test_that("urps_cadr_specialty_codes returns correct CMS specialty mappings", {
  codes <- urps_cadr_specialty_codes()
  expect_type(codes, "character")
  expect_equal(codes[["50"]], "Nurse Practitioner (NP)")
  expect_equal(codes[["97"]], "Physician Assistant (PA)")
  expect_equal(codes[["16"]], "Obstetrics & Gynecology")
  expect_equal(codes[["34"]], "Urology")
})

test_that("build_cadr_extract_request creates complete data dictionary", {
  req <- build_cadr_extract_request()
  expect_s3_class(req, "tbl_df")
  expect_true("prvdr_spclty" %in% req$field_name)
  expect_true("derived_provider_category" %in% req$field_name)
  expect_equal(nrow(req), 14L)
})

test_that("classify_cadr_provider_category correctly categorizes CMS specialty codes", {
  claims <- tibble::tribble(
    ~claim_id, ~prvdr_spclty,
    "C1", 50,  # NP
    "C2", 97,  # PA
    "C3", 16,  # OBGYN
    "C4", 34,  # Urology
    "C5", 11,  # PrimaryCare
    "C6", 65,  # PT
    "C7", 99,  # Other
    "C8", NA   # Missing
  )

  res <- classify_cadr_provider_category(claims)
  expect_s3_class(res, "tbl_df")
  expect_equal(res$derived_provider_category, c("NP", "PA", "OBGYN", "Urology", "PrimaryCare", "PT", "Other", "Missing"))
})

test_that("calibrate_cadr_delegation_bounds produces scenario grid for global post-op visits", {
  grid <- calibrate_cadr_delegation_bounds()
  expect_s3_class(grid, "tbl_df")
  expect_equal(unique(grid$scenario_id), c("global_app_25", "global_app_50", "global_app_75", "global_app_90"))

  # Intra-service must always be 0 share across all scenarios
  intra_rows <- grid |> dplyr::filter(phase == "intra_service")
  expect_equal(unique(intra_rows$app_share), 0)
})
