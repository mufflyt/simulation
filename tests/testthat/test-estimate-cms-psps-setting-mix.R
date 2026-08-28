# Unit tests for CMS PSPS 2024 CPT setting mix pipeline and lookup functions

test_that("estimate_cms_psps_setting_mix.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_cms_psps_setting_mix.R")
  skip_if_not(file.exists(script_path), "Script absent under R CMD check")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("cpt_setting_mix returns valid calibrated setting mix table", {
  mix <- cpt_setting_mix("all")
  expect_s3_class(mix, "data.frame")
  expect_gt(nrow(mix), 10L)

  required_cols <- c("cpt", "category", "p_facility", "p_office", "total_services", "calibration_status", "source")
  expect_true(all(required_cols %in% names(mix)))

  # Facility + office probabilities must sum to 1.0
  expect_equal(mix$p_facility + mix$p_office, rep(1.0, nrow(mix)), tolerance = 1e-4)

  # Check specific key CPT codes
  sling <- dplyr::filter(mix, cpt == "57288")
  expect_equal(nrow(sling), 1L)
  expect_equal(sling$p_facility, 0.9807)

  sacro <- dplyr::filter(mix, cpt == "57425")
  expect_equal(nrow(sacro), 1L)
  expect_equal(sacro$p_facility, 0.9884)

  urodynamic <- dplyr::filter(mix, cpt == "51729")
  expect_equal(nrow(urodynamic), 1L)
  expect_equal(urodynamic$p_office, 0.8985)
})

test_that("cpt_setting_mix category filtering works correctly", {
  slings <- cpt_setting_mix("slings")
  expect_true(all(slings$category == "slings"))

  prolapse <- cpt_setting_mix("prolapse")
  expect_true(all(prolapse$category == "prolapse"))

  urodynamics <- cpt_setting_mix("urodynamics")
  expect_true(all(urodynamics$category == "urodynamics"))

  cysto <- cpt_setting_mix("cystoscopy")
  expect_true(all(cysto$category == "cystoscopy"))
  expect_gt(nrow(cysto), 5L)

  botox <- dplyr::filter(cysto, cpt == "52287")
  expect_equal(nrow(botox), 1L)
  expect_equal(botox$p_facility, 0.5091)
})

test_that("prolapse_setting_mix helper returns exact empirical ratios", {
  psm <- prolapse_setting_mix()
  expect_type(psm, "list")
  expect_equal(psm$p_facility, 0.9841)
  expect_equal(psm$p_office, 0.0159)
  expect_equal(psm$p_facility + psm$p_office, 1.0)
  expect_equal(psm$total_services, 52616)
  expect_equal(length(psm$cpt_codes), 6L)
})

test_that("CMS PSPS 2024 raw CSV file exists and is non-empty when present", {
  psps_path <- .repo_path("data-raw", "cms_psps", "MUP_PHY_R26_P05_V10_D24_Geo.csv")
  skip_if_not(file.exists(psps_path), "CMS PSPS 2024 raw file not present")

  sz <- file.size(psps_path)
  expect_gt(sz, 10e6) # Should be ~42 MB
})
