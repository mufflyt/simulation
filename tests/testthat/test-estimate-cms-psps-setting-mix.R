# Unit tests for CMS PSPS 2024 CPT setting mix pipeline and lookup functions

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("estimate_cms_psps_setting_mix.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_cms_psps_setting_mix.R")
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
})

test_that("CMS PSPS 2024 raw CSV file exists and is non-empty when present", {
  psps_path <- .repo_path("data-raw", "cms_psps", "MUP_PHY_R26_P05_V10_D24_Geo.csv")
  skip_if_not(file.exists(psps_path), "CMS PSPS 2024 raw file not present")

  sz <- file.size(psps_path)
  expect_gt(sz, 10e6) # Should be ~42 MB
})
