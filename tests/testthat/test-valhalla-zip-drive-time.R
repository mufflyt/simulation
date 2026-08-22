# Unit tests for valhalla_zip_drive_time routing function

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("R/geography-chia_inpatient_flows.R script exists and has valid syntax", {
  script_path <- .repo_path("R", "geography-chia_inpatient_flows.R")
  testthat::skip_if_not(file.exists(script_path), "source file not shipped under R CMD check")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("valhalla_zip_drive_time validates missing required columns", {
  zip_pairs <- tibble::tibble(wrong_origin = "02115", wrong_dest = "02114")
  zip_centroids <- tibble::tibble(zip5 = "02115", lat = 42.3, lon = -71.1)

  expect_error(
    valhalla_zip_drive_time(zip_pairs, zip_centroids),
    "Missing pair column"
  )
})
