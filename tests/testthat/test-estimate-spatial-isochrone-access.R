# Unit tests for spatial access isochrone drive-time pipeline

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("estimate_spatial_isochrone_access.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_spatial_isochrone_access.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("e2sfca_bands returns canonical 30, 60, 120, 180 minute bands", {
  bands <- e2sfca_bands()
  expect_equal(bands, c(30L, 60L, 120L, 180L))
})

test_that("e2sfca_band_weights validates monotone decreasing weights", {
  weights <- E2SFCA_DEFAULT_WEIGHTS
  validated <- e2sfca_band_weights(weights)
  expect_named(validated, c("30", "60", "120", "180"))
  expect_equal(unname(validated["30"]), 1.00)
  expect_equal(unname(validated["60"]), 0.68)
})
