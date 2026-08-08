# ARCHIVED tests, moved from tests/testthat/test-real-spatial-access.R.
# They exercise functions now in inst/archive/ and are NOT run.

test_that("real_access_surface produces a surface over real tract demand", {
  skip_if_not(tract_available(), "tract demand file not present")
  skip_if_not(has_sf(), "sf not installed")
  provs <- sf::st_as_sf(
    data.frame(coord_id = c("denver", "nyc"), drive_time = c(60L, 60L),
               lon = c(-104.99, -74.00), lat = c(39.74, 40.71)),
    coords = c("lon", "lat"), crs = 4326)
  iso <- suppressWarnings(sf::st_buffer(provs, 60000))
  supply <- tibble::tibble(provider_id = c("denver", "nyc"), supply = c(8, 20))

  res <- real_access_surface(iso, supply)
  expect_equal(nrow(res$access), 83492L)             # one row per real tract
  expect_true(any(res$access$access_scaled > 0))     # covered tracts have access
  expect_true(any(res$access$access_scaled == 0))    # most tracts are uncovered
})

