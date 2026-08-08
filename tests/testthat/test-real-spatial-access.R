# Guards for the real spatial-access inputs: tract demand + isochrone membership.

tract_available <- function() {
  isTRUE(tryCatch({ resolve_canonical("tract_fem65_centroids"); TRUE },
                  error = function(e) FALSE))
}
has_sf <- function() requireNamespace("sf", quietly = TRUE)

test_that("load_tract_demand returns the real CONUS tract denominator", {
  skip_if_not(tract_available(), "tract demand file not present")
  d <- load_tract_demand(mode = "strict")           # strict verifies the checksum
  expect_setequal(names(d), c("demand_id", "population", "lon", "lat"))
  expect_equal(nrow(d), 83492L)
  expect_true(is.character(d$demand_id))
  expect_true(all(nchar(d$demand_id) == 11))         # census tract GEOIDs
  # Total female 65+ across tracts ~ 30.5M (ACS), a real national magnitude.
  expect_gt(sum(d$population), 29e6); expect_lt(sum(d$population), 32e6)
  # CONUS bounding box.
  expect_true(all(d$lon > -125 & d$lon < -66))
  expect_true(all(d$lat > 24 & d$lat < 50))
})

test_that("build_access_membership overlays polygons onto real tract centroids", {
  skip_if_not(tract_available(), "tract demand file not present")
  skip_if_not(has_sf(), "sf not installed")
  tracts <- load_tract_demand()

  # Two provider catchments as real-geometry buffers around actual city coords
  # (stand-ins for road-network isochrones; the function consumes real polygons).
  provs <- sf::st_as_sf(
    data.frame(coord_id = c("denver", "nyc"), drive_time = c(60L, 60L),
               lon = c(-104.99, -74.00), lat = c(39.74, 40.71)),
    coords = c("lon", "lat"), crs = 4326)
  iso <- suppressWarnings(sf::st_buffer(provs, 60000))   # ~60 km catchment

  m <- build_access_membership(iso, tracts)
  expect_setequal(names(m), c("demand_id", "provider_id", "band"))
  expect_gt(nrow(m), 0)
  expect_true(all(m$band == 60L))
  expect_true(all(m$provider_id %in% c("denver", "nyc")))
  # Colorado tracts (GEOID prefix 08) fall in the Denver catchment; NY (36) in NYC.
  denver_tracts <- m$demand_id[m$provider_id == "denver"]
  expect_true(any(substr(denver_tracts, 1, 2) == "08"))
  nyc_tracts <- m$demand_id[m$provider_id == "nyc"]
  expect_true(any(substr(nyc_tracts, 1, 2) == "36"))
})


test_that("load_provider_isochrones fails closed when the external artifact is absent", {
  skip_if_not(has_sf(), "sf not installed")
  expect_error(load_provider_isochrones(artifacts_dir = tempfile()),
               "not found|Missing isochrone")
})
