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

test_that("load_provider_isochrones unions polygon fragments for the same provider-band only", {
  skip_if_not(has_sf(), "sf not installed")

  poly <- function(cx, cy) {
    sf::st_polygon(list(rbind(c(cx, cy), c(cx + 1, cy), c(cx + 1, cy + 1),
                              c(cx, cy + 1), c(cx, cy))))
  }
  # P1/30min split into two adjacent (touching, non-overlapping) fragments --
  # the shape a drive-time band crossing a coastline or state boundary
  # produces from Valhalla. P2/30min and P1/60min are single fragments and
  # must be left alone: unioning must never cross providers or bands.
  df <- sf::st_sf(
    coord_id   = c("P1", "P1", "P2", "P1"),
    drive_time = c(30L, 30L, 30L, 60L),
    geometry   = sf::st_sfc(poly(0, 0), poly(1, 0), poly(10, 10), poly(0, 0)),
    crs = 4326
  )

  tmp_dir <- tempfile("iso_test_")
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)
  saveRDS(df, file.path(tmp_dir, "provider_isochrones.rds"))

  out <- load_provider_isochrones(artifacts_dir = tmp_dir, bands = c(30L, 60L),
                                  verify_checksums = FALSE)

  key <- paste(out$coord_id, out$drive_time)
  expect_equal(sort(unique(key)), sort(c("P1 30", "P1 60", "P2 30")))
  expect_equal(anyDuplicated(key), 0L)  # the two P1/30 fragments collapsed to one row

  p1_30 <- out[out$coord_id == "P1" & out$drive_time == 30L, ]
  expect_equal(nrow(p1_30), 1L)
  p2_30 <- out[out$coord_id == "P2" & out$drive_time == 30L, ]
  # Unioned area (two adjacent 1x1 fragments) should be roughly double a
  # single untouched fragment's area -- proof this is a real union, not a
  # silent pick-one-and-drop-the-other.
  expect_equal(as.numeric(sf::st_area(p1_30)) / as.numeric(sf::st_area(p2_30)),
              2, tolerance = 0.05)
})
