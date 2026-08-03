# Guards for the PI-facing summary report and the access composer.

.fake_result <- function() {
  list(
    run_id = "workforce_fpmrs_test",
    fte_gap = tibble::tibble(
      year = c(2025, 2050),
      supplied_fte = c(1306, 1400),
      required_fte = c(1377, 1620),
      gap_fte = c(-71, -220),
      gap_pct = c(-5.2, -13.6)),
    concordance = list(informative = TRUE, robust = FALSE, trough_year = 2050),
    outlook = tibble::tibble(
      scenario_label = c("Status quo", "Enhanced training"),
      replacement_ratio = c(0.96, 1.25),
      outlook = c("Marginal", "Adequate")),
    scenario_meta = list(subspecialty = "FPMRS", years = c(2025, 2050),
                         example_only = FALSE))
}

test_that("workforce_summary_report folds the headline results", {
  rep <- workforce_summary_report(.fake_result())
  expect_s3_class(rep, "workforce_summary_report")
  expect_equal(rep$gap$year, 2050)
  expect_equal(rep$gap$gap_pct, -13.6)
  expect_equal(rep$concordance$robust, FALSE)
  expect_equal(rep$subspecialty, "FPMRS")
  expect_true(is.data.frame(rep$outlook))
})

test_that("the report prints without error and shows the gap", {
  rep <- workforce_summary_report(.fake_result())
  out <- capture.output(print(rep))
  expect_true(any(grepl("workforce summary", out)))
  expect_true(any(grepl("2050 FTE gap", out)))
  expect_true(any(grepl("replacement-ratio outlook", out)))
})

test_that("access folds into the report when supplied", {
  access <- list(mean_access = 12.3, access_desert_share_pct = 18.4,
                 threshold_shares = tibble::tibble(threshold = c(0, 5),
                                                   pop_share_at_or_above = c(1, 0.6)),
                 n_tracts = 83492L)
  rep <- workforce_summary_report(.fake_result(), access = access)
  expect_equal(rep$access$access_desert_share_pct, 18.4)
  out <- capture.output(print(rep))
  expect_true(any(grepl("access desert share", out)))
})

# ---- access composer (real tracts + synthetic-geometry polygons) -----------

tract_ready <- function() isTRUE(tryCatch({ resolve_canonical("tract_fem65_centroids"); TRUE },
                                          error = function(e) FALSE))
has_sf <- function() requireNamespace("sf", quietly = TRUE)

test_that("workforce_access_summary composes the real access surface", {
  skip_if_not(tract_ready(), "tract demand not present")
  skip_if_not(has_sf(), "sf not installed")
  provs <- sf::st_as_sf(
    data.frame(coord_id = c("denver", "nyc"), drive_time = c(60L, 60L),
               lon = c(-104.99, -74.00), lat = c(39.74, 40.71)),
    coords = c("lon", "lat"), crs = 4326)
  iso <- suppressWarnings(sf::st_buffer(provs, 60000))
  supply <- tibble::tibble(provider_id = c("denver", "nyc"), supply = c(8, 20))

  a <- workforce_access_summary(iso, supply)
  expect_equal(a$n_tracts, 83492L)
  # Two metro catchments cover a tiny fraction of national tracts -> most are deserts.
  expect_gt(a$access_desert_share_pct, 90)
  expect_true(all(c("threshold", "pop_share_at_or_above") %in% names(a$threshold_shares)))
})
