# Real Spatial-Access Inputs (tract demand + isochrone membership) ----
#
# Turns the E2SFCA engine (R/geography-spatial_access_e2sfca) from a synthetic-membership demo into a REAL
# access model -- the spatial analog of what the Census NPP wiring did for the
# demand side. Two real inputs feed the base-R engine:
#
#   DEMAND  -- Census tract female-65+ population + centroids (83,492 CONUS
#              tracts, ~30.5M women), vendored and checksummed; loading it needs
#              no spatial stack.
#   SUPPLY  -- provider drive-time isochrone polygons (30/60/120/180 min). These
#              are LARGE external artifacts from the isochrones pipeline (Valhalla)
#              and are NOT vendored; load_provider_isochrones() reads a local set.
#
# build_access_membership() overlays the two (point-in-polygon) to produce the
# `membership` table the base-R engine consumes. Only the overlay needs `sf`
# (Suggests); the engine and the tract loader stay dependency-light. Because the
# geometry is year-agnostic, the membership is built ONCE and the microsim inner
# loop only swaps `supply` each simulated year.

#' Load the real tract demand denominator (female 65+, with centroids)
#'
#' @param mode Reproducibility mode (SHA-256 drift handling in the resolver).
#' @return Tibble `demand_id` (GEOID), `population` (fem65), `lon`, `lat`.
#' @export
load_tract_demand <- function(mode = resolve_reproducibility_mode()) {
  path <- resolve_canonical("tract_fem65_centroids", mode = mode)
  df <- utils::read.csv(path, stringsAsFactors = FALSE, colClasses = c(GEOID = "character"))
  tibble::tibble(
    demand_id = df$GEOID,
    population = as.numeric(df$fem65),
    lon = as.numeric(df$lon),
    lat = as.numeric(df$lat)
  )
}

#' Load provider drive-time isochrone polygons (external artifact, fail-closed)
#'
#' Reads `isochrones_{band}min_consolidated.rds` polygon sets from a local
#' directory (these are large Valhalla-produced artifacts, not vendored). Fails
#' closed if the directory or files are absent. Requires `sf`.
#'
#' @param artifacts_dir Directory holding the consolidated isochrone rds files.
#'   Defaults to the `ISOCHRONES_ARTIFACTS_DIR` env var.
#' @param bands Drive-time bands to load (minutes).
#' @return An `sf` object with `coord_id`, `drive_time` (band), and polygon
#'   geometry (rows stacked across bands).
#' @export
load_provider_isochrones <- function(artifacts_dir = Sys.getenv("ISOCHRONES_ARTIFACTS_DIR", ""),
                                     bands = c(30L, 60L, 120L, 180L)) {
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("load_provider_isochrones() needs the 'sf' package (Suggests).", call. = FALSE)
  }
  if (!nzchar(artifacts_dir) || !dir.exists(artifacts_dir)) {
    stop(sprintf(paste0("Provider isochrone artifacts not found (dir = '%s'). These are large ",
                        "external files; set ISOCHRONES_ARTIFACTS_DIR or pass artifacts_dir ",
                        "pointing at isochrones_{band}min_consolidated.rds."), artifacts_dir),
         call. = FALSE)
  }
  pieces <- lapply(bands, function(b) {
    f <- file.path(artifacts_dir, sprintf("isochrones_%dmin_consolidated.rds", b))
    if (!file.exists(f)) {
      stop(sprintf("Missing isochrone artifact for %d-min band: %s", b, f), call. = FALSE)
    }
    g <- readRDS(f)
    g$drive_time <- b
    g
  })
  do.call(rbind, pieces)
}

#' Build the E2SFCA membership table from isochrone polygons + tract points
#'
#' Point-in-polygon overlay: each tract centroid is assigned to every provider
#' isochrone band whose polygon contains it. Produces the `membership` table the
#' base-R engine ([compute_e2sfca_access()]) consumes. Requires `sf`.
#'
#' @param iso_sf Provider isochrones (`sf`) with a provider-id column and a
#'   band column (drive-time minutes).
#' @param tracts Demand tibble with `demand_id`, `lon`, `lat`
#'   ([load_tract_demand()]), or an `sf` of tract points.
#' @param provider_col Name of the provider-id column in `iso_sf`.
#' @param band_col Name of the band (minutes) column in `iso_sf`.
#' @param crs Coordinate reference system of the tract lon/lat (default 4326).
#' @return Tibble `demand_id`, `provider_id`, `band` (one row per containment).
#' @export
build_access_membership <- function(iso_sf, tracts,
                                    provider_col = "coord_id",
                                    band_col = "drive_time",
                                    crs = 4326) {
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("build_access_membership() needs the 'sf' package (Suggests).", call. = FALSE)
  }
  assertthat::assert_that(all(c(provider_col, band_col) %in% names(iso_sf)))

  pts <- if (inherits(tracts, "sf")) {
    tracts
  } else {
    assertthat::assert_that(all(c("demand_id", "lon", "lat") %in% names(tracts)))
    sf::st_as_sf(as.data.frame(tracts), coords = c("lon", "lat"), crs = crs)
  }
  # Align CRS, then assign each point the attributes of any polygon containing it.
  iso_al <- sf::st_transform(iso_sf, sf::st_crs(pts))
  joined <- sf::st_join(pts, iso_al[c(provider_col, band_col)], join = sf::st_within)

  out <- sf::st_drop_geometry(joined)
  out <- out[!is.na(out[[provider_col]]), , drop = FALSE]
  tibble::tibble(
    demand_id = out$demand_id,
    provider_id = as.character(out[[provider_col]]),
    band = as.integer(out[[band_col]])
  )
}

#' Compute a real access surface (tract demand x provider isochrones x supply)
#'
#' Convenience composer: loads the real tract demand, builds membership from the
#' supplied provider isochrones, and runs the E2SFCA engine. The provider
#' isochrones are the only non-vendored input (see [load_provider_isochrones()]).
#'
#' @param iso_sf Provider isochrones (`sf`); e.g. [load_provider_isochrones()].
#' @param supply Tibble `provider_id`, `supply` (S_j).
#' @param weights Cumulative band weights.
#' @param step2_power 1 = E2SFCA, 2 = M2SFCA.
#' @param provider_col,band_col Column names in `iso_sf`.
#' @param mode Reproducibility mode.
#' @return The [compute_e2sfca_access()] result over the real tract demand.
#' @export
real_access_surface <- function(iso_sf, supply,
                                weights = E2SFCA_DEFAULT_WEIGHTS,
                                step2_power = 1,
                                provider_col = "coord_id",
                                band_col = "drive_time",
                                mode = resolve_reproducibility_mode()) {
  tracts <- load_tract_demand(mode = mode)
  membership <- build_access_membership(iso_sf, tracts,
                                        provider_col = provider_col, band_col = band_col)
  demand <- dplyr::select(tracts, "demand_id", "population")
  compute_e2sfca_access(membership, supply, demand,
                        weights = weights, step2_power = step2_power)
}
