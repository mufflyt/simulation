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
#' @family spatial access data
#' @concept geography
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
#' Two loading paths, tried in order (unchanged from the original
#' implementation): a single consolidated `provider_isochrones.rds` (Option
#' A), else per-band `isochrones_{band}min_consolidated.rds` files via
#' `ISOCHRONE_BAND_FILE()` (Option B). Both paths now share the same
#' post-load discipline:
#'
#' \enumerate{
#'   \item When `verify_checksums` is `TRUE` (default), [assert_canonical_isochrones()]
#'     checks `artifacts_dir` against the pinned run registry and SHA-256
#'     checksums before anything is read -- fails closed (in strict
#'     reproducibility mode) rather than silently loading an unverified or
#'     stale artifact set.
#'   \item Multiple polygon fragments for the SAME `coord_id` x `drive_time`
#'     are geometrically unioned (`sf::st_union()`). Different providers or
#'     different bands are never combined -- only fragments of what is
#'     already the same provider-band. This is a real fix, not cosmetic: a
#'     provider isochrone split across multiple polygon parts (common
#'     Valhalla output for a band that crosses a coastline or state boundary)
#'     previously left duplicate `coord_id` x `drive_time` rows in the
#'     output, double-counting that provider in any point-in-polygon overlay
#'     ([build_access_membership()]) that assigns a tract to every
#'     containing row rather than every containing provider-band.
#' }
#'
#' @param artifacts_dir Directory holding the consolidated isochrone rds files.
#'   Defaults to [isochrone_source_dir()], falling back to the
#'   `ISOCHRONES_ARTIFACTS_DIR` env var if that directory doesn't exist.
#' @param bands Drive-time bands to load (minutes). Defaults to
#'   [ISOCHRONE_CANONICAL_BANDS].
#' @param verify_checksums When `TRUE` (default), verify the canonical run
#'   registry and SHA-256 checksums via [assert_canonical_isochrones()]
#'   before loading. `FALSE` skips this entirely (not just the checksum
#'   sub-check) -- use it for a directory that was never registered as a
#'   canonical run, e.g. the `ISOCHRONES_ARTIFACTS_DIR` ad hoc fallback.
#' @return An `sf` object with `coord_id`, `drive_time` (band), and polygon
#'   geometry (rows stacked across bands, unique on `coord_id` x `drive_time`).
#' @family spatial access data
#' @concept geography
#' @export
load_provider_isochrones <- function(artifacts_dir = isochrone_source_dir(),
                                     bands = ISOCHRONE_CANONICAL_BANDS,
                                     verify_checksums = TRUE) {
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("load_provider_isochrones() needs the 'sf' package (Suggests).", call. = FALSE)
  }
  if (is.na(artifacts_dir) || !nzchar(artifacts_dir) || !dir.exists(artifacts_dir)) {
    artifacts_dir <- Sys.getenv("ISOCHRONES_ARTIFACTS_DIR", "")
  }
  if (!nzchar(artifacts_dir) || !dir.exists(artifacts_dir)) {
    stop(sprintf(paste0("Provider isochrone artifacts not found (dir = '%s'). These are large ",
                        "external files; set ISOCHRONES_ARTIFACTS_DIR or config/paths.local.yml."), artifacts_dir),
         call. = FALSE)
  }

  # verify_checksums = FALSE skips verification entirely (registry AND
  # checksums), not just the checksum sub-step: assert_canonical_isochrones()
  # checks the run registry unconditionally, which would otherwise warn/error
  # on every directory that was never registered as a canonical run --
  # including the ISOCHRONES_ARTIFACTS_DIR ad hoc fallback this function has
  # always supported.
  if (isTRUE(verify_checksums)) {
    assert_canonical_isochrones(dir = artifacts_dir, verify_checksums = TRUE)
  }

  # Option A: Single consolidated file (provider_isochrones.rds)
  single_file <- file.path(artifacts_dir, "provider_isochrones.rds")
  if (file.exists(single_file)) {
    df <- readRDS(single_file)
    dt <- if ("drive_time" %in% names(df)) df$drive_time else if ("drive_time_minutes" %in% names(df)) df$drive_time_minutes else df$isochrone_minutes
    cid <- if ("coord_id" %in% names(df)) df$coord_id else df$npi

    df$drive_time <- as.integer(dt)
    df$coord_id <- as.character(cid)

    keep <- !is.na(df$drive_time) & df$drive_time %in% bands
    combined_sf <- df[keep, , drop = FALSE]
  } else {
    # Option B: Multi-file band structure (isochrones_{band}min_consolidated.rds)
    pieces <- lapply(bands, function(b) {
      f <- file.path(artifacts_dir, ISOCHRONE_BAND_FILE(b))
      if (!file.exists(f)) {
        stop(sprintf("Missing isochrone artifact for %d-min band: %s", b, f), call. = FALSE)
      }
      g <- readRDS(f)
      g$drive_time <- as.integer(b)
      g$coord_id <- as.character(if ("coord_id" %in% names(g)) g$coord_id else g$npi)
      g
    })
    combined_sf <- do.call(rbind, pieces)
  }

  # Union multiple polygon fragments for the SAME provider-band (never across
  # different providers or bands). Keeps whatever non-geometry columns the
  # source artifact carries -- takes the first fragment's attribute row and
  # replaces only its geometry with the union of all fragments in the group.
  dup_key <- paste(combined_sf$coord_id, combined_sf$drive_time, sep = "\r")
  dup <- duplicated(dup_key) | duplicated(dup_key, fromLast = TRUE)
  if (any(dup)) {
    dup_groups <- split(which(dup), dup_key[dup])
    unioned_rows <- lapply(dup_groups, function(idx) {
      row1 <- combined_sf[idx[1L], , drop = FALSE]
      sf::st_geometry(row1) <- sf::st_union(sf::st_geometry(combined_sf[idx, , drop = FALSE]))
      row1
    })
    combined_sf <- rbind(combined_sf[!dup, , drop = FALSE], do.call(rbind, unioned_rows))
  }

  combined_sf
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
#' @family spatial access data
#' @concept geography
#' @export
build_access_membership <- function(iso_sf, tracts,
                                    provider_col = "coord_id",
                                    band_col = "drive_time",
                                    crs = 4326) {
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("build_access_membership() needs the 'sf' package (Suggests).", call. = FALSE)
  }
  assertthat::assert_that(all(c(provider_col, band_col) %in% names(iso_sf)))

  old_s2 <- sf::sf_use_s2()
  on.exit(sf::sf_use_s2(old_s2), add = TRUE)
  sf::sf_use_s2(FALSE)

  pts <- if (inherits(tracts, "sf")) {
    assertthat::assert_that("demand_id" %in% names(tracts))
    tracts
  } else {
    assertthat::assert_that(all(c("demand_id", "lon", "lat") %in% names(tracts)))
    sf::st_as_sf(as.data.frame(tracts), coords = c("lon", "lat"), crs = crs)
  }
  # Align CRS, repair invalid geometries, and assign each point the attributes of any polygon containing it.
  iso_al <- sf::st_transform(iso_sf, sf::st_crs(pts))

  iso_valid <- tryCatch(sf::st_make_valid(iso_al), error = function(e) iso_al)
  joined <- sf::st_join(pts, iso_valid[c(provider_col, band_col)], join = sf::st_within)



  out <- sf::st_drop_geometry(joined)
  out <- out[!is.na(out[[provider_col]]), , drop = FALSE]
  tibble::tibble(
    demand_id = out$demand_id,
    provider_id = as.character(out[[provider_col]]),
    band = as.integer(out[[band_col]])
  )
}

#' Load pre-computed E2SFCA spatial access scores from twostep repository
#'
#' Reads pre-calculated 83,492 Census tract E2SFCA access scores and racial/ethnic
#' disparity metrics from `mufflyt/twostep` without requiring spatial point-in-polygon overlay recomputation.
#'
#' @param twostep_dir Directory path to twostep repository or artifacts folder.
#' @return A tibble with `demand_id` (GEOID), `access_score`, and demographic breakdown.
#' @family spatial access data
#' @concept geography
#' @export
load_precomputed_twostep_access <- function(
    twostep_dir = file.path(getwd(), "..", "twostep")) {
  candidate_paths <- c(
    file.path(twostep_dir, "artifacts", "2sfca", "spatial_outcomes", "spatial_outcomes_2020.csv"),
    file.path(Sys.getenv("HOME"), "twostep", "artifacts", "2sfca", "spatial_outcomes", "spatial_outcomes_2020.csv"),
    file.path(twostep_dir, "data", "step_4_access_by_group.csv"),
    file.path(Sys.getenv("HOME"), "twostep", "data", "step_4_access_by_group.csv")
  )
  found <- candidate_paths[file.exists(candidate_paths)]
  if (length(found) == 0L) {
    stop("load_precomputed_twostep_access(): twostep pre-computed access files not found.", call. = FALSE)
  }
  readr::read_csv(found[1], show_col_types = FALSE)
}


