#!/usr/bin/env Rscript
# Build the provider -> tract isochrone MEMBERSHIP table ----
#
#   Rscript scripts/data_acquisition/12_build_provider_isochrone_membership.R
#   Rscript scripts/data_acquisition/12_build_provider_isochrone_membership.R --self-test
#
# WHAT THIS PRODUCES, AND WHY IT IS THE IMPORT STEP.
# The E2SFCA access layer (R/geography-spatial_access_e2sfca.R::compute_access())
# consumes ONE input this repository does not yet carry: a long `membership`
# table -- (demand_id, provider_id, band) -- saying which provider is reachable
# from which demand tract within which drive-time band. geographic_access_status()
# lists "drive-time isochrones imported from mufflyt/isochrones" as the remaining
# blocker; this script is how that import lands as a compact, versioned artifact
# instead of the multi-hundred-MB polygon set.
#
# It overlays the demand-tract centroids this repo already ships
# (data-raw/spatial/tract_fem65_centroids.csv) on the provider isochrone polygons
# generated in mufflyt/isochrones (isochrones_{30,60,120,180}min_consolidated.rds),
# and records, for each (tract, provider), the SMALLEST band whose polygon
# contains the tract centroid. That is exactly the 2SFCA catchment relation.
#
# THE POLYGONS ARE NOT IN THIS REPO ON PURPOSE. They were expensive to generate
# (EC2 Valhalla) and are large; the ISOCHRONE_REGISTRY in mufflyt/isochrones
# keeps them on S3. So this script READS them from a path you point it at
# (--iso-dir) and WRITES only the derived membership table + a provenance
# sidecar. Run it where the .rds live (the isochrones checkout / an EC2 host),
# then commit the small output here.
#
# IT DOES NOT approximate. If a band file is missing it fails; it never
# substitutes state-centroid or nearest-origin geometry for a real polygon --
# that is the "plausible but meaningless access ratio" ordering trap that
# geographic_access_status() and validation_report()'s geographic gate exist to
# prevent. No polygons, no membership.
#
# INPUT SCHEMAS (auto-detected, case-insensitively, mirroring the isochrones
# repo's own normalisers):
#   isochrone sf  : a provider id column (provider_id | npi | coord_id) + polygon
#                   geometry; the band comes from the FILE name, not a column.
#   tract centroids: demand_id (GEOID) + lon + lat, WGS84.
# OUTPUT: data-raw/spatial/provider_isochrone_membership.rds (+ .provenance.json).

suppressWarnings(suppressMessages({
  ok <- requireNamespace("sf", quietly = TRUE) &&
        requireNamespace("dplyr", quietly = TRUE)
}))
if (!ok) stop("build_membership: 'sf' and 'dplyr' are required.", call. = FALSE)

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

# ---- Column auto-detection --------------------------------------------------

.pick_col <- function(nms, candidates) {
  hit <- which(tolower(nms) %in% tolower(candidates))
  if (length(hit)) nms[hit[1]] else NA_character_
}

.provider_id_col <- function(x) {
  col <- .pick_col(names(x), c("provider_id", "npi", "coord_id", "id"))
  if (is.na(col)) stop("build_membership: no provider id column ",
                       "(provider_id / npi / coord_id / id) in the isochrone layer.",
                       call. = FALSE)
  col
}

# ---- Core: pure, testable overlay -------------------------------------------

#' Build the (demand_id, provider_id, band) membership from per-band polygons.
#'
#' iso_by_band : named list, name = band in minutes, value = sf POLYGON/MULTIPOLYGON
#'               layer carrying a provider id column. One layer per band.
#' tract_pts   : sf POINT layer with a `demand_id` column (WGS84).
#' Returns a tibble (demand_id, provider_id, band) with the SMALLEST band kept
#' per (demand_id, provider_id).
build_isochrone_membership <- function(iso_by_band, tract_pts) {
  stopifnot(inherits(tract_pts, "sf"), "demand_id" %in% names(tract_pts))
  bands <- sort(as.integer(names(iso_by_band)))
  pieces <- list()
  for (b in bands) {
    iso <- iso_by_band[[as.character(b)]]
    if (is.null(iso) || nrow(iso) == 0L) next
    iso <- sf::st_transform(iso, sf::st_crs(tract_pts))
    idc <- .provider_id_col(iso)
    # st_intersects(points, polys): for each point, the polygons it falls in.
    # s2 makes this a correct spherical point-in-polygon, not a planar guess.
    hits <- suppressMessages(sf::st_intersects(tract_pts, iso))
    if (!length(hits)) next
    n_hit <- lengths(hits)
    if (!any(n_hit)) next
    tract_ix <- rep(seq_along(hits)[n_hit > 0], n_hit[n_hit > 0])
    poly_ix  <- unlist(hits[n_hit > 0])
    pieces[[as.character(b)]] <- data.frame(
      demand_id   = tract_pts$demand_id[tract_ix],
      provider_id = as.character(iso[[idc]][poly_ix]),
      band        = b,
      stringsAsFactors = FALSE
    )
  }
  if (!length(pieces))
    return(dplyr::tibble(demand_id = character(0), provider_id = character(0),
                         band = integer(0)))
  dplyr::as_tibble(do.call(rbind, pieces)) |>
    dplyr::filter(!is.na(.data$provider_id)) |>
    dplyr::group_by(.data$demand_id, .data$provider_id) |>
    dplyr::summarise(band = min(.data$band), .groups = "drop") |>
    dplyr::arrange(.data$band, .data$demand_id, .data$provider_id)
}

# ---- I/O helpers ------------------------------------------------------------

.read_iso_dir <- function(iso_dir, bands) {
  out <- list()
  for (b in bands) {
    f <- file.path(iso_dir, sprintf("isochrones_%dmin_consolidated.rds", b))
    if (!file.exists(f))
      stop(sprintf("build_membership: missing band file %s. All requested bands ",
                   "must be present -- no substitution.", f), call. = FALSE)
    layer <- readRDS(f)
    if (!inherits(layer, "sf"))
      stop(sprintf("build_membership: %s is not an sf layer.", f), call. = FALSE)
    out[[as.character(b)]] <- layer
  }
  out
}

.tract_points <- function(csv_path) {
  d <- utils::read.csv(csv_path, stringsAsFactors = FALSE)
  id  <- .pick_col(names(d), c("demand_id", "geoid"))
  lon <- .pick_col(names(d), c("lon", "longitude", "lng", "x"))
  lat <- .pick_col(names(d), c("lat", "latitude", "y"))
  if (anyNA(c(id, lon, lat)))
    stop("build_membership: tract centroids need demand_id/GEOID + lon + lat.",
         call. = FALSE)
  d$demand_id <- as.character(d[[id]])
  sf::st_as_sf(d, coords = c(lon, lat), crs = 4326, remove = FALSE)
}

.sha256 <- function(path) if (requireNamespace("digest", quietly = TRUE))
  digest::digest(file = path, algo = "sha256") else NA_character_

.iso_run_id <- function(iso_dir) {
  reg <- file.path(iso_dir, "ISOCHRONE_REGISTRY.json")
  if (!file.exists(reg) || !requireNamespace("jsonlite", quietly = TRUE))
    return(NA_character_)
  tryCatch(jsonlite::fromJSON(reg, simplifyVector = FALSE)$active_run_id %||% NA_character_,
           error = function(e) NA_character_)
}

# ---- Self-test: synthetic polygons, no external data ------------------------
# Two providers at (0,0) and (10,10). Nested square isochrones: the 30-min ring
# is small, the 60-min ring larger. Tracts placed so the assertions pin the
# smallest-band rule and containment. Proves the overlay before it ever meets
# the real (unreachable-here) polygons.
.self_test <- function() {
  sq <- function(cx, cy, r, id) {
    ring <- matrix(c(cx - r, cy - r, cx + r, cy - r, cx + r, cy + r,
                     cx - r, cy + r, cx - r, cy - r), ncol = 2, byrow = TRUE)
    sf::st_sf(provider_id = id,
              geometry = sf::st_sfc(sf::st_polygon(list(ring)), crs = 4326))
  }
  iso <- list(
    "30" = rbind(sq(0, 0, 1, "A"), sq(10, 10, 1, "B")),
    "60" = rbind(sq(0, 0, 3, "A"), sq(10, 10, 3, "B"))
  )
  tracts <- sf::st_as_sf(
    data.frame(demand_id = c("t_in30", "t_in60", "t_out", "t_B"),
               lon = c(0.5,  2.5,  50, 10.2),
               lat = c(0.5,  2.5,  50, 10.2)),
    coords = c("lon", "lat"), crs = 4326, remove = FALSE)

  m <- build_isochrone_membership(iso, tracts)
  band_of <- function(d, p) { r <- m$band[m$demand_id == d & m$provider_id == p]; if (length(r)) r else NA_integer_ }

  stopifnot(
    "point inside the 30-min ring is band 30"        = band_of("t_in30", "A") == 30L,
    "point in 60-min ring but not 30 is band 60"     = band_of("t_in60", "A") == 60L,
    "smallest band is kept, not the larger one"      = band_of("t_in30", "A") == 30L,
    "point outside every ring has no membership row" = nrow(m[m$demand_id == "t_out", ]) == 0L,
    "provider B is matched independently"            = band_of("t_B", "B") == 30L,
    "no cross-provider leakage (t_in30 not near B)"  = is.na(band_of("t_in30", "B")),
    "columns are exactly the contract"               = identical(names(m), c("demand_id", "provider_id", "band"))
  )
  cat("SELF-TEST OK: membership overlay reproduces the smallest-band catchment rule.\n")
  invisible(TRUE)
}

# ---- Main -------------------------------------------------------------------
# Guarded so the functions above can be sourced for testing without running the
# import: `.MEMBERSHIP_SOURCE_ONLY <- TRUE; source(this_file)`.
if (!exists(".MEMBERSHIP_SOURCE_ONLY")) {

args <- commandArgs(trailingOnly = TRUE)
opt <- function(flag, default) {
  i <- which(args == flag); if (length(i) && i < length(args)) args[i + 1L] else default
}

if ("--self-test" %in% args) { .self_test(); quit(status = 0) }

ISO_DIR  <- opt("--iso-dir", "../isochrones/artifacts/isochrones")
TRACTS   <- opt("--tracts", "data-raw/spatial/tract_fem65_centroids.csv")
OUT      <- opt("--out", "data-raw/spatial/provider_isochrone_membership.rds")
BANDS    <- as.integer(strsplit(opt("--bands", "30,60,120,180"), ",")[[1]])

message(sprintf("Reading %d band(s) from %s", length(BANDS), ISO_DIR))
iso_by_band <- .read_iso_dir(ISO_DIR, BANDS)
message(sprintf("Reading tract centroids from %s", TRACTS))
tract_pts <- .tract_points(TRACTS)

message(sprintf("Overlaying %s tracts on the provider isochrones...",
                format(nrow(tract_pts), big.mark = ",")))
membership <- build_isochrone_membership(iso_by_band, tract_pts)

dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
tmp <- paste0(OUT, ".tmp"); saveRDS(membership, tmp); file.rename(tmp, OUT)

prov <- list(
  output              = basename(OUT),
  n_membership_rows   = nrow(membership),
  n_tracts_covered    = length(unique(membership$demand_id)),
  n_providers_reached = length(unique(membership$provider_id)),
  bands               = BANDS,
  isochrone_run_id    = .iso_run_id(ISO_DIR),
  iso_band_sha256     = stats::setNames(
    vapply(BANDS, function(b) .sha256(file.path(
      ISO_DIR, sprintf("isochrones_%dmin_consolidated.rds", b))), ""),
    as.character(BANDS)),
  tracts_sha256       = .sha256(TRACTS),
  output_sha256       = .sha256(OUT),
  created_at          = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
  producer            = "scripts/data_acquisition/12_build_provider_isochrone_membership.R"
)
if (requireNamespace("jsonlite", quietly = TRUE)) {
  writeLines(jsonlite::toJSON(prov, auto_unbox = TRUE, pretty = TRUE),
             paste0(OUT, ".provenance.json"))
}
message(sprintf("Wrote %s: %s rows, %s tracts, %s providers.",
                OUT, format(nrow(membership), big.mark = ","),
                format(prov$n_tracts_covered, big.mark = ","),
                format(prov$n_providers_reached, big.mark = ",")))

}
