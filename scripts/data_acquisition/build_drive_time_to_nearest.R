#!/usr/bin/env Rscript
# Point-to-point drive time to the NEAREST urogynecologist, per demand tract.
#
#   Rscript scripts/data_acquisition/build_drive_time_to_nearest.R
#   Rscript scripts/data_acquisition/build_drive_time_to_nearest.R --self-test
#
# WHAT THIS PRODUCES. A continuous drive-time-to-nearest artifact
#   (demand_id, drive_minutes_to_nearest, nearest_provider_id, n_candidates)
# that the access-surface seam carries downstream (cliff Module D reports it as
# the drive-time analog of the retired straight-line miles_to_nearest). Unlike the
# isochrone BANDS (30/60/120/180), this is continuous minutes.
#
# WHERE IT RUNS. On the Valhalla host, exactly like generate_isochrones_standalone.R
# (see cliff/scripts/RUNBOOK_abu_urps_pipeline.md, "EC2 top-up"): a Valhalla Docker
# container is served against the s3://tyler-valhalla-tiles tiles + serve_config.json,
# and this script POSTs to its /sources_to_targets matrix endpoint. It CANNOT run
# without a reachable Valhalla; there is no offline fallback (a made-up drive time
# is worse than none). SIMULATION_VALHALLA_URL sets the endpoint (default
# http://localhost:8002).
#
# WHY IT IS BOUNDED. A full 72k-tract x ~1,300-provider matrix is ~96M pairs. The
# isochrone MEMBERSHIP table (demand_id, provider_id, band) already lists, per
# tract, the providers reachable within 180 min. We compute exact drive times only
# to those candidates and take the min -- correct (a nearer provider cannot be
# outside the 180-min set) and ~2-3 orders of magnitude cheaper. A tract with no
# candidate is >180 min from every provider -> drive_minutes_to_nearest = NA.
#
# INPUTS (paths via env, with repo defaults):
#   ORIGINS_CSV     demand_id, lon, lat        [data-raw/spatial/tract_fem65_centroids.csv]
#   PROVIDERS_CSV   provider_id, lon, lat      [data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv]
#   MEMBERSHIP_RDS  demand_id, provider_id, band  [data-raw/spatial/provider_isochrone_membership.rds]
#   OUT_CSV         output                     [data-raw/spatial/tract_drive_time_to_nearest.csv]
# Resumable: existing OUT_CSV rows are kept and their demand_ids skipped.

`%||%` <- function(a, b) if (is.null(a) || !nzchar(a)) b else a

## ── pure core (unit-tested; no network) ──────────────────────────────────────

# Reduce a vector of candidate travel times (seconds) to the nearest provider.
.nearest_from_times <- function(times_sec, provider_ids) {
  n <- length(provider_ids)
  ok <- is.finite(times_sec)
  if (!any(ok)) {
    return(list(drive_minutes_to_nearest = NA_real_,
                nearest_provider_id = NA, n_candidates = n))
  }
  ids <- provider_ids[ok]; t <- times_sec[ok]
  i <- which.min(t)
  list(drive_minutes_to_nearest = round(t[i] / 60, 2),
       nearest_provider_id = ids[i], n_candidates = n)
}

# Candidate providers per origin, from the isochrone membership (reachable <=180
# min). Returns a named list: demand_id -> character vector of provider_ids.
.candidates_by_origin <- function(membership) {
  miss <- setdiff(c("demand_id", "provider_id"), names(membership))
  if (length(miss))
    stop("membership needs columns: ", paste(miss, collapse = ", "), call. = FALSE)
  split(as.character(membership$provider_id),
        as.character(membership$demand_id))
}

## ── Valhalla matrix client (network; not unit-tested) ────────────────────────

# One origin -> many targets. Returns travel time (seconds) aligned to `targets`
# rows; NA where Valhalla returns no route.
.valhalla_sources_to_targets <- function(origin, targets, url, costing = "auto") {
  if (!requireNamespace("httr", quietly = TRUE))
    stop("httr is required to query Valhalla; install it on the routing host.",
         call. = FALSE)
  body <- list(
    sources = list(list(lat = origin[["lat"]], lon = origin[["lon"]])),
    targets = lapply(seq_len(nrow(targets)),
                     function(i) list(lat = targets$lat[i], lon = targets$lon[i])),
    costing = costing)
  resp <- httr::POST(paste0(url, "/sources_to_targets"),
                     body = body, encode = "json", httr::timeout(120))
  httr::stop_for_status(resp)
  parsed <- httr::content(resp, as = "parsed", simplifyVector = FALSE)
  row <- parsed$sources_to_targets[[1]]           # one row (our single source)
  vapply(row, function(x) if (is.null(x$time)) NA_real_ else as.numeric(x$time),
         numeric(1))
}

## ── driver ───────────────────────────────────────────────────────────────────

build_drive_time_to_nearest <- function(origins, providers, membership,
                                        url, out_csv,
                                        costing = "auto",
                                        valhalla_fn = .valhalla_sources_to_targets,
                                        resume = TRUE, log_every = 500L) {
  for (nm in c("demand_id", "lon", "lat"))
    if (!nm %in% names(origins)) stop("origins needs column ", nm, call. = FALSE)
  for (nm in c("provider_id", "lon", "lat"))
    if (!nm %in% names(providers)) stop("providers needs column ", nm, call. = FALSE)
  providers$provider_id <- as.character(providers$provider_id)
  prov_idx <- stats::setNames(seq_len(nrow(providers)), providers$provider_id)
  cand <- .candidates_by_origin(membership)

  done <- character(0)
  if (resume && file.exists(out_csv)) {
    done <- as.character(utils::read.csv(out_csv, stringsAsFactors = FALSE)$demand_id)
    message("Resuming: ", length(done), " origins already done in ", out_csv)
  }
  todo <- setdiff(as.character(origins$demand_id), done)
  message("Origins to route: ", length(todo), " of ", nrow(origins))

  first_write <- !file.exists(out_csv)
  for (k in seq_along(todo)) {
    did <- todo[k]
    o <- origins[as.character(origins$demand_id) == did, ][1, ]
    ids <- cand[[did]]
    ids <- ids[!is.na(ids) & ids %in% names(prov_idx)]
    if (!length(ids)) {
      res <- list(drive_minutes_to_nearest = NA_real_, nearest_provider_id = NA,
                  n_candidates = 0L)
    } else {
      tgt <- providers[prov_idx[ids], c("lon", "lat"), drop = FALSE]
      times <- valhalla_fn(c(lon = o$lon, lat = o$lat), tgt, url, costing)
      res <- .nearest_from_times(times, ids)
    }
    row <- data.frame(demand_id = did,
                      drive_minutes_to_nearest = res$drive_minutes_to_nearest,
                      nearest_provider_id = res$nearest_provider_id,
                      n_candidates = res$n_candidates, stringsAsFactors = FALSE)
    utils::write.table(row, out_csv, sep = ",", row.names = FALSE,
                       col.names = first_write, append = !first_write)
    first_write <- FALSE
    if (k %% log_every == 0L) message("  routed ", k, "/", length(todo))
  }
  message("Wrote drive-time-to-nearest for ", length(todo), " origins to ", out_csv)
  invisible(out_csv)
}

## ── self-test (no Valhalla) ──────────────────────────────────────────────────

.self_test <- function() {
  # nearest is min time -> minutes; no candidates -> NA
  r1 <- .nearest_from_times(c(600, 300, 900), c("a", "b", "c"))
  stopifnot(r1$nearest_provider_id == "b", r1$drive_minutes_to_nearest == 5,
            r1$n_candidates == 3L)
  r2 <- .nearest_from_times(c(Inf, NA), c("a", "b"))
  stopifnot(is.na(r2$drive_minutes_to_nearest), r2$n_candidates == 2L)

  origins  <- data.frame(demand_id = c("t1", "t2", "t3"),
                         lon = c(-96, -97, -98), lat = c(38, 39, 40),
                         stringsAsFactors = FALSE)
  providers <- data.frame(provider_id = c("p1", "p2", "p3"),
                          lon = c(-96.1, -97.1, -98.1), lat = c(38, 39, 40),
                          stringsAsFactors = FALSE)
  membership <- data.frame(
    demand_id  = c("t1", "t1", "t2"),          # t3 has NO candidate -> NA
    provider_id = c("p1", "p2", "p3"),
    band = c(30L, 60L, 60L), stringsAsFactors = FALSE)
  # mock Valhalla: time = 1000s * (target index) so the FIRST candidate is nearest
  mock <- function(origin, targets, url, costing) seq_len(nrow(targets)) * 1000
  out <- tempfile(fileext = ".csv")
  build_drive_time_to_nearest(origins, providers, membership, url = "mock",
                              out_csv = out, valhalla_fn = mock, resume = FALSE)
  res <- utils::read.csv(out, stringsAsFactors = FALSE)
  res <- res[match(c("t1", "t2", "t3"), res$demand_id), ]
  stopifnot(
    res$nearest_provider_id[res$demand_id == "t1"] == "p1",   # first candidate, 1000s
    res$drive_minutes_to_nearest[res$demand_id == "t1"] == round(1000/60, 2),
    res$n_candidates[res$demand_id == "t1"] == 2L,
    res$drive_minutes_to_nearest[res$demand_id == "t2"] == round(1000/60, 2),
    is.na(res$drive_minutes_to_nearest[res$demand_id == "t3"]),
    res$n_candidates[res$demand_id == "t3"] == 0L)
  cat("build_drive_time_to_nearest self-test: OK\n")
  invisible(TRUE)
}

## ── main ─────────────────────────────────────────────────────────────────────

if (identical(environment(), globalenv()) && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if ("--self-test" %in% args) {
    .self_test()
  } else {
    url         <- Sys.getenv("SIMULATION_VALHALLA_URL", "http://localhost:8002")
    origins_csv <- Sys.getenv("ORIGINS_CSV", "data-raw/spatial/tract_fem65_centroids.csv")
    providers_csv <- Sys.getenv("PROVIDERS_CSV",
                                "data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv")
    membership_rds <- Sys.getenv("MEMBERSHIP_RDS",
                                 "data-raw/spatial/provider_isochrone_membership.rds")
    out_csv <- Sys.getenv("OUT_CSV", "data-raw/spatial/tract_drive_time_to_nearest.csv")
    for (p in c(origins_csv, providers_csv, membership_rds))
      if (!file.exists(p)) stop("required input not found: ", p, call. = FALSE)

    origins   <- utils::read.csv(origins_csv, stringsAsFactors = FALSE)
    providers <- utils::read.csv(providers_csv, stringsAsFactors = FALSE)
    membership <- as.data.frame(readRDS(membership_rds))
    message("Valhalla endpoint: ", url)
    build_drive_time_to_nearest(origins, providers, membership, url, out_csv)
  }
}
