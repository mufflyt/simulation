# Geographic Access: E2SFCA + Isochrone Matching ----
#
# Gives the workforce microsimulation a SPATIAL dimension: not just "how many
# FPMRS providers exist nationally" but "how reachable are they for the women who
# need them", recomputed for each simulated workforce state.
#
# Ported approaches:
#   * twostep (R/desjardins7_e2sfca.R): base-R Enhanced Two-Step Floating
#     Catchment Area (E2SFCA, Luo & Qi 2009) with cumulative drive-time bands,
#     the diff(W^power) incremental-weight identity (power=2 => M2SFCA,
#     Delamater 2013), zero-demand -> NA (never 0) semantics, the Spatial Access
#     Ratio (SPAR, Wan 2012), and zero-access-share / access-quintile KPIs.
#     This is a pure-base-R engine (no sf/terra/dplyr) so it drops into a
#     microsim inner loop -- only `supply` changes each simulated year, the
#     year-agnostic band geometry is fixed.
#   * isochrones (R/match_points_to_isochrones.R): match providers to isochrone
#     origins by a 5 km HAVERSINE nearest-neighbour, NEVER exact coordinate
#     equality (cluster jitter otherwise fabricates coverage gaps), and CONUS-only
#     scoping (road-network travel model).

# ---- Constants ------------------------------------------------------------

# Cumulative drive-time distance-decay weights (Luo & Qi 2009 step weights).
E2SFCA_DEFAULT_WEIGHTS <- c("30" = 1.00, "60" = 0.68, "120" = 0.22, "180" = 0.09)
E2SFCA_BANDS <- c(30L, 60L, 120L, 180L)

ISOCHRONE_MATCH_KM <- 5.0             # isochrones ISOCHRONE_MATCH_M = 5000
EARTH_RADIUS_KM <- 6371.0088

# CONUS bounding box (48 states + DC), matching isochrones geographic scope.
CONUS_LAT_RANGE <- c(24.0, 49.5)
CONUS_LON_RANGE <- c(-125.0, -66.5)

# ---- Distance-decay weights -----------------------------------------------

#' Validate and order cumulative band weights
#' @param weights Named numeric vector keyed by drive-time minutes.
#' @return The weights sorted by ascending band.
#' @export
e2sfca_band_weights <- function(weights = E2SFCA_DEFAULT_WEIGHTS) {
  assertthat::assert_that(is.numeric(weights), length(weights) >= 1)
  ord <- order(as.numeric(names(weights)))
  w <- weights[ord]
  if (is.unsorted(rev(w))) {
    logger::log_warn("E2SFCA weights are not monotonically non-increasing by band")
  }
  w
}

#' Incremental band weights from cumulative weights: diff(W^power)
#'
#' The crux of running E2SFCA on NESTED/cumulative isochrones: convert cumulative
#' band weights W_b into incremental weights so summing over nested-isochrone
#' populations telescopes correctly. `step2_power = 2` gives M2SFCA (square the
#' cumulative weights THEN difference -- NOT diff(W)^2).
#'
#' @param weights Cumulative band weights ([e2sfca_band_weights()]).
#' @param step2_power 1 for E2SFCA (default), 2 for M2SFCA.
#' @return Named numeric vector of incremental weights (outermost = W_last^power).
#' @export
e2sfca_incremental_weights <- function(weights = E2SFCA_DEFAULT_WEIGHTS, step2_power = 1) {
  w <- e2sfca_band_weights(weights)
  wp <- w^step2_power
  n <- length(wp)
  incr <- numeric(n)
  incr[seq_len(n - 1)] <- wp[seq_len(n - 1)] - wp[2:n]
  incr[n] <- wp[n]
  names(incr) <- names(w)
  incr
}

# ---- Base-R E2SFCA engine (twostep dj7 port) ------------------------------

#' Compute E2SFCA accessibility (base R, no spatial dependencies)
#'
#' Two-step floating catchment over drive-time bands. Port of
#' twostep::dj7_tract_access.
#'
#'   Step 1 (provider ratio R_j): R_j = S_j / sum_b w'_b * Pop_b(j)
#'   Step 2 (accessibility A_i):  A_i = sum_{j reaching i} W_b(i,j) * R_j
#'
#' Zero-demand semantics are preserved exactly: a provider whose weighted demand
#' is 0 yields ratio = NA (undefined, not 0) and contributes no access; its
#' supply is booked into the audit block (never silently treated as zero
#' capacity).
#'
#' @param membership Long tibble: `demand_id`, `provider_id`, `band` (minutes),
#'   one row per (demand unit reachable from provider within that band).
#' @param supply Tibble: `provider_id`, `supply` (S_j, e.g. provider count/FTE).
#' @param demand Tibble: `demand_id`, `population` (P_i, the demand denominator).
#' @param weights Cumulative band weights.
#' @param step2_power 1 = E2SFCA, 2 = M2SFCA.
#' @param per_capita_scale Scale for reporting (e.g. 1e5 => access per 100k).
#' @return List: `access` (per demand unit), `provider_ratios`, `audit`, `meta`.
#' @export
compute_e2sfca_access <- function(membership, supply, demand,
                                  weights = E2SFCA_DEFAULT_WEIGHTS,
                                  step2_power = 1,
                                  per_capita_scale = 1e5) {
  assertthat::assert_that(all(c("demand_id", "provider_id", "band") %in% names(membership)))
  assertthat::assert_that(all(c("provider_id", "supply") %in% names(supply)))
  assertthat::assert_that(all(c("demand_id", "population") %in% names(demand)))

  w_cum <- e2sfca_band_weights(weights)
  w_incr <- e2sfca_incremental_weights(weights, step2_power)
  band_key <- as.character(membership$band)

  mem <- membership %>%
    dplyr::mutate(
      w_incr = unname(w_incr[band_key]),      # step-1 demand weight
      w_cum  = unname(w_cum[band_key])        # step-2 access weight
    ) %>%
    safe_left_join(demand, by = "demand_id", allow_fanout = TRUE)

  # --- Step 1: provider weighted demand and ratio R_j ---
  provider_demand <- mem %>%
    dplyr::group_by(.data$provider_id) %>%
    dplyr::summarise(weighted_demand = sum(.data$population * .data$w_incr, na.rm = TRUE),
                     .groups = "drop") %>%
    safe_left_join(supply, by = "provider_id") %>%
    dplyr::mutate(
      zero_demand = .data$weighted_demand <= 0,
      ratio = dplyr::if_else(.data$zero_demand, NA_real_, .data$supply / .data$weighted_demand),
      ratio_for_surface = dplyr::if_else(.data$zero_demand, 0, .data$ratio)
    )

  # --- Step 2: accessibility A_i ---
  access <- mem %>%
    safe_left_join(
      dplyr::select(provider_demand, "provider_id", "ratio_for_surface"),
      by = "provider_id", allow_fanout = TRUE
    ) %>%
    dplyr::group_by(.data$demand_id) %>%
    dplyr::summarise(
      access = sum(.data$w_cum * .data$ratio_for_surface, na.rm = TRUE),
      n_providers = dplyr::n_distinct(.data$provider_id),
      .groups = "drop"
    ) %>%
    dplyr::right_join(demand, by = "demand_id") %>%   # keep zero-access demand units
    dplyr::mutate(
      access = dplyr::coalesce(.data$access, 0),
      n_providers = dplyr::coalesce(.data$n_providers, 0L),
      access_scaled = .data$access * per_capita_scale
    )

  audit <- list(
    n_zero_demand_origins = sum(provider_demand$zero_demand, na.rm = TRUE),
    share_supply_zero_demand = {
      tot <- sum(provider_demand$supply, na.rm = TRUE)
      if (tot > 0) sum(provider_demand$supply[provider_demand$zero_demand], na.rm = TRUE) / tot else 0
    }
  )

  list(
    access = access,
    provider_ratios = provider_demand,
    audit = audit,
    meta = list(
      method = if (step2_power == 2) "M2SFCA" else "E2SFCA",
      step2_power = step2_power,
      band_weights = w_cum,
      incremental_weights = w_incr
    )
  )
}

#' Population-weighted mean access (SPAR denominator) and zero-access share
#'
#' Port of twostep::dj7_no_access_share + e2sfca_cell_summaries. The
#' population-weighted mean is the SPAR denominator; the zero-access share is a
#' ready workforce-adequacy KPI ("% of women with no modeled FPMRS access").
#'
#' @param access Access tibble from [compute_e2sfca_access()].
#' @param thresholds Access thresholds for population-share reporting.
#' @return List: `mean_access`, `zero_access_share`, `threshold_shares`.
#' @export
summarize_access <- function(access, thresholds = c(0, 1, 5, 10, 20, 50)) {
  w <- access$population
  a <- access$access_scaled
  tot_pop <- sum(w, na.rm = TRUE)

  mean_access <- if (tot_pop > 0) sum(a * w, na.rm = TRUE) / tot_pop else NA_real_
  zero_share <- if (tot_pop > 0) sum(w[a <= 0], na.rm = TRUE) / tot_pop else NA_real_

  threshold_shares <- tibble::tibble(
    threshold = thresholds,
    pop_share_at_or_above = vapply(thresholds, function(t) {
      if (tot_pop > 0) sum(w[a >= t], na.rm = TRUE) / tot_pop else NA_real_
    }, numeric(1))
  )

  list(mean_access = mean_access, zero_access_share = zero_share,
       threshold_shares = threshold_shares)
}

#' Spatial Access Ratio (SPAR): access relative to the national mean (=1.0)
#'
#' Port of the Wan 2012 SPAR. Makes surfaces from different scenarios /
#' subspecialties directly comparable on a fixed national-mean = 1.0 scale.
#'
#' @param access Access tibble ([compute_e2sfca_access()]).
#' @return The access tibble with a `relative_access` column added.
#' @export
spatial_access_ratio <- function(access) {
  s <- summarize_access(access)
  denom <- s$mean_access
  access %>%
    dplyr::mutate(relative_access = if (!is.na(denom) && denom > 0) .data$access_scaled / denom else NA_real_)
}

#' Assign access categories: 1 = zero-access, 2-5 = quartiles of positive access
#'
#' Port of twostep::dj7_assign_quintile.
#'
#' @param access_scaled Numeric scaled access values.
#' @return Integer category 1-5.
#' @export
assign_access_category <- function(access_scaled) {
  cat <- rep(1L, length(access_scaled))
  positive <- access_scaled > 0 & !is.na(access_scaled)
  if (any(positive)) {
    brks <- stats::quantile(access_scaled[positive], probs = c(0, .25, .5, .75, 1), na.rm = TRUE)
    q <- cut(access_scaled[positive], breaks = brks, labels = FALSE,
             include.lowest = TRUE)
    cat[positive] <- as.integer(q) + 1L
  }
  cat
}

# ---- Isochrone / haversine provider matching ------------------------------

#' Great-circle (haversine) distance in kilometres
#' @param lat1,lon1,lat2,lon2 Coordinates in decimal degrees (vectorised).
#' @return Distance(s) in km.
#' @export
haversine_km <- function(lat1, lon1, lat2, lon2) {
  to_rad <- pi / 180
  dlat <- (lat2 - lat1) * to_rad
  dlon <- (lon2 - lon1) * to_rad
  a <- sin(dlat / 2)^2 + cos(lat1 * to_rad) * cos(lat2 * to_rad) * sin(dlon / 2)^2
  2 * EARTH_RADIUS_KM * asin(pmin(1, sqrt(a)))
}

#' Is a coordinate inside the CONUS bounding box?
#'
#' Port of twostep::dj7_conus_ok. Study scope is CONUS only (Valhalla models
#' road-based travel; AK/HI/PR/territories are out of scope).
#'
#' @param lat,lon Decimal-degree coordinates.
#' @return Logical vector.
#' @export
conus_ok <- function(lat, lon) {
  !is.na(lat) & !is.na(lon) &
    lat >= CONUS_LAT_RANGE[1] & lat <= CONUS_LAT_RANGE[2] &
    lon >= CONUS_LON_RANGE[1] & lon <= CONUS_LON_RANGE[2]
}

#' Match provider points to isochrone origins by 5 km haversine nearest-neighbour
#'
#' Port of the isochrones canonical matcher's contract: NEVER exact-coordinate
#' equality. Providers cluster (5 km DBSCAN radius) and Step-3 matches at
#' ISOCHRONE_MATCH_M = 5000 m, so a cohort centroid 200 m from an existing origin
#' is the SAME physical cluster -- exact-key matching fabricates phantom coverage
#' gaps. Always match on the distance threshold.
#'
#' @param points Tibble with `id`, `lat`, `lon` (providers to place).
#' @param iso_centers Tibble with `coord_id`, `lat`, `lon` (isochrone origins).
#' @param threshold_km Match radius (default 5 km).
#' @return `points` with `matched_coord_id`, `match_km`, `matched` columns.
#'   Points outside CONUS or beyond the threshold are unmatched (matched = FALSE).
#' @export
match_points_to_isochrones <- function(points, iso_centers, threshold_km = ISOCHRONE_MATCH_KM) {
  assertthat::assert_that(all(c("id", "lat", "lon") %in% names(points)))
  assertthat::assert_that(all(c("coord_id", "lat", "lon") %in% names(iso_centers)))

  in_conus <- conus_ok(points$lat, points$lon)

  nearest <- lapply(seq_len(nrow(points)), function(i) {
    if (!in_conus[i]) return(c(idx = NA_real_, km = NA_real_))
    d <- haversine_km(points$lat[i], points$lon[i], iso_centers$lat, iso_centers$lon)
    j <- which.min(d)
    c(idx = j, km = d[j])
  })
  nearest <- do.call(rbind, nearest)

  points %>%
    dplyr::mutate(
      match_km = nearest[, "km"],
      matched = !is.na(nearest[, "km"]) & nearest[, "km"] <= threshold_km,
      matched_coord_id = dplyr::if_else(
        .data$matched, iso_centers$coord_id[nearest[, "idx"]], NA_character_
      )
    )
}
