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
#     Ratio (SPAR, Wan 2012), and zero-access-share / access-category KPIs.
#     No spatial dependencies (no sf/terra/GEOS) so it drops into a microsim
#     inner loop -- only `supply` changes each simulated year, the year-agnostic
#     band geometry is fixed. Table manipulation uses dplyr, as elsewhere in the
#     repo.
#   * isochrones (R/match_points_to_isochrones.R): match providers to isochrone
#     origins by a 5 km HAVERSINE nearest-neighbour, NEVER exact coordinate
#     equality (cluster jitter otherwise fabricates coverage gaps), and CONUS-only
#     scoping (road-network travel model).

# ---- Constants ------------------------------------------------------------

# Cumulative drive-time distance-decay weights (Luo & Qi 2009 step weights).
E2SFCA_DEFAULT_WEIGHTS <- c("30" = 1.00, "60" = 0.68, "120" = 0.22, "180" = 0.09)

#' Drive-time bands used by the access layer
#'
#' Sourced from `mufflyaccess::get_canonical_bands()` so this layer cannot
#' diverge from `twostep` and `isochrones`. A test asserts the shipped weights
#' still key to the canonical bands.
#' @return Integer vector of bands in minutes.
#' @export
e2sfca_bands <- function() ssot_access_bands()

# Retained for backward compatibility; prefer e2sfca_bands().
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
#' @keywords internal
e2sfca_band_weights <- function(weights = E2SFCA_DEFAULT_WEIGHTS) {
  assertthat::assert_that(is.numeric(weights), length(weights) >= 1)
  ord <- order(as.numeric(names(weights)))
  w <- weights[ord]
  if (is.unsorted(rev(w))) {
    .msg_warn("E2SFCA weights are not monotonically non-increasing by band")
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
#'   Step 1 (provider ratio R_j): `R_j = S_j / sum_b w'_b * Pop_b(j)`
#'   Step 2 (accessibility A_i):  `A_i = sum over j reaching i of W_b(i,j) * R_j`
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
#' Port of twostep::dj7_assign_quintile. Note the five categories are a
#' zero-access class plus QUARTILES of the positive-access distribution, not
#' quintiles of the whole distribution.
#'
#' @param access_scaled Numeric scaled access values.
#' @return Integer category 1-5.
#' @export
assign_access_category <- function(access_scaled) {
  cat <- rep(1L, length(access_scaled))
  positive <- access_scaled > 0 & !is.na(access_scaled)
  if (any(positive)) {
    brks <- unique(stats::quantile(access_scaled[positive],
                                   probs = c(0, .25, .5, .75, 1), na.rm = TRUE))
    if (length(brks) < 2L) {
      # Degenerate surface (all positive access values equal): quantile breaks
      # collapse and cut() would error; assign the middle band instead.
      cat[positive] <- 3L
    } else {
      q <- cut(access_scaled[positive], breaks = brks, labels = FALSE,
               include.lowest = TRUE)
      cat[positive] <- as.integer(q) + 1L
    }
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

  if (nrow(points) == 0) {
    return(dplyr::mutate(points, match_km = numeric(0), matched = logical(0),
                         matched_coord_id = character(0)))
  }
  if (nrow(iso_centers) == 0) {
    # No isochrone origins to match against: every point is unmatched rather
    # than a which.min(numeric(0)) that drops the idx column and corrupts the bind.
    return(dplyr::mutate(points, match_km = NA_real_, matched = FALSE,
                         matched_coord_id = NA_character_))
  }

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

# ---- M2SFCA toggle (convenience over compute_e2sfca_access) ----------------

#' Compute an accessibility surface by named method (E2SFCA or M2SFCA)
#'
#' Thin wrapper over [compute_e2sfca_access()] exposing the maldistribution
#' penalty as a first-class toggle rather than a numeric power. M2SFCA
#' (Delamater 2013) squares the CUMULATIVE band weights before differencing
#' (`step2_power = 2`), penalising supply that is present but poorly located.
#'
#' @param membership,supply,demand,weights,per_capita_scale Passed to
#'   [compute_e2sfca_access()].
#' @param method "E2SFCA" (default) or "M2SFCA".
#' @return The [compute_e2sfca_access()] result for the chosen method.
#' @export
compute_access <- function(membership, supply, demand,
                           method = c("E2SFCA", "M2SFCA"),
                           weights = E2SFCA_DEFAULT_WEIGHTS,
                           per_capita_scale = 1e5) {
  method <- match.arg(method)
  power <- if (identical(method, "M2SFCA")) 2 else 1
  compute_e2sfca_access(membership, supply, demand, weights = weights,
                        step2_power = power, per_capita_scale = per_capita_scale)
}

#' Compare E2SFCA and M2SFCA national access (with the monotonicity guard)
#'
#' Runs both methods on the same inputs and returns their population-weighted
#' national mean access. Enforces the twostep numerical contract that the M2SFCA
#' mean cannot exceed the E2SFCA mean (the maldistribution penalty only removes
#' access), so a violation signals a weighting bug rather than a finding.
#'
#' @inheritParams compute_access
#' @return List: `e2sfca` / `m2sfca` (full results), `mean_e2sfca` /
#'   `mean_m2sfca` (national pop-weighted means), `penalty_share`.
#' @export
compare_access_methods <- function(membership, supply, demand,
                                   weights = E2SFCA_DEFAULT_WEIGHTS,
                                   per_capita_scale = 1e5) {
  e2 <- compute_access(membership, supply, demand, "E2SFCA", weights, per_capita_scale)
  m2 <- compute_access(membership, supply, demand, "M2SFCA", weights, per_capita_scale)
  mean_e2 <- summarize_access(e2$access)$mean_access
  mean_m2 <- summarize_access(m2$access)$mean_access

  if (is.finite(mean_e2) && is.finite(mean_m2) && mean_m2 > mean_e2 + 1e-9) {
    stop(sprintf("M2SFCA mean (%.4f) exceeds E2SFCA mean (%.4f); weighting bug",
                 mean_m2, mean_e2), call. = FALSE)
  }

  list(
    e2sfca = e2, m2sfca = m2,
    mean_e2sfca = mean_e2, mean_m2sfca = mean_m2,
    penalty_share = if (is.finite(mean_e2) && mean_e2 > 0) 1 - mean_m2 / mean_e2 else NA_real_
  )
}

# ---- Access-disparity inference (twostep accessibility_stratification.R) ----

#' Population-weighted mean over ALL elements (sparse-group safe)
#'
#' Sums over every element rather than filtering `w > 0`, so a sparse group's
#' point estimate stays inside its own Monte-Carlo interval (twostep documents
#' that filtering pushes sparse-group estimates outside their CIs).
#'
#' @param a Numeric values.
#' @param w Numeric weights (same length as `a`).
#' @return Weighted mean, or NA if the weights sum to 0 / are non-finite.
#' @export
weighted_mean_all <- function(a, w) {
  stopifnot(length(a) == length(w))
  sw <- sum(w)
  if (!is.finite(sw) || sw == 0) return(NA_real_)
  sum(a * w) / sw
}

#' Zero-access share: percent of weighted population with access EXACTLY 0
#'
#' The workforce-adequacy KPI "share of women with no modelled access". Distinct
#' from the `zero_access_share` element of [summarize_access()], which counts
#' access <= 0 as a fraction; this is the twostep-faithful percent of == 0.
#'
#' @param access Numeric accessibility values.
#' @param w Numeric weights.
#' @return Percent in `[0, 100]` under non-negative weights, or NA.
#' @export
zero_access_share <- function(access, w) {
  stopifnot(length(access) == length(w))
  sw <- sum(w)
  if (!is.finite(sw) || sw == 0) return(NA_real_)
  100 * sum(w * (access == 0)) / sw
}

#' Monte-Carlo CI for a population-weighted access statistic (ACS MOE)
#'
#' Port of twostep::mc_weighted_ci. Propagates demographic (ACS) uncertainty by
#' redrawing the population weights ~ Normal(est, se) `B` times and taking the
#' quantiles of the resulting weighted statistic. This is ACS margin-of-error
#' propagation, NOT the microsimulation replicate-quantile band.
#'
#' Named `access_moe_ci()` rather than `mc_weighted_ci()` to avoid shadowing
#' `mufflyaccess::mc_weighted_ci()`, which the SSOT coverage report names as the
#' contract-owned variant of this same quantity. The caller's RNG stream is
#' saved and restored.
#'
#' @param access Numeric accessibility values.
#' @param est Numeric weight point estimates (e.g. ACS population).
#' @param se Numeric weight standard errors (ACS MOE / 1.645). NA -> 0.
#' @param stat "mean" ([weighted_mean_all()]) or "zero" ([zero_access_share()]).
#' @param B Monte-Carlo draws.
#' @param probs Interval quantiles.
#' @param seed RNG seed (restored on exit).
#' @return Named numeric `c(point, lo, hi)`.
#' @export
access_moe_ci <- function(access, est, se, stat = c("mean", "zero"),
                          B = 2000L, probs = c(0.025, 0.975), seed = 1L) {
  # Save/restore the caller's RNG stream FIRST, so the whole call is RNG-neutral.
  old <- if (exists(".Random.seed", envir = .GlobalEnv)) {
    get(".Random.seed", envir = .GlobalEnv)
  } else NULL
  on.exit(if (!is.null(old)) assign(".Random.seed", old, envir = .GlobalEnv))

  stat <- match.arg(stat)
  stopifnot(length(access) == length(est), length(est) == length(se))
  se[is.na(se)] <- 0
  f <- if (stat == "mean") weighted_mean_all else zero_access_share
  point <- f(access, est)

  set.seed(seed)
  n <- length(est)
  draws <- vapply(seq_len(B), function(b) f(access, stats::rnorm(n, est, se)), numeric(1))
  q <- stats::quantile(draws, probs, na.rm = TRUE)
  c(point = point, lo = unname(q[1]), hi = unname(q[2]))
}

#' OLS temporal trend of an annual series (95% t-interval on the slope)
#'
#' Port of twostep::annual_trend. Fits `value ~ year` and returns the slope with
#' its 95% t-interval and p-value; the access-per-year change over a study window.
#'
#' @param year Integer years.
#' @param value Numeric annual estimates.
#' @return Named numeric `c(slope, lo, hi, p)` (all NA if < 3 complete points).
#' @examples
#' annual_trend(2015:2020, c(100, 104, 110, 113, 119, 126))
#' @export
annual_trend <- function(year, value) {
  d <- data.frame(year = as.numeric(year), value = as.numeric(value))
  d <- d[stats::complete.cases(d), ]
  if (nrow(d) < 3) return(c(slope = NA_real_, lo = NA_real_, hi = NA_real_, p = NA_real_))
  m <- stats::lm(value ~ year, d)
  s <- summary(m)$coefficients["year", ]
  ci <- stats::confint(m)["year", ]
  c(slope = unname(s[["Estimate"]]), lo = unname(ci[1]), hi = unname(ci[2]),
    p = unname(s[["Pr(>|t|)"]]))
}

#' Wilson score confidence interval for a binomial proportion
#'
#' Port of cliff::calculate_proportion_ci. The Wilson interval is well-behaved
#' for small samples and near 0/1 (unlike the Wald interval), so it is the right
#' choice for rural/subgroup access-desert proportions and validation rates.
#'
#' @param successes Integer count(s) of successes.
#' @param n Integer sample size(s).
#' @param conf_level Confidence level (default 0.95).
#' @return Tibble `estimate`, `lo`, `hi` (one row per input element).
#' @examples
#' wilson_ci(successes = 42, n = 100)
#' @export
wilson_ci <- function(successes, n, conf_level = 0.95) {
  stopifnot(length(successes) == length(n), all(successes <= n, na.rm = TRUE))
  z <- stats::qnorm(1 - (1 - conf_level) / 2)
  phat <- ifelse(n > 0, successes / n, NA_real_)
  denom <- 1 + z^2 / n
  center <- (phat + z^2 / (2 * n)) / denom
  half <- z * sqrt(phat * (1 - phat) / n + z^2 / (4 * n^2)) / denom
  tibble::tibble(
    estimate = phat,
    lo = ifelse(n > 0, pmax(0, center - half), NA_real_),
    hi = ifelse(n > 0, pmin(1, center + half), NA_real_)
  )
}

# ---- E2SFCA distance-decay weight presets (sensitivity registry) -----------
#
# The default decay weights (Luo & Qi 2009) are an assumption. twostep ships
# alternative presets so a finding can be shown robust (or not) to the decay
# choice. Source: twostep scripts/sensitivity_e2sfca_2020.R.

#' Gaussian-kernel band weights normalised to the 30-min band
#'
#' Port of twostep::gaussian_band_weights. A continuous Gaussian distance-decay
#' G(d) = exp(-d^2 / (2 sigma^2)) evaluated at the band midpoints and rescaled so
#' the 30-min band weight is 1 -- an alternative to the step weights.
#'
#' @param bands Integer drive-time bands (minutes).
#' @param sigma Gaussian bandwidth in minutes.
#' @return Named numeric cumulative weights keyed by band.
#' @export
gaussian_band_weights <- function(bands = c(30L, 60L, 120L, 180L), sigma = 60) {
  g <- exp(-(bands^2) / (2 * sigma^2))
  w <- g / g[1]
  stats::setNames(w, as.character(bands))
}

# Named cumulative band-weight presets. All key to the 30/60/120/180 bands.
# (gaussian_band_weights() is defined above so this load-time call resolves.)
E2SFCA_WEIGHT_PRESETS <- list(
  base    = c("30" = 1.00, "60" = 0.68, "120" = 0.22, "180" = 0.09),  # Luo & Qi 2009
  sharper = c("30" = 1.00, "60" = 0.42, "120" = 0.09, "180" = 0.02),  # steeper decay
  slower  = c("30" = 1.00, "60" = 0.85, "120" = 0.55, "180" = 0.30),  # gentler decay
  drop180 = c("30" = 1.00, "60" = 0.68, "120" = 0.22, "180" = 0.00),  # exclude 180-min band
  gaussian = gaussian_band_weights()                                  # Gaussian kernel, sigma=60
)

#' List the available E2SFCA weight presets
#' @return Character vector of preset names.
#' @export
e2sfca_weight_presets <- function() names(E2SFCA_WEIGHT_PRESETS)

#' Compute an access surface under a named weight preset
#'
#' @param membership,supply,demand Passed to [compute_e2sfca_access()].
#' @param preset One of [e2sfca_weight_presets()].
#' @param step2_power 1 = E2SFCA, 2 = M2SFCA.
#' @param per_capita_scale Reporting scale.
#' @return The [compute_e2sfca_access()] result under that preset's weights.
#' @export
compute_access_preset <- function(membership, supply, demand,
                                  preset = "base", step2_power = 1,
                                  per_capita_scale = 1e5) {
  if (!preset %in% names(E2SFCA_WEIGHT_PRESETS)) {
    stop(sprintf("Unknown weight preset '%s'. Available: %s",
                 preset, paste(e2sfca_weight_presets(), collapse = ", ")), call. = FALSE)
  }
  compute_e2sfca_access(membership, supply, demand,
                        weights = E2SFCA_WEIGHT_PRESETS[[preset]],
                        step2_power = step2_power, per_capita_scale = per_capita_scale)
}

#' Sensitivity sweep: national mean access across every weight preset
#'
#' Runs the access surface under each decay preset and returns the
#' population-weighted national mean and zero-access share for each, so a finding
#' can be reported as robust (or not) to the distance-decay assumption.
#'
#' @param membership,supply,demand Passed to [compute_e2sfca_access()].
#' @param presets Presets to sweep (default all).
#' @param step2_power 1 = E2SFCA, 2 = M2SFCA.
#' @return Tibble: `preset`, `mean_access`, `zero_access_share`.
#' @export
access_weight_sensitivity <- function(membership, supply, demand,
                                      presets = e2sfca_weight_presets(),
                                      step2_power = 1) {
  rows <- lapply(presets, function(p) {
    res <- compute_access_preset(membership, supply, demand, preset = p,
                                 step2_power = step2_power)
    s <- summarize_access(res$access)
    tibble::tibble(preset = p, mean_access = s$mean_access,
                   zero_access_share = s$zero_access_share)
  })
  dplyr::bind_rows(rows)
}
