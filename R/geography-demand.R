# Geographic (isochrone) demand ----
#
# The demand-side complement to the E2SFCA supply access in R/geography-spatial_access_e2sfca. Given
# pelvic-floor NEED distributed across geographies (prevalent cases or expected
# service volume, e.g. from the life-course/DMDM prevalence applied to local
# population) and a travel-time (isochrone) to the nearest provider, this
# summarises how much NEED sits within 30/60/120/180 minutes of care and how much
# is effectively unreachable -- the demand half of the "demand-supply-isochrones"
# question. Where a per-geography supply capacity or E2SFCA access ratio is
# available, it computes need-weighted access and accessible-capacity-vs-need.
#
# Base R (matching R/geography-spatial_access_e2sfca's band conventions: 30/60/120/180 minutes), so it is
# unit-testable without the tidyverse. Real use needs tract-level population,
# provider locations and drive-time isochrones; this operates on the resulting
# per-geography tables.
#
# tract_need_from_population() is the bridge from the demographic inputs to that
# per-geography NEED: it turns a tract table of female population by demand age
# band (as produced by scripts/data_acquisition/08_download_acs_tracts.R) into
# expected prevalent PFD cases per tract, using the package's SSOT age-band
# prevalence (pfd_prevalence_by_band(), R/demand-urps). Join the result to tract centroids
# + a nearest-provider drive time and hand it to geographic_demand_summary().

# Default map from DEMAND_AGE_BANDS (R/demand-urps) to the wide population columns emitted
# by 08_download_acs_tracts.R. Kept as a literal so R/geography-demand sources standalone.
TRACT_AGE_BAND_COLUMNS <- c(
  "20-39" = "female_20_39", "40-59" = "female_40_59", "60-64" = "female_60_64",
  "65-79" = "female_65_79", "80+" = "female_80plus")

#' Distribute pelvic-floor NEED across travel-time (isochrone) bands
#'
#' @param geo_need Data frame with one row per geography: a need column and a
#'   travel-time-to-nearest-provider column.
#' @param bands Upper edges of the isochrone bands, in minutes. Default
#'   `c(30, 60, 120, 180)` (matching R/geography-spatial_access_e2sfca).
#' @param need_col,time_col Column names for need and travel time.
#' @return Data frame `threshold_min`, `need_within` (need reachable within that
#'   many minutes) and `share_within` (of total need). `attr(., "beyond")` gives
#'   the need (and share) farther than the largest band.
#' @family demand
#' @concept geography
#' @export
demand_by_travel_band <- function(geo_need, bands = c(30, 60, 120, 180),
                                  need_col = "need", time_col = "nearest_provider_min") {
  stopifnot(is.data.frame(geo_need), all(c(need_col, time_col) %in% names(geo_need)))
  bands <- sort(unique(bands))
  need <- geo_need[[need_col]]; tmin <- geo_need[[time_col]]
  total <- sum(need, na.rm = TRUE)
  within <- vapply(bands, function(b) sum(need[tmin <= b], na.rm = TRUE), numeric(1))
  out <- data.frame(threshold_min = bands, need_within = within,
                    share_within = if (total > 0) within / total else NA_real_,
                    stringsAsFactors = FALSE)
  beyond_need <- total - within[length(within)]
  attr(out, "beyond") <- c(need = beyond_need,
                           share = if (total > 0) beyond_need / total else NA_real_)
  out
}

#' Need-weighted access (weight an access ratio by where the need is)
#'
#' A national mean access ratio understates access problems if the low-access
#' geographies also carry disproportionate need. This weights the access ratio
#' (e.g. an E2SFCA ratio from R/geography-spatial_access_e2sfca) by each geography's need.
#'
#' @param geo Data frame with an access-ratio column and a need column.
#' @param access_col,need_col Column names.
#' @return Need-weighted mean access ratio (numeric scalar), or `NA` if no
#'   finite, positively-weighted rows.
#' @family demand
#' @concept geography
#' @export
need_weighted_access <- function(geo, access_col = "access_ratio", need_col = "need") {
  stopifnot(is.data.frame(geo), all(c(access_col, need_col) %in% names(geo)))
  w <- geo[[need_col]]; a <- geo[[access_col]]
  ok <- is.finite(w) & is.finite(a) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(w[ok] * a[ok]) / sum(w[ok])
}

#' Accessible capacity vs need, by geography and nationally
#'
#' @param geo Data frame with a need column and a capacity column (both in the
#'   same units, e.g. required vs available FTE, or cases vs served-cases).
#' @param need_col,capacity_col Column names.
#' @return List: `total_need`, `total_capacity`, `national_adequacy`
#'   (capacity/need), `underserved_need_share` (share of national need in
#'   geographies with capacity < need), and `by_geo` (input plus per-geography
#'   `adequacy`).
#' @family demand
#' @concept geography
#' @export
accessible_need_vs_capacity <- function(geo, need_col = "need", capacity_col = "capacity") {
  stopifnot(is.data.frame(geo), all(c(need_col, capacity_col) %in% names(geo)))
  need <- geo[[need_col]]; cap <- geo[[capacity_col]]
  adequacy <- ifelse(need > 0, cap / need, NA_real_)
  tot_need <- sum(need, na.rm = TRUE)
  underserved <- is.finite(adequacy) & adequacy < 1
  by_geo <- geo; by_geo$adequacy <- adequacy
  list(
    total_need = tot_need,
    total_capacity = sum(cap, na.rm = TRUE),
    national_adequacy = if (tot_need > 0) sum(cap, na.rm = TRUE) / tot_need else NA_real_,
    underserved_need_share = if (tot_need > 0)
      sum(need[underserved], na.rm = TRUE) / tot_need else NA_real_,
    by_geo = by_geo
  )
}

#' Expected prevalent PFD NEED per geography from age-band population
#'
#' Bridges the demographic inputs to the per-geography `need` the rest of this
#' module consumes: for each row (e.g. a census tract) it multiplies the female
#' population in each demand age band by that band's PFD prevalence and sums, so
#' `need` is the expected number of prevalent pelvic-floor cases. This is the
#' geographic analogue of the D1 prevalent-PFD denominator (`R/demand-urps`), applied to
#' local population instead of the national age structure.
#'
#' @param tracts Data frame with a geography id and one female-population column
#'   per demand age band (default columns are those written by
#'   `scripts/data_acquisition/08_download_acs_tracts.R`). Any other columns
#'   (centroids, `nearest_provider_min`, `access_ratio`, `capacity`) are carried
#'   through untouched so the result flows straight into
#'   [geographic_demand_summary()].
#' @param prevalence Named numeric PFD prevalence over the demand age bands.
#'   Defaults to the package SSOT [pfd_prevalence_by_band()]; must name every band
#'   in `band_cols`.
#' @param band_cols Named character vector mapping each demand age band to its
#'   population column in `tracts`. Default `TRACT_AGE_BAND_COLUMNS` (the columns
#'   written by `08_download_acs_tracts.R`).
#' @param need_col Name of the output need column. Default `"need"`.
#' @return `tracts` with a numeric `need` column added.
#' @family demand
#' @concept geography
#' @examples
#' # Age-banded tract population becomes expected PFD cases. Column names are
#' # fixed by TRACT_AGE_BAND_COLUMNS; a missing band is an error, not a zero.
#' tract_need_from_population(tibble::tibble(
#'   GEOID         = c("08031001", "08031002"),
#'   female_20_39  = c(900, 1200), female_40_59 = c(800, 700),
#'   female_60_64  = c(200, 180),  female_65_79 = c(300, 260),
#'   female_80plus = c(90, 70)
#' ))
#' @export
tract_need_from_population <- function(tracts,
                                       prevalence = pfd_prevalence_by_band(),
                                       band_cols = TRACT_AGE_BAND_COLUMNS,
                                       need_col = "need") {
  stopifnot(is.data.frame(tracts), length(band_cols) > 0, !is.null(names(band_cols)))
  bands <- names(band_cols)
  missing_cols <- setdiff(unname(band_cols), names(tracts))
  if (length(missing_cols))
    stop("tract_need_from_population(): missing population column(s): ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  missing_prev <- setdiff(bands, names(prevalence))
  if (length(missing_prev))
    stop("tract_need_from_population(): prevalence has no value for band(s): ",
         paste(missing_prev, collapse = ", "), call. = FALSE)
  rate <- as.numeric(prevalence[bands])            # align rate order to band_cols
  # Reject a non-finite prevalence up front: it would otherwise poison EVERY
  # tract's need via the matrix product (0 * NA = NA), silently, including tracts
  # with no population in that band. A missing SSOT band should fail loudly.
  if (any(!is.finite(rate)))
    stop("tract_need_from_population(): non-finite prevalence for band(s): ",
         paste(bands[!is.finite(rate)], collapse = ", "), call. = FALSE)

  # character/factor only: an all-NA logical column is the documented "NA
  # population -> treated as zero" case and must pass, but a character column
  # would coerce the whole matrix to character and make %*% error cryptically.
  .band_bad <- vapply(tracts[, unname(band_cols), drop = FALSE],
                      function(col) is.character(col) || is.factor(col), logical(1))
  if (any(.band_bad))
    stop("tract_need_from_population(): band population column(s) are character/factor: ",
         paste(unname(band_cols)[.band_bad], collapse = ", "),
         " -- as.matrix() would coerce to character and the %*% product would error.",
         call. = FALSE)
  pop <- as.matrix(tracts[, unname(band_cols), drop = FALSE])
  pop[is.na(pop)] <- 0
  tracts[[need_col]] <- as.numeric(pop %*% rate)   # sum_band(pop_band * prevalence_band)
  tracts
}

#' One-call isochrone demand from a tract age-band population table
#'
#' Convenience wrapper: build per-tract `need` with [tract_need_from_population()]
#' then summarise it with [geographic_demand_summary()]. `tracts` must already
#' carry a nearest-provider travel-time column (from the isochrone pipeline);
#' access-ratio / capacity columns are used when present.
#'
#' @param tracts Tract table with age-band population + `nearest_provider_min`.
#' @param prevalence,band_cols Passed to [tract_need_from_population()].
#' @param ... Passed to [geographic_demand_summary()] (e.g. `bands`, `time_col`).
#' @return The [geographic_demand_summary()] list for the tract need surface.
#' @family demand
#' @concept geography
#' @export
isochrone_demand_from_tracts <- function(tracts,
                                         prevalence = pfd_prevalence_by_band(),
                                         band_cols = TRACT_AGE_BAND_COLUMNS, ...) {
  need_tbl <- tract_need_from_population(tracts, prevalence, band_cols)
  geographic_demand_summary(need_tbl, ...)
}

#' One-call geographic (isochrone) demand summary
#'
#' Combines [demand_by_travel_band()] with the optional access measures into a
#' single summary for a projection year.
#'
#' @param geo_need Data frame with need + travel-time columns (and optionally an
#'   access-ratio and/or capacity column).
#' @param bands Isochrone band edges (minutes).
#' @param need_col,time_col,access_col,capacity_col Column names; access/capacity
#'   are used only when present.
#' @return A list with `total_need`, `by_band`, `beyond_share` (need farther than
#'   the largest band), and — when the columns exist — `need_weighted_access` and
#'   `adequacy`.
#' @family demand
#' @concept geography
#' @export
geographic_demand_summary <- function(geo_need, bands = c(30, 60, 120, 180),
                                      need_col = "need", time_col = "nearest_provider_min",
                                      access_col = "access_ratio", capacity_col = "capacity") {
  by_band <- demand_by_travel_band(geo_need, bands, need_col, time_col)
  out <- list(
    total_need = sum(geo_need[[need_col]], na.rm = TRUE),
    by_band = by_band,
    beyond_share = unname(attr(by_band, "beyond")["share"])
  )
  if (access_col %in% names(geo_need)) {
    out$need_weighted_access <- need_weighted_access(geo_need, access_col, need_col)
  }
  if (capacity_col %in% names(geo_need)) {
    out$adequacy <- accessible_need_vs_capacity(geo_need, need_col, capacity_col)[
      c("national_adequacy", "underserved_need_share")]
  }
  out
}
