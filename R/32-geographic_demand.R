# Geographic (isochrone) demand ----
#
# The demand-side complement to the E2SFCA supply access in R/14. Given
# pelvic-floor NEED distributed across geographies (prevalent cases or expected
# service volume, e.g. from the life-course/DMDM prevalence applied to local
# population) and a travel-time (isochrone) to the nearest provider, this
# summarises how much NEED sits within 30/60/120/180 minutes of care and how much
# is effectively unreachable -- the demand half of the "demand-supply-isochrones"
# question. Where a per-geography supply capacity or E2SFCA access ratio is
# available, it computes need-weighted access and accessible-capacity-vs-need.
#
# Base R (matching R/14's band conventions: 30/60/120/180 minutes), so it is
# unit-testable without the tidyverse. Real use needs tract-level population,
# provider locations and drive-time isochrones; this operates on the resulting
# per-geography tables.

#' Distribute pelvic-floor NEED across travel-time (isochrone) bands
#'
#' @param geo_need Data frame with one row per geography: a need column and a
#'   travel-time-to-nearest-provider column.
#' @param bands Upper edges of the isochrone bands, in minutes. Default
#'   `c(30, 60, 120, 180)` (matching R/14).
#' @param need_col,time_col Column names for need and travel time.
#' @return Data frame `threshold_min`, `need_within` (need reachable within that
#'   many minutes) and `share_within` (of total need). `attr(., "beyond")` gives
#'   the need (and share) farther than the largest band.
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
#' (e.g. an E2SFCA ratio from R/14) by each geography's need.
#'
#' @param geo Data frame with an access-ratio column and a need column.
#' @param access_col,need_col Column names.
#' @return Need-weighted mean access ratio (numeric scalar), or `NA` if no
#'   finite, positively-weighted rows.
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
