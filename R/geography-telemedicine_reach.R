# Telemedicine geographic reach (access layer) -------------------------------
#
# The telemedicine supply scenario (reporting-scenario_registry.R) models its
# throughput effect as a clinical-hours multiplier, and its own source string
# says the separate GEOGRAPHIC-REACH effect belongs HERE, in the access layer,
# as accessible reach -- "not a supply multiplier." This is that piece: telehealth
# extends a specialist's catchment into areas a patient could not previously
# reach in person, so a larger share of NONMETRO demand can be served. It is
# expressed as an uplift to a nonmetro catchment's accessible capacity, fed into
# clear_access() like any other capacity.
#
# OUTPUT-PRESERVING: with the default not applied (uplift 0, or no metro column),
# accessible_capacity is returned unchanged -- this never moves a baseline number
# unless a caller opts into the scenario.

#' Apply the telemedicine geographic-reach uplift to nonmetro catchments
#'
#' Raises `accessible_capacity` for catchments flagged nonmetro by a labeled
#' factor, modelling telehealth extending specialist reach into rural areas. The
#' result is an ordinary catchment table for [clear_access()] (and the
#' spatial-overflow clearing when present); under the uplift a nonmetro catchment
#' clears more of its demand (shorter wait, less unmet). Metro catchments are
#' untouched.
#'
#' The uplift is ASSUMED/ILLUSTRATIVE -- a scenario lever, not a measured
#' telehealth-reach effect. Supply a cited rural tele-reach estimate before any
#' published number rests on it.
#'
#' @param catchments A catchment table with `accessible_capacity` and a
#'   metro/nonmetro column.
#' @param nonmetro_uplift Fractional increase applied to a nonmetro catchment's
#'   `accessible_capacity` (e.g. 0.15 = +15%). Non-negative scalar. Default 0.15.
#' @param metro_col Name of the metro/nonmetro column. Default `"metro"`.
#' @param nonmetro_value The value in `metro_col` marking a nonmetro catchment.
#'   Default `"NonMetro"`.
#' @return The catchment table (tibble) with `accessible_capacity` raised on
#'   nonmetro rows and a logical `telemedicine_reach_applied` column. With
#'   `nonmetro_uplift = 0` or no `metro_col`, capacity is returned unchanged.
#' @export
telemedicine_reach <- function(catchments,
                               nonmetro_uplift = 0.15,
                               metro_col = "metro",
                               nonmetro_value = "NonMetro") {
  if (!is.data.frame(catchments) || !"accessible_capacity" %in% names(catchments)) {
    stop("telemedicine_reach(): `catchments` needs an `accessible_capacity` column.",
         call. = FALSE)
  }
  stopifnot(length(nonmetro_uplift) == 1L, is.finite(nonmetro_uplift),
            nonmetro_uplift >= 0)
  out <- catchments
  applied <- rep(FALSE, nrow(out))
  if (nonmetro_uplift > 0 && metro_col %in% names(out)) {
    is_nonmetro <- !is.na(out[[metro_col]]) & out[[metro_col]] == nonmetro_value
    hit <- is_nonmetro & !is.na(out$accessible_capacity)
    out$accessible_capacity[hit] <- out$accessible_capacity[hit] * (1 + nonmetro_uplift)
    applied <- hit
  }
  out$telemedicine_reach_applied <- applied
  tibble::as_tibble(out)
}
