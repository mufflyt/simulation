# ARCHIVED geography capability -- NOT part of the package.
#
# Each function here was exported, tested, and reachable from no pipeline. They
# are archived rather than deleted because the implementation is the expensive
# part and each is a real capability the model may want: provider migration is
# the mechanism for relocation, and advance_urps_agents() is a step of an
# alternative (Fraher) agent engine.
#
# inst/archive is .Rbuildignore'd, so nothing here is installed, loaded, or
# checked. RESTORING ONE MEANS MOVING IT BACK INTO R/, RE-EXPORTING IT, AND
# WIRING IT TO A CALLER. An export with no caller is the defect that put it
# here -- see tests/export-registry.csv and docs/GUARDS.md section 1.

# ---- apply_provider_migration()  (was R/geography-provider_geography.R) ----------------------

#' Apply one year of stochastic provider migration
#'
#' @param agents Agent tibble with `state`, `entry_year`.
#' @param year Current calendar year.
#' @param shares Destination share distribution (`geo`, `share`).
#' @param hazards Migration hazards.
#' @return The agent tibble with updated `state` and an incremented `n_moves`.
#' @family provider geography
#' @concept geography
#' @export
apply_provider_migration <- function(agents, year, shares,
                                     hazards = PROVIDER_MIGRATION_HAZARD) {
  if (!"state" %in% names(agents)) return(agents)
  if (!"entry_year" %in% names(agents))
    stop("provider migration requires an `entry_year` column on `agents`; without it the ",
         "hazard is length-0 and migration silently becomes a no-op.", call. = FALSE)
  if (!"n_moves" %in% names(agents)) agents$n_moves <- 0L

  yrs <- year - agents$entry_year
  h <- migration_hazard(yrs, agents$age, hazards)
  moves <- stats::runif(nrow(agents)) < h & !is.na(agents$state)

  if (any(moves)) {
    agents$state[moves] <- assign_entrant_geography(sum(moves), shares, stochastic = TRUE)
    agents$n_moves[moves] <- agents$n_moves[moves] + 1L
  }
  agents
}

# ---- apply_urps_migration()  (was R/geography-urps_migration.R) --------------------------

#' Apply one year of URPS-specific provider migration
#'
#' Convenience wrapper that selects per-agent hazards from [URPS_MIGRATION_HAZARD]
#' and dispatches to [apply_provider_migration_matrix()] using a pre-built or
#' on-the-fly origin-by-destination matrix. Agents without a `state` column
#' are returned unchanged.
#'
#' @param agents Agent tibble with at least `state` and `entry_year`.
#' @param year Current calendar year.
#' @param migration_matrix Pre-built matrix from [urps_migration_matrix()].
#'   When `NULL`, one is built on the fly from the agent states using equal
#'   destination weights — useful for prototyping but slower in a loop.
#' @param hazards Named migration hazard vector. Defaults to
#'   [URPS_MIGRATION_HAZARD].
#' @param urbanicity State urbanicity lookup. Defaults to
#'   [CONUS_STATE_URBANICITY].
#' @param ... Additional arguments passed to [urps_migration_matrix()] when
#'   `migration_matrix = NULL`.
#' @return The agent tibble with updated `state`, `n_moves`, and `left_country`.
#' @family urps migration
#' @concept geography
#' @export
#'
#' @examples
#' \dontrun{
#' # Build matrix once before the loop, reuse every year
#' states <- unique(na.omit(agents$state))
#' mat    <- urps_migration_matrix(states)
#' for (yr in 2026:2050) {
#'   agents <- apply_urps_migration(agents, yr, migration_matrix = mat)
#'   agents <- agents[!isTRUE(agents$left_country), ]   # filter gone providers
#' }
#' }
apply_urps_migration <- function(agents,
                                  year,
                                  migration_matrix = NULL,
                                  hazards          = URPS_MIGRATION_HAZARD,
                                  urbanicity       = CONUS_STATE_URBANICITY,
                                  ...) {
  if (!"state" %in% names(agents) || nrow(agents) == 0L) return(agents)

  if (is.null(migration_matrix)) {
    states <- sort(unique(na.omit(agents$state)))
    if (length(states) < 2L) return(agents)
    migration_matrix <- urps_migration_matrix(states, urbanicity = urbanicity, ...)
  }

  apply_provider_migration_matrix(agents, year = year, matrix = migration_matrix,
                                  hazards = hazards)
}

# ---- blend_placement_shares()  (was R/geography-provider_geography.R) ------------------------

#' Blend historical and opportunity-responsive placement
#'
#' @param historical Tibble from [historical_placement_shares()].
#' @param opportunity Tibble from [opportunity_placement_shares()].
#' @param weight Weight on the opportunity-responsive shares (0 = purely
#'   historical, 1 = purely opportunity-responsive).
#' @return Tibble with `geo` and blended `share`.
#' @family provider geography
#' @concept geography
#' @export
blend_placement_shares <- function(historical, opportunity, weight = 0.5) {
  assertthat::assert_that(weight >= 0, weight <= 1)
  h <- dplyr::select(historical, "geo", historical_share = "share")
  o <- dplyr::select(opportunity, "geo", opportunity_share = "share")
  out <- dplyr::full_join(h, o, by = "geo")
  out$historical_share[is.na(out$historical_share)] <- 0
  out$opportunity_share[is.na(out$opportunity_share)] <- 0
  out <- dplyr::mutate(
    out,
    share = (1 - weight) * .data$historical_share + weight * .data$opportunity_share
  )
  dplyr::mutate(out, share = .data$share / sum(.data$share))
}

# ---- real_access_surface()  (was R/geography-spatial_access_data.R) ---------------------------

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
#' @family spatial access data
#' @concept geography
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

