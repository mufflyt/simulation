# Provider Ascertainment as a Property of an Access Surface -------------------
#
# An E2SFCA surface answers "how reachable is care from this tract". It says
# nothing about how many of the eligible physicians were actually in the
# calculation, and for FPMRS that share moves from 70.5% in 2019 to 89.7% in
# 2023. Comparing those two surfaces without saying so measures a 19-point
# improvement in provider-location ascertainment alongside whatever access did.
#
# THREE LOSSES, NEVER MERGED. Each has a different cause and a different fix:
#
#   no_provider_year_address  no address row for that provider-year
#   address_not_geocodable    address exists, no coordinate came back
#   no_qualifying_isochrone   coordinate is known precisely, but no isochrone
#                             centre lies within the match threshold, so the
#                             provider has no computed catchment
#
# The third is NOT a geocoding failure. Those physicians' locations are known;
# what is missing is a drive-time polygon at that location. Counting them as
# geocoding failures would point the remedy at the wrong pipeline.
#
# There is deliberately no residual bucket. Anything left over is
# `unexplained_pipeline_loss` and the validator fails on it.

#' Terminal dispositions an eligible provider may receive
#'
#' @format Character vector. `unexplained_pipeline_loss` is a failure state,
#'   never an acceptable outcome.
#' @family access
#' @concept validation
#' @export
ACCESS_PROVIDER_DISPOSITIONS <- c(
  "included_in_surface",
  "no_provider_year_address",
  "address_not_geocodable",
  "no_qualifying_isochrone",
  "unexplained_pipeline_loss"
)

#' Provider ascertainment flow for the access surfaces
#'
#' Reads the committed artifact written by
#' `scripts/data_acquisition/build_access_ascertainment.R`.
#'
#' @param path Flow CSV.
#' @return Tibble with nested counts and rates by analysis year.
#' @family access
#' @concept validation
#' @export
access_provider_flow <- function(
    path = artifact_path("access_ascertainment", "provider_flow_fpmrs.csv")) {
  if (is.null(path)) path <- "artifacts/access_ascertainment/provider_flow_fpmrs.csv"
  if (!file.exists(path)) {
    stop("access_provider_flow(): ", path, " not found. Build it with ",
         "scripts/data_acquisition/build_access_ascertainment.R.", call. = FALSE)
  }
  tibble::as_tibble(utils::read.csv(path, stringsAsFactors = FALSE))
}

#' Ascertainment status for one analysis year
#'
#' @param year Analysis year.
#' @param flow Flow table; defaults to [access_provider_flow()].
#' @return List with the counts, the rates, and `all_losses_explained`.
#' @family access
#' @concept validation
#' @export
#' @examples
#' \dontrun{ access_ascertainment_status(2023) }
access_ascertainment_status <- function(year, flow = access_provider_flow()) {
  r <- flow[flow$analysis_year == year, , drop = FALSE]
  if (nrow(r) != 1L) {
    stop(sprintf("access_ascertainment_status(): no flow row for year %s. Have: %s",
                 year, paste(flow$analysis_year, collapse = ", ")), call. = FALSE)
  }
  explained <- isTRUE(r$spatially_eligible_provider_n == r$surface_provider_n)
  list(
    analysis_year = r$analysis_year,
    eligible_provider_n = r$eligible_provider_n,
    provider_year_address_n = r$provider_year_address_n,
    usable_coordinate_n = r$usable_coordinate_n,
    spatially_eligible_provider_n = r$spatially_eligible_provider_n,
    surface_provider_n = r$surface_provider_n,
    usable_coordinate_rate = r$usable_coordinate_rate,
    surface_rate = r$surface_rate,
    all_losses_explained = explained
  )
}

#' The best-ascertained analysis year available
#'
#' @details
#' PICK THE YEAR BY ASCERTAINMENT, NOT BY CONVENIENCE. The historical supply
#' back-test starts in 2020, which makes 2020 the tempting default. Its access
#' surface represents 73.2% of eligible physicians; 2023 represents 89.7%. For a
#' contemporary geographic-access anchor the better-ascertained surface wins,
#' and the back-test's origin year has no bearing on it.
#'
#' @param flow Flow table; defaults to [access_provider_flow()].
#' @return List with `year`, `surface_rate`, and `reason`.
#' @family access
#' @concept validation
#' @export
contemporary_access_year <- function(flow = access_provider_flow()) {
  ok <- flow[is.finite(flow$surface_rate), , drop = FALSE]
  if (!nrow(ok)) stop("contemporary_access_year(): no year has a scored surface.",
                      call. = FALSE)
  i <- which.max(ok$surface_rate)
  list(year = ok$analysis_year[i], surface_rate = ok$surface_rate[i],
       reason = sprintf(paste("Highest provider ascertainment in the panel:",
                              "%.1f%% of eligible physicians reach the surface,",
                              "against %.1f%% in %d."),
                        100 * ok$surface_rate[i],
                        100 * min(ok$surface_rate), ok$analysis_year[which.min(ok$surface_rate)]))
}

#' Modes under which a temporal access comparison may proceed
#'
#' @format Named character vector of mode to meaning.
#' @family access
#' @concept validation
#' @export
TEMPORAL_ACCESS_MODES <- c(
  cross_sectional = "One year only. No change over time is asserted.",
  ascertainment_aware = "Change is reported jointly with the ascertainment change and never attributed to access alone.",
  common_provider_cohort = "Restricted to providers spatially eligible in every compared year, so the sampling frame is constant."
)

#' Refuse a naive temporal comparison of access surfaces
#'
#' Call before comparing E2SFCA across years. Fails closed unless the caller
#' names a mode from [TEMPORAL_ACCESS_MODES].
#'
#' @details
#' A CHANGE IN E2SFCA IS NOT A CHANGE IN ACCESS unless it has been separated
#' from the change in provider-location ascertainment. Between 2019 and 2023 the
#' share of eligible FPMRS physicians reaching the surface rose 18.7 points. Any
#' apparent improvement in accessibility over that window is confounded with it,
#' and the confounder is larger than most plausible access effects.
#'
#' @param years Years being compared.
#' @param mode One of [TEMPORAL_ACCESS_MODES].
#' @param flow Flow table; defaults to [access_provider_flow()].
#' @return Invisibly, a list with the mode and per-year rates. Errors otherwise.
#' @family access
#' @concept validation
#' @export
#' @examples
#' \dontrun{ assert_temporal_access_comparison(c(2020, 2023), "ascertainment_aware") }
assert_temporal_access_comparison <- function(years, mode = NULL,
                                              flow = access_provider_flow()) {
  stopifnot(length(years) >= 1L)
  rates <- vapply(years, function(y) {
    r <- flow$surface_rate[flow$analysis_year == y]
    if (length(r) != 1L) NA_real_ else r
  }, numeric(1))
  spread <- if (all(is.finite(rates))) max(rates) - min(rates) else NA_real_

  if (length(unique(years)) == 1L) {
    return(invisible(list(mode = "cross_sectional", rates = rates)))
  }
  if (is.null(mode) || !mode %in% names(TEMPORAL_ACCESS_MODES)) {
    stop(sprintf(paste(
      "TEMPORAL ACCESS COMPARISON REFUSED for years %s.\n",
      "Provider ascertainment differs across these surfaces by %.1f percentage",
      "points (%s), so a difference in E2SFCA is not a difference in access.\n",
      "Name a mode explicitly:\n%s"),
      paste(years, collapse = ", "), 100 * (spread %||% NA_real_),
      paste(sprintf("%d: %.1f%%", years, 100 * rates), collapse = "; "),
      paste(sprintf("  %s - %s", names(TEMPORAL_ACCESS_MODES),
                    TEMPORAL_ACCESS_MODES), collapse = "\n")),
      call. = FALSE)
  }
  invisible(list(mode = mode, rates = stats::setNames(rates, years),
                 ascertainment_spread = spread))
}

#' The validated access surface a demand model may use
#'
#' The single entry point. Selects the best-ascertained year, loads the archived
#' surface and its provider artifact, runs every gate in
#' [validate_access_surface()], and refuses to return anything that fails.
#'
#' @details
#' THIS DOES NOT ALLOCATE DEMAND. It returns a validated surface and the
#' ascertainment metadata that must travel with it. Wiring it into demand
#' allocation is a separate decision, and the metadata exists so that decision
#' can be made with the denominator visible.
#'
#' The cross-sectional temporal guard is asserted here so that a caller who
#' takes one year cannot later diff it against another without going through
#' [assert_temporal_access_comparison()].
#'
#' @param year Analysis year; defaults to [contemporary_access_year()].
#' @param root Directory holding the unpacked surfaces.
#' @return List with `year`, `surface`, `providers`, `validation`,
#'   `ascertainment`, and `provenance`. Errors if any gate fails.
#' @family access
#' @concept validation
#' @export
access_surface_for_demand <- function(
    year = NULL,
    root = Sys.getenv("E2SFCA_SURFACE_DIR", "")) {
  flow <- access_provider_flow()
  if (is.null(year)) year <- contemporary_access_year(flow)$year
  assert_temporal_access_comparison(year, flow = flow)

  sp <- file.path(root, sprintf("step_4_2sfca_FPMRS_%d.rds", year))
  pp <- file.path(root, sprintf("step_4_2sfca_FPMRS_%d_providers.rds", year))
  if (!nzchar(root) || !file.exists(sp) || !file.exists(pp)) {
    stop(sprintf(paste0("E2SFCA surface artifacts not found (dir = '%s'). These are ",
                        "large external files; set E2SFCA_SURFACE_DIR or pass root ",
                        "pointing at step_4_2sfca_FPMRS_%d{,_providers}.rds."),
                 root, year), call. = FALSE)
  }
  prov <- tryCatch({
    v <- utils::read.csv(artifact_path("access_ascertainment", "surface_provenance.csv"),
                         stringsAsFactors = FALSE)
    v <- v[v$analysis_year == year, , drop = FALSE]
    list(path = v$surface_artifact_path, sha256 = v$surface_artifact_sha256)
  }, error = function(e) NULL)

  surface <- readRDS(sp); providers <- readRDS(pp)
  v <- validate_access_surface(surface, providers = providers,
                               surface_year = as.integer(year), provenance = prov)
  assert_access_surface_usable(v)
  list(year = as.integer(year), surface = surface, providers = providers,
       validation = v, ascertainment = access_ascertainment_status(year, flow),
       provenance = prov)
}
