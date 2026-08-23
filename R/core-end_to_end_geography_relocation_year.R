# Per-year county-endogenous geography relocation for run_end_to_end_simulation

#' Apply one simulation year's county-endogenous geography relocation
#'
#' Extracted from [run_end_to_end_simulation()]'s per-year loop so that
#' function stays under the repository's module code-line ceiling; this is a
#' pure code move (identical logic, now callable on its own) not a behavior
#' change. A no-op (returns the inputs unchanged, with `NA` iteration
#' diagnostics) unless `geography_engine == "county_endogenous"`.
#'
#' @param provider_cohort Full provider cohort for the run.
#' @param active_providers Active-provider tibble for `simulation_year`.
#' @param simulation_year The calendar year being simulated.
#' @param geography_engine Either `"hrr_balance"` or `"county_endogenous"`.
#' @param county_market_tbl County market tibble, optionally year-varying
#'   (filtered to `simulation_year` when it has a `year` column).
#' @param geography_control Extra arguments passed to `geography_solver`.
#' @param geography_solver Function `(provider_roster, county_market_tbl,
#'   year, ...) -> data.frame | list(provider_roster, iterations,
#'   converged)`. Must return `provider_id` and `county_fips`.
#'
#' @return A list with `provider_cohort` (county_fips updated),
#'   `active_providers` (recomputed from the updated cohort),
#'   `geography_iterations`, and `geography_converged`.
#' @keywords internal
.run_geography_relocation_year <- function(provider_cohort,
                                            active_providers,
                                            simulation_year,
                                            geography_engine,
                                            county_market_tbl,
                                            geography_control,
                                            geography_solver) {
  geography_iterations <- NA_integer_
  geography_converged <- NA
  if (geography_engine == "county_endogenous") {
    base::message("Applying county endogenous geography for ",
      simulation_year, ".")
    if ("year" %in% base::names(county_market_tbl)) {
      year_markets <- county_market_tbl |>
        dplyr::filter(.data$year == simulation_year)
    } else {
      year_markets <- county_market_tbl
    }
    geography_arguments <- base::c(
      base::list(
        provider_roster = active_providers,
        county_market_tbl = year_markets,
        year = simulation_year
      ),
      geography_control
    )
    geography_solution <- base::do.call(
      geography_solver,
      geography_arguments
    )
    if (base::is.data.frame(geography_solution)) {
      relocated_providers <- geography_solution
    } else {
      relocated_providers <- geography_solution$provider_roster
      geography_iterations <- .urps_null_or(geography_solution$iterations, NA_integer_)
      geography_converged <- .urps_null_or(geography_solution$converged, NA)
    }
    required_geo_columns <- base::c("provider_id", "county_fips")
    missing_geo_columns <- base::setdiff(
      required_geo_columns,
      base::names(relocated_providers)
    )
    if (base::length(missing_geo_columns) > 0L) {
      base::stop(
        "The geography solver must return provider_id and county_fips.",
        call. = FALSE
      )
    }
    provider_cohort <- provider_cohort |>
      dplyr::select(-dplyr::any_of("county_fips")) |>
      dplyr::left_join(
        relocated_providers |>
          dplyr::select(.data$provider_id, .data$county_fips),
        by = "provider_id"
      )
    active_providers <- provider_cohort |>
      dplyr::filter(.data$active)
  }

  base::list(
    provider_cohort = provider_cohort,
    active_providers = active_providers,
    geography_iterations = geography_iterations,
    geography_converged = geography_converged
  )
}
