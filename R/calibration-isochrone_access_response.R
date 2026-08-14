# Isochrone access-response glue: E2SFCA -> catchments -> Lizeth join ----
#
# THE WIRING BETWEEN THREE ENGINES THAT NEVER TOUCHED.
#
# compute_e2sfca_access() (R/geography-spatial_access_e2sfca.R) turns the imported
# drive-time membership into a per-PROVIDER ratio R_j = supply_j / weighted_demand_j,
# where weighted_demand_j is the decay-weighted population that competes for
# provider j. That ratio is exactly the provider's relative adequacy: R_j is
# proportional to 1 / rho_j (rho = demand / capacity), so the spatial layer
# already carries the DISPERSION of catchment adequacy the access-response bridge
# (R/calibration-access_response_bridge.R) says it "refuses to invent."
#
# The inverse-adequacy fit (R/calibration-lizeth_inverse_adequacy.R) and the
# closed-form fit_wait_scale() (R/validation-access.R) both consume a `catchments`
# table in clear_access() currency: demand_workload + accessible_capacity, plus an
# optional weight and a rank-carrying column. Nothing converted the E2SFCA
# provider ratios into that table, and nothing attached the fielded Lizeth waits
# to the provider that was actually called. These two functions are that glue.
#
# CATCHMENT UNIT: one provider = one catchment. Lizeth measures wait at the
# PROVIDER level, so a provider-catchment gives a 1:1 join by NPI with no
# ecological aggregation, and it maps straight onto provider_ratios. The
# tract-level surface (compute_e2sfca_access()$access) is retained separately for
# the downstream consumer (cliff Module D); this module is the FIT side.
#
# ANCHOR: accessible_capacity is the provider's effective clinical capacity. The
# strongest anchor for a fem65 demand denominator is realized Medicare procedure
# volume for the 65+ population -- same population as the demand, observed rather
# than modeled -- carried in as `supply` on the E2SFCA input. This module does not
# invent capacity; it forwards whatever `supply` the access computation was given
# and records the anchor label on the result.
#
# IDENTIFICATION: demand_workload here is weighted_demand * workload_per_capita.
# The per-capita constant is a pure SCALE that cancels in the wait_scale fit
# (fit_wait_scale is closed-form linear in wait_scale) and is absorbed by the
# fitted wait_scale; it is carried explicitly so the currency is honest, not so it
# adds a free parameter. What the isochrones identify is the ORDERING and spread
# of adequacy across providers; the level rides on the capacity anchor.

.iaccess_provider_ratios <- function(access) {
  # Accept either a compute_e2sfca_access() result (list with $provider_ratios)
  # or a provider_ratios tibble directly, so callers can pass whichever they hold.
  if (base::is.list(access) && !base::is.data.frame(access) &&
      "provider_ratios" %in% base::names(access)) {
    return(access$provider_ratios)
  }
  if (base::is.data.frame(access)) {
    return(access)
  }
  base::stop(
    "`access` must be a compute_e2sfca_access() result (a list with ",
    "$provider_ratios) or a provider_ratios data frame.",
    call. = FALSE
  )
}

#' Build provider-catchment loads from an E2SFCA access computation
#'
#' Converts the per-provider ratios from [compute_e2sfca_access()] into the
#' `catchments` table that [fit_wait_scale()], [forward_lizeth_adequacy()] and
#' [fit_lizeth_inverse_adequacy()] consume: one row per provider carrying
#' `demand_workload` and `accessible_capacity` in [clear_access()] currency, plus
#' a demand `weight` for quantile matching and `adequacy_relative`, the
#' isochrone-derived adequacy dispersion (rank-preserving input to the fit).
#'
#' @details
#' For provider \eqn{j}, `accessible_capacity` is the provider's `supply`
#' (effective clinical capacity, e.g. realized Medicare procedure volume for the
#' fem65 population) and `demand_workload` is `weighted_demand * workload_per_capita`,
#' the decay-weighted population that competes for the provider scaled to workload.
#' Utilisation is then `rho = demand_workload / accessible_capacity` and adequacy
#' is `accessible_capacity / demand_workload`. `workload_per_capita` is a pure
#' scale absorbed by the fitted `wait_scale`; it does not add a free parameter.
#'
#' Providers with non-positive weighted demand carry an undefined ratio and are
#' dropped by default (`drop_zero_demand = TRUE`): a zero-demand provider cannot
#' inform a congestion fit.
#'
#' @param access A [compute_e2sfca_access()] result, or its `provider_ratios`
#'   tibble (columns `provider_id`, `weighted_demand`, `supply`).
#' @param workload_per_capita Positive scalar converting competed-for population
#'   to workload units. Default `1` (population-as-workload; the scale is absorbed
#'   by `wait_scale`).
#' @param drop_zero_demand Drop providers with `weighted_demand <= 0`. Default
#'   `TRUE`.
#' @param capacity_anchor Label recorded on the result describing the `supply`
#'   currency. Default `"medicare_procedure_volume_fem65"`.
#'
#' @return A tibble with one row per provider: `catchment` (the provider id),
#'   `demand_workload`, `accessible_capacity`, `weight` (competed-for demand),
#'   `adequacy_relative` (= capacity / workload), and `e2sfca_ratio` (the raw
#'   `supply / weighted_demand`). The `capacity_anchor` and `workload_per_capita`
#'   are attached as attributes.
#'
#' @concept calibration
#' @family isochrone access response
#' @export
e2sfca_catchments_from_access <- function(access,
                                          workload_per_capita = 1,
                                          drop_zero_demand = TRUE,
                                          capacity_anchor =
                                            "medicare_procedure_volume_fem65") {
  base::message("Building provider-catchment loads from E2SFCA provider ratios.")
  if (!(base::is.numeric(workload_per_capita) &&
        base::length(workload_per_capita) == 1L &&
        base::is.finite(workload_per_capita) && workload_per_capita > 0)) {
    base::stop("`workload_per_capita` must be a single positive number.",
               call. = FALSE)
  }
  pr <- .iaccess_provider_ratios(access)
  required <- c("provider_id", "weighted_demand", "supply")
  missing_cols <- base::setdiff(required, base::names(pr))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "provider ratios missing column(s): ",
      base::paste(missing_cols, collapse = ", "),
      ". Expected the output of compute_e2sfca_access()$provider_ratios.",
      call. = FALSE
    )
  }

  catchments <- tibble::tibble(
    catchment = pr$provider_id,
    weighted_demand = base::as.numeric(pr$weighted_demand),
    accessible_capacity = base::as.numeric(pr$supply)
  ) |>
    dplyr::mutate(
      zero_demand = !base::is.finite(.data$weighted_demand) |
        .data$weighted_demand <= 0,
      demand_workload = .data$weighted_demand * workload_per_capita,
      weight = .data$weighted_demand,
      e2sfca_ratio = dplyr::if_else(
        .data$zero_demand, NA_real_,
        .data$accessible_capacity / .data$weighted_demand
      ),
      adequacy_relative = dplyr::if_else(
        .data$zero_demand, NA_real_,
        .data$accessible_capacity / .data$demand_workload
      )
    )

  n_zero <- base::sum(catchments$zero_demand, na.rm = TRUE)
  if (base::isTRUE(drop_zero_demand) && n_zero > 0L) {
    base::message(
      "Dropping ", .iaccess_comma(n_zero),
      " provider-catchment(s) with non-positive competed-for demand."
    )
    catchments <- catchments[!catchments$zero_demand, , drop = FALSE]
  }

  catchments <- dplyr::select(
    catchments,
    "catchment", "demand_workload", "accessible_capacity",
    "weight", "adequacy_relative", "e2sfca_ratio"
  )

  base::message(
    "Built ", .iaccess_comma(base::nrow(catchments)),
    " provider-catchment(s); capacity anchor: ", capacity_anchor, "."
  )
  base::attr(catchments, "capacity_anchor") <- capacity_anchor
  base::attr(catchments, "workload_per_capita") <- workload_per_capita
  catchments
}

#' Attach fielded Lizeth waits to their provider-catchment by NPI
#'
#' Joins call-level Lizeth access records (from [prepare_lizeth_access()]) to the
#' provider-catchment loads from [e2sfca_catchments_from_access()], so each
#' observed wait carries the isochrone-derived access of the provider that was
#' actually called. This is the paired (local access, realized wait) dataset that
#' identifies the wait response and the decay parameter.
#'
#' @details
#' The canonical isochrone cohort is NPI-keyed, so by default the Lizeth `npi`
#' joins directly to the catchment id. When the isochrone layer used a non-NPI
#' provider id (e.g. `coord_id`), pass a `crosswalk` data frame mapping `npi` to
#' `catchment`. Calls whose NPI is missing or matches no catchment are retained
#' with `matched = FALSE` and `NA` access, so the match rate is auditable rather
#' than silently dropped.
#'
#' @param lizeth_access Call-level records from [prepare_lizeth_access()], with at
#'   least `npi` and `wait_business_days`.
#' @param catchments Provider-catchment table from
#'   [e2sfca_catchments_from_access()].
#' @param crosswalk Optional data frame with columns `npi` and `catchment` mapping
#'   Lizeth NPIs to catchment ids. When `NULL` (default), `npi` is matched
#'   directly against `catchment`.
#' @param npi_col Name of the NPI column in `lizeth_access`. Default `"npi"`.
#'
#' @return `lizeth_access` with added columns `catchment`, `accessible_capacity`,
#'   `demand_workload`, `adequacy_relative`, and `matched` (logical). The overall
#'   match rate is attached as attribute `match_rate`.
#'
#' @concept calibration
#' @family isochrone access response
#' @export
join_lizeth_to_catchments <- function(lizeth_access, catchments,
                                      crosswalk = NULL, npi_col = "npi") {
  base::message("Joining Lizeth calls to provider-catchments by NPI.")
  if (!base::is.data.frame(lizeth_access)) {
    base::stop("`lizeth_access` must be a data frame.", call. = FALSE)
  }
  if (!npi_col %in% base::names(lizeth_access)) {
    base::stop("`lizeth_access` has no `", npi_col, "` column.", call. = FALSE)
  }
  if (!base::is.data.frame(catchments) ||
      !all(c("catchment", "accessible_capacity", "demand_workload") %in%
           base::names(catchments))) {
    base::stop(
      "`catchments` must come from e2sfca_catchments_from_access() ",
      "(columns catchment, accessible_capacity, demand_workload).",
      call. = FALSE
    )
  }

  call_npi <- base::as.character(lizeth_access[[npi_col]])
  if (base::is.null(crosswalk)) {
    catchment_id <- call_npi
  } else {
    if (!all(c("npi", "catchment") %in% base::names(crosswalk))) {
      base::stop("`crosswalk` must have columns `npi` and `catchment`.",
                 call. = FALSE)
    }
    map <- stats::setNames(
      base::as.character(crosswalk$catchment),
      base::as.character(crosswalk$npi)
    )
    catchment_id <- unname(map[call_npi])
  }

  catchment_lookup <- catchments[
    !base::duplicated(base::as.character(catchments$catchment)), ,
    drop = FALSE
  ]
  rownames_key <- base::as.character(catchment_lookup$catchment)
  idx <- base::match(catchment_id, rownames_key)

  out <- lizeth_access
  out$catchment <- catchment_id
  out$accessible_capacity <- catchment_lookup$accessible_capacity[idx]
  out$demand_workload <- catchment_lookup$demand_workload[idx]
  out$adequacy_relative <- catchment_lookup$adequacy_relative[idx]
  out$matched <- !base::is.na(idx)

  match_rate <- base::mean(out$matched)
  base::message(
    "Matched ", .iaccess_comma(base::sum(out$matched)), " of ",
    .iaccess_comma(base::nrow(out)), " calls to a catchment (",
    base::formatC(100 * match_rate, format = "f", digits = 1), "%)."
  )
  base::attr(out, "match_rate") <- match_rate
  out
}

.iaccess_comma <- function(x) {
  base::format(x, big.mark = ",", trim = TRUE, scientific = FALSE)
}
