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

#' Closed-form wait-response fit and loss for NPI-joined Lizeth calls
#'
#' Given calls already joined to their provider-catchment
#' ([join_lizeth_to_catchments()]), fits the single wait-response constant
#' `wait_scale` by least squares through the origin and returns the residual sum
#' of squares. This is the per-provider objective that identifies the decay
#' parameter in [fit_decay_sigma()]: the queue map `wait = wait_scale * rho/(1-rho)`
#' is linear in `wait_scale`, so for a fixed catchment structure the best
#' `wait_scale` and its error are closed-form.
#'
#' @details
#' For each matched call, `rho = demand_workload / accessible_capacity` and
#' `x = rho / (1 - rho)`. Only unsaturated calls (`0 <= rho < 1`, finite wait)
#' enter the fit; saturated calls (`rho >= 1`, infinite modelled wait) are counted
#' but excluded, mirroring [fit_wait_scale()]. The weighted least-squares estimate
#' through the origin is `wait_scale = sum(w * W * x) / sum(w * x^2)`, and the loss
#' is `sum(w * (W - wait_scale * x)^2)`. Raw (not log) waits keep the estimate
#' closed-form; the reference-adequacy inverse fit, which matches log-wait
#' quantiles, is a separate step.
#'
#' @param response_table Matched call-level rows with `wait_col`,
#'   `demand_workload` and `accessible_capacity` (from
#'   [join_lizeth_to_catchments()], filtered to `matched`).
#' @param wait_col Observed wait column. Default `"wait_business_days"`.
#' @param weight_col Optional per-call weight column (e.g. an ascertainment
#'   weight). `NULL` (default) weights every call equally.
#' @param wait_ceiling Retained for interface symmetry with [clear_access()]; the
#'   loss excludes saturated calls rather than censoring them. Default `365`.
#'
#' @return A list: `wait_scale` (fitted constant), `sse`, `rmse`, `n_used`
#'   (unsaturated calls fitted), and `n_censored` (saturated calls excluded).
#'
#' @concept calibration
#' @family isochrone access response
#' @export
lizeth_wait_response_loss <- function(response_table,
                                      wait_col = "wait_business_days",
                                      weight_col = NULL,
                                      wait_ceiling = 365) {
  needed <- c(wait_col, "demand_workload", "accessible_capacity")
  missing_cols <- base::setdiff(needed, base::names(response_table))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "`response_table` missing column(s): ",
      base::paste(missing_cols, collapse = ", "),
      ". Pass matched rows from join_lizeth_to_catchments().",
      call. = FALSE
    )
  }
  wait <- base::as.numeric(response_table[[wait_col]])
  capacity <- base::as.numeric(response_table$accessible_capacity)
  workload <- base::as.numeric(response_table$demand_workload)
  rho <- workload / capacity
  weight <- if (base::is.null(weight_col)) {
    base::rep(1, base::length(wait))
  } else {
    base::as.numeric(response_table[[weight_col]])
  }

  unsaturated <- base::is.finite(rho) & rho >= 0 & rho < 1 &
    base::is.finite(wait) & base::is.finite(weight)
  n_censored <- base::sum(base::is.finite(rho) & rho >= 1, na.rm = TRUE)
  if (base::sum(unsaturated) < 2L) {
    base::stop(
      "Need at least two unsaturated matched calls (0 <= rho < 1) to fit ",
      "the wait response; got ", base::sum(unsaturated), ".",
      call. = FALSE
    )
  }

  x <- rho[unsaturated] / (1 - rho[unsaturated])
  w <- weight[unsaturated]
  y <- wait[unsaturated]
  denom <- base::sum(w * x * x)
  if (!base::is.finite(denom) || denom <= 0) {
    base::stop(
      "Degenerate wait-response fit: no positive access variation among ",
      "matched calls (sum(w * x^2) = 0).",
      call. = FALSE
    )
  }
  wait_scale <- base::sum(w * y * x) / denom
  predicted <- wait_scale * x
  sse <- base::sum(w * (y - predicted)^2)
  rmse <- base::sqrt(sse / base::sum(w))

  base::list(
    wait_scale = wait_scale,
    sse = sse,
    rmse = rmse,
    n_used = base::sum(unsaturated),
    n_censored = n_censored
  )
}

#' Fit the E2SFCA decay parameter sigma to fielded Lizeth waits
#'
#' Fits the single Gaussian decay parameter `sigma` (see [gaussian_band_weights()])
#' by minimising the per-provider wait-response loss over the drive-time bands:
#' the sigma whose isochrone-derived access, pushed through the queue, best
#' predicts the observed Lizeth waits. `wait_scale` is refit closed-form at every
#' candidate sigma ([lizeth_wait_response_loss()]), so this is a clean 1-D search,
#' not a joint optimisation -- one shape parameter, identified by the curvature of
#' wait against access, with the level absorbed by `wait_scale`.
#'
#' @details
#' `catchments_for_sigma` is a function of one argument, `sigma`, returning the
#' provider-catchment table ([e2sfca_catchments_from_access()]) computed with
#' `gaussian_band_weights(bands, sigma)`. Injecting it keeps this optimiser
#' independent of the spatial recompute: the standard closure (recompute E2SFCA
#' per sigma) is built in the runner, while tests supply a lightweight map. Each
#' evaluation joins the Lizeth calls to that sigma's catchments and scores the
#' unsaturated pairs; folds with fewer than two usable pairs score `Inf` so the
#' search avoids degenerate regions. The SSE surface has a sharp global well on
#' an otherwise flat plateau, so a coarse grid of `n_grid` points brackets the
#' minimum and [stats::optimize()] refines within one grid step of the best
#' point.
#'
#' @param lizeth_access Call-level records with `npi_col` and `wait_col` (from
#'   [prepare_lizeth_access()]).
#' @param catchments_for_sigma Function mapping a scalar `sigma` to a
#'   provider-catchment table (see Details).
#' @param sigma_bounds Length-2 positive numeric search interval for `sigma`, in
#'   minutes. Default `c(15, 240)`.
#' @param wait_col,npi_col,weight_col,wait_ceiling Passed to
#'   [join_lizeth_to_catchments()] / [lizeth_wait_response_loss()].
#' @param bands Optional band vector; when supplied, the fitted weights
#'   `gaussian_band_weights(bands, sigma)` are returned. Default `NULL`.
#' @param n_grid Number of points in the coarse grid scanned across
#'   `sigma_bounds` before refinement. Must be at least 3. Default `25L`.
#'
#' @return A list: `sigma` (fitted), `wait_scale` (at the fitted sigma), `sse`,
#'   `rmse`, `n_pairs` (unsaturated matched calls), `weights` (if `bands` given),
#'   `calibration_status = "fitted_to_lizeth_wait_response"`, and a
#'   `summary_sentence`.
#'
#' @concept calibration
#' @family isochrone access response
#' @export
fit_decay_sigma <- function(lizeth_access, catchments_for_sigma,
                            sigma_bounds = c(15, 240),
                            wait_col = "wait_business_days",
                            npi_col = "npi", weight_col = NULL,
                            wait_ceiling = 365, bands = NULL, n_grid = 25L) {
  base::message("Fitting the E2SFCA decay parameter sigma to Lizeth waits.")
  if (!base::is.function(catchments_for_sigma)) {
    base::stop("`catchments_for_sigma` must be a function of one argument (sigma).",
               call. = FALSE)
  }
  if (!(base::is.numeric(sigma_bounds) && base::length(sigma_bounds) == 2L &&
        all(base::is.finite(sigma_bounds)) && all(sigma_bounds > 0) &&
        sigma_bounds[1] < sigma_bounds[2])) {
    base::stop("`sigma_bounds` must be two increasing positive numbers.",
               call. = FALSE)
  }
  if (!(base::is.numeric(n_grid) && n_grid >= 3)) {
    base::stop("`n_grid` must be at least 3.", call. = FALSE)
  }

  score_at <- function(sigma) {
    catchments <- catchments_for_sigma(sigma)
    joined <- join_lizeth_to_catchments(lizeth_access, catchments,
                                        npi_col = npi_col)
    matched <- joined[joined$matched %in% TRUE, , drop = FALSE]
    if (base::nrow(matched) < 2L) {
      return(base::list(sse = Inf, fit = NULL))
    }
    loss <- base::tryCatch(
      lizeth_wait_response_loss(matched, wait_col = wait_col,
                                weight_col = weight_col,
                                wait_ceiling = wait_ceiling),
      error = function(e) NULL
    )
    if (base::is.null(loss)) base::list(sse = Inf, fit = NULL)
    else base::list(sse = loss$sse, fit = loss)
  }

  # Grid-bracket THEN refine. The SSE surface has a sharp global well (a good
  # sigma fits closely, a wrong one badly) on an otherwise flat plateau, so a
  # bare optimize() walks past the minimum. A coarse grid brackets it; optimize()
  # then refines inside one grid step of the best point.
  grid <- base::seq(sigma_bounds[1], sigma_bounds[2],
                    length.out = base::as.integer(n_grid))
  grid_sse <- base::vapply(grid, function(s) score_at(s)$sse, base::numeric(1))
  if (!base::any(base::is.finite(grid_sse))) {
    base::stop(
      "Decay fit found no sigma with >=2 usable matched calls in ",
      "[", sigma_bounds[1], ", ", sigma_bounds[2], "].",
      call. = FALSE
    )
  }
  best_i <- base::which.min(grid_sse)
  step <- (sigma_bounds[2] - sigma_bounds[1]) / (base::as.integer(n_grid) - 1L)
  lo <- base::max(sigma_bounds[1], grid[best_i] - step)
  hi <- base::min(sigma_bounds[2], grid[best_i] + step)
  opt <- stats::optimize(function(s) score_at(s)$sse, interval = c(lo, hi))
  sigma_hat <- if (base::is.finite(opt$objective) &&
                   opt$objective <= grid_sse[best_i]) {
    opt$minimum
  } else {
    grid[best_i]
  }
  best <- score_at(sigma_hat)
  if (base::is.null(best$fit)) {
    base::stop(
      "Decay fit did not find a sigma with >=2 usable matched calls in ",
      "[", sigma_bounds[1], ", ", sigma_bounds[2], "].",
      call. = FALSE
    )
  }

  weights <- if (!base::is.null(bands)) {
    gaussian_band_weights(bands = bands, sigma = sigma_hat)
  } else {
    NULL
  }
  summary_sentence <- base::sprintf(
    base::paste(
      "Fitted Gaussian decay sigma = %.1f min and wait_scale = %.2f to %s",
      "unsaturated Lizeth calls (RMSE %.2f business days)."
    ),
    sigma_hat, best$fit$wait_scale, .iaccess_comma(best$fit$n_used),
    best$fit$rmse
  )
  base::message(summary_sentence)

  base::list(
    sigma = sigma_hat,
    wait_scale = best$fit$wait_scale,
    sse = best$fit$sse,
    rmse = best$fit$rmse,
    n_pairs = best$fit$n_used,
    weights = weights,
    calibration_status = "fitted_to_lizeth_wait_response",
    summary_sentence = summary_sentence
  )
}

#' Leave-one-region-out holdout of the fitted wait response
#'
#' Cross-validates the fitted access -> wait response across geography using
#' [geographic_holdout_cv()] with `scheme = "region"`: `wait_scale` is refit on
#' the training regions and used to predict the held-out region's waits, so the
#' score is genuinely out-of-sample along a dimension that played no part in the
#' fit. This is the guard against a response overfit to the spatial sample before
#' it is allowed to resolve base-year adequacy.
#'
#' @details
#' Each matched call contributes its access term `x = rho / (1 - rho)`
#' (`rho = demand_workload / accessible_capacity`); saturated calls (`rho >= 1`)
#' are excluded. The per-fold model is the closed-form origin least-squares
#' `wait_scale = sum(x * wait) / sum(x^2)` fit on training calls and applied to
#' the held-out region's `x`. `sigma` is held at the value used to build
#' `response_table`, so this tests transportability of the response given the
#' fitted decay, not a per-fold refit of the decay itself.
#'
#' @param response_table Matched call-level rows with `wait_col`,
#'   `demand_workload`, `accessible_capacity` and `region_col` (join the Lizeth
#'   `state`/region onto the output of [join_lizeth_to_catchments()]).
#' @param wait_col Observed wait column. Default `"wait_business_days"`.
#' @param region_col Region grouping column for the leave-one-region-out folds.
#'   Default `"region"`.
#' @param geo_col Optional per-call id column for labelling. Default `NULL`.
#' @param min_regions Minimum distinct regions required to run the holdout.
#'   Default `4`.
#' @param seed Passed through to [geographic_holdout_cv()] (unused by the region
#'   scheme, which is deterministic). Default `NULL`.
#'
#' @return The [geographic_holdout_cv()] result (`predictions`, `metrics`,
#'   `scheme`, `family`) with added `n_regions` and `n_calls`.
#'
#' @concept validation
#' @family isochrone access response
#' @export
wait_response_region_holdout <- function(response_table,
                                         wait_col = "wait_business_days",
                                         region_col = "region", geo_col = NULL,
                                         min_regions = 4L, seed = NULL) {
  base::message("Cross-validating the wait response across held-out regions.")
  needed <- c(wait_col, "demand_workload", "accessible_capacity", region_col)
  missing_cols <- base::setdiff(needed, base::names(response_table))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "`response_table` missing column(s): ",
      base::paste(missing_cols, collapse = ", "),
      ". Join the Lizeth region onto the matched catchment rows first.",
      call. = FALSE
    )
  }
  wait <- base::as.numeric(response_table[[wait_col]])
  rho <- base::as.numeric(response_table$demand_workload) /
    base::as.numeric(response_table$accessible_capacity)
  region <- base::as.character(response_table[[region_col]])
  keep <- base::is.finite(rho) & rho >= 0 & rho < 1 &
    base::is.finite(wait) & !base::is.na(region)

  df <- base::data.frame(
    wait = wait[keep],
    x = rho[keep] / (1 - rho[keep]),
    region = region[keep],
    stringsAsFactors = FALSE
  )
  base::names(df)[1] <- wait_col
  if (!base::is.null(geo_col)) df$geo <- base::as.character(response_table[[geo_col]])[keep]

  n_regions <- base::length(base::unique(df$region))
  if (n_regions < min_regions) {
    base::stop(
      "Need at least ", min_regions, " distinct regions for a region holdout; ",
      "got ", n_regions, ".",
      call. = FALSE
    )
  }

  # Per-fold model: closed-form origin least-squares wait_scale on train,
  # applied to the held-out region's access. Never looks at test$wait.
  fit_predict <- function(train, test) {
    k <- base::sum(train$x * train[[wait_col]]) / base::sum(train$x * train$x)
    k * test$x
  }

  hold <- geographic_holdout_cv(
    data = df, observed = wait_col, predictors = "x",
    region = "region", geo = if (!base::is.null(geo_col)) "geo" else NULL,
    scheme = "region", family = "gaussian",
    fit_predict = fit_predict, seed = seed
  )
  hold$n_regions <- n_regions
  hold$n_calls <- base::nrow(df)
  base::message(
    "Held out ", n_regions, " regions over ", .iaccess_comma(base::nrow(df)),
    " calls; out-of-sample calibration slope ",
    base::formatC(hold$metrics$calibration_slope, format = "f", digits = 2),
    ", R2 ", base::formatC(hold$metrics$r2_oos, format = "f", digits = 2), "."
  )
  hold
}

#' Resolve base-year capacity status from the validated isochrone wait response
#'
#' Applies the geographic-holdout verdict to the base-year capacity anchor: only
#' if the fitted wait response transports out-of-sample across held-out regions
#' (calibration slope near 1 and non-negative out-of-sample R-squared) is
#' `resolved` set `TRUE`. Otherwise the anchor stays unresolved with the reason,
#' exactly as [capacity_status_with_lizeth()] keeps it unresolved for a measured
#' input without a validated response.
#'
#' @param sigma_fit The [fit_decay_sigma()] result (`sigma`, `wait_scale`).
#' @param holdout The [wait_response_region_holdout()] result.
#' @param slope_tol Allowed absolute deviation of the out-of-sample calibration
#'   slope from 1. Default `0.25`.
#' @param min_r2_oos Minimum out-of-sample R-squared to accept. Default `0`.
#' @param base_status Starting status list; `NULL` (default) calls
#'   [capacity_status()].
#'
#' @return The status list with `resolved`, the fitted `sigma`/`wait_scale`, the
#'   holdout metrics, a `calibration_status`, and (when unresolved) a
#'   `why_unresolved` naming the failed criterion.
#'
#' @concept calibration
#' @family isochrone access response
#' @export
capacity_status_with_isochrone_response <- function(sigma_fit, holdout,
                                                    slope_tol = 0.25,
                                                    min_r2_oos = 0,
                                                    base_status = NULL) {
  base::message("Resolving base-year capacity from the isochrone wait response.")
  if (!all(c("sigma", "wait_scale") %in% base::names(sigma_fit))) {
    base::stop("`sigma_fit` must be a fit_decay_sigma() result.", call. = FALSE)
  }
  if (base::is.null(holdout$metrics)) {
    base::stop("`holdout` must be a wait_response_region_holdout() result.",
               call. = FALSE)
  }
  status <- if (base::is.null(base_status)) capacity_status() else base_status
  m <- holdout$metrics
  slope_ok <- base::is.finite(m$calibration_slope) &&
    base::abs(m$calibration_slope - 1) <= slope_tol
  r2_ok <- base::is.finite(m$r2_oos) && m$r2_oos >= min_r2_oos
  transported <- slope_ok && r2_ok

  status$resolved <- base::isTRUE(transported)
  status$access_response_source <-
    "isochrone drive-time + fielded Lizeth waits"
  status$fitted_sigma <- sigma_fit$sigma
  status$fitted_wait_scale <- sigma_fit$wait_scale
  status$holdout_calibration_slope <- m$calibration_slope
  status$holdout_r2_oos <- m$r2_oos
  status$holdout_regions <- holdout$n_regions
  status$calibration_status <- if (transported) {
    "fitted_and_geographically_validated"
  } else {
    "fitted_but_not_transportable"
  }
  if (!transported) {
    reasons <- c(
      if (!slope_ok) base::sprintf(
        "out-of-sample calibration slope %.2f outside 1 +/- %.2f",
        m$calibration_slope, slope_tol),
      if (!r2_ok) base::sprintf(
        "out-of-sample R2 %.2f below %.2f", m$r2_oos, min_r2_oos)
    )
    status$why_unresolved <- base::paste(
      "The fitted wait response did not transport across held-out regions:",
      base::paste(reasons, collapse = "; "),
      "-- base-year adequacy stays unresolved."
    )
    base::message(status$why_unresolved)
  } else {
    base::message(
      "Base-year adequacy resolved: response transports out-of-sample ",
      "(slope ", base::formatC(m$calibration_slope, format = "f", digits = 2),
      ", R2 ", base::formatC(m$r2_oos, format = "f", digits = 2), ")."
    )
  }
  status
}

.iaccess_comma <- function(x) {
  base::format(x, big.mark = ",", trim = TRUE, scientific = FALSE)
}
