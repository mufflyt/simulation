# Fit the URPS wait response from drive-time isochrones + fielded Lizeth waits.
#
# THE PIPELINE THIS WIRES (Stages 0-3a of the isochrone access-response plan):
#   0. Provenance-check the canonical isochrone artifacts (fail-closed).
#   1. E2SFCA per-provider ratios -> provider-catchment loads
#      (e2sfca_catchments_from_access): demand_workload + accessible_capacity in
#      clear_access() currency, carrying the isochrone-derived adequacy dispersion.
#   2. Attach each fielded Lizeth wait to the provider that was called, by NPI
#      (join_lizeth_to_catchments): the paired (local access, realized wait) set.
#   3a. Fit the wait-response constant wait_scale closed-form (fit_wait_scale)
#       against the Lizeth national median wait.
#
# STILL TO COME (builds #3-#4), deliberately NOT done here:
#   3b. Fit the single decay parameter sigma (gaussian_band_weights) in an outer
#       loop around Stages 1-3a, scored on the paired per-provider waits.
#   4.  Guard the identified base-year adequacy with geographic_holdout_cv
#       (leave-one-region-out) before letting capacity_status() resolve.
#
# ANCHOR: accessible_capacity is realized Medicare procedure volume for the fem65
# population -- same denominator as demand, observed not modeled. Supply it as the
# `supply` column of the E2SFCA input (provider_supply below).
#
# Run from the package root, where the isochrone .rds and ../lizeth are reachable:
#   SIMULATION_ISOCHRONE_ROOT=/path/to/isochrones \
#     Rscript scripts/fit_isochrone_access_response.R

pkgload::load_all(".", quiet = TRUE)

# ---- Stage 0: provenance ----------------------------------------------------
# Fail-closed against ISOCHRONE_CANONICAL_RUN_ID + per-band SHA-256. In strict
# reproducibility mode a mismatch stops here rather than fitting on the wrong run.
isochrone_report <- assert_canonical_isochrones()
base::message("Isochrone run verified: ", isochrone_report$run_id)

# ---- Stage 1: E2SFCA -> provider-catchment loads ----------------------------
# Three inputs must be present (bind them before sourcing, or point the loads at
# real artifacts). The FIT path needs compute_e2sfca_access() directly, because
# it exposes the per-provider ratios; run_geographic_access() is the tract-surface
# orchestrator for the downstream (cliff) hand-off, a different output.
#
#   membership : (demand_id, provider_id, band) from
#                scripts/data_acquisition/12_build_provider_isochrone_membership.R
#   provider_supply : (provider_id, supply) -- realized Medicare fem65 procedure
#                     volume per provider (the capacity anchor)
#   tract_demand    : (demand_id, population) -- fem65 population per demand tract
#                     (isochrone_demand_from_tracts() allocates this)
if (!all(base::vapply(c("membership", "provider_supply", "tract_demand"),
                      base::exists, logical(1)))) {
  base::stop(
    "Bind `membership` (demand_id/provider_id/band), `provider_supply` ",
    "(provider_id/supply = Medicare fem65 volume), and `tract_demand` ",
    "(demand_id/population) before sourcing. These are the real pipeline inputs; ",
    "the fit does not invent them.",
    call. = FALSE
  )
}

e2sfca <- compute_e2sfca_access(
  membership = membership,
  supply = provider_supply,
  demand = tract_demand
)
catchments <- e2sfca_catchments_from_access(
  e2sfca,
  workload_per_capita = 1,
  capacity_anchor = "medicare_procedure_volume_fem65"
)

# ---- Stage 2: NPI-join the fielded Lizeth waits -----------------------------
lizeth_calibration <- build_lizeth_access_anchor(lizeth_dir = "../lizeth")
lizeth_joined <- join_lizeth_to_catchments(
  lizeth_access = lizeth_calibration$calls,
  catchments = catchments
)
base::message(
  "Lizeth<->catchment match rate: ",
  base::formatC(100 * base::attr(lizeth_joined, "match_rate"),
                format = "f", digits = 1), "%"
)

# ---- Stage 3a: fit wait_scale closed-form -----------------------------------
observed_median_wait <- stats::median(
  lizeth_joined$wait_business_days[lizeth_joined$matched],
  na.rm = TRUE
)
wait_scale_fit <- fit_wait_scale(
  catchments = catchments,
  observed_wait = observed_median_wait
)
base::cat("Fitted wait_scale (fixed decay):", wait_scale_fit$wait_scale, "\n")

# ---- Stage 3b: fit the decay parameter sigma (build #3) ----------------------
# The catchments depend on sigma through the Gaussian band weights, so the fit
# recomputes E2SFCA at each candidate sigma. wait_scale is refit closed-form
# inside every evaluation; sigma is the single shape parameter.
bands <- e2sfca_bands()
catchments_for_sigma <- function(sigma) {
  e2sfca_catchments_from_access(
    compute_e2sfca_access(
      membership = membership,
      supply = provider_supply,
      demand = tract_demand,
      weights = gaussian_band_weights(bands = bands, sigma = sigma)
    ),
    workload_per_capita = 1
  )
}
sigma_fit <- fit_decay_sigma(
  lizeth_access = lizeth_calibration$calls,
  catchments_for_sigma = catchments_for_sigma,
  sigma_bounds = c(15, 240),
  bands = bands
)
base::cat(sigma_fit$summary_sentence, "\n")

# ---- Stage 4: geographic holdout + resolve base-year adequacy (build #4) -----
# Attach each fielded wait to its catchment at the FITTED sigma, then leave one
# region (state) out at a time to test that the response transports.
fitted_catchments <- catchments_for_sigma(sigma_fit$sigma)
response_table <- join_lizeth_to_catchments(
  lizeth_access = lizeth_calibration$calls,
  catchments = fitted_catchments
)
response_table <- response_table[response_table$matched %in% TRUE, , drop = FALSE]

holdout <- wait_response_region_holdout(
  response_table = response_table,
  region_col = "state"
)
capacity <- capacity_status_with_isochrone_response(
  sigma_fit = sigma_fit,
  holdout = holdout
)

base::cat("Out-of-sample calibration slope:",
          holdout$metrics$calibration_slope, "\n")
base::cat("Out-of-sample R2:", holdout$metrics$r2_oos, "\n")
base::cat("Base-year adequacy resolved:", capacity$resolved, "\n")
base::cat("Calibration status:", capacity$calibration_status, "\n")
if (!isTRUE(capacity$resolved)) base::cat("Why unresolved:", capacity$why_unresolved, "\n")

# ---- Stage 5: export the tract access surface for cliff Module D v2 ----------
# Ship the tract-level E2SFCA surface (recomputed at the fitted sigma) with the
# fit provenance, so cliff's read_access_surface() / Module D v2 can consume it.
final_e2 <- compute_e2sfca_access(
  membership = membership, supply = provider_supply, demand = tract_demand,
  weights = gaussian_band_weights(bands = bands, sigma = sigma_fit$sigma))
export_access_surface(
  final_e2, output_directory = "outputs/access_response",
  sigma_fit = sigma_fit, capacity = capacity,
  isochrone_run_id = isochrone_report$run_id, allow_unvalidated = TRUE)
