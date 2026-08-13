# Inverse-calibrate base-year URPS adequacy to observed Lizeth wait times.
#
# The forward map (clear_access, R/geography-access_clearing.R) is now tested, so
# this is a legitimate inverse problem: for unsaturated catchments
# rho_i = D_i/C_i = 1/A_i and W_i = k * rho_i/(1 - rho_i). This fits a lognormal
# catchment adequacy distribution (national mean + between-catchment
# heterogeneity) to Lizeth's p25/median/p75, then bootstraps by physician.
#
# PRIMARY ANALYSIS: wait_scale is FIXED. Estimating wait_scale and adequacy from
# the same three wait quantiles is not identified -- low adequacy + small
# wait_scale mimics higher adequacy + larger wait_scale -- so joint fitting is a
# sensitivity analysis only, never the headline number.
#
# Run from the package root (needs a ../lizeth checkout):
#   Rscript scripts/fit_lizeth_inverse_adequacy.R

pkgload::load_all(".", quiet = TRUE)

lizeth_calibration <- build_lizeth_access_anchor(
  lizeth_dir = "../lizeth"
)

# Base-year access-clearing catchments: one row per catchment with a positive
# demand_workload, from the geography demand chain (isochrone_demand_from_tracts
# / compute_e2sfca_access). Supply your real catchment panel as `catchment_panel`
# before sourcing; the synthetic fallback below only makes the script runnable as
# a demonstration and is NOT a data source.
if (base::exists("catchment_panel")) {
  base_year_catchments <- catchment_panel |>
    dplyr::filter(year == 2026) |>
    dplyr::filter(
      base::is.finite(demand_workload),
      demand_workload > 0
    )
} else {
  base::message("`catchment_panel` not found; using a synthetic illustrative panel.")
  base_year_catchments <- tibble::tibble(
    catchment = base::paste0("c", base::seq_len(300)),
    demand_workload = base::rep(100, 300)
  )
}

inverse_fit <- fit_lizeth_inverse_adequacy(
  catchments = base_year_catchments,
  lizeth_calls = lizeth_calibration$calls,
  wait_scale = 30,
  initial_mean = REFERENCE_ADEQUACY_CALIBRATION,
  initial_log_sd = 0.20,
  reference_adequacy = REFERENCE_ADEQUACY_CALIBRATION,
  weight_col = "demand_workload",
  wait_ceiling = 365
)
print(inverse_fit$comparison)
base::cat("Fitted mean adequacy:", inverse_fit$mean_adequacy, "\n")
base::cat("Ratio to 0.948 reference:", inverse_fit$ratio_to_reference, "\n")
base::cat(inverse_fit$summary_sentence, "\n")

# Uncertainty, clustered by physician/NPI (multiple calls per office).
inverse_bootstrap <- bootstrap_lizeth_inverse_adequacy(
  catchments = base_year_catchments,
  lizeth_calls = lizeth_calibration$calls,
  wait_scale = 30,
  n_boot = 1000L,
  seed = 20260812L,
  initial_mean = REFERENCE_ADEQUACY_CALIBRATION,
  initial_log_sd = 0.20,
  reference_adequacy = REFERENCE_ADEQUACY_CALIBRATION,
  weight_col = "demand_workload",
  wait_ceiling = 365
)
print(inverse_bootstrap$interval)
base::cat(
  "P(mean adequacy < 0.948 reference):",
  inverse_bootstrap$probability_below_reference, "\n"
)
base::cat(inverse_bootstrap$summary_sentence, "\n")
