#!/usr/bin/env Rscript
# CI entry point: run the isochrone access-response pipeline on staged real data.
#
#   Rscript scripts/ci/run_access_response.R
#
# This is the non-interactive sibling of scripts/fit_isochrone_access_response.R.
# The interactive runner expects the inputs bound as R objects; this one reads
# them from files staged by the fit-access-response.yaml workflow (pulled from
# S3), so a `workflow_dispatch` run is fully reproducible from three artifacts.
#
# INPUTS (paths via environment variables, with defaults):
#   ISOCHRONE_ROOT        dir with isochrones_{30,60,120,180}min_consolidated.rds
#                         + ISOCHRONE_REGISTRY.json   [SIMULATION_ISOCHRONE_ROOT]
#   MEMBERSHIP_RDS        (demand_id, provider_id, band) from script 12
#   PROVIDER_SUPPLY_CSV   (provider_id, supply)  -- Medicare fem65 procedure volume
#   TRACT_DEMAND_CSV      (demand_id, population) -- fem65 population per tract
#   LIZETH_DIR            dir holding the labeled REDCap export CSV
#   OUT_DIR               where results are written        [outputs/access_response]
#
# It fails LOUDLY on any missing file or column: the first dispatch is meant to
# be diagnostic, not to produce a silently wrong number. It writes nothing to the
# repo tree except OUT_DIR.

suppressWarnings(suppressMessages(pkgload::load_all(".", quiet = TRUE)))

`%||%` <- function(a, b) if (is.null(a) || !nzchar(a)) b else a

membership_rds <- Sys.getenv("MEMBERSHIP_RDS",
                             "data-raw/spatial/provider_isochrone_membership.rds")
supply_csv     <- Sys.getenv("PROVIDER_SUPPLY_CSV",
                             "data-raw/spatial/provider_supply.csv")
tract_csv      <- Sys.getenv("TRACT_DEMAND_CSV",
                             "data-raw/spatial/tract_fem65_demand.csv")
lizeth_dir     <- Sys.getenv("LIZETH_DIR", "../lizeth")
out_dir        <- Sys.getenv("OUT_DIR", "outputs/access_response")

.need_file <- function(path, what) {
  if (!file.exists(path)) {
    stop(sprintf("%s not found at '%s'. Stage it before dispatching.", what, path),
         call. = FALSE)
  }
  path
}
.need_cols <- function(df, cols, what) {
  miss <- setdiff(cols, names(df))
  if (length(miss)) {
    stop(sprintf("%s missing column(s): %s. Present: %s.", what,
                 paste(miss, collapse = ", "), paste(names(df), collapse = ", ")),
         call. = FALSE)
  }
  df
}

# ---- Stage 0: provenance (fail-closed against the canonical run) -------------
message("Stage 0: verifying canonical isochrone artifacts.")
iso_report <- assert_canonical_isochrones()
message("  isochrone run verified: ", iso_report$run_id)

# ---- Read staged inputs -----------------------------------------------------
message("Reading staged inputs.")
membership <- readRDS(.need_file(membership_rds, "Membership table"))
membership <- .need_cols(as.data.frame(membership),
                         c("demand_id", "provider_id", "band"), "Membership")

provider_supply <- utils::read.csv(.need_file(supply_csv, "Provider supply"),
                                   stringsAsFactors = FALSE)
provider_supply <- .need_cols(provider_supply, c("provider_id", "supply"),
                              "Provider supply")
provider_supply$provider_id <- as.character(provider_supply$provider_id)

tract_demand <- utils::read.csv(.need_file(tract_csv, "Tract demand"),
                                stringsAsFactors = FALSE)
tract_demand <- .need_cols(tract_demand, c("demand_id", "population"),
                           "Tract demand")

# ---- Stages 1-2 wiring: catchments(sigma) + Lizeth ingest -------------------
bands <- e2sfca_bands()
catchments_for_sigma <- function(sigma) {
  e2sfca_catchments_from_access(
    compute_e2sfca_access(
      membership = membership, supply = provider_supply, demand = tract_demand,
      weights = gaussian_band_weights(bands = bands, sigma = sigma)
    ),
    workload_per_capita = 1
  )
}

message("Ingesting the fielded Lizeth export.")
lizeth <- build_lizeth_access_anchor(lizeth_dir = lizeth_dir)
lizeth_calls <- lizeth$calls
if (!"state" %in% names(lizeth_calls)) {
  stop("Lizeth calls carry no `state` column for the region holdout.",
       call. = FALSE)
}

# ---- Stage 3: fit sigma + wait_scale ----------------------------------------
message("Stage 3: fitting the decay parameter sigma and wait_scale.")
sigma_fit <- fit_decay_sigma(
  lizeth_access = lizeth_calls, catchments_for_sigma = catchments_for_sigma,
  sigma_bounds = c(15, 240), bands = bands
)
message("  ", sigma_fit$summary_sentence)

# ---- Stage 4: region holdout + resolve --------------------------------------
message("Stage 4: leave-one-region-out holdout and resolution.")
fitted_catchments <- catchments_for_sigma(sigma_fit$sigma)
response_table <- join_lizeth_to_catchments(lizeth_calls, fitted_catchments)
response_table <- response_table[response_table$matched %in% TRUE, , drop = FALSE]

holdout <- wait_response_region_holdout(response_table, region_col = "state")
capacity <- capacity_status_with_isochrone_response(sigma_fit, holdout)

# ---- Stage 5: export the tract access surface for cliff Module D v2 ----------
# The tract-level surface (not the provider-catchment fit table) is what cliff
# consumes; recompute it at the fitted sigma and ship it with fit provenance.
# allow_unvalidated = TRUE so the artifact is always emitted with its honest
# calibration_status; the downstream consumer gates on it.
message("Stage 5: exporting the tract-level access surface.")
final_e2 <- compute_e2sfca_access(
  membership = membership, supply = provider_supply, demand = tract_demand,
  weights = gaussian_band_weights(bands = bands, sigma = sigma_fit$sigma))
export_access_surface(
  final_e2, output_directory = out_dir, sigma_fit = sigma_fit, capacity = capacity,
  isochrone_run_id = iso_report$run_id, allow_unvalidated = TRUE)

# ---- Write results ----------------------------------------------------------
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
summary_out <- list(
  isochrone_run_id = iso_report$run_id,
  sigma = sigma_fit$sigma,
  wait_scale = sigma_fit$wait_scale,
  n_pairs = sigma_fit$n_pairs,
  band_weights = as.list(sigma_fit$weights),
  match_rate = attr(response_table, "match_rate"),
  holdout_regions = holdout$n_regions,
  holdout_calibration_slope = holdout$metrics$calibration_slope,
  holdout_r2_oos = holdout$metrics$r2_oos,
  holdout_mape = holdout$metrics$mape,
  base_year_resolved = capacity$resolved,
  calibration_status = capacity$calibration_status,
  why_unresolved = capacity$why_unresolved %||% NA_character_
)
jsonlite::write_json(summary_out, file.path(out_dir, "access_response_fit.json"),
                     auto_unbox = TRUE, pretty = TRUE, na = "string")
utils::write.csv(holdout$predictions,
                 file.path(out_dir, "holdout_predictions.csv"), row.names = FALSE)

message("Wrote results to ", out_dir, ":")
message("  sigma = ", round(sigma_fit$sigma, 1),
        ", wait_scale = ", round(sigma_fit$wait_scale, 2),
        ", resolved = ", capacity$resolved,
        " (", capacity$calibration_status, ").")
