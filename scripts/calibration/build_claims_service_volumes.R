# scripts/calibration/build_claims_service_volumes.R
#
# Build a claims-calibrated URPS service-volume basket and check whether it
# clears the productivity plausibility band -- the demand-side counterpart of
# scripts/build_and_run_real_supply_calibration.R.
#
# example_service_volumes() (R/core-run_workforce_microsimulation.R) rides
# hardcoded per-service ratios (calibration tier "uncalibrated_illustrative"),
# and because calibrate_wrvu_per_fte() SOLVES productivity from them, a wrong
# basket surfaces as a solved wRVU/FTE outside the 3,500-12,000 benchmark. This
# runner replaces those ratios, service by service, with REAL claims counts via
# claims_service_volumes(), then reports the implied productivity so you can see
# whether the calibrated basket lands in the plausible band.
#
# Run from the simulation repository root:
#
#   source("scripts/calibration/build_claims_service_volumes.R")
#   vols <- build_claims_service_volumes(
#     claims_path   = "data-raw/demand/urps_claims_volumes.csv",  # your file
#     fallback_path = "data-raw/demand/illustrative_volumes.csv", # optional
#     base_year = 2023L, base_required_fte = 1306
#   )
#
# FAIL-CLOSED. It needs a real claims table and fabricates nothing. With no
# claims source present it prints exactly what is missing and stops. The unit
# contract of the combiner is pinned in
# tests/testthat/test-claims-service-volumes.R.
#
# Claims CSV schema (one row per service-year you actually have claims for):
#   service, year, volume, [source], [calibration_status]
# Services use the model's names: new_consultation, return_visit, pessary_care,
# urodynamics, cystoscopy, botox_bladder, ptns, bladder_instillation,
# sling_procedure, prolapse_procedure, postoperative_care.

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(tibble)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# Map a raw CADR Medicare procedure export to the model's service names. CADR
# supplies operative and device counts (slings, pessaries) and pelvic-floor PT;
# it does NOT cover the ambulatory E/M or diagnostic services, which is exactly
# why the result is only PARTIALLY claims-backed and claims_service_volumes()
# reports the weakest tier.
.cadr_service_map <- c(
  sling = "sling_procedure",
  pessary = "pessary_care",
  pelvic_floor_pt = "postoperative_care"  # PT visits proxy an ambulatory service
)
read_cadr_claims <- function(cadr_path, year) {
  base::message("Reading CADR procedure counts: ", cadr_path)
  cadr <- readr::read_csv(cadr_path, show_col_types = FALSE, progress = FALSE)
  if (!all(c("procedure", "annual_episodes") %in% names(cadr))) {
    base::stop("CADR file must have columns `procedure` and `annual_episodes`.",
               call. = FALSE)
  }
  cadr |>
    dplyr::filter(.data$procedure %in% base::names(.cadr_service_map)) |>
    dplyr::transmute(
      service = base::unname(.cadr_service_map[.data$procedure]),
      year = base::as.integer(year),
      volume = base::as.numeric(.data$annual_episodes),
      source = "CADR Medicare",
      calibration_status = "calibrated")
}

read_claims_csv <- function(claims_path) {
  base::message("Reading claims table: ", claims_path)
  claims <- readr::read_csv(claims_path, show_col_types = FALSE, progress = FALSE)
  if (!all(c("service", "year", "volume") %in% names(claims))) {
    base::stop("claims CSV must have columns `service`, `year`, `volume`.",
               call. = FALSE)
  }
  claims
}

build_claims_service_volumes <- function(claims_path = NULL,
                                         cadr_path = NULL,
                                         cadr_year = NULL,
                                         fallback_path = NULL,
                                         base_year = NULL,
                                         base_required_fte = 1306,
                                         target_dir = "data-raw/demand") {
  base::message("===============================================")
  base::message("CLAIMS-CALIBRATED SERVICE VOLUMES BUILD")
  base::message("===============================================")

  # --- Assemble the real claims table (fail-closed) ------------------------
  claims_parts <- list()
  if (!is.null(claims_path) && base::file.exists(claims_path)) {
    claims_parts[["csv"]] <- read_claims_csv(claims_path)
  }
  if (!is.null(cadr_path) && base::file.exists(cadr_path)) {
    if (is.null(cadr_year)) {
      base::stop("cadr_year is required when cadr_path is supplied.", call. = FALSE)
    }
    claims_parts[["cadr"]] <- read_cadr_claims(cadr_path, cadr_year)
  }
  if (base::length(claims_parts) == 0L) {
    base::message("No claims source present, so nothing is computed (fail-closed).")
    base::message("Provide claims_path (service, year, volume) and/or cadr_path, then re-run.")
    base::message("The combiner's unit contract is exercised in ",
                  "tests/testthat/test-claims-service-volumes.R.")
    return(base::invisible(NULL))
  }
  claims <- dplyr::bind_rows(claims_parts)
  base::message("Claims rows assembled: ", base::nrow(claims))

  # --- Illustrative fallback for services claims does not cover ------------
  fallback <- NULL
  if (!is.null(fallback_path) && base::file.exists(fallback_path)) {
    base::message("Reading illustrative fallback: ", fallback_path)
    fallback <- readr::read_csv(fallback_path, show_col_types = FALSE, progress = FALSE)
  } else {
    base::message("No fallback provided; using claims alone (services absent from ",
                  "claims are simply not represented).")
  }

  # --- Merge, with per-service tiers and weakest-wins overall status -------
  volumes <- claims_service_volumes(claims = claims, fallback = fallback)
  overall <- base::attr(volumes, "overall_status")
  base::message("Per-service calibration tiers:")
  base::print(volumes |>
                dplyr::distinct(.data$service, .data$calibration_status, .data$source) |>
                dplyr::arrange(.data$service))

  # --- Implied productivity: does the calibrated basket clear the band? ----
  by <- if (is.null(base_year)) base::min(volumes$year, na.rm = TRUE) else base_year
  base_vol <- dplyr::filter(volumes, .data$year == by)
  if (base::nrow(base_vol) > 0L) {
    base_wrvu <- service_volume_to_wrvu(base_vol)
    wrvu_per_fte <- calibrate_wrvu_per_fte(base_wrvu$work_rvu, base_required_fte)
    check_productivity_plausible(wrvu_per_fte)
  } else {
    base::message("No volumes in base year ", by, "; skipping productivity check.")
  }

  # --- Write the canonical basket + provenance -----------------------------
  base::dir.create(target_dir, recursive = TRUE, showWarnings = FALSE)
  out_path <- base::file.path(target_dir, "urps_service_volumes.csv")
  readr::write_csv(dplyr::select(volumes, "year", "service", "volume"), out_path)
  base::message("Saved canonical basket: ",
                base::normalizePath(out_path, mustWork = TRUE))

  provenance <- volumes |>
    dplyr::distinct(.data$service, .data$calibration_status, .data$source) |>
    dplyr::mutate(overall_status = overall)
  prov_path <- base::file.path(target_dir, "urps_service_volumes_provenance.csv")
  readr::write_csv(provenance, prov_path)
  base::message("Saved provenance: ", base::normalizePath(prov_path, mustWork = TRUE))

  base::message("Overall basket tier: ", overall,
                if (identical(overall, "calibrated")) " (fully claims-backed)."
                else " (some services still on the illustrative fallback).")
  base::message("===============================================")
  base::invisible(volumes)
}

# ---------------------------------------------------------------------------
# Next step to wire this into a run: point the orchestrator's demand path at the
# saved data-raw/demand/urps_service_volumes.csv instead of
# example_service_volumes(demand_long). That is a one-line swap in
# R/core-run_workforce_microsimulation.R and is intentionally NOT done here, so
# this PR ships only the calibrated-volume ASSEMBLY, not an orchestrator change.
# ---------------------------------------------------------------------------
