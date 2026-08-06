#!/usr/bin/env Rscript
# =============================================================================
# Demand pipeline orchestrator + provenance dashboard
# =============================================================================
# Runs the demand stack's runners in one shot and prints a single status table so
# "which parts are real?" is one command instead of a hunt through artifacts:
#
#   * Isochrone demand   (R/geography-demand)  real tract file if present, else example
#   * Calibration + back-test    scripts/run_demand_calibration_backtest.R
#   * SWAN -> DMDM UI fit         scripts/run_swan_dmdm_fit.R (skipped w/o SWAN)
#
# Each step is wrapped so a missing input (no SWAN download, no ACS tracts, no
# anchors) SKIPS rather than aborts the run; the dashboard then reports each
# component's calibration_status / data source so the placeholder-vs-fitted state
# of the whole stack is visible at a glance.
#
#   Rscript scripts/run_demand_pipeline.R
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}
dir.create("artifacts", showWarnings = FALSE)
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

status <- list()   # collect one row per component for the dashboard
add <- function(component, state, detail, artifact = NA_character_)
  status[[length(status) + 1L]] <<- data.frame(
    component = component, state = state, detail = detail,
    artifact = artifact, stringsAsFactors = FALSE)

run_step <- function(label, expr) {
  message("\n>>> ", label)
  tryCatch(expr, error = function(e) { message("    skipped: ", conditionMessage(e)); NULL })
}

# ---- 1. Isochrone (geographic) demand ---------------------------------------
run_step("Isochrone demand (R/geography-demand)", {
  tract_csv <- "data-raw/spatial/acs5_2023_tract_female_by_ageband.csv"
  if (file.exists(tract_csv)) {
    tr <- utils::read.csv(tract_csv)
    if (!"nearest_provider_min" %in% names(tr)) tr$nearest_provider_min <- NA_real_
    nt <- tract_need_from_population(tr)
    utils::write.csv(nt, "artifacts/tract_pfd_need.csv", row.names = FALSE)
    add("isochrone_demand", "real_population",
        sprintf("%s tracts; total need %.0f", nrow(nt), sum(nt$need, na.rm = TRUE)),
        "artifacts/tract_pfd_need.csv")
  } else {
    add("isochrone_demand", "example_only",
        paste0("no ", tract_csv, " -- run 08_download_acs_tracts.R (Census key)"))
  }
})

# ---- 2. Calibration + back-test ---------------------------------------------
run_step("Calibration + back-test (run_demand_calibration_backtest.R)", {
  suppressWarnings(source("scripts/run_demand_calibration_backtest.R", local = new.env()))
  src <- if (file.exists("artifacts/demand_calibration_scalars.csv")) {
    d <- utils::read.csv("artifacts/demand_calibration_scalars.csv")
    if ("anchors_source" %in% names(d)) d$anchors_source[1] else "unknown"
  } else "not_written"
  mape <- if (file.exists("artifacts/demand_backtest_summary.csv"))
    utils::read.csv("artifacts/demand_backtest_summary.csv")$mape[1] else NA_real_
  add("calibration", if (identical(src, "anchors")) "calibrated" else "illustrative",
      sprintf("anchors=%s; backtest MAPE=%s", src,
              if (is.na(mape)) "NA" else sprintf("%.1f%%", mape)),
      "artifacts/demand_calibration_scalars.csv")
})

# ---- 3. SWAN -> DMDM UI fit --------------------------------------------------
run_step("SWAN -> DMDM UI fit (run_swan_dmdm_fit.R)", {
  suppressWarnings(source("scripts/run_swan_dmdm_fit.R", local = new.env()))
  if (file.exists("artifacts/swan_dmdm_transitions.rds")) {
    tr <- readRDS("artifacts/swan_dmdm_transitions.rds")
    add("dmdm_ui_hazards", unname(tr$provenance$ui %||% "unknown"),
        "UI onset/remission fitted from SWAN", "artifacts/swan_dmdm_transitions.rds")
  } else {
    add("dmdm_ui_hazards", "placeholder",
        "no SWAN download -- run 09_download_swan_icpsr.R (ICPSR account)")
  }
})

# ---- Provenance of the assembled transition object --------------------------
# Independent of whether the SWAN step ran: report each condition's status.
tr <- if (file.exists("artifacts/swan_dmdm_transitions.rds"))
  readRDS("artifacts/swan_dmdm_transitions.rds") else dmdm_transitions_with_pop_literature()
for (cc in c("ui", "pop", "ai"))
  add(paste0("dmdm_", cc), unname(tr$provenance[[cc]] %||% "placeholder"),
      "DMDM condition transition provenance")

# ---- Dashboard --------------------------------------------------------------
dash <- do.call(rbind, status)
utils::write.csv(dash, "artifacts/demand_pipeline_status.csv", row.names = FALSE)
cat("\n===================== DEMAND PIPELINE STATUS =====================\n")
print(dash, row.names = FALSE)
cat("\nLegend: real_population/calibrated/fitted/derived_by_analogy are progressively\n")
cat("stronger than example_only/illustrative/placeholder. Each weak row names the\n")
cat("pull that upgrades it. Wrote artifacts/demand_pipeline_status.csv\n")
