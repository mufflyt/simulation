#!/usr/bin/env Rscript
# =============================================================================
# Demand calibration + back-test runner
# =============================================================================
# Anchors the life-course demand model's base-year service volumes to independent
# national totals and back-tests it against a held-out year -- the two credibility
# steps that turn the model off "placeholder_uncalibrated". Wires the package's
# own machinery (R/calibration-demand_lifecourse): calibrate_lifecourse_demand() (scalar = observed /
# predicted, HDMM Exhibit 11) and backtest_lifecourse() (MAPE at a held-out year).
#
#   Rscript scripts/run_demand_calibration_backtest.R
#
# ANCHORS (independent national totals, by category):
#   urps_office_visits         NAMCS/NHAMCS or MEPS office-visit totals
#   sling_procedure_volume     HCUP NASS + Medicare Part B carrier (CPT 57288)
#   prolapse_procedure_volume  HCUP NASS + Medicare Part B carrier
# Produced by scripts/data_acquisition/10_ingest_hcup_nass.R into data/anchors/;
# the config lives in config/calibration_targets.yml. If no anchor files are
# present the runner falls back to ILLUSTRATIVE values and stamps every output
# accordingly -- structure, not results.
#
# Writes:
#   artifacts/demand_calibration_scalars.csv       (category, predicted, observed, scalar)
#   artifacts/demand_backtest_by_category.csv      (per-category predicted vs observed)
#   artifacts/demand_backtest_summary.csv          (target_year, n, mape, anchors_source)
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# ---- Config: base year + back-test window -----------------------------------
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
read_targets <- function(path = "config/calibration_targets.yml") {
  if (requireNamespace("yaml", quietly = TRUE) && file.exists(path))
    return(yaml::read_yaml(path))
  list()
}
cfg <- read_targets()
FIT_YEAR  <- as.integer(cfg$backtest$fit_through_year %||% 2017L)
TGT_YEAR  <- as.integer(cfg$backtest$target_year %||% 2023L)
MAX_SCALAR <- as.numeric(cfg$max_scalar %||% 3)
BASE_YEAR <- as.integer(Sys.getenv("DEMAND_BASE_YEAR", as.character(FIT_YEAR)))

# ---- Independent national anchors -------------------------------------------
ANCHOR_CATEGORIES <- c("urps_office_visits", "sling_procedure_volume",
                       "prolapse_procedure_volume")
# Illustrative fallback (matches the vignette): HCUP SASD / Medicare / NAMCS
# ballpark. NOT results -- used only so the runner executes end to end.
ILLUSTRATIVE_ANCHORS <- data.frame(
  category = ANCHOR_CATEGORIES,
  observed = c(3.6e6, 1.0e5, 1.0e5),
  stringsAsFactors = FALSE)

# Read every data/anchors/*.csv (category, observed[, year]); optionally filter
# to `year`; keep the latest year per category. Returns list(anchors, source).
load_demand_anchors <- function(year = NULL, dir = "data/anchors") {
  files <- if (dir.exists(dir)) list.files(dir, "\\.csv$", full.names = TRUE) else character(0)
  if (!length(files)) {
    return(list(anchors = ILLUSTRATIVE_ANCHORS, source = "illustrative_fallback"))
  }
  rows <- do.call(rbind, lapply(files, function(f) {
    d <- utils::read.csv(f, stringsAsFactors = FALSE)
    if (!all(c("category", "observed") %in% names(d))) return(NULL)
    if (!"year" %in% names(d)) d$year <- NA_integer_
    d[, c("category", "observed", "year")]
  }))
  if (is.null(rows) || !nrow(rows))
    return(list(anchors = ILLUSTRATIVE_ANCHORS, source = "illustrative_fallback"))
  if (!is.null(year) && any(!is.na(rows$year) & rows$year == year))
    rows <- rows[is.na(rows$year) | rows$year == year, , drop = FALSE]
  # latest available year per category (NA years sort last)
  rows <- rows[order(rows$category, is.na(rows$year), -rows$year), , drop = FALSE]
  anchors <- rows[!duplicated(rows$category), c("category", "observed")]
  missing <- setdiff(ANCHOR_CATEGORIES, anchors$category)
  if (length(missing)) {  # backfill any absent category from the fallback
    anchors <- rbind(anchors, ILLUSTRATIVE_ANCHORS[
      ILLUSTRATIVE_ANCHORS$category %in% missing, ])
  }
  list(anchors = anchors[match(ANCHOR_CATEGORIES, anchors$category), ],
       source = if (length(missing)) "anchors_partial+fallback" else "anchors")
}

# ---- Life-course service volumes over the window ----------------------------
years <- sort(unique(c(FIT_YEAR:TGT_YEAR, BASE_YEAR)))
pop_by_age_year <- tidyr::expand_grid(year = years, age = 40:85) %>%
  dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))
message("Building life-course service volumes for ", min(years), "-", max(years), " ...")
traj <- lifecourse_demand_trajectory(pop_by_age_year, n = 2e4, seed = 1)
sv <- traj$service_volumes

# ---- 1. Base-year calibration -----------------------------------------------
anc <- load_demand_anchors(year = BASE_YEAR)
if (anc$source != "anchors")
  warning("Demand anchors: ", anc$source,
          " -- calibration scalars are ILLUSTRATIVE, not results. Run ",
          "scripts/data_acquisition/10_ingest_hcup_nass.R to produce real anchors.",
          call. = FALSE)
cal <- calibrate_lifecourse_demand(sv, anc$anchors, base_year = BASE_YEAR,
                                   max_scalar = MAX_SCALAR)
dir.create("artifacts", showWarnings = FALSE)
cal_out <- cal$scalars
cal_out$anchors_source <- anc$source
utils::write.csv(cal_out, "artifacts/demand_calibration_scalars.csv", row.names = FALSE)

cat("\n== Base-year calibration (", BASE_YEAR, ", anchors: ", anc$source, ") ==\n", sep = "")
print(cal$scalars)
cat("  scalar = observed / predicted; a value far from 1 signals a structural",
    "mismatch, not an offset (HDMM Exhibit 11).\n")

# ---- 2. Back-test against a held-out year -----------------------------------
obs_fit <- load_demand_anchors(year = FIT_YEAR)
obs_tgt <- load_demand_anchors(year = TGT_YEAR)
bt <- backtest_lifecourse(sv, obs_fit$anchors, obs_tgt$anchors,
                          fit_through_year = FIT_YEAR, target_year = TGT_YEAR)
bt$summary$anchors_source <- paste(obs_fit$source, obs_tgt$source, sep = "/")
utils::write.csv(bt$by_category, "artifacts/demand_backtest_by_category.csv", row.names = FALSE)
utils::write.csv(bt$summary, "artifacts/demand_backtest_summary.csv", row.names = FALSE)

cat("\n== Back-test: fit through ", FIT_YEAR, ", project to ", TGT_YEAR,
    " (anchors: ", bt$summary$anchors_source, ") ==\n", sep = "")
print(bt$by_category)
cat(sprintf("MAPE at %d: %.1f%% over %d anchor categor%s\n",
            TGT_YEAR, bt$summary$mape, bt$summary$n,
            if (bt$summary$n == 1) "y" else "ies"))
if (!identical(obs_fit$source, "anchors") || !identical(obs_tgt$source, "anchors"))
  cat("  NOTE: back-test used illustrative anchors for at least one year; the",
      "MAPE is structure, not a validated credibility number. Supply historical",
      "per-year anchors (a `year` column in data/anchors/*.csv) for a real",
      "back-test.\n")

cat("\nWrote artifacts/demand_calibration_scalars.csv, demand_backtest_*.csv\n")
