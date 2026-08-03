#!/usr/bin/env Rscript
# =============================================================================
# BRFSS 2023 Download Script
# Behavioral Risk Factor Surveillance System
# =============================================================================
#
# PURPOSE:
#   Download the 2023 BRFSS combined landline + cellular XPT file from the CDC.
#   The BRFSS is a freely available, no-DUA-required public data source.
#
# KEY VARIABLES FOR URPS DEMAND MODELING:
#   BLADCON  - Bladder/bowel control problem (urinary/fecal incontinence)
#   URINCON  - Urinary incontinence indicator (optional module, subset of states)
#   _SEX     - Sex of respondent
#   _AGEG5YR - Age in five-year groups (computed variable)
#   _LLCPWT  - Final weight (complex survey weight, use this for estimates)
#   _PSU     - Primary sampling unit (for survey variance)
#   _STSTR   - Stratum (for survey variance)
#   _STATE   - State FIPS code
#   _PRACE1  - Race/ethnicity (computed)
#   _INCOMG1 - Income group (computed)
#   _RFHLTH  - Self-reported health status
#   HLTHPLN1 - Health plan / insurance coverage
#
# NOTE ON BLADCON / URINCON:
#   These come from optional state-added modules; not all 50 states + DC
#   administer the urinary incontinence module. Check which states used it:
#   https://www.cdc.gov/brfss/questionnaires/modules/state2023.htm
#   For national estimates, use _RFHLTH + self-reported chronic conditions
#   as proxies, or use the urinary incontinence NHANES or SWAN modules.
#
# DATA ACCESS:
#   Freely available. No registration, no DUA required.
#   Primary URL: https://www.cdc.gov/brfss/annual_data/annual_2023.html
#   Direct ZIP:  https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip
#
# OUTPUT:
#   data-raw/brfss/LLCP2023.XPT     (SAS transport format, ~200 MB unzipped)
#   data-raw/brfss/LLCP2023.rds     (R native, filtered to women 18+)
#   data-raw/brfss/brfss_2023_manifest.txt
#
# SURVEY DESIGN (use with survey::svydesign):
#   ids     = ~_PSU
#   strata  = ~_STSTR
#   weights = ~_LLCPWT
#   nest    = TRUE
# =============================================================================

library(haven)
library(dplyr)
library(readr)

# ---- Paths ------------------------------------------------------------------
here_root <- function(...) {
  root <- normalizePath(file.path(dirname(sys.frame(1)$ofile), "../.."),
                        mustWork = FALSE)
  if (!dir.exists(root)) root <- getwd()
  file.path(root, ...)
}

out_dir  <- here::here("data-raw", "brfss")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

zip_url  <- "https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip"
zip_path <- file.path(out_dir, "LLCP2023XPT.zip")
xpt_path <- file.path(out_dir, "LLCP2023.XPT")
rds_path <- file.path(out_dir, "brfss_2023_women18plus.rds")
manifest <- file.path(out_dir, "brfss_2023_manifest.txt")

# ---- Download ---------------------------------------------------------------
message("Downloading 2023 BRFSS XPT ZIP (~64 MB compressed) ...")
if (!file.exists(zip_path)) {
  download.file(zip_url, zip_path, mode = "wb", method = "auto")
  message("  Downloaded: ", zip_path)
} else {
  message("  ZIP already exists, skipping download.")
}

# ---- Unzip ------------------------------------------------------------------
if (!file.exists(xpt_path)) {
  message("Unzipping ...")
  unzip(zip_path, exdir = out_dir)
  # CDC sometimes names it LLCP2023.XPT or LLCP2023.xpt; normalise
  xpt_candidates <- list.files(out_dir, pattern = "\\.XPT$|\\.xpt$",
                                full.names = TRUE, ignore.case = TRUE)
  if (length(xpt_candidates) == 0) stop("XPT file not found after unzip")
  if (tools::file_ext(xpt_candidates[1]) != "XPT") {
    file.rename(xpt_candidates[1], xpt_path)
  }
  message("  Unzipped: ", xpt_path)
} else {
  message("  XPT already exists, skipping unzip.")
}

# ---- Read & filter ----------------------------------------------------------
message("Reading XPT (this may take 1-2 minutes for ~500K rows x 345 vars) ...")
brfss_raw <- haven::read_xpt(xpt_path)
n_raw <- nrow(brfss_raw)
message("  Raw rows: ", format(n_raw, big.mark = ","))

# Normalise column names to uppercase (CDC uses uppercase, but be safe)
names(brfss_raw) <- toupper(names(brfss_raw))

# Key URPS columns (select what exists)
urps_vars <- c(
  # Survey design
  "_PSU", "_STSTR", "_LLCPWT",
  # Geography
  "_STATE", "IYEAR", "IMONTH",
  # Demographics
  "_SEX", "_AGEG5YR", "_RACE", "_PRACE1", "_MRACE1",
  # Socioeconomic
  "_INCOMG1", "_EDUCAG", "EMPLOY1",
  # Insurance
  "HLTHPLN1", "_HCVU651",
  # Pelvic floor / chronic conditions (may be in optional modules)
  "BLADCON", "URINCON", "LOSECON",
  # General health
  "_RFHLTH", "GENHLTH",
  # BMI / obesity (risk factor for pelvic floor disorders)
  "_BMI5", "_BMI5CAT",
  # Smoking, alcohol (covariates)
  "_SMOKER3", "_RFSMOK3",
  # Hysterectomy (risk factor) -- not standard in BRFSS core
  "HYSTOVRY",
  # Physical activity (covariate)
  "_TOTINDA"
)

# Only keep columns that exist
keep <- urps_vars[gsub("^_", "X_", urps_vars) %in% names(brfss_raw) |
                  urps_vars %in% names(brfss_raw)]
# BRFSS stores _-prefixed vars with X_ prefix after reading with haven
# Remap: column names in XPT start with X_ for _-prefixed CDC vars
names_in_data <- names(brfss_raw)
# Find all columns we want
want_cols <- unique(c(
  names_in_data[names_in_data %in% urps_vars],
  names_in_data[paste0("_", sub("^X_", "", names_in_data)) %in% urps_vars]
))
# Always include design variables regardless of prefix style
design_cols <- names_in_data[grepl("^(X_PSU|X_STSTR|X_LLCPWT|X_STATE|X_SEX|X_AGEG5YR)", names_in_data)]
want_cols   <- unique(c(want_cols, design_cols))

brfss_sub <- brfss_raw[, want_cols, drop = FALSE]

# Filter: female respondents (_SEX == 2), age >= 18 (AGEG5YR categories 1+)
sex_col <- names_in_data[grepl("SEX", names_in_data)][1]
age_col <- names_in_data[grepl("AGEG5YR", names_in_data)][1]

if (!is.null(sex_col) && !is.null(age_col)) {
  brfss_women <- brfss_sub[
    !is.na(brfss_sub[[sex_col]]) & brfss_sub[[sex_col]] == 2 &
    !is.na(brfss_sub[[age_col]]) & brfss_sub[[age_col]] >= 1,
  ]
} else {
  warning("Could not identify SEX or AGE column; saving full dataset")
  brfss_women <- brfss_sub
}

n_women <- nrow(brfss_women)
message("  Rows after filtering to women 18+: ", format(n_women, big.mark = ","))

# ---- Save -------------------------------------------------------------------
message("Saving filtered RDS ...")
saveRDS(brfss_women, rds_path)
message("  Saved: ", rds_path, " (", round(file.size(rds_path) / 1e6, 1), " MB)")

# ---- Manifest ---------------------------------------------------------------
pelvic_floor_vars <- c("BLADCON", "URINCON", "LOSECON")
pf_found <- pelvic_floor_vars[pelvic_floor_vars %in% names(brfss_women)]
pf_missing <- pelvic_floor_vars[!pelvic_floor_vars %in% names(brfss_women)]

writeLines(c(
  "BRFSS 2023 Download Manifest",
  paste("Generated:", Sys.time()),
  paste("Source URL:", zip_url),
  paste("ZIP size (MB):", round(file.size(zip_path) / 1e6, 1)),
  paste("XPT size (MB):", round(file.size(xpt_path) / 1e6, 1)),
  paste("RDS size (MB):", round(file.size(rds_path) / 1e6, 1)),
  paste("Raw record count:", format(n_raw, big.mark = ",")),
  paste("Women 18+ record count:", format(n_women, big.mark = ",")),
  paste("Pelvic floor vars found:", paste(pf_found, collapse = ", ")),
  paste("Pelvic floor vars MISSING (optional module):",
        paste(pf_missing, collapse = ", ")),
  "",
  "NOTE: BLADCON/URINCON are in the optional urinary incontinence module.",
  "Only states that opted in to that module will have non-missing values.",
  "See: https://www.cdc.gov/brfss/questionnaires/modules/state2023.htm",
  "",
  "Survey design for svydesign():",
  "  ids     = ~X_PSU",
  "  strata  = ~X_STSTR",
  "  weights = ~X_LLCPWT",
  "  nest    = TRUE"
), manifest)

message("Manifest written: ", manifest)
message("\nDone. BRFSS 2023 download complete.")
message("Next step: check BLADCON/URINCON non-missing counts by state.")
