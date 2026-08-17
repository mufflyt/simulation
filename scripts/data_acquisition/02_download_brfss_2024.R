#!/usr/bin/env Rscript
# =============================================================================
# BRFSS 2024 Download Script
# Behavioral Risk Factor Surveillance System
# =============================================================================
#
# PURPOSE:
#   Download the 2024 BRFSS combined landline + cellular XPT file from the CDC
#   and produce a filtered RDS (women 18+) compatible with load_brfss_women().
#
# VARIABLES REQUIRED BY load_brfss_women() / build_urps_population_cells():
#
#   Survey design
#     SEQNO      - Sequence number (row identifier)
#     _STATE     - State FIPS code
#     _LLCPWT    - Final combined weight (use for all population estimates)
#     _PSU       - Primary sampling unit (survey variance)
#     _STSTR     - Stratum (survey variance)
#
#   Stratification / demand cell dimensions
#     _AGEG5YR   - Age in 5-yr groups (computed; 1=18-24 ... 14=85+)
#     _IMPRACE   - Imputed race/ethnicity (computed; 1=White NH ... 6=Other NH)
#     _HLTHPL1   - Health plan coverage (computed; 1=Insured, 2=Uninsured, 9=Unknown)
#                  NOTE: renamed to _HLTHPL2 in 2024. Coding identical.
#                  This script normalises _HLTHPL2 -> _HLTHPL1 before saving.
#     INCOME3    - Annual household income (11-level; 1=<$10k ... 11>=$200k)
#     _METSTAT   - Metropolitan status (computed; 1=Metro, 2=NonMetro)
#     _BMI5CAT   - BMI category (computed; 1=Underweight ... 4=Obese)
#     _SMOKER3   - Smoking status (computed; 1=Daily ... 4=Never)
#     CHILDREN   - Number of children in household (88=none, 99=refused)
#
#   Pelvic floor disorder flags (optional state modules -- not all states)
#     BLADCON    - Bladder/bowel control problem
#     URINCON    - Urinary incontinence indicator
#     INCONTI    - Incontinence (alternate name used in some years)
#     PROPLAP    - Pelvic organ prolapse
#     PELVORGAN  - Pelvic organ (alternate name)
#     BOWLLEA    - Bowel leakage
#     BOWLINC    - Bowel incontinence (alternate name)
#
#   Additional covariates retained for future use
#     _SEX       - Sex (filter: keep 2 = female)
#     _RFHLTH    - Self-reported health status
#     GENHLTH    - General health (raw)
#     EMPLOY1    - Employment status
#     _EDUCAG    - Education level (computed)
#     HYSTOVRY   - Hysterectomy/oophorectomy (optional module)
#
# 2024-SPECIFIC CHANGES VS 2023:
#   - _HLTHPL1 renamed to _HLTHPL2 (value coding unchanged: 1/2/9)
#   - XPT stores computed vars with bare _ prefix (not X_); haven reads as-is
#   - Zip file may contain filename with trailing whitespace (handled below)
#   - Pelvic floor optional module: check state participation at
#     https://www.cdc.gov/brfss/questionnaires/modules/state2024.htm
#
# DATA ACCESS:
#   Freely available. No registration, no DUA required.
#   Primary page: https://www.cdc.gov/brfss/annual_data/annual_2024.html
#   Direct ZIP:   https://www.cdc.gov/brfss/annual_data/2024/files/LLCP2024XPT.zip
#
# OUTPUT:
#   data-raw/brfss/LLCP2024.XPT               (~250 MB unzipped)
#   data-raw/brfss/brfss_2024_women18plus.rds  (women 18+ only, ~40 MB)
#   data-raw/brfss/brfss_2024_manifest.txt
#
# USING 2024 DATA WITH THE URPS DEMAND PIPELINE:
#   brfss <- load_brfss_women("data-raw/brfss/brfss_2024_women18plus.rds")
#   cells <- build_urps_population_cells(brfss_women = brfss)
#   demand <- project_urps_demand(cells, access_scenario = "status_quo")
#
# SURVEY DESIGN (use with survey::svydesign):
#   ids     = ~_PSU
#   strata  = ~_STSTR
#   weights = ~_LLCPWT
#   nest    = TRUE
# =============================================================================

library(haven)
library(dplyr)

# ---- Paths ------------------------------------------------------------------
out_dir  <- here::here("data-raw", "brfss")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

zip_url  <- "https://www.cdc.gov/brfss/annual_data/2024/files/LLCP2024XPT.zip"
zip_path <- file.path(out_dir, "LLCP2024XPT.zip")
xpt_path <- file.path(out_dir, "LLCP2024.XPT")
rds_path <- file.path(out_dir, "brfss_2024_women18plus.rds")
manifest <- file.path(out_dir, "brfss_2024_manifest.txt")

# ---- Download ---------------------------------------------------------------
message("Downloading 2024 BRFSS XPT ZIP (~83 MB compressed) ...")
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
  # CDC zips sometimes produce filenames with trailing whitespace; match loosely
  xpt_candidates <- list.files(out_dir, pattern = "LLCP2024",
                                full.names = TRUE, ignore.case = TRUE)
  xpt_candidates <- xpt_candidates[grepl("\\.XPT\\s*$", xpt_candidates,
                                         ignore.case = TRUE, perl = TRUE)]
  if (length(xpt_candidates) == 0) stop("XPT file not found after unzip")
  if (!file.exists(xpt_path)) file.rename(trimws(xpt_candidates[1]), xpt_path)
  message("  Unzipped: ", xpt_path)
} else {
  message("  XPT already exists, skipping unzip.")
}

# ---- Read -------------------------------------------------------------------
message("Reading XPT (1-2 minutes for ~500K rows) ...")
brfss_raw <- haven::read_xpt(xpt_path)
names(brfss_raw) <- toupper(names(brfss_raw))
n_raw <- nrow(brfss_raw)
message("  Raw rows: ", format(n_raw, big.mark = ","))
message("  Columns:  ", ncol(brfss_raw))

# ---- Normalise 2024 renames -------------------------------------------------
# _HLTHPL1 (2023) -> _HLTHPL2 (2024); coding identical (1=Insured/2=Uninsured/9=Unknown).
# Rename back so load_brfss_women() finds X_HLTHPL1 after its sub("^_","X_") step.
if ("_HLTHPL2" %in% names(brfss_raw) && !"_HLTHPL1" %in% names(brfss_raw)) {
  names(brfss_raw)[names(brfss_raw) == "_HLTHPL2"] <- "_HLTHPL1"
  message("  Normalised _HLTHPL2 -> _HLTHPL1 for pipeline compatibility.")
}

# ---- Variable audit ---------------------------------------------------------
# These are the raw _ names as they appear in the XPT (before load_brfss_women
# applies its sub("^_","X_") rename).
required_vars <- c(
  "SEQNO", "_STATE", "_LLCPWT", "_PSU", "_STSTR",
  "_AGEG5YR", "_IMPRACE", "_HLTHPL1", "INCOME3",
  "_METSTAT", "_BMI5CAT", "_SMOKER3", "CHILDREN", "_SEX"
)
optional_pfd <- c("BLADCON", "URINCON", "INCONTI", "PROPLAP", "PELVORGAN",
                  "BOWLLEA", "BOWLINC")

present     <- required_vars[required_vars %in% names(brfss_raw)]
missing_req <- required_vars[!required_vars %in% names(brfss_raw)]
pfd_found   <- optional_pfd[optional_pfd %in% names(brfss_raw)]
pfd_missing <- optional_pfd[!optional_pfd %in% names(brfss_raw)]

if (length(missing_req) > 0) {
  warning("MISSING required variables (load_brfss_women() will fail or degrade):\n  ",
          paste(missing_req, collapse = ", "),
          "\nCheck the 2024 codebook for renames.")
}
message("  Required vars present (", length(present), "/", length(required_vars), "): ",
        paste(present, collapse = ", "))
if (length(missing_req) > 0)
  message("  MISSING: ", paste(missing_req, collapse = ", "))
message("  PFD optional vars found: ",
        if (length(pfd_found) > 0) paste(pfd_found, collapse = ", ") else "none")

# ---- Select & filter --------------------------------------------------------
keep_vars <- unique(c(required_vars, optional_pfd,
                      intersect(c("_RFHLTH", "GENHLTH", "EMPLOY1", "_EDUCAG",
                                  "HLTHPLN1", "HYSTOVRY"),
                                names(brfss_raw))))
keep_vars <- intersect(keep_vars, names(brfss_raw))

brfss_sub <- brfss_raw[, keep_vars, drop = FALSE]

# Filter to female respondents (_SEX == 2) aged 18+ (_AGEG5YR >= 1)
brfss_women <- brfss_sub[
  !is.na(brfss_sub[["_SEX"]])     & brfss_sub[["_SEX"]] == 2 &
  !is.na(brfss_sub[["_AGEG5YR"]]) & brfss_sub[["_AGEG5YR"]] >= 1, ]

n_women <- nrow(brfss_women)
message("  Rows after filtering to women 18+: ", format(n_women, big.mark = ","))

# ---- Save -------------------------------------------------------------------
message("Saving filtered RDS ...")
saveRDS(brfss_women, rds_path)
message("  Saved: ", rds_path, " (", round(file.size(rds_path) / 1e6, 1), " MB)")

# ---- Manifest ---------------------------------------------------------------
writeLines(c(
  "BRFSS 2024 Download Manifest",
  paste("Generated:", Sys.time()),
  paste("Source URL:", zip_url),
  paste("ZIP size (MB):", round(file.size(zip_path) / 1e6, 1)),
  paste("XPT size (MB):", round(file.size(xpt_path) / 1e6, 1)),
  paste("RDS size (MB):", round(file.size(rds_path) / 1e6, 1)),
  paste("Raw record count:", format(n_raw, big.mark = ",")),
  paste("Women 18+ record count:", format(n_women, big.mark = ",")),
  paste("Required vars present:", paste(present, collapse = ", ")),
  paste("Required vars MISSING:", if (length(missing_req)) paste(missing_req, collapse = ", ") else "none"),
  paste("PFD optional vars found:", if (length(pfd_found)) paste(pfd_found, collapse = ", ") else "none"),
  paste("PFD optional vars missing:", paste(pfd_missing, collapse = ", ")),
  "",
  "2024-specific notes:",
  "  - _HLTHPL1 renamed to _HLTHPL2 in 2024; normalised to _HLTHPL1 in this RDS.",
  "  - No pelvic floor optional module columns found in 2024 core file.",
  "",
  "To use with the URPS demand pipeline:",
  "  brfss <- load_brfss_women('data-raw/brfss/brfss_2024_women18plus.rds')",
  "  cells <- build_urps_population_cells(brfss_women = brfss)",
  "  demand <- project_urps_demand(cells, access_scenario = 'status_quo')",
  "",
  "Survey design for svydesign():",
  "  ids     = ~_PSU",
  "  strata  = ~_STSTR",
  "  weights = ~_LLCPWT",
  "  nest    = TRUE"
), manifest)

message("Manifest written: ", manifest)
message("\nDone. BRFSS 2024 download complete.")
if (length(missing_req) > 0) {
  message("\nACTION REQUIRED: ", length(missing_req), " required variable(s) missing.")
  message("Check the 2024 BRFSS codebook and update R/data-urps_population.R if needed.")
}
