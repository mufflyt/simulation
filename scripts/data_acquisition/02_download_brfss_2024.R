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
#     INCOME3    - Annual household income (11-level; 1=<$10k ... 11>=$200k)
#     _METSTAT   - Metropolitan status (computed; 1=Metro, 2=NonMetro)
#     _BMI5CAT   - BMI category (computed; 1=Underweight ... 4=Obese)
#     _SMOKER3   - Smoking status (computed; 1=Daily ... 4=Never)
#     CHILDREN   - Number of children in household (88=none, 99=refused)
#
#   Pelvic floor disorder flags (optional state modules — not all states)
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
#     HYSTOVRY   - Hysterectomy/oophorectomy (not in core; present in some states)
#
# NOTES ON 2024 VARIABLES:
#   - INCOME3 (11-level, introduced 2021) replaces the legacy INCOME2 (8-level).
#     Verify it is present; if absent the income_tier column will be all-NA.
#   - _IMPRACE (6-category imputed race) was stable through 2023; check for
#     rename to _IMPRACE1 or _RACE1 if the harmonisation step fails.
#   - _HLTHPL1 is the computed insurance indicator; HLTHPLN1 is the raw question.
#     load_brfss_women() reads _HLTHPL1 (X_HLTHPL1 after column rename).
#   - Pelvic floor optional modules: check which states opted in for 2024 at
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
# UPDATING load_brfss_women() TO USE 2024 DATA:
#   Pass the path explicitly:
#     cells <- build_urps_population_cells(
#       brfss_women = load_brfss_women("data-raw/brfss/brfss_2024_women18plus.rds")
#     )
#   Or update the default path in load_brfss_women() (R/data-urps_population.R).
#
# SURVEY DESIGN (use with survey::svydesign):
#   ids     = ~X_PSU
#   strata  = ~X_STSTR
#   weights = ~X_LLCPWT
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
  # CDC zips sometimes contain filenames with trailing spaces; match loosely
  xpt_candidates <- list.files(out_dir, pattern = "LLCP2024",
                                full.names = TRUE, ignore.case = TRUE)
  xpt_candidates <- xpt_candidates[grepl("\\.XPT\\s*$", xpt_candidates,
                                         ignore.case = TRUE, perl = TRUE)]
  if (length(xpt_candidates) == 0) stop("XPT file not found after unzip")
  if (!file.exists(xpt_path)) file.rename(xpt_candidates[1], xpt_path)
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

# ---- Variable audit ---------------------------------------------------------
# Confirm the variables load_brfss_women() requires are present.
# X_ prefix: CDC stores _-prefixed computed vars; haven reads them as-is.
required_vars <- c(
  "SEQNO", "X_STATE", "X_LLCPWT", "X_PSU", "X_STSTR",
  "X_AGEG5YR", "X_IMPRACE", "X_HLTHPL1", "INCOME3",
  "X_METSTAT", "X_BMI5CAT", "X_SMOKER3", "CHILDREN", "X_SEX"
)
optional_pfd <- c("BLADCON", "URINCON", "INCONTI", "PROPLAP", "PELVORGAN",
                  "BOWLLEA", "BOWLINC")

present  <- required_vars[required_vars %in% names(brfss_raw)]
missing  <- required_vars[!required_vars %in% names(brfss_raw)]
pfd_found   <- optional_pfd[optional_pfd %in% names(brfss_raw)]
pfd_missing <- optional_pfd[!optional_pfd %in% names(brfss_raw)]

if (length(missing) > 0) {
  warning("MISSING required variables (load_brfss_women() will fail or degrade):\n  ",
          paste(missing, collapse = ", "),
          "\nCheck the 2024 codebook for renames.")
}
message("  Required vars present (", length(present), "/", length(required_vars), "): ",
        paste(present, collapse = ", "))
if (length(missing) > 0)
  message("  MISSING: ", paste(missing, collapse = ", "))
message("  PFD optional vars found: ",
        if (length(pfd_found) > 0) paste(pfd_found, collapse = ", ") else "none")

# ---- Select & filter --------------------------------------------------------
keep_vars <- c(required_vars, optional_pfd,
               intersect(c("X_RFHLTH", "GENHLTH", "EMPLOY1", "X_EDUCAG",
                           "HLTHPLN1", "HYSTOVRY"),
                         names(brfss_raw)))
keep_vars <- intersect(keep_vars, names(brfss_raw))

brfss_sub <- brfss_raw[, keep_vars, drop = FALSE]

# Filter to female respondents (X_SEX == 2) aged 18+ (X_AGEG5YR >= 1)
brfss_women <- brfss_sub[
  !is.na(brfss_sub[["X_SEX"]])     & brfss_sub[["X_SEX"]] == 2 &
  !is.na(brfss_sub[["X_AGEG5YR"]]) & brfss_sub[["X_AGEG5YR"]] >= 1, ]

# Strip X_ prefix back to _ so load_brfss_women()'s sub("^_","X_") works
names(brfss_women) <- sub("^X_", "_", names(brfss_women))

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
  paste("Required vars MISSING:", if (length(missing)) paste(missing, collapse = ", ") else "none"),
  paste("PFD optional vars found:", if (length(pfd_found)) paste(pfd_found, collapse = ", ") else "none"),
  paste("PFD optional vars missing:", paste(pfd_missing, collapse = ", ")),
  "",
  "To use with the URPS demand pipeline:",
  "  brfss <- load_brfss_women('data-raw/brfss/brfss_2024_women18plus.rds')",
  "  cells <- build_urps_population_cells(brfss_women = brfss)",
  "",
  "Survey design for svydesign():",
  "  ids     = ~_PSU",
  "  strata  = ~_STSTR",
  "  weights = ~_LLCPWT",
  "  nest    = TRUE"
), manifest)

message("Manifest written: ", manifest)
message("\nDone. BRFSS 2024 download complete.")
if (length(missing) > 0) {
  message("\nACTION REQUIRED: ", length(missing), " required variable(s) missing.")
  message("Check the 2024 BRFSS codebook and update R/data-urps_population.R if variable names changed.")
}
