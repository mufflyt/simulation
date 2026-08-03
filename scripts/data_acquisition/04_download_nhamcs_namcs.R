#!/usr/bin/env Rscript
# =============================================================================
# NHAMCS / NAMCS Download Script
# Ambulatory Care Utilization — Physician Office Visit Data
# =============================================================================
#
# IMPORTANT NOTE ON DATA AVAILABILITY:
#   "NHAMCS" historically covered two settings:
#     (a) Outpatient Department (OPD) visits — hospital-based clinics
#     (b) Emergency Department (ED) visits
#   The OPD component was DISCONTINUED after 2017.
#   2022 was the LAST year of NHAMCS collection (ED only).
#
#   For PHYSICIAN OFFICE VISITS, the correct survey is:
#     NAMCS — National Ambulatory Medical Care Survey
#     Collected annually by NCHS/CDC since 1973
#     Most recent public-use year: 2019 (2020-2022 had pandemic disruptions;
#     2022 saw redesign; 2023 was pilot/provider-only, no visit file)
#
# STRATEGY FOR THIS SCRIPT:
#   1. Download NHAMCS 2022 ED file (available, free) — useful for ER visits
#      with ICD-10 pelvic floor disorder codes (R32, N39.3, N81.x, R15)
#   2. Download NAMCS 2019 physician office visit file (last full public-use
#      microdata with national estimates)
#   3. Check for any 2021 NAMCS health center component file
#
# KEY ICD-10 CODES FOR PELVIC FLOOR DISORDERS (URPS demand signal):
#   N39.3  - Stress urinary incontinence
#   N39.41 - Urge incontinence
#   N39.46 - Mixed incontinence
#   R32    - Unspecified urinary incontinence
#   N81.0  - Urethrocele
#   N81.1x - Cystocele
#   N81.2  - Incomplete uterovaginal prolapse
#   N81.3  - Complete uterovaginal prolapse
#   N81.4  - Unspecified uterovaginal prolapse
#   N81.5  - Vaginal enterocele
#   N81.6  - Rectocele
#   R15    - Fecal incontinence (fecal incontinence = "pelvic floor disorder")
#   N39.0  - Urinary tract infection
#
# KEY NAMCS/NHAMCS VARIABLES:
#   DIAG1-5     - Diagnoses (ICD-10-CM codes)
#   PAYTYPE     - Primary payer (Medicare, Medicaid, private, etc.)
#   SEX         - Patient sex
#   AGE         - Patient age (years)
#   AGER        - Age recode
#   REGION      - Census region
#   PATWT       - Patient visit weight (for national estimates)
#   VSITE       - Visit setting
#   SPECCAT     - Physician specialty category
#   SPECR       - Physician specialty recode
#
# DATA ACCESS:
#   Freely available. No DUA required. Hosted on CDC FTP server.
#   NHAMCS: https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NHAMCS/
#   NAMCS:  https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/
#
# OUTPUT:
#   data-raw/nhamcs/nhamcs_2022_ed_raw.rds
#   data-raw/nhamcs/namcs_2019_raw.rds
#   data-raw/nhamcs/nhamcs_2022_ed_pelvic_floor.rds  (filtered)
#   data-raw/nhamcs/namcs_2019_pelvic_floor.rds      (filtered)
#   data-raw/nhamcs/nhamcs_namcs_manifest.txt
# =============================================================================

library(haven)
library(dplyr)
library(readr)

out_dir <- here::here("data-raw", "nhamcs")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# CDC FTP base URLs
nhamcs_ftp <- "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NHAMCS/"
namcs_ftp  <- "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/"

# ---- Pelvic floor ICD-10 codes for filtering --------------------------------
pfd_icd10 <- c(
  # Urinary incontinence
  "N393", "N3940", "N3941", "N3942", "N3943", "N3944", "N3945", "N3946",
  "N3949", "R32",
  # Pelvic organ prolapse
  "N810", "N811", "N8110", "N8111", "N8112", "N812", "N813", "N814",
  "N8140", "N8141", "N8142", "N8143", "N8144", "N8145", "N815", "N816",
  "N8189", "N819",
  # Fecal incontinence
  "R150", "R151", "R159"
)

# Regex pattern for ICD-10 matching
pfd_pattern <- paste0("^(", paste(pfd_icd10, collapse = "|"), ")")

filter_pelvic_floor <- function(df) {
  # Find diagnosis columns
  diag_cols <- names(df)[grepl("^DIAG|^ICD", names(df), ignore.case = TRUE)]
  if (length(diag_cols) == 0) {
    message("  WARNING: No diagnosis columns found; returning all rows")
    return(df)
  }
  # Any diagnosis matches a pelvic floor code
  diag_matrix <- sapply(diag_cols, function(col) {
    grepl(pfd_pattern, toupper(gsub("\\.", "", df[[col]])), perl = TRUE)
  })
  if (is.vector(diag_matrix)) diag_matrix <- matrix(diag_matrix, ncol = 1)
  has_pfd <- rowSums(diag_matrix, na.rm = TRUE) > 0
  df[has_pfd, ]
}

# =============================================================================
# 1. NHAMCS 2022 Emergency Department (last year available)
# =============================================================================
message("[1/2] NHAMCS 2022 Emergency Department file ...")
message("  Note: NHAMCS OPD component discontinued after 2017")
message("  Only ED data available for 2022")

nhamcs_ed_url <- paste0(nhamcs_ftp, "ED2022.zip")
nhamcs_ed_zip <- file.path(out_dir, "ED2022.zip")
nhamcs_ed_xpt <- file.path(out_dir, "ED2022.xpt")
nhamcs_ed_rds <- file.path(out_dir, "nhamcs_2022_ed_raw.rds")
nhamcs_ed_pfd <- file.path(out_dir, "nhamcs_2022_ed_pelvic_floor.rds")

if (!file.exists(nhamcs_ed_rds)) {
  message("  Downloading NHAMCS 2022 ED ZIP ...")
  result <- tryCatch({
    download.file(nhamcs_ed_url, nhamcs_ed_zip, mode = "wb", method = "auto")
    message("  Downloaded: ", nhamcs_ed_zip, " (",
            round(file.size(nhamcs_ed_zip) / 1e6, 1), " MB)")
    TRUE
  }, error = function(e) {
    message("  ERROR: ", conditionMessage(e))
    message("  Check NHAMCS FTP: ", nhamcs_ftp)
    FALSE
  })

  if (result) {
    message("  Unzipping ...")
    unzip(nhamcs_ed_zip, exdir = out_dir)
    xpt_files <- list.files(out_dir, pattern = "\\.xpt$|\\.XPT$",
                            full.names = TRUE, ignore.case = TRUE)
    if (length(xpt_files) > 0) {
      message("  Reading XPT: ", xpt_files[1])
      ed_raw <- haven::read_xpt(xpt_files[1])
      names(ed_raw) <- toupper(names(ed_raw))
      message("  Raw rows: ", format(nrow(ed_raw), big.mark = ","),
              " | Columns: ", ncol(ed_raw))
      saveRDS(ed_raw, nhamcs_ed_rds)
      message("  Saved: ", nhamcs_ed_rds)

      # Filter to pelvic floor visits
      ed_pfd <- filter_pelvic_floor(ed_raw)
      message("  Pelvic floor ED visits: ", format(nrow(ed_pfd), big.mark = ","))
      saveRDS(ed_pfd, nhamcs_ed_pfd)
      message("  Saved: ", nhamcs_ed_pfd)
    } else {
      # Try SAS format
      sas_files <- list.files(out_dir, pattern = "\\.sas7bdat$",
                               full.names = TRUE, ignore.case = TRUE)
      if (length(sas_files) > 0) {
        ed_raw <- haven::read_sas(sas_files[1])
        names(ed_raw) <- toupper(names(ed_raw))
        saveRDS(ed_raw, nhamcs_ed_rds)
      } else {
        message("  WARNING: No XPT or SAS file found after unzip")
        message("  Manual download: ", nhamcs_ftp)
      }
    }
  }
} else {
  message("  NHAMCS 2022 ED raw RDS already exists, loading ...")
  ed_raw <- readRDS(nhamcs_ed_rds)
  message("  Rows: ", format(nrow(ed_raw), big.mark = ","))
  if (!file.exists(nhamcs_ed_pfd)) {
    ed_pfd <- filter_pelvic_floor(ed_raw)
    saveRDS(ed_pfd, nhamcs_ed_pfd)
    message("  PFD filter applied: ", format(nrow(ed_pfd), big.mark = ","),
            " rows saved")
  }
}

# =============================================================================
# 2. NAMCS 2019 Physician Office Visit File
#    (last year with full public-use microdata and national estimates)
# =============================================================================
message("\n[2/2] NAMCS 2019 Physician Office Visit file ...")
message("  Reason for 2019: 2020-2022 had pandemic disruptions/redesign;")
message("  2023 = pilot year, no visit-level public-use file available")

namcs_2019_url  <- paste0(namcs_ftp, "NAMCS2019.zip")
namcs_2019_zip  <- file.path(out_dir, "NAMCS2019.zip")
namcs_2019_rds  <- file.path(out_dir, "namcs_2019_raw.rds")
namcs_2019_pfd  <- file.path(out_dir, "namcs_2019_pelvic_floor.rds")

if (!file.exists(namcs_2019_rds)) {
  message("  Downloading NAMCS 2019 ZIP ...")
  result <- tryCatch({
    download.file(namcs_2019_url, namcs_2019_zip, mode = "wb", method = "auto")
    message("  Downloaded: ", namcs_2019_zip, " (",
            round(file.size(namcs_2019_zip) / 1e6, 1), " MB)")
    TRUE
  }, error = function(e) {
    message("  ERROR: ", conditionMessage(e))
    message("  Trying alternative filename pattern ...")
    # Try alternate naming
    alt_url <- paste0(namcs_ftp, "namcs2019-ASCII.zip")
    tryCatch({
      download.file(alt_url, namcs_2019_zip, mode = "wb", method = "auto")
      TRUE
    }, error = function(e2) {
      message("  ERROR on alt URL too: ", conditionMessage(e2))
      message("  Check NAMCS FTP: ", namcs_ftp)
      FALSE
    })
  })

  if (result) {
    message("  Unzipping ...")
    unzip(namcs_2019_zip, exdir = out_dir)

    # Look for various formats: XPT, SAS, ASCII
    xpt_files <- list.files(out_dir, pattern = "2019.*\\.xpt$|namcs19.*\\.xpt$",
                             full.names = TRUE, ignore.case = TRUE)
    sas_files <- list.files(out_dir, pattern = "2019.*\\.sas7bdat$",
                             full.names = TRUE, ignore.case = TRUE)
    txt_files <- list.files(out_dir, pattern = "2019.*\\.txt$|namcs19.*\\.txt$",
                             full.names = TRUE, ignore.case = TRUE)

    namcs_raw <- NULL
    if (length(xpt_files) > 0) {
      message("  Reading XPT: ", xpt_files[1])
      namcs_raw <- haven::read_xpt(xpt_files[1])
    } else if (length(sas_files) > 0) {
      message("  Reading SAS: ", sas_files[1])
      namcs_raw <- haven::read_sas(sas_files[1])
    } else {
      message("  No XPT/SAS found. NAMCS often distributes as ASCII + SAS input code.")
      message("  Manual download required from: ", namcs_ftp)
      message("  Look for NAMCS2019.zip or namcs2019-ASCII.zip")
    }

    if (!is.null(namcs_raw)) {
      names(namcs_raw) <- toupper(names(namcs_raw))
      message("  Raw rows: ", format(nrow(namcs_raw), big.mark = ","),
              " | Columns: ", ncol(namcs_raw))
      saveRDS(namcs_raw, namcs_2019_rds)
      message("  Saved: ", namcs_2019_rds)

      # Filter to women + pelvic floor codes
      sex_col <- names(namcs_raw)[grepl("^SEX$", names(namcs_raw))][1]
      if (!is.na(sex_col)) {
        namcs_women <- namcs_raw[!is.na(namcs_raw[[sex_col]]) &
                                  namcs_raw[[sex_col]] == 2, ]
      } else {
        namcs_women <- namcs_raw
      }

      namcs_pfd <- filter_pelvic_floor(namcs_women)
      message("  Female pelvic floor office visits: ",
              format(nrow(namcs_pfd), big.mark = ","))
      saveRDS(namcs_pfd, namcs_2019_pfd)
      message("  Saved: ", namcs_2019_pfd)
    }
  }
} else {
  message("  NAMCS 2019 raw RDS already exists, loading ...")
  namcs_raw <- readRDS(namcs_2019_rds)
  message("  Rows: ", format(nrow(namcs_raw), big.mark = ","))
  if (!file.exists(namcs_2019_pfd)) {
    sex_col <- names(namcs_raw)[grepl("^SEX$", names(namcs_raw))][1]
    namcs_women <- if (!is.na(sex_col))
      namcs_raw[!is.na(namcs_raw[[sex_col]]) & namcs_raw[[sex_col]] == 2, ]
    else namcs_raw
    namcs_pfd <- filter_pelvic_floor(namcs_women)
    saveRDS(namcs_pfd, namcs_2019_pfd)
    message("  PFD filter: ", format(nrow(namcs_pfd), big.mark = ","), " rows")
  }
}

# =============================================================================
# Survey design note
# =============================================================================
message("\nSurvey design for NAMCS/NHAMCS (use with survey::svydesign):")
message("  NAMCS:  ids = ~CPSUM, strata = ~CSTRATM, weights = ~PATWT")
message("  NHAMCS: ids = ~CPSUM, strata = ~CSTRATM, weights = ~PATWT")

# =============================================================================
# Manifest
# =============================================================================
manifest_path <- file.path(out_dir, "nhamcs_namcs_manifest.txt")
writeLines(c(
  "NHAMCS/NAMCS Download Manifest",
  paste("Generated:", Sys.time()),
  "",
  "CRITICAL NOTE:",
  "  NHAMCS OPD (Outpatient Department) discontinued after 2017.",
  "  NHAMCS ED (Emergency Department) last collected 2022.",
  "  For PHYSICIAN OFFICE VISITS, use NAMCS, not NHAMCS.",
  "",
  "Files attempted:",
  paste("  NHAMCS 2022 ED (raw):", nhamcs_ed_rds),
  paste("  NHAMCS 2022 ED (pelvic floor only):", nhamcs_ed_pfd),
  paste("  NAMCS 2019 office visits (raw):", namcs_2019_rds),
  paste("  NAMCS 2019 office visits (pelvic floor only):", namcs_2019_pfd),
  "",
  "Pelvic floor ICD-10 codes used for filtering:",
  "  Urinary incontinence: N39.3, N39.4x, R32",
  "  Pelvic organ prolapse: N81.x",
  "  Fecal incontinence: R15.x",
  "",
  "Survey design:",
  "  ids = ~CPSUM, strata = ~CSTRATM, weights = ~PATWT, nest = TRUE",
  "",
  "FTP Sources (no DUA required):",
  paste("  NHAMCS:", nhamcs_ftp),
  paste("  NAMCS: ", namcs_ftp),
  "",
  "Key documentation:",
  "  NHAMCS 2022 ED: https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/NHAMCS/doc22-ed-508.pdf",
  "  NAMCS 2019: https://www.cdc.gov/nchs/namcs/documentation/"
), manifest_path)

message("\nManifest written: ", manifest_path)
message("Done. NHAMCS/NAMCS download attempt complete.")
