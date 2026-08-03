#!/usr/bin/env Rscript
# =============================================================================
# MCBS 2022 Download Script
# Medicare Current Beneficiary Survey — Public Use File (PUF)
# =============================================================================
#
# PURPOSE:
#   Download the 2022 MCBS Survey File Public Use File (PUF) from CMS.
#   The PUF is freely available; NO Data Use Agreement (DUA) is required.
#   The PUF is a de-identified, suppressed version of the Limited Data Set.
#
# TWO-TIER DATA ACCESS:
# ┌────────────────────┬──────────────┬──────────────────────────────────────┐
# │ File               │ DUA Required │ Notes                                │
# ├────────────────────┼──────────────┼──────────────────────────────────────┤
# │ Survey File PUF    │ NO  (free)   │ De-identified; no geography < state  │
# │ Cost Supplement PUF│ NO  (free)   │ Health care utilization & costs      │
# │ Survey File LDS    │ YES          │ More variables, county FIPS          │
# │ Cost Supplement LDS│ YES          │ Linked to Medicare claims            │
# └────────────────────┴──────────────┴──────────────────────────────────────┘
#
# KEY VARIABLES (PUF Survey File) FOR URPS DEMAND:
#   AGE_AT_INT     - Age at interview
#   SEX_TRM        - Sex
#   RACE_TRM       - Race/ethnicity
#   INSURED        - Insurance coverage indicator
#   REGION_TRM     - Census region (only geography in PUF)
#   HI_TRM         - Hospitalization indicator
#   URICOND_TRM    - Urinary conditions (if available)
#   CHRONIC_*      - Chronic condition indicators
#   WTFA_SU        - Survey weight (Fall)
#   WTFA_WIN       - Survey weight (Winter)
#
# NOTE ON GEOGRAPHY:
#   The PUF suppresses small geographies. State-level and county-level
#   identifiers require the LDS (Limited Data Set), which requires a DUA.
#
# DUA PROCESS (for LDS files):
#   See the documentation block at the bottom of this script.
#
# CURRENT PUF DOWNLOAD URLS (2022 data, released Oct 2024):
#   Survey File PUF: https://data.cms.gov/medicare-current-beneficiary-survey-mcbs/medicare-current-beneficiary-survey-survey-file
#   Cost Supplement PUF: https://data.cms.gov/medicare-current-beneficiary-survey-mcbs/medicare-current-beneficiary-survey-cost-supplement
#
# OUTPUT:
#   data-raw/mcbs/mcbs_2022_survey_puf_fall.csv
#   data-raw/mcbs/mcbs_2022_survey_puf_winter.csv
#   data-raw/mcbs/mcbs_2022_survey_puf_summer.csv
#   data-raw/mcbs/mcbs_2022_survey_puf_women65plus.rds (filtered)
#   data-raw/mcbs/mcbs_2022_manifest.txt
#   data-raw/mcbs/DUA_PROCESS.md (instructions for LDS application)
# =============================================================================

library(readr)
library(dplyr)

out_dir <- here::here("data-raw", "mcbs")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# =============================================================================
# PUF Download via CMS data.cms.gov API
# The MCBS PUF is available via the Socrata Open Data API (SODA)
# =============================================================================

# CMS data portal base URL for MCBS Survey File PUF
# Dataset identifiers from data.cms.gov:
#   Survey File 2022: https://data.cms.gov/resource/[id].csv
# The stable download page is:
#   https://www.cms.gov/data-research/research/medicare-current-beneficiary-survey

message("MCBS 2022 Public Use File (PUF) Download")
message("=========================================")
message("")

# Direct download URLs from data.cms.gov
# These are the CSV exports of the MCBS Survey File PUF 2022
# (three interview rounds: Fall, Winter, Summer)
puf_urls <- list(
  fall   = "https://data.cms.gov/sites/default/files/2024-10/SFPUF2022Fall.csv",
  winter = "https://data.cms.gov/sites/default/files/2024-10/SFPUF2022Winter.csv",
  summer = "https://data.cms.gov/sites/default/files/2024-10/SFPUF2022Summer.csv"
)

# Fallback: direct download page instructions if URLs above are stale
puf_page_url <- "https://www.cms.gov/data-research/research/medicare-current-beneficiary-survey"

downloaded <- list()
for (wave in names(puf_urls)) {
  csv_path <- file.path(out_dir, paste0("mcbs_2022_survey_puf_", wave, ".csv"))
  if (file.exists(csv_path) && file.size(csv_path) > 1e5) {
    message("  ", wave, ": already downloaded, skipping")
    downloaded[[wave]] <- csv_path
    next
  }
  message("  Downloading ", wave, " wave ...")
  result <- tryCatch({
    download.file(puf_urls[[wave]], csv_path, mode = "wb", method = "auto",
                  quiet = FALSE)
    message("  Downloaded: ", csv_path)
    csv_path
  }, error = function(e) {
    message("  ERROR downloading ", wave, ": ", conditionMessage(e))
    message("  URL may have changed. Check: ", puf_page_url)
    message("  See also: data-raw/mcbs/DUA_PROCESS.md for manual download instructions.")
    NA_character_
  })
  downloaded[[wave]] <- result
}

# ---- Read and combine -------------------------------------------------------
message("\nReading PUF CSVs ...")

puf_list <- list()
for (wave in names(downloaded)) {
  path <- downloaded[[wave]]
  if (!is.na(path) && file.exists(path)) {
    df <- tryCatch(read_csv(path, show_col_types = FALSE), error = function(e) NULL)
    if (!is.null(df)) {
      df$wave <- wave
      puf_list[[wave]] <- df
      message("  ", wave, ": ", format(nrow(df), big.mark = ","), " rows, ",
              ncol(df), " columns")
    }
  }
}

if (length(puf_list) == 0) {
  message("\nNo PUF files were successfully downloaded.")
  message("Please download manually from:")
  message("  ", puf_page_url)
  message("and place the CSV files in: ", out_dir)
} else {
  # Combine all waves
  puf_all <- bind_rows(puf_list)
  message("\nCombined: ", format(nrow(puf_all), big.mark = ","), " rows across ",
          length(puf_list), " wave(s)")

  # Filter to women 65+ (core Medicare urogynecology population)
  # Column names vary by release; search for sex and age columns
  sex_col <- names(puf_all)[grepl("SEX|sex", names(puf_all), ignore.case = TRUE)][1]
  age_col <- names(puf_all)[grepl("AGE|age", names(puf_all), ignore.case = TRUE)][1]

  message("  Sex column: ", sex_col, " | Age column: ", age_col)

  puf_women <- puf_all
  if (!is.na(sex_col)) {
    # Sex coding varies; 2 = female is standard in CMS data
    sex_vals <- unique(puf_all[[sex_col]])
    message("  Sex values found: ", paste(sort(sex_vals), collapse = ", "))
    # Try female = 2 first; if all are 1-2 coded
    female_code <- if (all(sex_vals %in% c(1, 2))) 2L else {
      sex_vals[grepl("female|woman|2", tolower(as.character(sex_vals)))][1]
    }
    puf_women <- puf_women[!is.na(puf_women[[sex_col]]) &
                           puf_women[[sex_col]] == female_code, ]
  }
  if (!is.na(age_col)) {
    puf_women <- puf_women[!is.na(puf_women[[age_col]]) &
                           as.numeric(puf_women[[age_col]]) >= 65, ]
  }

  message("  Women 65+ rows: ", format(nrow(puf_women), big.mark = ","))

  rds_path <- file.path(out_dir, "mcbs_2022_survey_puf_women65plus.rds")
  saveRDS(puf_women, rds_path)
  message("  Saved: ", rds_path)
}

# =============================================================================
# Write DUA process documentation
# =============================================================================
dua_doc_path <- file.path(out_dir, "DUA_PROCESS.md")
writeLines(c(
  "# MCBS Limited Data Set (LDS) — Data Use Agreement Process",
  "",
  "## Why You May Need the LDS (Not the PUF)",
  "",
  "The Public Use File (PUF) suppresses:",
  "- County-level geography (only region available)",
  "- Some demographic cells with small counts",
  "- Certain health utilization variables",
  "",
  "For URPS demand modeling, the LDS adds:",
  "- State and county FIPS codes (needed for geographic demand mapping)",
  "- Linked Medicare claims (FFS utilization of urogynecology services)",
  "- More detailed condition flags",
  "",
  "## DUA Application Process",
  "",
  "1. **Eligibility:** Researchers at academic institutions, government agencies,",
  "   and non-profit research organizations.",
  "",
  "2. **Apply via CMS ResDAC:**",
  "   - URL: https://www.resdac.org/cms-data/files/mcbs",
  "   - ResDAC (Research Data Assistance Center) assists with CMS data requests",
  "   - Start with the 'Request CMS Data' form on the ResDAC site",
  "",
  "3. **Required documents:**",
  "   - Data Use Agreement signed by your institution's Authorized Official",
  "   - Research protocol / IRB approval (or determination of exempt status)",
  "   - IT security plan for your computing environment",
  "   - CV/biosketch of principal investigator",
  "",
  "4. **Timeline:** Typically 3-6 months from application to data receipt.",
  "",
  "5. **Costs:** CMS charges a data use fee (typically $1,000-5,000 per file).",
  "",
  "6. **Data delivery:** Encrypted DVD or secure file transfer.",
  "",
  "## Key CMS Links",
  "",
  "- MCBS overview: https://www.cms.gov/data-research/research/medicare-current-beneficiary-survey",
  "- ResDAC MCBS page: https://resdac.org/cms-data/files/mcbs",
  "- PUF page (no DUA): https://www.cms.gov/data-research/research/medicare-current-beneficiary-survey/data-tables",
  "- Data User Guide 2022: https://data.cms.gov/sites/default/files/2024-10/SFPUF2022_DUG.pdf",
  "",
  "## Alternative: Use MCBS PUF for National Estimates",
  "",
  "The PUF is sufficient for:",
  "- National prevalence estimates of chronic conditions in Medicare beneficiaries",
  "- Age/sex distribution of Medicare beneficiaries",
  "- Insurance coverage patterns (Medicare Advantage vs. Traditional)",
  "- Health service utilization rates at national or regional level",
  "",
  "The PUF is NOT sufficient for:",
  "- State-level or county-level geographic demand mapping",
  "- Linking to Medicare claims for urogynecology procedure counts",
  "- Small-area estimation below census region",
  "",
  "For the URPS simulation, consider using MCBS PUF for calibration of the",
  "Medicare-aged demand arm, and ACS + NBER Medicare claims for geography.",
  "",
  paste("Last updated:", Sys.time())
), dua_doc_path)

message("\nDUA process documented: ", dua_doc_path)

# ---- Manifest ---------------------------------------------------------------
manifest_path <- file.path(out_dir, "mcbs_2022_manifest.txt")
writeLines(c(
  "MCBS 2022 Public Use File Download Manifest",
  paste("Generated:", Sys.time()),
  paste("Source:", puf_page_url),
  "",
  "PUF Files (NO DUA required):",
  "  mcbs_2022_survey_puf_fall.csv",
  "  mcbs_2022_survey_puf_winter.csv",
  "  mcbs_2022_survey_puf_summer.csv",
  "  mcbs_2022_survey_puf_women65plus.rds (filtered)",
  "",
  "LDS Files (DUA required — see DUA_PROCESS.md):",
  "  Survey File LDS: adds county FIPS, more variables",
  "  Cost Supplement LDS: links to Medicare claims",
  "",
  "Key reference: https://data.cms.gov/sites/default/files/2024-10/SFPUF2022_DUG.pdf",
  "",
  "For URPS demand arm:",
  "  PUF is sufficient for national/regional estimates",
  "  LDS needed for state/county geographic demand mapping"
), manifest_path)

message("Manifest written: ", manifest_path)
message("\nDone. MCBS 2022 PUF download attempt complete.")
message("  If download URLs failed, see: ", dua_doc_path)
