# MCBS Data Acquisition ----
#
# Source: Medicare Current Beneficiary Survey (MCBS), CMS
# Survey: Annual longitudinal survey of Medicare beneficiaries
# Format: SAS Transport (.XPT) or CSV — provided via CMS data portal
# DUA:    None required for Public Use Files (PUFs)
#
# How the raw files were obtained:
#
#   2022 (SFPUF2022):
#     Manually downloaded from:
#       https://data.cms.gov/medicare-current-beneficiary-survey-mcbs
#     File: SFPUF2022_Data.zip → extract → sfpuf2022/sfpuf2022.csv
#     Also available: CSPUF2022_Data.zip (Cost Supplement PUF)
#
#   2023 (SFPUF2023):
#     Download from CMS data portal:
#       https://data.cms.gov/medicare-current-beneficiary-survey-mcbs
#     Click "Survey File PUF 2023" → download SFPUF2023_Data.zip
#     Extract to: data-raw/mcbs/sfpuf2023/
#     Expected files: sfpuf2023.csv, SFPUF2023_Data_User_Guide.pdf
#
#     Alternatively, try these direct URL patterns (may change with each release):
#       Announced July 2025 per CMS MCBS announcements page.
#       Check: https://www.cms.gov/data-research/research/medicare-current-beneficiary-survey-mcbs/announcements
#
# Key variables (stable across 2022-2023):
#   PUFFWGT     — Analytic weight
#   HLT_LOSTURIN — Lost urine (urinary incontinence)
#   HLT_TALKURIN — Talked to doctor about UI
#   HLT_SURGURIN — Had UI surgery
#   HLT_OCSTROKE, HLT_OCCANCER, HLT_OCDEPRSS, HLT_OCOSTEOP — Comorbidities
#   AGE          — Age at time of interview
#   SEX_IDENT    — Sex (1=Male, 2=Female; exact name may vary by year)
#   RACE_5WAY    — Race/ethnicity
#   MDCR_ENTLMT  — Medicare entitlement type

library(dplyr)
library(haven)

# ---- Helper to process an MCBS Survey File PUF CSV -------------------------

process_mcbs_sfpuf <- function(year, csv_path = NULL, xpt_path = NULL) {
  if (is.null(csv_path) && is.null(xpt_path)) {
    csv_path <- file.path("data-raw", "mcbs",
                          paste0("sfpuf", year), paste0("sfpuf", year, ".csv"))
    xpt_path <- file.path("data-raw", "mcbs",
                          paste0("sfpuf", year), paste0("sfpuf", year, ".xpt"))
  }

  # Read CSV or XPT (whichever is present)
  if (!is.null(csv_path) && file.exists(csv_path)) {
    cat("Reading MCBS", year, "from CSV:", csv_path, "\n")
    raw <- read.csv(csv_path, stringsAsFactors = FALSE)
  } else if (!is.null(xpt_path) && file.exists(xpt_path)) {
    cat("Reading MCBS", year, "from XPT:", xpt_path, "\n")
    raw <- haven::read_xpt(xpt_path)
  } else {
    stop(
      "MCBS ", year, " data not found.\n",
      "Expected CSV at: ", csv_path %||% "(not specified)", "\n",
      "or XPT at: ", xpt_path %||% "(not specified)", "\n\n",
      "Download SFPUF", year, "_Data.zip from:\n",
      "  https://data.cms.gov/medicare-current-beneficiary-survey-mcbs\n",
      "and extract to: data-raw/mcbs/sfpuf", year, "/"
    )
  }

  cat("Raw rows:", nrow(raw), "| cols:", ncol(raw), "\n")

  # Detect sex variable (name differs slightly across years)
  sex_var <- intersect(c("SEX_IDENT", "BENE_SEX_CVR_CD", "SEX"), names(raw))[1]
  if (is.na(sex_var)) stop("Cannot find sex variable in MCBS ", year, " file")

  # Filter to women 65+
  women65 <- raw |>
    dplyr::filter(
      .data[[sex_var]] == 2,          # 2 = Female (standard MCBS coding)
      .data$AGE >= 65
    )
  cat("Women 65+:", nrow(women65), "\n")

  out_rds <- file.path("data-raw", "mcbs",
                       paste0("mcbs_", year, "_women65plus.rds"))
  saveRDS(women65, out_rds)
  cat("Saved:", out_rds, "\n")

  # Manifest
  manifest_lines <- c(
    paste("MCBS", year, "Survey File PUF — Women 65+ Sub-file"),
    paste("Generated:", Sys.time()),
    paste("Source: SFPUF", year, "_Data.zip (CMS data.cms.gov)", sep = ""),
    paste("Raw rows (all respondents):", nrow(raw)),
    paste("Women 65+ rows:", nrow(women65)),
    "",
    "Key urinary incontinence variables:",
    "  HLT_LOSTURIN  — lost urine (UI)",
    "  HLT_TALKURIN  — talked to doctor about UI",
    "  HLT_REASURIN  — reason for not talking to doctor",
    "  HLT_SURGURIN  — had UI surgery",
    "",
    "Chronic conditions:",
    "  HLT_OCSTROKE, HLT_OCCANCER, HLT_OCDEPRSS, HLT_OCOSTEOP",
    "",
    paste("Survey weight: PUFFWGT"),
    "DUA required: NO (Public Use File)"
  )
  manifest_path <- file.path("data-raw", "mcbs",
                             paste0("mcbs_", year, "_manifest.txt"))
  writeLines(manifest_lines, manifest_path)
  cat("Manifest:", manifest_path, "\n\n")

  invisible(women65)
}

# Infix coalesce for NULL
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ---- Run for 2023 -----------------------------------------------------------
#
# Once SFPUF2023_Data.zip is downloaded and extracted to data-raw/mcbs/sfpuf2023/:
process_mcbs_sfpuf(2023)

# For 2022 (already processed, can re-run if needed):
# process_mcbs_sfpuf(2022,
#   csv_path = "data-raw/mcbs/cspuf2022/cspuf2022.csv")
