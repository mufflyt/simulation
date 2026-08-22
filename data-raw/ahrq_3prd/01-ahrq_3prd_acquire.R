# AHRQ 3P-RD Medicare/Medicaid Claims-Volume Acquisition -----------------
#
# Source: Physician and Physician Practice Research Database (3P-RD),
#         Physician Geographic Public Use File
# Agency: Agency for Healthcare Research and Quality (AHRQ)
# URL:    https://www.ahrq.gov/data/innovations/3p-rd.html
# States: AR, AZ, CA, CO, FL, MA, MD, MN, MO, MT, NY, TX, WA (13 states)
# Data year: 2019-2020 (SMB_REF_YEAR / NPPES_FILE_REF / PECOS_FILE_REF vary
#   by state; see the codebook for exact per-state reference periods)
#
# How the raw files were obtained (free PUF, no data use agreement required):
#   For each state abbreviation ST in the 13 states above, download:
#   https://www.ahrq.gov/sites/default/files/wysiwyg/data/3prd/ST_PUF_csv.zip
#   Each ZIP contains 4 files per state; only *_Physician_Geographic_PUF.csv
#   (or *_Physician_PUF.csv for the one state, MA, using shorter file names)
#   is used here -- summarized at the 3-digit ZIP code level.
#
# Column definitions confirmed against the official AHRQ codebook, not
# assumed: https://www.ahrq.gov/sites/default/files/wysiwyg/data/
#   3P-RD-geographic-physician-puf-codebook.pdf
#   MCARE_COUNT / MCAID_COUNT: count of physicians accepting Medicare /
#     Medicaid in the ZIP3 (source: APCD/CMS claims).
#   AVG_CLMS_PERMONTH_MCAREFFS / AVG_CLMS_PERMONTH_MCAID: average number of
#     Medicare-FFS / Medicaid (FFS & MCO) claims per month per physician
#     across all 3P-RD physicians in the ZIP3.
#
# IMPORTANT COVERAGE CAVEAT, confirmed by inspecting all 13 raw files
# directly (not assumed): AVG_CLMS_PERMONTH_CMRCL (commercial claims) and
# the Medicaid-MCO / Medicare Advantage claims columns are populated in only
# 4 of the 13 states (AR, CO, MD, WA), and sparsely even there (~14-17 ZIP3
# rows per state). Medicare-FFS and Medicaid (FFS & MCO combined) columns
# are populated in all 13 states (325 ZIP3 rows total). For this reason only
# the Medicare/Medicaid claims-volume columns are vendored here; commercial
# and self-pay shares for the practice-economics payer mix come from NAMCS
# instead (see R/supply-practice_payer_mix.R and namcs_urps_payer_mix()).
#
# WHAT THIS NUMBER IS NOT: physician_geographic is a ZIP3-level aggregate
# across ALL physician specialties in the 3P-RD sample, not filterable to
# OB/GYN or urology at this grain (physician-level specialty fields live in
# a different 3P-RD table, physician_directory, which cannot be joined back
# to these payer columns since they are pre-aggregated). A direct comparison
# against NAMCS's URPS-specific Medicare:Medicaid visit-share ratio (see
# namcs_urps_payer_mix()) found the two disagree substantially (3P-RD:
# ~59% Medicare / 41% Medicaid within the government-payer bucket, all
# physician specialties pooled; NAMCS, URPS-specific: ~92%/8%) -- plausibly
# because URPS conditions skew heavily toward the Medicare-eligible 65+
# population, which a general, all-specialty claims sample does not
# reflect. ahrq_3prd_medicare_medicaid_ratio() is therefore reported ONLY as
# an independent, clearly-labeled cross-check value alongside the NAMCS-
# primary payer mix -- never blended into practice_payer_mix_defaults().
#
# This script downloads the 13 state ZIPs, extracts the Physician Geographic
# PUF from each, and aggregates to the small per-state summary vendored at
# data-raw/ahrq_3prd/ahrq_3prd_medicare_medicaid_claims_by_state.csv (the
# 25MB+ raw per-physician-ZIP3 files are not vendored; only this ~1KB
# derived summary is).

library(dplyr)

states <- c(
  "AR", "AZ", "CA", "CO", "FL", "MA", "MD", "MN", "MO", "MT", "NY", "TX", "WA"
)

download_dir <- tempfile("ahrq_3prd_puf_")
dir.create(download_dir)

geo_files <- character(0)
for (st in states) {
  url <- sprintf(
    "https://www.ahrq.gov/sites/default/files/wysiwyg/data/3prd/%s_PUF_csv.zip",
    st
  )
  zip_path <- file.path(download_dir, paste0(st, "_PUF_csv.zip"))
  utils::download.file(url, zip_path, mode = "wb", quiet = TRUE)
  extract_dir <- file.path(download_dir, st)
  dir.create(extract_dir)
  utils::unzip(zip_path, exdir = extract_dir)
  # Most states name the file "*_Physician_Geographic_PUF.csv"; a few
  # (e.g. MA) use the shorter "*_Physician_PUF.csv" naming.
  candidate <- list.files(
    extract_dir,
    pattern = "(Physician_Geographic_PUF|Physician_PUF)\\.csv$",
    full.names = TRUE, ignore.case = TRUE
  )
  candidate <- candidate[grepl("Geographic", candidate, ignore.case = TRUE)]
  geo_files <- c(geo_files, candidate)
}

read_one <- function(f) {
  readr::read_csv(f, col_types = readr::cols(.default = "c"), show_col_types = FALSE) |>
    transmute(
      state = STATE,
      zip3 = ZIP3_CD,
      mcare_count = suppressWarnings(as.numeric(MCARE_COUNT)),
      mcaid_count = suppressWarnings(as.numeric(MCAID_COUNT)),
      avg_clms_mcareffs = suppressWarnings(as.numeric(AVG_CLMS_PERMONTH_MCAREFFS)),
      avg_clms_mcaid = suppressWarnings(as.numeric(AVG_CLMS_PERMONTH_MCAID))
    )
}

all_geo <- purrr::map_dfr(geo_files, read_one)

summary_tbl <- all_geo |>
  mutate(
    medicare_claims_permonth = mcare_count * avg_clms_mcareffs,
    medicaid_claims_permonth = mcaid_count * avg_clms_mcaid
  ) |>
  group_by(state) |>
  summarise(
    n_zip3 = dplyr::n(),
    total_mcare_providers = sum(mcare_count, na.rm = TRUE),
    total_mcaid_providers = sum(mcaid_count, na.rm = TRUE),
    total_medicare_claims_permonth = sum(medicare_claims_permonth, na.rm = TRUE),
    total_medicaid_claims_permonth = sum(medicaid_claims_permonth, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(state)

readr::write_csv(
  summary_tbl,
  "data-raw/ahrq_3prd/ahrq_3prd_medicare_medicaid_claims_by_state.csv"
)
