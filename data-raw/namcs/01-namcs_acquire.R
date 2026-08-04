# NAMCS 2019 Data Acquisition ----
#
# Source: 2019 National Ambulatory Medical Care Survey (NAMCS) Public Use File
# Agency: National Center for Health Statistics (NCHS), CDC
# URL:    https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/namcs2019.zip
# Docs:   https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/NAMCS/2019/
#
# How the raw file was obtained:
#   1. Download namcs2019.zip from the CDC NAMCS FTP server (link above).
#   2. Unzip → namcs2019 (fixed-width ASCII, 8,250 rows × 2,543 chars/row).
#   3. Place the unzipped file at data-raw/nhamcs/namcs2019 (already done).
#
# The Stata dictionary (namdict2019.dct) was fetched from:
#   https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/NAMCS/2019/namdict2019.dct
# and used to confirm all column positions below.  The SPSS syntax file (namcs2019.sps)
# was used to confirm variable labels and value codes.
#
# DATA USE RESTRICTIONS: NAMCS data may be used only for statistical reporting
# and analysis. Do not attempt to identify individuals or establishments.
# See data-raw/nhamcs/namcs2019_readme.txt for the full NCHS data use agreement.
#
# This script reads the fixed-width file, selects the columns needed for
# URPS demand estimation, and saves data-raw/namcs/namcs2019_clean.rds.

library(readr)
library(dplyr)

# ---- Column layout (from namdict2019.dct) ------------------------------------
#
# Each position is 1-based. Widths and types follow the Stata dictionary.
# Only columns used in the URPS demand module are retained.

NAMCS_2019_COLS <- readr::fwf_positions(
  start = c(  1,   3,   4,  11,  16,  18,  20,  21,  22,  32,  34,  95, 101, 107, 166, 703, 2502, 2510, 2516, 2521),
  end   = c(  2,   3,   6,  11,  17,  19,  20,  21,  22,  33,  35,  98, 104, 110, 173, 703, 2509, 2515, 2519, 2533),
  col_names = c(
    "VMONTH",    # 1-2   Visit month (1-12)
    "VDAYR",     # 3     Day of week (1=Sunday)
    "AGE",       # 4-6   Patient age in years
    "SEX",       # 11    Sex (1=FEMALE, 2=MALE -- NAMCS convention, the reverse
                 #       of Census/ACS/BRFSS/MEPS. Confirmed against N40 and C61
                 #       (male-only, all SEX=2) and Z34/N81/C50 (female-only,
                 #       all SEX=1) in this file.)
    "ETHUN",     # 16-17 Ethnicity unimputed
    "RACEUN",    # 18-19 Race unimputed
    "ETHIM",     # 20    Ethnicity imputed
    "RACER",     # 21    Race imputed (1=White, 2=Black, 3=Other)
    "RACERETH",  # 22    Race/ethnicity imputed (1=NH-White, 2=NH-Black, 3=Hispanic, 4=NH-Other)
    "PAYTYPER",  # 32-33 Type of payment (1=Private, 2=Medicare, 3=Medicaid/CHIP, 5=Self-pay, 7=Other)
    "USETOBAC",  # 34-35 Tobacco use (1=Current, 2=Former, 3=Never, -8=Unknown)
    "DIAG1",     # 95-98 ICD-10-CM diagnosis 1
    "DIAG2",     # 101-104 ICD-10-CM diagnosis 2
    "DIAG3",     # 107-110 ICD-10-CM diagnosis 3
    "BMI",       # 166-173 Body mass index (format xxxxx.xx stored as 00xxx.xx)
    "SPECCAT",   # 703   Specialty category (1=Primary, 2=Medical specialist, 3=Surgical specialist)
    "CSTRATM",   # 2502-2509 Survey stratum (for svydesign)
    "CPSUM",     # 2510-2515 PSU cluster (for svydesign)
    "YEAR",      # 2516-2519 Survey year (should be 2019 for all rows)
    "PATWT"      # 2521-2533 Patient visit weight (use to compute national totals)
  )
)

# ---- Read fixed-width file ---------------------------------------------------
raw_path <- file.path("data-raw", "nhamcs", "namcs2019")
if (!file.exists(raw_path)) {
  stop(
    "NAMCS 2019 raw file not found at ", raw_path, "\n",
    "Download namcs2019.zip from:\n",
    "  https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/namcs2019.zip\n",
    "and unzip the ASCII file to data-raw/nhamcs/namcs2019"
  )
}

namcs_raw <- readr::read_fwf(
  raw_path,
  col_positions = NAMCS_2019_COLS,
  col_types = readr::cols(
    VMONTH   = readr::col_integer(),
    VDAYR    = readr::col_integer(),
    AGE      = readr::col_integer(),
    SEX      = readr::col_integer(),
    ETHUN    = readr::col_integer(),
    RACEUN   = readr::col_integer(),
    ETHIM    = readr::col_integer(),
    RACER    = readr::col_integer(),
    RACERETH = readr::col_integer(),
    PAYTYPER = readr::col_integer(),
    USETOBAC = readr::col_integer(),
    DIAG1    = readr::col_character(),
    DIAG2    = readr::col_character(),
    DIAG3    = readr::col_character(),
    BMI      = readr::col_double(),
    SPECCAT  = readr::col_integer(),
    CSTRATM  = readr::col_integer(),
    CPSUM    = readr::col_integer(),
    YEAR     = readr::col_integer(),
    PATWT    = readr::col_double()
  ),
  show_col_types = FALSE
)

# ---- Basic cleaning ---------------------------------------------------------
#
# NAMCS uses -9 for "blank/not applicable" and -8 for "unknown".
# Replace negative sentinel values with NA in integer columns.

namcs_clean <- namcs_raw |>
  dplyr::mutate(
    across(c(AGE, SEX, RACER, RACERETH, PAYTYPER, USETOBAC, SPECCAT, ETHUN, RACEUN, ETHIM),
           ~ dplyr::if_else(.x < 0, NA_integer_, .x)),
    BMI = dplyr::if_else(BMI <= 0, NA_real_, BMI),
    PATWT = dplyr::if_else(PATWT <= 0, NA_real_, PATWT),
    # Trim ICD codes: NAMCS pads with "-" and spaces
    across(c(DIAG1, DIAG2, DIAG3), ~ trimws(gsub("-$", "", trimws(.x))))
  )

cat("NAMCS 2019 parsed:", nrow(namcs_clean), "visits\n")
cat("YEAR check (all 2019):", all(namcs_clean$YEAR == 2019, na.rm = TRUE), "\n")
cat("SEX distribution:\n"); print(table(namcs_clean$SEX, useNA = "ifany"))
cat("SPECCAT distribution:\n"); print(table(namcs_clean$SPECCAT, useNA = "ifany"))
cat("PATWT range:", range(namcs_clean$PATWT, na.rm = TRUE), "\n")

# ---- Save cleaned file -------------------------------------------------------
out_path <- file.path("data-raw", "namcs", "namcs2019_clean.rds")
saveRDS(namcs_clean, out_path)
cat("Saved:", out_path, "\n")
