#!/usr/bin/env Rscript
# =============================================================================
# ACS 2023 5-Year Estimates Download Script
# American Community Survey via tidycensus
# =============================================================================
#
# PURPOSE:
#   Pull ACS 2019-2023 5-year PUMS microdata and summary-table totals for
#   the URPS demand denominator: female population by age band, insurance
#   status, income, and rural/urban geography.
#
# KEY TABLES:
#   B01001  - Sex by Age (universe: total population)
#   B27001  - Health Insurance Coverage Status by Sex by Age
#   B19001  - Household Income (for poverty-ratio proxying)
#   B17001  - Poverty Status by Sex by Age
#   B08201  - Household Size by Vehicles Available (proxy for access)
#
# KEY PUMS VARIABLES (person-level microdata):
#   AGEP    - Age
#   SEX     - Sex (1=male, 2=female)
#   HINS1-6 - Health insurance type indicators
#   PINCP   - Personal income
#   POVPIP  - Income-to-poverty ratio (0-501)
#   ST      - State FIPS
#   PUMA    - Public Use Microdata Area
#   PWGTP   - Person weight (primary)
#   PWGTP1-80 - Replicate weights for variance estimation
#
# DATA ACCESS:
#   Requires a free Census API key. Register at:
#   https://api.census.gov/data/key_signup.html
#   No DUA required. Freely available.
#
# OUTPUT:
#   data-raw/acs/acs5_2023_sex_by_age_state.rds       (B01001 by state)
#   data-raw/acs/acs5_2023_insurance_by_age_state.rds (B27001 by state)
#   data-raw/acs/acs5_2023_income_state.rds            (B19001 by state)
#   data-raw/acs/acs5_2023_pums_women18plus.rds        (person-level PUMS)
#   data-raw/acs/acs_2023_manifest.txt
# =============================================================================

# ---- Check for API key -------------------------------------------------------
if (!requireNamespace("tidycensus", quietly = TRUE)) {
  stop("Package 'tidycensus' is required. Install with: install.packages('tidycensus')")
}
library(tidycensus)
library(dplyr)
library(readr)
library(purrr)

# Check for Census API key
census_key <- Sys.getenv("CENSUS_API_KEY")
if (nchar(census_key) == 0) {
  # Try tidycensus stored key
  census_key <- tryCatch(
    tidycensus:::get_census_api_key(),
    error = function(e) ""
  )
}
if (nchar(census_key) == 0) {
  stop(
    "No Census API key found.\n",
    "  1. Register for a free key at: https://api.census.gov/data/key_signup.html\n",
    "  2. Set it with: tidycensus::census_api_key('YOUR_KEY', install = TRUE)\n",
    "  3. Or set env var: CENSUS_API_KEY=YOUR_KEY\n",
    "  Then re-run this script."
  )
}
census_api_key(census_key)
message("Census API key found.")

# ---- Paths ------------------------------------------------------------------
out_dir <- here::here("data-raw", "acs")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

acs_year   <- 2023
acs_survey <- "acs5"

# ---- Helper: save with message ----------------------------------------------
save_rds <- function(df, path, label) {
  saveRDS(df, path)
  message("  Saved: ", path,
          " (", format(nrow(df), big.mark = ","), " rows, ",
          round(file.size(path) / 1e6, 1), " MB)")
  invisible(df)
}

# =============================================================================
# 1. B01001: Sex by Age (state-level) — demand denominators
# =============================================================================
message("\n[1/4] Pulling B01001 Sex by Age (state level) ...")

# Selected female age groups relevant to URPS (women 18+)
# B01001_026E = Women 18-19, _027 = 20-24, ... _049 = 85+
# Pull entire table and filter in R
b01001_raw <- get_acs(
  geography = "state",
  table     = "B01001",
  year      = acs_year,
  survey    = acs_survey,
  cache_table = TRUE
)

# Female rows: variable numbers 026-049
b01001_female <- b01001_raw %>%
  filter(grepl("^B01001_0(2[6-9]|[34][0-9])E?$",
               gsub("E$", "", variable), perl = TRUE) |
         variable %in% paste0("B01001_0", sprintf("%02d", 26:49)))

save_rds(b01001_female,
         file.path(out_dir, "acs5_2023_sex_by_age_state.rds"),
         "B01001 sex by age")

# =============================================================================
# 2. B27001: Health Insurance Coverage Status by Sex by Age (state)
# =============================================================================
message("\n[2/4] Pulling B27001 Health Insurance by Sex by Age (state level) ...")

b27001_raw <- get_acs(
  geography = "state",
  table     = "B27001",
  year      = acs_year,
  survey    = acs_survey,
  cache_table = TRUE
)

save_rds(b27001_raw,
         file.path(out_dir, "acs5_2023_insurance_by_age_state.rds"),
         "B27001 insurance")

# =============================================================================
# 3. B17001: Poverty Status by Sex by Age (state)
# =============================================================================
message("\n[3/4] Pulling B17001 Poverty Status by Sex by Age (state level) ...")

b17001_raw <- get_acs(
  geography = "state",
  table     = "B17001",
  year      = acs_year,
  survey    = acs_survey,
  cache_table = TRUE
)

save_rds(b17001_raw,
         file.path(out_dir, "acs5_2023_poverty_state.rds"),
         "B17001 poverty")

# =============================================================================
# 4. PUMS: Person-level microdata (women 18+)
# =============================================================================
message("\n[4/4] Pulling ACS PUMS person-level microdata (women 18+) ...")
message("  This may take 5-15 minutes — downloads state-by-state ...")

# Key PUMS variables for URPS demand
pums_vars <- c(
  "AGEP",    # Age
  "SEX",     # Sex (1=male, 2=female)
  "HINS1",   # Insurance through employer
  "HINS2",   # Insurance purchased directly
  "HINS3",   # Medicare
  "HINS4",   # Medicaid/CHIP
  "HINS5",   # Tricare/military
  "HINS6",   # VA
  "HINS7",   # Indian Health Service
  "PINCP",   # Personal income
  "POVPIP",  # Income-to-poverty ratio
  "RACBLK",  # Black/African American
  "RACWHT",  # White
  "HISP",    # Hispanic origin
  "RACASN",  # Asian
  "RACAIAN", # American Indian/Alaska Native
  "ST",      # State
  "PUMA",    # PUMA geography
  "PWGTP"    # Person weight
)

# Pull PUMS for all states
# Note: tidycensus downloads PUMS by state; national pull is implicit
pums_raw <- get_pums(
  variables = pums_vars,
  state     = "all",
  year      = acs_year,
  survey    = acs_survey,
  rep_weights = "person",   # include 80 replicate weights for variance
  recode    = FALSE
)

# Filter to women 18+
pums_women <- pums_raw %>%
  filter(SEX == 2, AGEP >= 18)

# Derive key variables
pums_women <- pums_women %>%
  mutate(
    age_band_urps = cut(AGEP,
      breaks = c(17, 39, 59, 64, 79, Inf),
      labels = c("18-39", "40-59", "60-64", "65-79", "80+"),
      right  = TRUE),
    insured = as.integer(HINS1 == 1 | HINS2 == 1 | HINS3 == 1 |
                         HINS4 == 1 | HINS5 == 1 | HINS6 == 1),
    medicare   = as.integer(HINS3 == 1),
    medicaid   = as.integer(HINS4 == 1),
    commercial = as.integer(HINS1 == 1 | HINS2 == 1),
    uninsured  = as.integer(insured == 0),
    poverty_lt100 = as.integer(!is.na(POVPIP) & POVPIP < 100),
    poverty_lt200 = as.integer(!is.na(POVPIP) & POVPIP < 200)
  )

save_rds(pums_women,
         file.path(out_dir, "acs5_2023_pums_women18plus.rds"),
         "PUMS women 18+")

# =============================================================================
# Manifest
# =============================================================================
manifest_path <- file.path(out_dir, "acs_2023_manifest.txt")
writeLines(c(
  "ACS 2023 5-Year Estimates Download Manifest",
  paste("Generated:", Sys.time()),
  paste("Survey:", acs_survey, "| Year:", acs_year),
  "",
  "Files:",
  paste("  acs5_2023_sex_by_age_state.rds  —",
        format(nrow(b01001_female), big.mark = ","), "rows"),
  paste("  acs5_2023_insurance_by_age_state.rds —",
        format(nrow(b27001_raw), big.mark = ","), "rows"),
  paste("  acs5_2023_poverty_state.rds —",
        format(nrow(b17001_raw), big.mark = ","), "rows"),
  paste("  acs5_2023_pums_women18plus.rds —",
        format(nrow(pums_women), big.mark = ","), "rows"),
  "",
  "Key PUMS demand-denominator columns:",
  "  AGEP, SEX, HINS1-7, PINCP, POVPIP, ST, PUMA, PWGTP",
  "  Derived: age_band_urps, insured, medicare, medicaid,",
  "           commercial, uninsured, poverty_lt100, poverty_lt200",
  "",
  "Survey design for srvyr/survey::svrepdesign:",
  "  weights = ~PWGTP",
  "  repweights = paste0('PWGTP', 1:80)",
  "  type = 'successive-difference'",
  "  mse  = TRUE",
  "",
  "API key registration: https://api.census.gov/data/key_signup.html",
  "DUA required: NO"
), manifest_path)

message("\nManifest written: ", manifest_path)
message("Done. ACS 2023 download complete.")
