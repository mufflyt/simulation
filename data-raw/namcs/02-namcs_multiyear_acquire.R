# NAMCS Multi-Year Pooled Acquisition (2015, 2016, 2018, 2019) ----
#
# Source: National Ambulatory Medical Care Survey, NCHS/CDC
# URL:    https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/
# Docs:   https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/NAMCS/<year>/
#
# Pooling rationale: 2019 alone yields only 7 URPS visits for women 65+,
# making 5-year age × 4-race cell estimates unreliable.  Pooling 2015, 2016,
# 2018, 2019 gives ~4× the sample (~28-35 URPS visits for women 65+).
#
# NOTE: 2017 is NOT available on the CDC FTP server (directory absent, 404).
#
# Column positions differ by year (confirmed from namdict<year>.dct):
#   Core demographics (VMONTH, AGE, SEX, RACERETH, PAYTYPER) are stable at
#   the same positions across all four years.
#   DIAG1-3 and BMI shift in 2015.
#   All tail-end sampling variables (SPECCAT, CSTRATM, CPSUM, YEAR, PATWT)
#   differ for every year.
#
# Strategy: download zip (~90 MB), extract ASCII (~1.2 GB), parse to RDS
# (~2 MB), then delete zip and ASCII to conserve disk space.

library(readr)
library(dplyr)

# ---- Year-specific column layouts -------------------------------------------
# Each entry: list(start=, end=, col_names=) passed to readr::fwf_positions()
# Stable across all years: VMONTH=1-2, AGE=4-6, SEX=11, RACERETH=22,
#   PAYTYPER=32-33, USETOBAC=34-35

.namcs_cols <- function(year) {
  # Year-specific tail positions (from namdict<year>.dct)
  tail <- switch(as.character(year),
    "2015" = list(diag1=c(113,117), diag2=c(120,124), diag3=c(127,131),
                  bmi=c(216,223), speccat=c(665,665),
                  cstratm=c(2663,2670), cpsum=c(2671,2676),
                  yr=c(2677,2680), patwt=c(2682,2692)),
    "2016" = list(diag1=c(95,98),  diag2=c(101,104), diag3=c(107,110),
                  bmi=c(166,173), speccat=c(678,678),
                  cstratm=c(2596,2603), cpsum=c(2604,2609),
                  yr=c(2610,2613), patwt=c(2615,2626)),
    "2018" = list(diag1=c(95,98),  diag2=c(101,104), diag3=c(107,110),
                  bmi=c(166,173), speccat=c(703,703),
                  cstratm=c(2620,2627), cpsum=c(2628,2633),
                  yr=c(2634,2637), patwt=c(2639,2650)),
    "2019" = list(diag1=c(95,98),  diag2=c(101,104), diag3=c(107,110),
                  bmi=c(166,173), speccat=c(703,703),
                  cstratm=c(2502,2509), cpsum=c(2510,2515),
                  yr=c(2516,2519), patwt=c(2521,2533)),
    stop("No column layout for NAMCS year: ", year)
  )
  readr::fwf_positions(
    start = c( 1,  3,  4, 11, 16, 18, 20, 21, 22, 32, 34,
               tail$diag1[1], tail$diag2[1], tail$diag3[1],
               tail$bmi[1], tail$speccat[1],
               tail$cstratm[1], tail$cpsum[1], tail$yr[1], tail$patwt[1]),
    end   = c( 2,  3,  6, 11, 17, 19, 20, 21, 22, 33, 35,
               tail$diag1[2], tail$diag2[2], tail$diag3[2],
               tail$bmi[2], tail$speccat[2],
               tail$cstratm[2], tail$cpsum[2], tail$yr[2], tail$patwt[2]),
    col_names = c(
      "VMONTH","VDAYR","AGE","SEX","ETHUN","RACEUN","ETHIM","RACER","RACERETH",
      "PAYTYPER","USETOBAC","DIAG1","DIAG2","DIAG3","BMI","SPECCAT",
      "CSTRATM","CPSUM","YEAR","PATWT"
    )
  )
}

.namcs_col_types <- readr::cols(
  VMONTH=readr::col_integer(), VDAYR=readr::col_integer(),
  AGE=readr::col_integer(),    SEX=readr::col_integer(),
  ETHUN=readr::col_integer(),  RACEUN=readr::col_integer(),
  ETHIM=readr::col_integer(),  RACER=readr::col_integer(),
  RACERETH=readr::col_integer(), PAYTYPER=readr::col_integer(),
  USETOBAC=readr::col_integer(), DIAG1=readr::col_character(),
  DIAG2=readr::col_character(),  DIAG3=readr::col_character(),
  BMI=readr::col_double(),       SPECCAT=readr::col_integer(),
  CSTRATM=readr::col_integer(),  CPSUM=readr::col_integer(),
  YEAR=readr::col_integer(),     PATWT=readr::col_double()
)

# ---- Download and parse a single year ---------------------------------------

process_namcs_year <- function(year, keep_raw = FALSE) {
  zip_url  <- sprintf(
    "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/namcs%d.zip", year)
  zip_path <- file.path(tempdir(), sprintf("namcs%d.zip", year))
  asc_path <- file.path(tempdir(), sprintf("namcs%d", year))
  out_rds  <- file.path("data-raw", "namcs", sprintf("namcs%d_clean.rds", year))

  if (file.exists(out_rds)) {
    cat("Already parsed:", out_rds, "— skipping download.\n")
    return(invisible(readRDS(out_rds)))
  }

  # Download
  cat(sprintf("Downloading NAMCS %d from CDC FTP...\n", year))
  utils::download.file(zip_url, zip_path, mode = "wb", quiet = FALSE)
  cat(sprintf("  ZIP size: %.1f MB\n", file.size(zip_path) / 1e6))

  # Extract (zip contains a single file named namcs<year>)
  utils::unzip(zip_path, exdir = tempdir())
  if (!file.exists(asc_path)) {
    # Some years use uppercase
    asc_path_upper <- file.path(tempdir(), sprintf("NAMCS%d", year))
    if (file.exists(asc_path_upper)) asc_path <- asc_path_upper
  }
  cat(sprintf("  ASCII size: %.1f MB\n", file.size(asc_path) / 1e6))

  # Parse
  raw <- readr::read_fwf(asc_path, col_positions = .namcs_cols(year),
                         col_types = .namcs_col_types, show_col_types = FALSE)
  cat(sprintf("  Parsed: %d rows\n", nrow(raw)))

  # Clean sentinels
  clean <- raw |>
    dplyr::mutate(
      dplyr::across(c(AGE, SEX, RACER, RACERETH, PAYTYPER, USETOBAC,
                      SPECCAT, ETHUN, RACEUN, ETHIM),
                    ~ dplyr::if_else(.x < 0, NA_integer_, .x)),
      BMI   = dplyr::if_else(BMI   <= 0, NA_real_, BMI),
      PATWT = dplyr::if_else(PATWT <= 0, NA_real_, PATWT),
      dplyr::across(c(DIAG1, DIAG2, DIAG3),
                    ~ trimws(gsub("-$", "", trimws(.x))))
    )

  saveRDS(clean, out_rds)
  cat(sprintf("  Saved: %s (%.1f MB)\n", out_rds, file.size(out_rds) / 1e6))

  # Clean up raw files to save disk space
  if (!keep_raw) {
    file.remove(zip_path)
    file.remove(asc_path)
    cat("  Cleaned up temp files.\n")
  }

  invisible(clean)
}

# ---- Pool all years ---------------------------------------------------------

#' Download, parse, and pool NAMCS 2015-2019 into one clean RDS
#'
#' Saves `data-raw/namcs/namcs_pooled_2015_2019.rds` with a `namcs_year`
#' column added.  2017 is absent from the CDC FTP and is skipped.
pool_namcs_years <- function(years = c(2015, 2016, 2018, 2019),
                             out_path = "data-raw/namcs/namcs_pooled_2015_2019.rds") {
  parts <- lapply(years, function(y) {
    d <- process_namcs_year(y)
    dplyr::mutate(d, namcs_year = y)
  })
  pooled <- dplyr::bind_rows(parts)
  cat(sprintf("\nPooled: %d visits across %d years\n", nrow(pooled), length(years)))

  # Verify YEAR column matches namcs_year for each row
  mismatch <- sum(pooled$YEAR != pooled$namcs_year, na.rm = TRUE)
  if (mismatch > 0) warning(mismatch, " rows have YEAR != namcs_year")

  saveRDS(pooled, out_path)
  cat("Pooled file saved:", out_path, "\n")
  invisible(pooled)
}

# ---- Run --------------------------------------------------------------------
# 2019 is already parsed; the others will be downloaded fresh.
pool_namcs_years()
