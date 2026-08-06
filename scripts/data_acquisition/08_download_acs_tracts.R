#!/usr/bin/env Rscript
# =============================================================================
# ACS 2023 5-Year — TRACT-level female population by demand age band
# American Community Survey via tidycensus
# =============================================================================
#
# PURPOSE:
#   The geographic (isochrone) demand layer (R/geography-demand.R) needs
#   pelvic-floor NEED distributed across census tracts. Today the repo vendors
#   only female-65+ per tract (data-raw/spatial/tract_fem65_centroids.csv); the
#   life-course / DMDM demand is defined over the full 40+ age structure. This
#   script pulls TRACT-level female population by the model's demand age bands
#   (20-39 / 40-59 / 60-64 / 65-79 / 80+) so tract need can be built as
#   sum_band(female_pop[tract, band] * age_band_rate[band]) and fed to
#   demand_by_travel_band() / geographic_demand_summary().
#
#   The existing 02_download_acs.R pulls B01001 at STATE level (+ PUMS); this is
#   its tract-level complement. Bands match data-raw/census/README.md and
#   R/demand-obstetric_exposure's .obstetric_band_ages().
#
# KEY TABLE:
#   B01001 - Sex by Age. Female branch variables (sex-by-age):
#     _032..037 -> 20-39   (20,21,22-24,25-29,30-34,35-39)
#     _038..041 -> 40-59   (40-44,45-49,50-54,55-59)
#     _042..043 -> 60-64   (60-61,62-64)
#     _044..047 -> 65-79   (65-66,67-69,70-74,75-79)
#     _048..049 -> 80+     (80-84,85+)
#
# DATA ACCESS:
#   Free Census API key. Register: https://api.census.gov/data/key_signup.html
#   No DUA. Tract geography requires a per-state pull, so this loops states.
#
# OUTPUT:
#   data-raw/spatial/acs5_2023_tract_female_by_ageband.csv
#     GEOID, state_fips, female_20_39, female_40_59, female_60_64,
#     female_65_79, female_80plus, female_40plus
#   data-raw/spatial/acs5_2023_tract_female_by_ageband_manifest.txt
#
#   Join to tract_fem65_centroids.csv on GEOID for lon/lat, or to a TIGER/Line
#   tract shapefile, to obtain the coordinates the isochrone overlay needs. Then
#   turn the age-band population into per-tract NEED with
#   tract_need_from_population() and summarise with isochrone_demand_from_tracts()
#   / geographic_demand_summary() (R/geography-demand).
# =============================================================================

suppressPackageStartupMessages({
  for (p in c("tidycensus", "dplyr", "tidyr", "readr", "purrr", "here")) {
    if (!requireNamespace(p, quietly = TRUE))
      stop("Package '", p, "' is required. install.packages('", p, "')", call. = FALSE)
  }
  library(tidycensus); library(dplyr); library(tidyr)
  library(readr); library(purrr)
})

# ---- API key ----------------------------------------------------------------
census_key <- Sys.getenv("CENSUS_API_KEY")
if (nchar(census_key) == 0)
  census_key <- tryCatch(tidycensus:::get_census_api_key(), error = function(e) "")
if (nchar(census_key) == 0) {
  stop(
    "No Census API key found.\n",
    "  1. Free key: https://api.census.gov/data/key_signup.html\n",
    "  2. tidycensus::census_api_key('YOUR_KEY', install = TRUE)  (or set CENSUS_API_KEY)\n",
    call. = FALSE)
}
census_api_key(census_key)
message("Census API key found.")

acs_year   <- 2023L
acs_survey <- "acs5"
out_dir    <- here::here("data-raw", "spatial")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# ---- Female B01001 variable -> demand-band map ------------------------------
v <- function(i) sprintf("B01001_%03dE", i)
band_vars <- list(
  female_20_39  = v(c(32:37)),
  female_40_59  = v(c(38:41)),
  female_60_64  = v(c(42:43)),
  female_65_79  = v(c(44:47)),
  female_80plus = v(c(48:49))
)
all_vars <- unname(unlist(band_vars))

# States to pull. Continental + AK/HI/DC by default; override with a subset for
# a quick test (e.g. states <- c("08") for Colorado).
states <- unique(tidycensus::fips_codes$state_code)
states <- states[as.integer(states) <= 56]   # drop territories (>56)
message("Pulling B01001 for ", length(states), " states at tract level ...")

# ---- Pull per state, sum into bands -----------------------------------------
pull_state <- function(st) {
  raw <- tryCatch(
    get_acs(geography = "tract", state = st, year = acs_year, survey = acs_survey,
            variables = all_vars, output = "wide", cache_table = TRUE),
    error = function(e) { warning("state ", st, ": ", conditionMessage(e)); NULL })
  if (is.null(raw)) return(NULL)
  est <- raw[, c("GEOID", intersect(all_vars, names(raw)))]
  band <- function(cols) rowSums(est[, intersect(cols, names(est)), drop = FALSE], na.rm = TRUE)
  tibble(
    GEOID         = est$GEOID,
    state_fips    = st,
    female_20_39  = band(band_vars$female_20_39),
    female_40_59  = band(band_vars$female_40_59),
    female_60_64  = band(band_vars$female_60_64),
    female_65_79  = band(band_vars$female_65_79),
    female_80plus = band(band_vars$female_80plus)
  ) %>% mutate(female_40plus = female_40_59 + female_60_64 + female_65_79 + female_80plus)
}

tracts <- purrr::map_dfr(states, pull_state)
stopifnot(nrow(tracts) > 0)

out_csv <- file.path(out_dir, "acs5_2023_tract_female_by_ageband.csv")
readr::write_csv(tracts, out_csv)
message("Saved: ", out_csv, " (", format(nrow(tracts), big.mark = ","), " tracts)")

# ---- Manifest ---------------------------------------------------------------
csv_md5 <- tryCatch(unname(tools::md5sum(out_csv)), error = function(e) NA_character_)
writeLines(c(
  "ACS 2023 5-Year — tract female population by demand age band",
  paste("Generated:", Sys.time()),
  paste("Survey:", acs_survey, "| Year:", acs_year, "| Table: B01001"),
  paste("Tracts:", format(nrow(tracts), big.mark = ",")),
  paste("Total female 40+:", format(round(sum(tracts$female_40plus)), big.mark = ",")),
  paste("md5:", csv_md5),
  "",
  "Bands: 20-39 / 40-59 / 60-64 / 65-79 / 80+ (match data-raw/census/README.md).",
  "Consumed by R/geography-demand.R after a GEOID join to tract centroids",
  "(data-raw/spatial/tract_fem65_centroids.csv) or a TIGER/Line tract layer.",
  "API key: https://api.census.gov/data/key_signup.html | DUA required: NO"
), file.path(out_dir, "acs5_2023_tract_female_by_ageband_manifest.txt"))

message("Done. Tract age-band population ready for the isochrone demand layer.")
