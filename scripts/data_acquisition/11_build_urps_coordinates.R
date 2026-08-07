#!/usr/bin/env Rscript
# Build the URPS provider coordinate extract ----
#
#   Rscript scripts/data_acquisition/11_build_urps_coordinates.R
#
# WHAT THIS PRODUCES. data-raw/urps_roster/urps_provider_coordinates_*.csv --
# one point per URPS provider NPI, with the run it came from and the date it was
# taken. Physician names are excluded, as everywhere else in this repository:
# the access calculation needs a point and an identifier, never a name.
#
# THIS SCRIPT WILL NOT RUN WITHOUT mufflyt/isochrones CHECKED OUT ALONGSIDE.
# Neither the roster nor the coordinates are in this repository --
# data-raw/urps_roster is deliberately not whitelisted in .gitignore, because
# the extract carries NPIs. Set URPS_ISOCHRONES_DIR if it lives elsewhere.
#
# WHY SIX SOURCES. The primary geocoding run covers the ABOG pathway only. Built
# on that alone, coverage is 72% overall and 0% for urology -- an access surface
# that would run, produce plausible ratios, and omit 23% of the workforce
# wherever it practises. Each source below closed a specific hole:
#
#   1. artifacts/20260802_101936_ce1223fc            primary run, ABOG
#   2. data/abu_urology/abu_fpmrs_net_new_geocoded   the ABU pathway  (0% -> 87%)
#   3. data/abu_urology/abog_fpmrs_geocoded          ABOG stragglers
#   4. cliff/data/all_obgyn_geocoded.csv             pre-2015 certifications
#   5. artifacts/production/20260627_182158_4377042a earlier production run
#   6. artifacts/ac587845_full table1 + cliff module D   the last 15
#
# THE LAST SOURCE IS SCREENED, THE OTHERS ARE NOT. Source 6 is a name-and-
# identifier match table rather than a geocoding run, so its points are checked
# against the address the same row records -- see screen_new_coordinates(). Of
# 13 candidates, 12 landed within 7.4 km of their own ZIP centroid and one, NPI
# 1073505681, landed 131 km away in the wrong state. That record is REJECTED
# here rather than repaired: a point matched on name alone, disagreeing with its
# own address, is not evidence about where anyone practises.
#
# Final coverage: 1,336 of 1,339 (99.8%), 99.9% ABOG and 99.4% ABU.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})

ISO <- Sys.getenv("URPS_ISOCHRONES_DIR", unset = "../isochrones")
OUT <- "data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv"
TAKEN_ON <- Sys.getenv("URPS_COORD_RETRIEVED_ON", unset = format(Sys.Date()))

if (!dir.exists(ISO)) {
  stop("mufflyt/isochrones not found at '", ISO, "'. Set URPS_ISOCHRONES_DIR.",
       call. = FALSE)
}
if (!requireNamespace("zipcodeR", quietly = TRUE)) {
  stop("zipcodeR is required to screen source 6. Install it rather than ",
       "merging those points unscreened.", call. = FALSE)
}

roster <- load_urps_roster()
message(sprintf("Roster: %d providers", nrow(roster)))

# ---- Source 6: the last 15, screened against their own recorded address -----
#
# Read narrowly. table1_physician_characteristics.csv is 7,208 x 134 and takes
# minutes to read whole; only these columns matter.
t1_path <- file.path(ISO, "artifacts", "ac587845_full", "manuscript", "tables",
                     "table1_physician_characteristics.csv")
missing_npi <- setdiff(roster$npi, load_urps_provider_coordinates(OUT)$npi)
message(sprintf("Without a coordinate before this source: %d", length(missing_npi)))

t1 <- utils::read.csv(t1_path, colClasses = c(npi = "character"),
                      stringsAsFactors = FALSE)
t1 <- t1[t1$npi %in% missing_npi & !is.na(t1$geocode_success) & t1$geocode_success &
           is.finite(t1$latitude) & is.finite(t1$longitude), ]

cand <- data.frame(npi = t1$npi, lat = t1$latitude, lon = t1$longitude,
                   state = t1$practice_state, zip5 = t1$practice_zip,
                   quality = t1$geocode_quality,
                   source_run = paste("isochrones artifacts/ac587845_full table1",
                                      "(NPPES/PhysicianCompare practice address)"),
                   stringsAsFactors = FALSE)

screened <- urpssim:::screen_new_coordinates(cand)
rejected <- screened[!screened$address_ok, ]
if (nrow(rejected)) {
  message("REJECTED by the address screen (kept out of the extract):")
  for (i in seq_len(nrow(rejected))) {
    message(sprintf("  %s  %.0f km from ZIP %s  [%s]", rejected$npi[i],
                    rejected$address_km[i], rejected$zip5[i], rejected$quality[i]))
  }
}
keep <- screened[screened$address_ok, ]
message(sprintf("Source 6 accepted %d of %d candidates", nrow(keep), nrow(screened)))

new <- data.frame(
  npi = keep$npi, lat = keep$lat, lon = keep$lon, state = keep$state,
  cert_year = roster$cert_year[match(keep$npi, roster$npi)],
  is_retired = FALSE, retirement_year = NA_integer_,
  source_run = keep$source_run, retrieved_on = TAKEN_ON,
  stringsAsFactors = FALSE)

existing <- utils::read.csv(OUT, colClasses = c(npi = "character"),
                            stringsAsFactors = FALSE)
new <- new[!new$npi %in% existing$npi, names(existing)]

# safe_rbind, not rbind: binding these independently-read frames is exactly the
# operation that coerced retrieved_on to Date and NA'd 364 of 1,540 rows.
merged <- urpssim:::safe_rbind(list(existing, new),
                               no_new_missing = c("source_run", "retrieved_on"))
stopifnot(!anyDuplicated(merged$npi))
utils::write.csv(merged, OUT, row.names = FALSE, na = "")

cov <- provider_coordinate_coverage(roster = roster,
                                    coords = load_urps_provider_coordinates(OUT))
cat(sprintf("\ncoverage: %d/%d = %.2f%%   usable_for_access=%s\n",
            cov$n_with_coordinates, cov$n_roster, 100 * cov$overall_share,
            cov$usable_for_access))
print(cov$by_pathway)
if (!is.na(cov$blocker)) cat(strwrap(paste("BLOCKER:", cov$blocker), 78), sep = "\n")
