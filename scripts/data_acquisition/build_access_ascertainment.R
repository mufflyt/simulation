#!/usr/bin/env Rscript
# Provider ascertainment for the E2SFCA access surfaces -----------------------
#
#   Rscript scripts/data_acquisition/build_access_ascertainment.R
#
# WHY THIS EXISTS. An E2SFCA surface reports accessibility per tract. It does
# not report how many of the eligible physicians actually made it into the
# calculation. For FPMRS that share runs from 70% in 2019 to 89% in 2023, so a
# reader comparing 2019 and 2023 accessibility is looking at a real access
# signal PLUS a 19-point improvement in provider-location ascertainment, with
# no way to tell them apart from the surface alone.
#
# THREE LOSSES, KEPT SEPARATE. They have different causes and different
# remedies, and collapsing them into one "coverage" number hides all three:
#
#   1. ADDRESS ascertainment  - no provider-year address row at all.
#   2. GEOCODING ascertainment - an address exists but yields no coordinate.
#   3. SPATIAL eligibility     - a valid coordinate exists, but no isochrone
#      centre lies within the match threshold, so the provider has no computed
#      catchment. These are NOT geocoding failures and must not be counted as
#      such: their locations are known precisely.
#
# Anything unexplained after those three is a pipeline defect and the validator
# fails on it. There is deliberately no generic "dropped" bucket.
#
# READ ONLY. This script reads the isochrones artifacts and writes summary and
# disposition tables. It regenerates no geography and mutates no upstream input.

suppressPackageStartupMessages({ library(dplyr) })

ISO_ROOT <- Sys.getenv("ISOCHRONES_ROOT", "/Users/tylermuffly/isochrones")
RUN      <- file.path(ISO_ROOT, "artifacts", "20260702_120134_90bf52ef")
COHORT   <- file.path(RUN, "step_2.5_final_cohort.rds")
YCM      <- file.path(RUN, "step_3_year_coord_map.rds")
SURFDIR  <- file.path(ISO_ROOT, "artifacts/2sfca/ec2/e2sfca_20260712_190734")
OUT      <- "artifacts/access_ascertainment"
LAB      <- "Female Pelvic Medicine & Reconstructive Surgery"
YEARS    <- 2018:2023

stopifnot(file.exists(COHORT), file.exists(YCM))
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

cohort <- readRDS(COHORT)
ycm    <- readRDS(YCM)
f      <- cohort[cohort$subspecialty == LAB, , drop = FALSE]
cmf    <- ycm[ycm$subspecialty_name == LAB, , drop = FALSE]

# Eligible = certified by the year, not retired by it, with a cohort coordinate
# inside CONUS. This is the DENOMINATOR: the physicians a complete surface for
# that year would represent.
eligible_npi <- function(Y) {
  b <- f[!is.na(f$cert_year) & f$cert_year <= Y, , drop = FALSE]
  b <- b[is.na(b$retirement_year) | b$retirement_year > Y, , drop = FALSE]
  b <- b[!is.na(b$lat) & !is.na(b$lon), , drop = FALSE]
  # CONUS comes from the SSOT state list, not a hand-written bounding box.
  # twostep::dj7_conus_ok() is the same predicate expressed in coordinates; the
  # state form is preferable because it cannot admit an offshore point that
  # happens to fall inside a rectangle.
  st <- ifelse(is.na(b$practice_state), b$state, b$practice_state)
  b <- b[!st %in% mufflyaccess::NON_CONTIGUOUS_CODES, , drop = FALSE]
  sort(unique(b$npi))
}

surface_coord_ids <- function(Y) {
  p <- file.path(SURFDIR, "unpacked", sprintf("step_4_2sfca_FPMRS_%d_providers.rds", Y))
  if (!file.exists(p)) return(NULL)
  unique(readRDS(p)$coord_id)
}

flow <- list(); disp <- list()
for (Y in YEARS) {
  elig <- eligible_npi(Y)
  my   <- cmf[cmf$analysis_year == Y, , drop = FALSE]
  my   <- my[!duplicated(my$npi), , drop = FALSE]      # one row per provider-year

  has_addr  <- intersect(elig, my$npi)
  with_coord <- intersect(elig, my$npi[!is.na(my$lat) & !is.na(my$lon)])
  spat_ok    <- intersect(elig, my$npi[!is.na(my$coord_id) & !is.na(my$match_source)])
  scid <- surface_coord_ids(Y)
  in_surf <- if (is.null(scid)) NA_integer_ else
    intersect(elig, my$npi[my$coord_id %in% scid])

  # Terminal disposition, keyed by npi. Order matters: each provider falls out
  # at the FIRST stage it fails, and never appears in a later bucket.
  d <- data.frame(npi = elig, analysis_year = Y, stringsAsFactors = FALSE)
  d$disposition <- NA_character_
  d$disposition[is.na(d$disposition) & !(d$npi %in% has_addr)]  <- "no_provider_year_address"
  d$disposition[is.na(d$disposition) & !(d$npi %in% with_coord)] <- "address_not_geocodable"
  d$disposition[is.na(d$disposition) & !(d$npi %in% spat_ok)]    <- "no_qualifying_isochrone"
  if (!is.null(scid)) {
    d$disposition[is.na(d$disposition) & !(d$npi %in% in_surf)]  <- "unexplained_pipeline_loss"
  }
  d$disposition[is.na(d$disposition)] <- "included_in_surface"
  stopifnot(!any(is.na(d$disposition)), nrow(d) == length(elig), !anyDuplicated(d$npi))
  disp[[as.character(Y)]] <- d

  flow[[as.character(Y)]] <- data.frame(
    analysis_year                = Y,
    eligible_provider_n          = length(elig),
    provider_year_address_n      = length(has_addr),
    usable_coordinate_n          = length(with_coord),
    spatially_eligible_provider_n = length(spat_ok),
    surface_provider_n           = if (is.null(scid)) NA_integer_ else length(in_surf),
    address_rate                 = mufflyaccess::safe_divide(length(has_addr), length(elig)),
    usable_coordinate_rate       = mufflyaccess::safe_divide(length(with_coord), length(elig)),
    spatial_eligible_rate        = mufflyaccess::safe_divide(length(spat_ok), length(elig)),
    surface_rate                 = if (is.null(scid)) NA_real_ else
                                     mufflyaccess::safe_divide(length(in_surf), length(elig))
  )
}

flow_df <- dplyr::bind_rows(flow)
disp_df <- dplyr::bind_rows(disp)

# Nesting must hold: each stage is a subset of the one before it.
stopifnot(all(flow_df$provider_year_address_n <= flow_df$eligible_provider_n),
          all(flow_df$usable_coordinate_n     <= flow_df$provider_year_address_n),
          all(flow_df$spatially_eligible_provider_n <= flow_df$usable_coordinate_n),
          all(is.na(flow_df$surface_provider_n) |
              flow_df$surface_provider_n <= flow_df$spatially_eligible_provider_n))

utils::write.csv(flow_df, file.path(OUT, "provider_flow_fpmrs.csv"), row.names = FALSE)
utils::write.csv(disp_df, file.path(OUT, "provider_disposition_fpmrs.csv"), row.names = FALSE)

sha <- function(p) if (file.exists(p)) as.character(tools::md5sum(p)) else NA_character_
manifest <- list(
  subspecialty = LAB,
  years = YEARS,
  cohort_path = COHORT,
  coord_map_path = YCM,
  surface_dir = SURFDIR,
  producing_run_git_sha = "ff3aac4a7aa97094fc3a9e69422425fe1a52b091",
  isochrone_match_threshold_km = 5,
  note = paste("Read-only summary of an existing run. No geography was",
               "regenerated. Surfaces for other subspecialties in the same run",
               "share this coordinate map.")
)
jsonlite::write_json(manifest, file.path(OUT, "manifest.json"),
                     auto_unbox = TRUE, pretty = TRUE)

cat("\n=== FPMRS provider ascertainment ===\n")
print(as.data.frame(flow_df), row.names = FALSE, digits = 4)
cat("\n=== dispositions by year ===\n")
print(table(disp_df$analysis_year, disp_df$disposition))
cat("\nWrote ", OUT, "/{provider_flow_fpmrs.csv,provider_disposition_fpmrs.csv,manifest.json}\n", sep = "")
