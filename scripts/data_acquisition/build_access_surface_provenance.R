#!/usr/bin/env Rscript
# Provenance sidecar for the archived E2SFCA surfaces -------------------------
#
#   Rscript scripts/data_acquisition/build_access_surface_provenance.R
#
# WHY A SIDECAR AND NOT A REWRITE. The surface artifacts carry an all-NA
# `n_providers` column. That is not corruption: `two_step_floating_catchment.R`
# sets `n_providers = NA_integer_` in the RASTER code path (line 786 at run SHA
# ff3aac4a), because accessibility there is extracted zonally from a continuous
# surface and no per-tract list of contributing providers exists. The vector
# path computes it (`n_distinct(coord_id[ratio > 0])`); the raster path cannot.
# So the per-tract column is uncomputable by the method that produced these
# files, and filling it would be invention.
#
# The SURFACE-LEVEL provider count is a different quantity and is fully
# recoverable. `compute_provider_supply()` sets supply = n_distinct(npi) per
# coord_id, so sum(supply) is the number of distinct physicians entering the
# calculation. Verified: no provider occupies two coord_ids in a year, so the
# sum does not double count (2020: 783 = 783 distinct NPIs; 2023: 947 = 947).
#
# The count is derived ONLY from the provider artifact. Not from the eligible
# cohort, not from the usable-coordinate count, not from the ascertainment
# table, and never from a constant.
#
# Writes artifacts/access_ascertainment/surface_provenance.csv

suppressPackageStartupMessages({ library(dplyr) })

ISO_ROOT <- Sys.getenv("ISOCHRONES_ROOT", "/Users/tylermuffly/isochrones")
RUNDIR   <- file.path(ISO_ROOT, "artifacts/2sfca/ec2/e2sfca_20260712_190734")
UNPACKED <- file.path(RUNDIR, "unpacked")
FLOW     <- "artifacts/access_ascertainment/provider_flow_fpmrs.csv"
OUT      <- "artifacts/access_ascertainment/surface_provenance.csv"
GIT_SHA  <- "ff3aac4a7aa97094fc3a9e69422425fe1a52b091"

stopifnot(file.exists(FLOW))
flow <- utils::read.csv(FLOW, stringsAsFactors = FALSE)

# digest is a declared Import and returns a BARE character; openssl::sha256()
# returns a classed object that survives as.character() and compares TRUE under
# `==` but FALSE under identical(). core-repro_provenance.R already settled this.
sha256 <- function(p) {
  if (!file.exists(p)) return(NA_character_)
  digest::digest(file = p, algo = "sha256")
}

rows <- list()
for (Y in flow$analysis_year) {
  ppath <- file.path(UNPACKED, sprintf("step_4_2sfca_FPMRS_%d_providers.rds", Y))
  spath <- file.path(UNPACKED, sprintf("step_4_2sfca_FPMRS_%d.rds", Y))
  if (!file.exists(ppath)) next
  p <- readRDS(ppath)

  # THE COUNT. Distinct physicians entering E2SFCA, read off the provider
  # artifact itself. `supply` is already n_distinct(npi) per location.
  n_prov <- sum(p$supply)
  stopifnot(is.finite(n_prov), n_prov > 0,
            all(c("coord_id", "supply", "weighted_demand", "ratio") %in% names(p)))

  fr <- flow[flow$analysis_year == Y, ]
  rows[[as.character(Y)]] <- data.frame(
    analysis_year          = Y,
    subspecialty           = "FPMRS",
    n_providers_in_surface = n_prov,
    n_provider_locations   = nrow(p),
    eligible_provider_n    = fr$eligible_provider_n,
    usable_coordinate_n    = fr$usable_coordinate_n,
    surface_provider_n     = fr$surface_provider_n,
    surface_rate           = fr$surface_rate,
    provider_artifact_path = ppath,
    provider_artifact_sha256 = sha256(ppath),
    surface_artifact_path  = if (file.exists(spath)) spath else NA_character_,
    surface_artifact_sha256 = sha256(spath),
    producing_git_sha      = GIT_SHA,
    per_tract_n_providers  = "not_computable_raster_path",
    stringsAsFactors = FALSE
  )
}
out <- dplyr::bind_rows(rows)
stopifnot(nrow(out) > 0, !anyDuplicated(out$analysis_year))

dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, OUT, row.names = FALSE)
cat("\n=== surface provenance ===\n")
print(as.data.frame(out[, c("analysis_year", "n_providers_in_surface",
                            "n_provider_locations", "eligible_provider_n",
                            "surface_rate")]), row.names = FALSE, digits = 4)
cat("\nWrote ", OUT, "\n", sep = "")
