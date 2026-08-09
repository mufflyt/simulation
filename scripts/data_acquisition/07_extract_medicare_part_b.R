#!/usr/bin/env Rscript
# Build the Medicare-FFS Part B urogyn utilization VALIDATION series using the
# CANONICAL pipeline (no bespoke code set or aggregator):
#   urps_medicare_service_crosswalk()  -> codes from URPS_CPT_BASKET
#   read_part_b_claims()               -> thin DuckDB reader (this package)
#   aggregate_medicare_realized_care() -> services + benes + bene-day, labeled
# Output is a Medicare-FFS validation series in data-raw/medicare_part_b/, never
# a calibrated anchor and never data/anchors/.
#
#   Rscript scripts/data_acquisition/07_extract_medicare_part_b.R [duckdb_path]

suppressWarnings(suppressMessages({
  if (!requireNamespace("urpssim", quietly = TRUE) ||
      !exists("aggregate_medicare_realized_care", mode = "function")) {
    pkgload::load_all(".", quiet = TRUE, export_all = TRUE)
  }
  library(dplyr)
}))

args <- commandArgs(trailingOnly = TRUE)
duckdb_path <- if (length(args) >= 1) args[1] else default_part_b_duckdb()
out_dir <- file.path("data-raw", "medicare_part_b")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
if (!file.exists(duckdb_path)) {
  stop(sprintf("DuckDB not found at '%s'. Mount the drive or set MEDICARE_PARTB_DUCKDB.", duckdb_path))
}

xwalk  <- urps_medicare_service_crosswalk()                     # canonical codes
message("Reading Part B claims for ", nrow(xwalk), " canonical HCPCS codes ...")
claims <- read_part_b_claims(hcpcs = xwalk$hcpcs, duckdb_path = duckdb_path)

# Headline: year x service (all three CMS measures + distinct billing NPIs).
by_service <- aggregate_medicare_realized_care(
  claims, crosswalk = xwalk, year = "year",
  state = NULL, provider_type = NULL, place_of_service = NULL, npi = "Rndrng_NPI")

# Specialty split: year x service x provider type.
by_specialty <- aggregate_medicare_realized_care(
  claims, crosswalk = xwalk, year = "year",
  state = NULL, provider_type = "Rndrng_Prvdr_Type", place_of_service = NULL, npi = "Rndrng_NPI")

utils::write.csv(arrange(by_service, service, year),
                 file.path(out_dir, "urps_part_b_by_service.csv"), row.names = FALSE)
utils::write.csv(arrange(by_specialty, service, year),
                 file.path(out_dir, "urps_part_b_by_specialty.csv"), row.names = FALSE)

# Provenance reuses the canonical fingerprint helpers (R/core-repro_provenance.R:
# fingerprint_files/make_run_id) rather than hand-rolling; content SHA-256 of each
# written CSV via digest, matching the package's provenance contract.
code_paths <- c("R/data-medicare_part_b.R", "R/supply-medicare_capacity.R",
                "scripts/data_acquisition/07_extract_medicare_part_b.R")
csvs <- file.path(out_dir, c("urps_part_b_by_service.csv", "urps_part_b_by_specialty.csv"))
prov <- list(
  source = "Medicare Physician & Other Practitioners - by Provider and Service (CMS PUF)",
  cms_dataset = "CMS Original Medicare FFS Part B; provider x HCPCS x place of service",
  duckdb_path = duckdb_path,
  source_md5 = attr(claims, "source_md5"),
  years = sort(unique(by_service$year)),
  codes_from = "urps_medicare_service_crosswalk() / URPS_CPT_BASKET (canonical SSOT)",
  aggregator = "aggregate_medicare_realized_care() (canonical)",
  estimand = unique(by_service$estimand),
  payer_scope = unique(by_service$payer_scope),
  unmapped_service_fraction = attr(by_service, "unmapped_service_fraction"),
  caveat = attr(by_service, "caveat"),
  run_id = make_run_id(tag = "part_b_series"),
  code_fingerprint = fingerprint_files(code_paths),
  content_sha256 = stats::setNames(
    vapply(csvs, function(f) digest::digest(file = f, algo = "sha256"), character(1)),
    basename(csvs)),
  extraction_date = as.character(Sys.Date()),
  no_extrapolation = TRUE)
jsonlite::write_json(prov, file.path(out_dir, "urps_part_b_provenance.json"),
                     auto_unbox = TRUE, pretty = TRUE)

message("Wrote ", out_dir, "/urps_part_b_by_service.csv + urps_part_b_by_specialty.csv + provenance.json")
cat("\n=== billed_services by service x year ===\n")
print(tidyr::pivot_wider(by_service[, c("year", "service", "billed_services")],
                         names_from = service, values_from = billed_services), n = 50)
