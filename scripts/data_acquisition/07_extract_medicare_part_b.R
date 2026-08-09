#!/usr/bin/env Rscript
# Extract the Medicare Part B (by Provider and Service) urogynecology utilization
# series and write it as a Medicare-FFS VALIDATION series. This is data
# infrastructure, NOT a calibrated anchor: output goes to data-raw/medicare_part_b/,
# never to data/anchors/. Reproducible replacement for ad-hoc SQL.
#
# Usage:
#   Rscript scripts/data_acquisition/07_extract_medicare_part_b.R [duckdb_path]
# DuckDB path resolves from arg 1, else $MEDICARE_PARTB_DUCKDB, else the drive.

suppressWarnings(suppressMessages({
  if (!requireNamespace("urpssim", quietly = TRUE) ||
      !exists("extract_part_b_utilization", mode = "function")) {
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

message("Extracting HCPCS-level series (all years, all URPS groups)...")
detail <- extract_part_b_utilization(duckdb_path = duckdb_path)      # year x hcpcs
prov <- attr(detail, "provenance")

# Roll HCPCS up to code groups (services / bene-day-services summable; benes_sum
# stays a labeled sum, never a unique-patient count).
group_series <- detail %>%
  group_by(year, code_group) %>%
  summarise(tot_srvcs = sum(tot_srvcs, na.rm = TRUE),
            tot_benes_sum = sum(tot_benes_sum, na.rm = TRUE),
            tot_bene_day_srvcs = sum(tot_bene_day_srvcs, na.rm = TRUE),
            n_distinct_npi = sum(n_distinct_npi, na.rm = TRUE),
            rows_with_na_benes = sum(rows_with_na_benes, na.rm = TRUE),
            .groups = "drop") %>%
  arrange(code_group, year)

message("Extracting specialty split for the headline codes...")
by_spec <- extract_part_b_utilization(
  duckdb_path = duckdb_path,
  provider_type = c("Obstetrics & Gynecology", "Urology"))

utils::write.csv(detail, file.path(out_dir, "urps_part_b_by_hcpcs.csv"), row.names = FALSE)
utils::write.csv(group_series, file.path(out_dir, "urps_part_b_by_group.csv"), row.names = FALSE)
utils::write.csv(by_spec, file.path(out_dir, "urps_part_b_by_specialty.csv"), row.names = FALSE)
jsonlite::write_json(prov, file.path(out_dir, "urps_part_b_provenance.json"),
                     auto_unbox = TRUE, pretty = TRUE)

message("Wrote: ", out_dir, "/{urps_part_b_by_hcpcs,urps_part_b_by_group,urps_part_b_by_specialty}.csv + provenance.json")
cat("\n=== code-group series (services) ===\n")
print(tidyr::pivot_wider(group_series[, c("year", "code_group", "tot_srvcs")],
                         names_from = code_group, values_from = tot_srvcs), n = 50)
