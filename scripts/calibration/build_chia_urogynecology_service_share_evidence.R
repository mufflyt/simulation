#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    pkgload::load_all(".", quiet = TRUE)
  } else {
    library(urpssim)
  }
})

chia_path <- Sys.getenv("URPS_CHIA_DUCKDB", "")
taxonomy_path <- Sys.getenv("URPS_NPI_TAXONOMY_CSV", "")
roster_path <- Sys.getenv(
  "URPS_LINKAGE_ROSTER_2024",
  file.path(
    "data-raw", "urps_roster",
    "urps_linkage_roster_2024.csv"
  )
)
cms_evidence_path <- Sys.getenv("URPS_CMS_SERVICE_SHARE_EVIDENCE", "")
output_dir <- Sys.getenv(
  "URPS_SERVICE_SHARE_OUTPUT_DIR",
  file.path("artifacts", "service_shares")
)

if (!base::nzchar(chia_path) || !base::file.exists(chia_path)) {
  base::stop(
    "Set URPS_CHIA_DUCKDB to the mounted CHIA Case Mix DuckDB.",
    call. = FALSE
  )
}
if (!base::nzchar(taxonomy_path) || !base::file.exists(taxonomy_path)) {
  base::stop(
    "Set URPS_NPI_TAXONOMY_CSV to the NPI taxonomy extract used for CHIA.",
    call. = FALSE
  )
}
if (!base::file.exists(roster_path)) {
  base::stop("Frozen URPS linkage roster not found: ", roster_path,
    call. = FALSE)
}

con <- chia_casemix_con(chia_path)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

events <- read_chia_service_share_events(con)
npi_taxonomy <- readr::read_csv(
  taxonomy_path,
  show_col_types = FALSE,
  progress = interactive()
)
roster <- readr::read_csv(
  roster_path,
  show_col_types = FALSE,
  progress = FALSE
)
if (!"npi" %in% base::names(roster)) {
  base::stop("Frozen URPS roster must contain `npi`.", call. = FALSE)
}

evidence <- classify_chia_service_share_events(
  events = events,
  npi_taxonomy = npi_taxonomy,
  urps_roster = roster
)
evidence$provenance$chia_duckdb_sha256 <- digest::digest(
  file = chia_path,
  algo = "sha256"
)
evidence$provenance$taxonomy_file_sha256 <- digest::digest(
  file = taxonomy_path,
  algo = "sha256"
)
evidence$provenance$roster_file_sha256 <- digest::digest(
  file = roster_path,
  algo = "sha256"
)
evidence$provenance$created_at <- base::format(
  base::Sys.time(),
  "%Y-%m-%dT%H:%M:%S%z"
)

comparison <- NULL
if (base::nzchar(cms_evidence_path)) {
  if (!base::file.exists(cms_evidence_path)) {
    base::stop(
      "URPS_CMS_SERVICE_SHARE_EVIDENCE does not exist: ",
      cms_evidence_path,
      call. = FALSE
    )
  }
  cms_evidence <- base::readRDS(cms_evidence_path)
  comparison <- compare_chia_to_cms_service_share_evidence(
    evidence,
    cms_evidence
  )
}

base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
bundle_path <- base::file.path(
  output_dir,
  base::paste0("chia_service_share_evidence_", timestamp, ".rds")
)
provider_path <- base::file.path(
  output_dir,
  base::paste0("chia_provider_shares_", timestamp, ".csv")
)
physician_path <- base::file.path(
  output_dir,
  base::paste0("chia_urps_given_physician_", timestamp, ".csv")
)

base::saveRDS(evidence, bundle_path)
readr::write_csv(evidence$provider_shares, provider_path)
readr::write_csv(evidence$physician_share, physician_path)
base::message("Saved CHIA evidence bundle: ", base::normalizePath(bundle_path))

if (!base::is.null(comparison)) {
  comparison_path <- base::file.path(
    output_dir,
    base::paste0("chia_vs_cms_transport_", timestamp, ".csv")
  )
  readr::write_csv(comparison, comparison_path)
  base::message(
    "Saved CHIA/CMS transport comparison: ",
    base::normalizePath(comparison_path)
  )
}
