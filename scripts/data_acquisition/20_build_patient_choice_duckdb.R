#!/usr/bin/env Rscript
# Build the eight-source patient destination choice data mart. Large existing
# CMS/NPPES tables are attached read-only; public-use files supplied through the
# manifest are imported with hashes. Restricted patient OD data are never
# downloaded or committed.

suppressPackageStartupMessages({
  if (!base::requireNamespace("urpssim", quietly = TRUE)) {
    pkgload::load_all(".", quiet = TRUE, export_all = TRUE)
  }
})

timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S", tz = "UTC")
target_path <- base::Sys.getenv(
  "PATIENT_CHOICE_DUCKDB",
  base::file.path("data-raw", "patient_choice",
                  "patient_choice.duckdb")
)
base::dir.create(base::dirname(target_path), recursive = TRUE,
                 showWarnings = FALSE)
connection <- connect_patient_choice_duckdb(target_path)
base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
initialize_patient_choice_catalog(connection)

existing_path <- base::Sys.getenv("MEDICARE_PARTB_DUCKDB", "")
if (base::nzchar(existing_path) && base::file.exists(existing_path)) {
  attached_tables <- attach_patient_choice_duckdb(
    connection,
    source_duckdb = existing_path,
    alias = "existing_store",
    read_only = TRUE
  )
  base::message(
    "Existing DuckDB tables available: ",
    scales::comma(base::nrow(attached_tables)), "."
  )
} else {
  base::message(
    "No existing DuckDB attached; set MEDICARE_PARTB_DUCKDB to reuse it."
  )
}

manifest_path <- base::Sys.getenv(
  "PATIENT_CHOICE_FILE_MANIFEST",
  base::file.path("config", "patient_choice_files.csv")
)
if (base::file.exists(manifest_path)) {
  file_manifest <- readr::read_csv(
    manifest_path,
    show_col_types = FALSE
  )
  required_names <- c("source_id", "path", "table", "source_uri")
  if (!base::all(required_names %in% base::names(file_manifest))) {
    base::stop(
      "Manifest must contain: ",
      base::paste(required_names, collapse = ", "), "."
    )
  }
  for (row_index in base::seq_len(base::nrow(file_manifest))) {
    source_row <- file_manifest[row_index, ]
    if (!base::file.exists(source_row$path[[1]])) {
      base::message("Skipping absent source: ", source_row$path[[1]], ".")
      next
    }
    ingest_patient_choice_file(
      connection = connection,
      path = source_row$path[[1]],
      table = source_row$table[[1]],
      source_id = source_row$source_id[[1]],
      source_uri = source_row$source_uri[[1]],
      overwrite = TRUE
    )
  }
}

inventory <- inventory_existing_patient_choice_tables(connection)
inventory_path <- base::file.path(
  "data-raw", "patient_choice",
  base::paste0("source_inventory_", timestamp, ".csv")
)
readr::write_csv(inventory, inventory_path)
base::message("Saved exact source inventory: ", inventory_path, ".")

evidence <- validate_patient_choice_evidence(connection)
base::print(evidence)
if (!evidence$estimation_allowed[[1]]) {
  base::message(
    "Empirical coefficient estimation remains disabled. Public aggregate ",
    "sources may enrich and validate choices but cannot replace patient OD."
  )
}
