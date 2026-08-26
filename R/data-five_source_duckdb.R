# Five-source empirical data integration ---------------------------------

#' Build five-source empirical model inputs in DuckDB
#'
#' @description
#' Ingests CMS provider-service, longitudinal NPPES, CMS Doctors and
#' Clinicians, ACS PUMS/migration, and MEPS extracts into DuckDB. Existing
#' files are reused. Missing required files stop the build; absence is never
#' converted to zero utilization or zero providers.
#'
#' @param source_manifest A data frame with `source_id`, `year`, `file_path`,
#'   `file_format`, `required`, `source_url`, and `sha256`.
#' @param duckdb_path Destination DuckDB path.
#' @param overwrite Logical; replace existing source tables.
#'
#' @return A tibble containing source-level ingestion diagnostics.
#' @family empirical data
#' @concept data
#' @export
build_five_source_duckdb <- function(
    source_manifest,
    duckdb_path,
    overwrite = FALSE) {
  base::message("Starting five-source DuckDB build.")
  base::message("DuckDB destination: ", duckdb_path)
  validated_manifest <- validate_five_source_manifest(source_manifest)

  destination_dir <- base::dirname(duckdb_path)
  if (!base::dir.exists(destination_dir)) {
    base::dir.create(destination_dir, recursive = TRUE)
  }
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdb_path)
  base::on.exit(
    DBI::dbDisconnect(connection, shutdown = TRUE),
    add = TRUE
  )

  DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS raw")
  DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS model")
  DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS metadata")

  diagnostics <- base::lapply(
    base::seq_len(base::nrow(validated_manifest)),
    function(row_index) {
      manifest_row <- validated_manifest[row_index, , drop = FALSE]
      ingest_five_source_file(
        connection = connection,
        manifest_row = manifest_row,
        overwrite = overwrite
      )
    }
  )
  ingestion_diagnostics <- dplyr::bind_rows(diagnostics)
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "metadata", table = "source_ingestion"),
    ingestion_diagnostics,
    overwrite = TRUE
  )

  base::message("Creating harmonized empirical model tables.")
  create_empirical_model_tables(connection)
  base::message("Five-source DuckDB build completed: ", duckdb_path)
  ingestion_diagnostics
}

#' Validate a five-source manifest
#'
#' @param source_manifest Source manifest data frame.
#'
#' @return A validated tibble.
#' @keywords internal
validate_five_source_manifest <- function(source_manifest) {
  required_columns <- c(
    "source_id", "year", "file_path", "file_format", "required",
    "source_url", "sha256"
  )
  missing_columns <- base::setdiff(
    required_columns,
    base::names(source_manifest)
  )
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Source manifest is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  validated_manifest <- tibble::as_tibble(source_manifest) |>
    dplyr::mutate(
      source_id = base::as.character(.data$source_id),
      year = base::as.integer(.data$year),
      file_path = base::as.character(.data$file_path),
      file_format = base::tolower(.data$file_format),
      required = base::as.logical(.data$required),
      source_url = base::as.character(.data$source_url),
      sha256 = base::tolower(base::as.character(.data$sha256))
    )
  valid_sources <- c(
    "cms_provider_service", "nppes", "doctors_clinicians",
    "acs_pums", "acs_migration", "meps"
  )
  invalid_sources <- base::setdiff(
    base::unique(validated_manifest$source_id),
    valid_sources
  )
  if (base::length(invalid_sources) > 0L) {
    base::stop(
      "Unsupported source_id: ",
      base::paste(invalid_sources, collapse = ", "),
      call. = FALSE
    )
  }
  missing_files <- validated_manifest |>
    dplyr::filter(.data$required, !base::file.exists(.data$file_path))
  if (base::nrow(missing_files) > 0L) {
    base::stop(
      "Required empirical files are missing: ",
      base::paste(missing_files$file_path, collapse = ", "),
      ". Download them or correct the manifest paths.",
      call. = FALSE
    )
  }
  duplicated_keys <- validated_manifest |>
    dplyr::count(.data$source_id, .data$year, name = "manifest_n") |>
    dplyr::filter(.data$manifest_n > 1L)
  if (base::nrow(duplicated_keys) > 0L) {
    base::stop(
      "Manifest contains duplicate source-year entries.",
      call. = FALSE
    )
  }
  validated_manifest
}

#' Ingest one empirical source file
#'
#' @param connection Open DuckDB connection.
#' @param manifest_row One-row manifest.
#' @param overwrite Replace existing table.
#'
#' @return One-row diagnostic tibble.
#' @keywords internal
ingest_five_source_file <- function(
    connection,
    manifest_row,
    overwrite = FALSE) {
  source_id <- manifest_row$source_id[[1L]]
  source_year <- manifest_row$year[[1L]]
  file_path <- manifest_row$file_path[[1L]]
  file_format <- manifest_row$file_format[[1L]]
  is_required <- manifest_row$required[[1L]]
  table_name <- base::paste0(source_id, "_", source_year)

  base::message(
    "Ingesting source=", source_id,
    "; year=", source_year,
    "; file=", file_path
  )
  if (!base::file.exists(file_path)) {
    if (base::isTRUE(is_required)) {
      base::stop("Required file disappeared: ", file_path, call. = FALSE)
    }
    return(tibble::tibble(
      source_id = source_id,
      year = source_year,
      table_name = NA_character_,
      row_n = NA_real_,
      file_size_bytes = NA_real_,
      sha256_observed = NA_character_,
      status = "optional_missing",
      ingested_at_utc = base::format(
        base::Sys.time(), tz = "UTC", usetz = TRUE
      )
    ))
  }

  observed_sha256 <- digest::digest(
    file_path,
    algo = "sha256",
    file = TRUE,
    serialize = FALSE
  )
  expected_sha256 <- manifest_row$sha256[[1L]]
  if (base::nzchar(expected_sha256) &&
      !base::identical(observed_sha256, expected_sha256)) {
    base::stop(
      "Checksum mismatch for ", file_path,
      "; expected=", expected_sha256,
      "; observed=", observed_sha256,
      call. = FALSE
    )
  }

  quoted_path <- DBI::dbQuoteString(connection, file_path)
  quoted_table <- DBI::dbQuoteIdentifier(
    connection,
    DBI::Id(schema = "raw", table = table_name)
  )
  reader_sql <- switch(
    file_format,
    csv = base::paste0(
      "read_csv_auto(", quoted_path,
      ", header = true, all_varchar = true, sample_size = -1)"
    ),
    parquet = base::paste0("read_parquet(", quoted_path, ")"),
    base::stop(
      "Unsupported file format: ", file_format,
      call. = FALSE
    )
  )
  table_exists <- DBI::dbExistsTable(
    connection,
    DBI::Id(schema = "raw", table = table_name)
  )
  if (table_exists && !base::isTRUE(overwrite)) {
    base::stop(
      "Raw table already exists: raw.", table_name,
      ". Use overwrite = TRUE only for an intentional rebuild.",
      call. = FALSE
    )
  }
  if (table_exists) {
    DBI::dbExecute(
      connection,
      base::paste0("DROP TABLE ", quoted_table)
    )
  }
  DBI::dbExecute(
    connection,
    base::paste0(
      "CREATE TABLE ", quoted_table, " AS SELECT * FROM ", reader_sql
    )
  )
  row_n <- DBI::dbGetQuery(
    connection,
    base::paste0("SELECT COUNT(*) AS row_n FROM ", quoted_table)
  )$row_n[[1L]]
  if (row_n == 0) {
    base::stop(
      "Ingested table is empty: raw.", table_name,
      call. = FALSE
    )
  }
  file_info <- base::file.info(file_path)
  base::message(
    "Ingested raw.", table_name,
    " with ", scales::comma(row_n), " rows."
  )
  tibble::tibble(
    source_id = source_id,
    year = source_year,
    table_name = base::paste0("raw.", table_name),
    row_n = base::as.numeric(row_n),
    file_size_bytes = base::as.numeric(file_info$size),
    sha256_observed = observed_sha256,
    status = "ingested",
    ingested_at_utc = base::format(
      base::Sys.time(), tz = "UTC", usetz = TRUE
    )
  )
}

#' Create harmonized model tables
#'
#' @param connection Open DuckDB connection.
#'
#' @return Invisibly returns `TRUE`.
#' @keywords internal
create_empirical_model_tables <- function(connection) {
  # DBI::dbListObjects()$table returns a list of Id S4 objects, not plain
  # strings -- as.character() on an Id does not extract the table name (it
  # stringifies the whole S4 object, e.g. "new(\"Id\", name = c(table =
  # \"cms_provider_service_2023\"))"), which silently broke every downstream
  # source_id/year match and made create_source_union_view() skip every
  # source as "not available." information_schema.tables gives plain
  # character table names directly.
  table_names <- DBI::dbGetQuery(
    connection,
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
  )$table_name
  source_table_map <- tibble::tibble(
    table_name = table_names
  ) |>
    dplyr::mutate(
      source_id = stringr::str_remove(.data$table_name, "_[0-9]{4}$"),
      year = base::as.integer(stringr::str_extract(
        .data$table_name,
        "[0-9]{4}$"
      ))
    )
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "metadata", table = "source_table_map"),
    source_table_map,
    overwrite = TRUE
  )

  create_source_union_view(
    connection,
    source_table_map,
    source_id = "cms_provider_service",
    view_name = "cms_provider_service_all"
  )
  create_source_union_view(
    connection,
    source_table_map,
    source_id = "nppes",
    view_name = "nppes_all"
  )
  create_source_union_view(
    connection,
    source_table_map,
    source_id = "doctors_clinicians",
    view_name = "doctors_clinicians_all"
  )
  create_source_union_view(
    connection,
    source_table_map,
    source_id = "acs_pums",
    view_name = "acs_pums_all"
  )
  create_source_union_view(
    connection,
    source_table_map,
    source_id = "acs_migration",
    view_name = "acs_migration_all"
  )
  create_source_union_view(
    connection,
    source_table_map,
    source_id = "meps",
    view_name = "meps_all"
  )
  invisible(TRUE)
}

#' Create a union-by-name view for one source
#'
#' @param connection Open DuckDB connection.
#' @param source_table_map Source-table map.
#' @param source_id Source identifier.
#' @param view_name Destination view name.
#'
#' @return Invisibly returns `TRUE` when created and `FALSE` when unavailable.
#' @keywords internal
create_source_union_view <- function(
    connection,
    source_table_map,
    source_id,
    view_name) {
  selected_tables <- source_table_map |>
    dplyr::filter(.data$source_id == .env$source_id) |>
    dplyr::arrange(.data$year)
  if (base::nrow(selected_tables) == 0L) {
    base::message("No tables available for optional source: ", source_id)
    return(invisible(FALSE))
  }
  selects <- base::vapply(
    base::seq_len(base::nrow(selected_tables)),
    function(row_index) {
      quoted_table <- DBI::dbQuoteIdentifier(
        connection,
        DBI::Id(
          schema = "raw",
          table = selected_tables$table_name[[row_index]]
        )
      )
      base::paste0(
        "SELECT *, ", selected_tables$year[[row_index]],
        "::INTEGER AS source_year FROM ", quoted_table
      )
    },
    character(1L)
  )
  quoted_view <- DBI::dbQuoteIdentifier(
    connection,
    DBI::Id(schema = "model", table = view_name)
  )
  DBI::dbExecute(
    connection,
    base::paste0(
      "CREATE OR REPLACE VIEW ", quoted_view, " AS ",
      base::paste(selects, collapse = " UNION ALL BY NAME ")
    )
  )
  invisible(TRUE)
}

#' Open the empirical five-source database read-only
#'
#' @param required Logical; stop if the database is absent.
#'
#' @return Output from [open_research_db()].
#' @family empirical data
#' @concept data
#' @export
open_five_source_db <- function(required = TRUE) {
  database_path <- resolve_research_db(
    relative_path = "DuckDB/urps_five_source.duckdb",
    volume_pattern = "MufflySamsung*",
    env_var = "URPS_FIVE_SOURCE_DUCKDB",
    required = required,
    what = "URPS five-source empirical database"
  )
  if (base::is.na(database_path)) {
    return(NULL)
  }
  open_research_db(
    path = database_path,
    required_tables = c("source_ingestion", "source_table_map"),
    what = "URPS five-source empirical database"
  )
}
