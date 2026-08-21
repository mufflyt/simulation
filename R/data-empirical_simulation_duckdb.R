# Empirical simulation data warehouse -------------------------------------

#' Build the empirical simulation DuckDB database
#'
#' @description
#' Loads six free national data sources into DuckDB and constructs model-ready
#' provider, productivity, county-market, and population tables. Existing local
#' files are reused before any download is attempted. Every load is registered
#' with its source URL, release, size, checksum, row count, and load time.
#'
#' @param manifest A tibble created by [empirical_source_manifest()].
#' @param database_path Destination DuckDB database path.
#' @param raw_directory Directory containing downloaded source files.
#' @param download_missing Whether missing files may be downloaded.
#' @param overwrite Whether existing warehouse tables may be replaced.
#'
#' @return The normalized absolute DuckDB path.
#' @export
build_empirical_simulation_database <- function(
    manifest = empirical_source_manifest(),
    database_path = "data-raw/empirical/urps_empirical.duckdb",
    raw_directory = "data-raw/empirical",
    download_missing = TRUE,
    overwrite = FALSE) {
  base::message("Building empirical simulation DuckDB database.")
  base::message("Manifest rows: ", scales::comma(base::nrow(manifest)), ".")
  validate_empirical_manifest(manifest)
  if (!base::dir.exists(raw_directory)) {
    base::dir.create(raw_directory, recursive = TRUE)
  }
  database_path <- base::normalizePath(database_path, mustWork = FALSE)
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = database_path)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  initialize_empirical_registry(connection)

  for (source_index in base::seq_len(base::nrow(manifest))) {
    source_record <- manifest[source_index, , drop = FALSE]
    source_path <- resolve_empirical_source(
      source_record = source_record,
      raw_directory = raw_directory,
      download_missing = download_missing
    )
    ingest_empirical_source(
      connection = connection,
      source_record = source_record,
      source_path = source_path,
      overwrite = overwrite
    )
  }
  build_empirical_model_tables(connection)
  registry_count <- DBI::dbGetQuery(
    connection,
    "SELECT COUNT(*) AS n FROM empirical_source_registry"
  )$n[[1L]]
  base::message("Registered empirical loads: ",
    scales::comma(registry_count), ".")
  base::message("DuckDB database saved to: ", database_path)
  database_path
}

#' Define the empirical-source manifest
#'
#' @description
#' The default URLs are stable landing pages rather than guessed release-file
#' URLs. Set `download_url` to the exact versioned file selected for an
#' analysis. This prevents an unannounced upstream release from changing a
#' frozen simulation.
#'
#' @return A manifest tibble.
#' @export
empirical_source_manifest <- function() {
  tibble::tribble(
    ~source_id, ~table_name, ~local_path, ~download_url, ~release,
    ~format, ~required,
    "cms_provider_service", "cms_provider_service_raw",
    "data-raw/cms_psps/provider_service.csv.gz", NA_character_,
    "user_selected", "csv", TRUE,
    "nppes", "nppes_raw",
    "data-raw/nppes/nppes.csv", NA_character_,
    "user_selected", "csv", TRUE,
    "doctors_clinicians", "doctors_clinicians_raw",
    "data-raw/cms_doctors_clinicians/national.csv", NA_character_,
    "user_selected", "csv", TRUE,
    "census_county", "census_county_raw",
    "data-raw/census/county_age_sex.csv", NA_character_,
    "user_selected", "csv", TRUE,
    "ahrf", "ahrf_raw",
    "data-raw/ahrf/ahrf_county.csv", NA_character_,
    "user_selected", "csv", TRUE,
    "cdc_places", "cdc_places_raw",
    "data-raw/brfss/places_county.csv", NA_character_,
    "user_selected", "csv", TRUE
  )
}

#' Validate an empirical-source manifest
#'
#' @param manifest Source manifest.
#'
#' @return Invisibly returns `TRUE`.
#' @keywords internal
validate_empirical_manifest <- function(manifest) {
  required_columns <- base::c(
    "source_id", "table_name", "local_path", "download_url",
    "release", "format", "required"
  )
  missing_columns <- base::setdiff(required_columns, base::names(manifest))
  if (base::length(missing_columns) > 0L) {
    base::stop("Manifest is missing: ",
      base::paste(missing_columns, collapse = ", "), ".",
      call. = FALSE)
  }
  if (base::anyDuplicated(manifest$source_id) > 0L ||
      base::anyDuplicated(manifest$table_name) > 0L) {
    base::stop("Manifest source_id and table_name must be unique.",
      call. = FALSE)
  }
  if (!base::all(manifest$format %in% base::c("csv", "parquet"))) {
    base::stop("Supported formats are csv and parquet.", call. = FALSE)
  }
  base::invisible(TRUE)
}

#' Resolve or download one empirical source
#'
#' @param source_record One-row manifest tibble.
#' @param raw_directory Raw-data directory.
#' @param download_missing Whether downloading is allowed.
#'
#' @return Existing local file path.
#' @keywords internal
resolve_empirical_source <- function(
    source_record,
    raw_directory,
    download_missing) {
  candidate_path <- source_record$local_path[[1L]]
  if (base::file.exists(candidate_path)) {
    base::message("Reusing local source: ", candidate_path)
    return(base::normalizePath(candidate_path))
  }
  source_url <- source_record$download_url[[1L]]
  can_download <- base::isTRUE(download_missing) &&
    !base::is.na(source_url) && base::nzchar(source_url)
  if (!can_download) {
    if (base::isTRUE(source_record$required[[1L]])) {
      base::stop(
        "Required source is absent and has no pinned download URL: ",
        source_record$source_id[[1L]], ".",
        call. = FALSE
      )
    }
    return(NA_character_)
  }
  destination <- base::file.path(
    raw_directory,
    base::basename(candidate_path)
  )
  base::message("Downloading ", source_record$source_id[[1L]], ".")
  request <- httr2::request(source_url) |>
    httr2::req_retry(max_tries = 4L) |>
    httr2::req_timeout(seconds = 3600)
  response <- httr2::req_perform(request, path = destination)
  httr2::resp_check_status(response)
  base::message("Downloaded source to: ", destination)
  base::normalizePath(destination)
}

#' Initialize the empirical-source registry
#'
#' @param connection DuckDB connection.
#'
#' @return Invisibly returns the DBI result.
#' @keywords internal
initialize_empirical_registry <- function(connection) {
  DBI::dbExecute(
    connection,
    paste0(
      "CREATE TABLE IF NOT EXISTS empirical_source_registry (",
      "source_id VARCHAR, table_name VARCHAR, release VARCHAR, ",
      "source_url VARCHAR, local_path VARCHAR, sha256 VARCHAR, ",
      "size_bytes BIGINT, row_count BIGINT, loaded_at TIMESTAMP)"
    )
  )
}

#' Ingest one empirical file into DuckDB
#'
#' @param connection DuckDB connection.
#' @param source_record One-row manifest tibble.
#' @param source_path Resolved local path.
#' @param overwrite Whether an existing table may be replaced.
#'
#' @return Invisibly returns the table name.
#' @keywords internal
ingest_empirical_source <- function(
    connection,
    source_record,
    source_path,
    overwrite) {
  if (base::is.na(source_path)) {
    return(base::invisible(NULL))
  }
  table_name <- source_record$table_name[[1L]]
  exists <- DBI::dbExistsTable(connection, table_name)
  if (exists && !base::isTRUE(overwrite)) {
    base::message("Reusing DuckDB table: ", table_name)
    return(base::invisible(table_name))
  }
  if (exists) {
    DBI::dbRemoveTable(connection, table_name)
  }
  escaped_path <- base::gsub("'", "''", source_path, fixed = TRUE)
  escaped_table <- DBI::dbQuoteIdentifier(connection, table_name)
  read_expression <- if (source_record$format[[1L]] == "parquet") {
    base::paste0("read_parquet('", escaped_path, "')")
  } else {
    base::paste0(
      "read_csv_auto('", escaped_path,
      "', header = true, sample_size = -1, ignore_errors = false)"
    )
  }
  DBI::dbExecute(
    connection,
    base::paste("CREATE TABLE", escaped_table, "AS SELECT * FROM",
      read_expression)
  )
  row_count <- DBI::dbGetQuery(
    connection,
    base::paste("SELECT COUNT(*) AS n FROM", escaped_table)
  )$n[[1L]]
  registry_record <- tibble::tibble(
    source_id = source_record$source_id[[1L]],
    table_name = table_name,
    release = source_record$release[[1L]],
    source_url = source_record$download_url[[1L]],
    local_path = source_path,
    sha256 = digest::digest(file = source_path, algo = "sha256"),
    size_bytes = base::file.info(source_path)$size,
    row_count = row_count,
    loaded_at = base::Sys.time()
  )
  DBI::dbExecute(
    connection,
    "DELETE FROM empirical_source_registry WHERE source_id = ?",
    params = base::list(source_record$source_id[[1L]])
  )
  DBI::dbAppendTable(
    connection,
    "empirical_source_registry",
    registry_record
  )
  base::message("Loaded ", scales::comma(row_count), " rows into ",
    table_name, ".")
  base::invisible(table_name)
}

#' Construct model-ready empirical tables
#'
#' @param connection DuckDB connection.
#'
#' @return Invisibly returns `TRUE`.
#' @keywords internal
build_empirical_model_tables <- function(connection) {
  required_tables <- base::c(
    "cms_provider_service_raw", "nppes_raw",
    "doctors_clinicians_raw", "census_county_raw", "ahrf_raw",
    "cdc_places_raw"
  )
  table_exists <- base::vapply(
    required_tables,
    function(table_name) {
      DBI::dbExistsTable(connection, table_name)
    },
    logical(1)
  )
  missing_tables <- required_tables[!table_exists]
  if (base::length(missing_tables) > 0L) {
    base::stop("Cannot build empirical tables; missing: ",
      base::paste(missing_tables, collapse = ", "), ".",
      call. = FALSE)
  }
  create_empirical_view(
    connection,
    "empirical_provider_service",
    "SELECT * FROM cms_provider_service_raw"
  )
  create_empirical_view(
    connection,
    "empirical_provider_roster",
    paste0(
      "SELECT n.*, d.* EXCLUDE (NPI) FROM nppes_raw n ",
      "LEFT JOIN doctors_clinicians_raw d USING (NPI)"
    )
  )
  create_empirical_view(
    connection,
    "empirical_county_market",
    paste0(
      "SELECT c.*, a.* EXCLUDE (county_fips), ",
      "p.* EXCLUDE (county_fips) FROM census_county_raw c ",
      "LEFT JOIN ahrf_raw a USING (county_fips) ",
      "LEFT JOIN cdc_places_raw p USING (county_fips)"
    )
  )
  create_empirical_view(
    connection,
    "empirical_productivity_panel",
    paste0(
      "SELECT s.*, r.* EXCLUDE (NPI) ",
      "FROM empirical_provider_service s ",
      "LEFT JOIN empirical_provider_roster r USING (NPI)"
    )
  )
  base::message("Built empirical model-ready views.")
  base::invisible(TRUE)
}

#' Create or replace a DuckDB view
#'
#' @param connection DuckDB connection.
#' @param view_name View name.
#' @param select_sql Validated internal SELECT statement.
#'
#' @return Invisibly returns the DBI result.
#' @keywords internal
create_empirical_view <- function(connection, view_name, select_sql) {
  quoted_view <- DBI::dbQuoteIdentifier(connection, view_name)
  DBI::dbExecute(
    connection,
    base::paste("CREATE OR REPLACE VIEW", quoted_view, "AS", select_sql)
  )
}

#' Load empirical inputs for the master simulation runner
#'
#' @param database_path Existing empirical DuckDB database.
#' @param provider_npis Optional vector restricting the provider cohort.
#'
#' @return Named list of lazy duckplyr tables and source provenance.
#' @export
load_empirical_runner_inputs <- function(
    database_path = "data-raw/empirical/urps_empirical.duckdb",
    provider_npis = NULL) {
  if (!base::file.exists(database_path)) {
    base::stop("Empirical DuckDB database does not exist: ", database_path,
      call. = FALSE)
  }
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = TRUE
  )
  provider_roster <- dplyr::tbl(
    connection,
    "empirical_provider_roster"
  )
  if (!base::is.null(provider_npis)) {
    provider_roster <- provider_roster |>
      dplyr::filter(.data$NPI %in% provider_npis)
  }
  source_registry <- dplyr::tbl(
    connection,
    "empirical_source_registry"
  ) |>
    dplyr::collect()
  base::message("Loaded empirical runner inputs from: ",
    base::normalizePath(database_path))
  base::list(
    connection = connection,
    provider_roster = provider_roster,
    provider_service = dplyr::tbl(
      connection,
      "empirical_provider_service"
    ),
    productivity_panel = dplyr::tbl(
      connection,
      "empirical_productivity_panel"
    ),
    county_market = dplyr::tbl(
      connection,
      "empirical_county_market"
    ),
    provenance = source_registry
  )
}
