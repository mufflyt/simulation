# DuckDB data layer for patient destination choice -------------------------

#' Patient destination source registry
#'
#' Describes the eight complementary source families used by the travel model.
#' A source is never treated as individual revealed choice unless its grain is
#' an observed patient encounter with both origin and chosen destination.
#'
#' @return A tibble describing source roles, grains, and inferential limits.
#' @family patient destination choice
#' @concept data
#' @export
patient_destination_source_registry <- function() {
  tibble::tribble(
    ~source_id, ~source_family, ~grain, ~role, ~revealed_choice,
    "patient_od", "CHIA or local all-payer encounters",
    "encounter x patient origin x rendering NPI",
    "estimate destination-choice coefficients", TRUE,
    "cms_part_b", "CMS Physician and Other Practitioners",
    "provider x HCPCS x POS x year",
    "lagged destination volume and procedure mix", FALSE,
    "cms_dac", "CMS Doctors and Clinicians",
    "clinician x enrollment x group x address",
    "practice-site construction and affiliations", FALSE,
    "nppes", "NPPES monthly or temporal snapshots",
    "NPI x practice location x observation date",
    "locations, taxonomy, openings, closures, moves", FALSE,
    "acs", "ACS five-year and PUMS",
    "tract or PUMA x year",
    "origin population and travel constraints", FALSE,
    "access_surveys", "NHIS, MEPS, and NHTS",
    "survey respondent or published stratum x year",
    "travel-barrier and care-realization priors", FALSE,
    "cms_geo", "CMS Geographic Variation PUF",
    "county or HRR x year",
    "ecological utilization validation", FALSE,
    "context", "AHRF, PLACES, SVI, and RUCA",
    "county, tract, or ZCTA x release",
    "origin and destination context", FALSE
  )
}

#' Connect to the patient-choice DuckDB
#'
#' @param duckdb_path File path for the analytical DuckDB.
#' @param read_only Open the database without write permission.
#' @return A DBI connection. The caller must disconnect it.
#' @family patient destination choice
#' @concept data
#' @export
connect_patient_choice_duckdb <- function(
    duckdb_path,
    read_only = FALSE) {
  if (!base::is.character(duckdb_path) ||
      base::length(duckdb_path) != 1L || !base::nzchar(duckdb_path)) {
    base::stop("`duckdb_path` must be one non-empty path.", call. = FALSE)
  }
  base::message(
    "connect_patient_choice_duckdb(): path = ", duckdb_path,
    "; read_only = ", read_only, "."
  )
  DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = duckdb_path,
    read_only = read_only
  )
}

.choice_table_exists <- function(connection, schema, table) {
  query <- paste0(
    "SELECT COUNT(*) AS n FROM information_schema.tables ",
    "WHERE table_schema = ? AND table_name = ?"
  )
  DBI::dbGetQuery(
    connection,
    query,
    params = list(schema, table)
  )$n[[1]] > 0
}

.choice_assert_identifier <- function(value, argument) {
  if (!base::is.character(value) || base::length(value) != 1L ||
      !base::grepl("^[A-Za-z][A-Za-z0-9_]*$", value)) {
    base::stop(
      "`", argument, "` must be a safe SQL identifier.",
      call. = FALSE
    )
  }
  invisible(value)
}

#' Register the source inventory and ingestion audit
#'
#' @param connection Writable DuckDB connection.
#' @return The registered source table, invisibly.
#' @family patient destination choice
#' @concept data
#' @export
initialize_patient_choice_catalog <- function(connection) {
  base::message(
    "initialize_patient_choice_catalog(): creating catalog tables."
  )
  DBI::dbExecute(
    connection,
    "CREATE SCHEMA IF NOT EXISTS patient_choice"
  )
  source_catalog <- patient_destination_source_registry() |>
    dplyr::mutate(
      retrieved_at_utc = NA_character_,
      source_uri = NA_character_,
      source_sha256 = NA_character_,
      row_count = NA_real_,
      status = "registered",
      status_detail = NA_character_
    )
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "patient_choice", table = "source_catalog"),
    source_catalog,
    overwrite = TRUE
  )
  DBI::dbExecute(
    connection,
    paste0(
      "CREATE TABLE IF NOT EXISTS patient_choice.ingestion_audit (",
      "source_id VARCHAR, source_uri VARCHAR, table_name VARCHAR, ",
      "retrieved_at_utc TIMESTAMP, source_sha256 VARCHAR, ",
      "row_count BIGINT, status VARCHAR, status_detail VARCHAR)"
    )
  )
  invisible(source_catalog)
}

#' Inventory existing source tables without copying them
#'
#' @param connection Connection to the existing analytical DuckDB.
#' @return Tibble of detected tables and source assignments.
#' @family patient destination choice
#' @concept data
#' @export
inventory_existing_patient_choice_tables <- function(connection) {
  base::message(
    "inventory_existing_patient_choice_tables(): scanning DuckDB metadata."
  )
  tables <- DBI::dbGetQuery(
    connection,
    paste0(
      "SELECT table_schema, table_name FROM information_schema.tables ",
      "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    )
  ) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      source_id = dplyr::case_when(
        base::grepl("medicare_part_b", .data$table_name,
                    ignore.case = TRUE) ~ "cms_part_b",
        base::grepl("temporal_nppes|nppes", .data$table_name,
                    ignore.case = TRUE) ~ "nppes",
        base::grepl("dac|doctor.*clinician", .data$table_name,
                    ignore.case = TRUE) ~ "cms_dac",
        base::grepl("chia|patient.*origin|encounter", .data$table_name,
                    ignore.case = TRUE) ~ "patient_od",
        base::grepl("acs|pums", .data$table_name,
                    ignore.case = TRUE) ~ "acs",
        base::grepl("nhis|meps|nhts", .data$table_name,
                    ignore.case = TRUE) ~ "access_surveys",
        base::grepl("geographic.*variation|cms_geo", .data$table_name,
                    ignore.case = TRUE) ~ "cms_geo",
        base::grepl("ahrf|places|svi|ruca", .data$table_name,
                    ignore.case = TRUE) ~ "context",
        TRUE ~ NA_character_
      ),
      qualified_table = base::paste(
        .data$table_schema,
        .data$table_name,
        sep = "."
      )
    ) |>
    dplyr::filter(!base::is.na(.data$source_id))
  base::message(
    "inventory_existing_patient_choice_tables(): detected ",
    scales::comma(base::nrow(tables)), " relevant tables."
  )
  tables
}

#' Ingest a delimited public-use source into DuckDB
#'
#' @param connection Writable DuckDB connection.
#' @param path Local CSV, TSV, CSV.GZ, or ZIP file supported by DuckDB.
#' @param table Destination table name.
#' @param source_id One source ID from
#'   [patient_destination_source_registry()].
#' @param schema Destination schema.
#' @param source_uri Original public URL or restricted source description.
#' @param overwrite Replace an existing table.
#' @return Ingestion audit row.
#' @family patient destination choice
#' @concept data
#' @export
ingest_patient_choice_file <- function(
    connection,
    path,
    table,
    source_id,
    schema = "patient_choice",
    source_uri = path,
    overwrite = FALSE) {
  .choice_assert_identifier(schema, "schema")
  .choice_assert_identifier(table, "table")
  valid_source_ids <- patient_destination_source_registry()$source_id
  if (!source_id %in% valid_source_ids) {
    base::stop("Unknown `source_id`: ", source_id, ".", call. = FALSE)
  }
  if (!base::file.exists(path)) {
    base::stop("Source file does not exist: ", path, ".", call. = FALSE)
  }
  DBI::dbExecute(
    connection,
    base::paste("CREATE SCHEMA IF NOT EXISTS", schema)
  )
  if (.choice_table_exists(connection, schema, table) && !overwrite) {
    base::stop(
      "Destination table already exists: ", schema, ".", table, ".",
      call. = FALSE
    )
  }
  source_hash <- digest::digest(path, algo = "sha256", file = TRUE)
  quoted_path <- DBI::dbQuoteString(connection, path)
  qualified_table <- base::paste(schema, table, sep = ".")
  base::message(
    "ingest_patient_choice_file(): loading ", path, " into ",
    qualified_table, "."
  )
  transaction_action <- function() {
    if (overwrite) {
      DBI::dbExecute(
        connection,
        base::paste("DROP TABLE IF EXISTS", qualified_table)
      )
    }
    DBI::dbExecute(
      connection,
      base::paste0(
        "CREATE TABLE ", qualified_table,
        " AS SELECT * FROM read_csv_auto(", quoted_path,
        ", header = TRUE, sample_size = -1, all_varchar = FALSE)"
      )
    )
  }
  DBI::dbWithTransaction(connection, transaction_action())
  row_count <- DBI::dbGetQuery(
    connection,
    base::paste("SELECT COUNT(*) AS n FROM", qualified_table)
  )$n[[1]]
  audit_row <- tibble::tibble(
    source_id = source_id,
    source_uri = source_uri,
    table_name = qualified_table,
    retrieved_at_utc = base::format(
      base::Sys.time(), tz = "UTC", usetz = TRUE
    ),
    source_sha256 = source_hash,
    row_count = base::as.numeric(row_count),
    status = "loaded",
    status_detail = NA_character_
  )
  DBI::dbAppendTable(
    connection,
    DBI::Id(schema = "patient_choice", table = "ingestion_audit"),
    audit_row
  )
  base::message(
    "ingest_patient_choice_file(): loaded ",
    scales::comma(row_count), " rows; SHA-256 = ", source_hash, "."
  )
  audit_row
}

#' Attach an existing DuckDB without copying large source tables
#'
#' @param connection Writable destination connection.
#' @param source_duckdb Existing source DuckDB path.
#' @param alias Safe database alias.
#' @param read_only Attach source in read-only mode.
#' @return Inventory of relevant attached tables.
#' @family patient destination choice
#' @concept data
#' @export
attach_patient_choice_duckdb <- function(
    connection,
    source_duckdb,
    alias = "source_store",
    read_only = TRUE) {
  .choice_assert_identifier(alias, "alias")
  if (!base::file.exists(source_duckdb)) {
    base::stop("DuckDB does not exist: ", source_duckdb, ".",
               call. = FALSE)
  }
  quoted_path <- DBI::dbQuoteString(connection, source_duckdb)
  option <- if (read_only) " (READ_ONLY)" else ""
  base::message(
    "attach_patient_choice_duckdb(): attaching ", source_duckdb,
    " as ", alias, option, "."
  )
  DBI::dbExecute(
    connection,
    base::paste0("ATTACH ", quoted_path, " AS ", alias, option)
  )
  DBI::dbGetQuery(
    connection,
    base::paste0(
      "SELECT database_name, schema_name, table_name ",
      "FROM duckdb_tables() WHERE database_name = '", alias, "'"
    )
  ) |>
    tibble::as_tibble()
}

#' Validate the evidence hierarchy for destination-choice estimation
#'
#' @param connection DuckDB connection containing `patient_choice` tables.
#' @param patient_table Qualified patient origin-destination table.
#' @return One-row tibble describing whether empirical estimation is allowed.
#' @family patient destination choice
#' @concept data
#' @export
validate_patient_choice_evidence <- function(
    connection,
    patient_table = "patient_choice.patient_od") {
  .choice_assert_identifier(
    base::sub("^.*\\.", "", patient_table),
    "patient_table"
  )
  parts <- base::strsplit(patient_table, "\\.")[[1]]
  schema <- if (base::length(parts) == 2L) parts[[1]] else "main"
  table <- parts[[base::length(parts)]]
  exists <- .choice_table_exists(connection, schema, table)
  needed <- c(
    "choice_event_id", "origin_id", "destination_id", "chosen"
  )
  present <- character(0)
  if (exists) {
    present <- DBI::dbGetQuery(
      connection,
      paste0(
        "SELECT column_name FROM information_schema.columns ",
        "WHERE table_schema = ? AND table_name = ?"
      ),
      params = list(schema, table)
    )$column_name
  }
  missing_names <- base::setdiff(needed, present)
  tibble::tibble(
    patient_table_exists = exists,
    required_columns_present = base::length(missing_names) == 0L,
    missing_columns = base::paste(missing_names, collapse = ","),
    estimation_allowed = exists && base::length(missing_names) == 0L,
    reason = dplyr::case_when(
      !exists ~ "No observed patient origin-destination table",
      base::length(missing_names) > 0L ~
        "Patient table cannot identify full revealed choices",
      TRUE ~ "Observed choices available"
    )
  )
}
