#' Open IPUMS and LODES evidence database
#'
#' Opens a DuckDB database used to stage IPUMS microdata and Census LODES
#' (LEHD Origin-Destination Employment Statistics) commuter flow matrices.
#'
#' @param duckdb_path Destination DuckDB file path.
#' @param overwrite Whether to replace an existing database file.
#'
#' @return An open DuckDB connection.
#' @family data
#' @concept duckdb
#' @export
open_ipums_lodes_duckdb <- function(
    duckdb_path,
    overwrite = FALSE) {
  base::stopifnot(
    base::is.character(duckdb_path),
    base::length(duckdb_path) == 1L,
    !base::is.na(duckdb_path),
    base::nzchar(duckdb_path)
  )

  base::message("[ipums-lodes] DuckDB path: ", duckdb_path)

  if (base::file.exists(duckdb_path) && base::isTRUE(overwrite)) {
    base::message("[ipums-lodes] Removing existing DuckDB database.")
    base::unlink(duckdb_path)
  }

  parent <- base::dirname(duckdb_path)
  if (!base::dir.exists(parent)) {
    base::dir.create(parent, recursive = TRUE)
  }

  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = duckdb_path,
    read_only = FALSE
  )

  manifest <- tibble::tibble(
    source_id = character(),
    source_name = character(),
    vintage = character(),
    file_path = character(),
    row_count = integer(),
    ingested_at_utc = character()
  )

  if (!DBI::dbExistsTable(connection, "ipums_lodes_manifest")) {
    DBI::dbWriteTable(
      connection,
      "ipums_lodes_manifest",
      manifest,
      overwrite = TRUE
    )
  }

  base::message("[ipums-lodes] Opened IPUMS/LODES DuckDB database.")
  connection
}

#' Source catalog for IPUMS and Census LODES evidence
#'
#' Document official sources for harmonized population microdata (IPUMS) and
#' workplace-residence commute matrices (Census LODES LEHD).
#'
#' @return A tibble with source metadata and official access endpoints.
#' @family data
#' @concept catalog
#' @export
ipums_lodes_source_catalog <- function() {
  tibble::tribble(
    ~source_id, ~official_name, ~url, ~grain, ~description,

    "ipums_usa",
    "IPUMS USA Microdata",
    "https://usa.ipums.org/usa/",
    "person-year",
    "Harmonized 1-year and 5-year ACS PUMS microdata across Census years.",

    "ipums_cps",
    "IPUMS CPS Microdata",
    "https://cps.ipums.org/cps/",
    "person-month",
    "Harmonized Current Population Survey labor force participation & health insurance.",

    "ipums_nhis",
    "IPUMS Health Surveys (NHIS)",
    "https://nhis.ipums.org/nhis/",
    "person-year",
    "Harmonized National Health Interview Survey PFD & disability microdata.",

    "lodes_od",
    "Census LODES LEHD Origin-Destination",
    "https://lehd.ces.census.gov/data/lodes/",
    "block-pair-year",
    "Workplace-to-residence job flow matrices by 2-digit & 3-digit NAICS industry.",

    "lodes_wac",
    "Census LODES Workplace Area Characteristics",
    "https://lehd.ces.census.gov/data/lodes/",
    "block-year",
    "Workplace employment totals by worker age, earnings, and industry.",

    "lodes_rac",
    "Census LODES Residence Area Characteristics",
    "https://lehd.ces.census.gov/data/lodes/",
    "block-year",
    "Residence worker totals by earnings, age, and broad industry sector."
  )
}

#' Ingest IPUMS harmonized microdata into DuckDB
#'
#' Stages harmonized IPUMS ACS/CPS/NHIS person-level records with standard
#' demographic, insurance, labor force, and geographic variables.
#'
#' @param connection Open DuckDB connection.
#' @param ipums_data Tibble or data frame containing IPUMS extracts.
#' @param source_id Label for IPUMS subset (`"ipums_usa"`, `"ipums_cps"`, etc.).
#' @param overwrite Whether to overwrite the target table.
#'
#' @return Table summary containing row count and table name.
#' @family data
#' @concept duckdb
#' @export
ingest_ipums_microdata <- function(
    connection,
    ipums_data,
    source_id = "ipums_usa",
    overwrite = TRUE) {

  required_columns <- c("year", "age", "sex")
  missing_cols <- base::setdiff(required_columns, base::names(ipums_data))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Missing required IPUMS columns: ",
      base::paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  table_name <- base::paste0("raw_", source_id)
  base::message("[ipums] Ingesting ", scales::comma(base::nrow(ipums_data)),
                " rows into `", table_name, "`.")

  DBI::dbWriteTable(
    connection,
    table_name,
    ipums_data,
    overwrite = overwrite
  )

  manifest_entry <- tibble::tibble(
    source_id = source_id,
    source_name = "IPUMS Harmonized Microdata",
    vintage = base::as.character(base::max(ipums_data$year, na.rm = TRUE)),
    file_path = table_name,
    row_count = base::as.integer(base::nrow(ipums_data)),
    ingested_at_utc = base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  )

  DBI::dbWriteTable(
    connection,
    "ipums_lodes_manifest",
    manifest_entry,
    append = TRUE
  )

  base::list(
    table_name = table_name,
    row_count = base::nrow(ipums_data)
  )
}

#' Ingest Census LODES commute flow matrices into DuckDB
#'
#' Stages Census LEHD Origin-Destination (LODES) commute flow records between
#' workplace (w_geocode) and residence (h_geocode) census blocks/counties.
#'
#' @param connection Open DuckDB connection.
#' @param lodes_data Tibble or data frame containing LODES OD records.
#' @param vintage Publication year/vintage.
#' @param overwrite Whether to overwrite the target table.
#'
#' @return Table summary containing row count and table name.
#' @family data
#' @concept duckdb
#' @export
ingest_lodes_commute_flows <- function(
    connection,
    lodes_data,
    vintage = "2021",
    overwrite = TRUE) {

  required_columns <- c("w_geocode", "h_geocode", "S000")
  missing_cols <- base::setdiff(required_columns, base::names(lodes_data))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Missing required LODES columns: ",
      base::paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  normalized_lodes <- lodes_data |>
    dplyr::mutate(
      w_county = stringr::str_sub(stringr::str_pad(base::as.character(.data$w_geocode), 15, "left", "0"), 1, 5),
      h_county = stringr::str_sub(stringr::str_pad(base::as.character(.data$h_geocode), 15, "left", "0"), 1, 5),
      total_jobs = base::as.numeric(.data$S000),
      healthcare_jobs = if ("CNS16" %in% base::names(lodes_data)) base::as.numeric(.data$CNS16) else NA_real_
    )

  table_name <- "raw_lodes_od"
  base::message("[lodes] Ingesting ", scales::comma(base::nrow(normalized_lodes)),
                " LODES commute flow rows into `", table_name, "`.")

  DBI::dbWriteTable(
    connection,
    table_name,
    normalized_lodes,
    overwrite = overwrite
  )

  manifest_entry <- tibble::tibble(
    source_id = "lodes_od",
    source_name = "Census LEHD LODES Origin-Destination Flows",
    vintage = base::as.character(vintage),
    file_path = table_name,
    row_count = base::as.integer(base::nrow(normalized_lodes)),
    ingested_at_utc = base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  )

  DBI::dbWriteTable(
    connection,
    "ipums_lodes_manifest",
    manifest_entry,
    append = TRUE
  )

  base::list(
    table_name = table_name,
    row_count = base::nrow(normalized_lodes)
  )
}
