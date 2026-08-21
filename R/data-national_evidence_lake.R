# National empirical evidence lake -----------------------------------------

#' National sources used by the URPS microsimulation
#'
#' The registry separates a source landing page from a local extract. Large
#' public files are intentionally kept outside the package tarball and loaded
#' into DuckDB with an auditable manifest.
#'
#' @return A tibble with one row per source family.
#' @family data acquisition
#' @concept data
#' @export
urps_national_source_registry <- function() {
  tibble::tribble(
    ~source_id, ~publisher, ~source_name, ~model_component,
    ~landing_url, ~evidence_tier,
    "cms_provider_service", "CMS",
    "Medicare Physician and Other Practitioners",
    "productivity and procedure mix",
    base::paste0(
      "https://data.cms.gov/provider-summary-by-type-of-service/",
      "medicare-physician-other-practitioners"
    ),
    "direct_public_use",
    "nppes", "CMS", "NPPES monthly replacement files",
    "provider entry, exit, taxonomy, and migration",
    base::paste0(
      "https://www.cms.gov/medicare/regulations-guidance/",
      "administrative-simplification/data-dissemination"
    ),
    "direct_public_use",
    "cms_clinicians", "CMS", "Doctors and Clinicians national file",
    "practice sites and organizational affiliation",
    "https://data.cms.gov/provider-data/dataset/mj5m-pzi6",
    "direct_public_use",
    "acs", "US Census Bureau", "ACS estimates and PUMS",
    "population, insurance, income, and migration",
    "https://www.census.gov/programs-surveys/acs.html",
    "direct_population_survey",
    "meps", "AHRQ", "Medical Expenditure Panel Survey",
    "care seeking, utilization, payer, and expenditures",
    "https://meps.ahrq.gov/mepsweb/",
    "direct_population_survey",
    "nhanes", "CDC NCHS", "National Health and Nutrition Survey",
    "pelvic floor prevalence and joint comorbidity",
    "https://www.cdc.gov/nchs/nhanes/",
    "direct_population_survey",
    "brfss_places", "CDC", "BRFSS and PLACES",
    "county health and comorbidity reweighting",
    "https://www.cdc.gov/places/",
    "modeled_small_area_and_survey",
    "migration", "US Census Bureau and IRS SOI",
    "ACS and IRS migration flows",
    "population redistribution and Sunbelt scenarios",
    "https://www.irs.gov/statistics/soi-tax-stats-migration-data-downloads",
    "direct_administrative_and_survey",
    "training", "ACGME and NRMP",
    "URPS program and match reports",
    "fellowship capacity and entrant production",
    base::paste0(
      "https://www.nrmp.org/fellowship-applicants/",
      "participating-fellowships/urogyn/"
    ),
    "direct_program_census",
    "hcris_ahrf", "CMS and HRSA", "HCRIS and AHRF",
    "facility economics and county workforce context",
    "https://data.hrsa.gov/topics/health-workforce/ahrf",
    "direct_administrative"
  )
}

.urps_safe_name <- function(value, argument) {
  if (!base::is.character(value) || base::length(value) != 1L ||
      !base::grepl("^[A-Za-z][A-Za-z0-9_]*$", value)) {
    base::stop(
      "`", argument, "` must contain letters, numbers, and underscores.",
      call. = FALSE
    )
  }
  value
}

.urps_file_sha256 <- function(path) {
  base::unname(digest::digest(path, algo = "sha256", file = TRUE))
}

.urps_disconnect <- function(connection) {
  if (DBI::dbIsValid(connection)) {
    DBI::dbDisconnect(connection, shutdown = TRUE)
  }
  base::invisible(NULL)
}

#' Open or create the national evidence DuckDB
#'
#' @param duckdb_path Path to the DuckDB database.
#' @param read_only Open without permitting writes.
#'
#' @return A DBI connection. The caller must disconnect it.
#' @family data acquisition
#' @concept data
#' @export
open_urps_evidence_db <- function(duckdb_path, read_only = FALSE) {
  if (!base::is.character(duckdb_path) || base::length(duckdb_path) != 1L ||
      !base::nzchar(duckdb_path)) {
    base::stop("`duckdb_path` must be one non-empty path.", call. = FALSE)
  }
  parent_dir <- base::dirname(duckdb_path)
  if (!base::isTRUE(read_only) && !base::dir.exists(parent_dir)) {
    base::dir.create(parent_dir, recursive = TRUE)
  }
  base::message("Opening URPS evidence DuckDB: ", duckdb_path)
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = duckdb_path,
    read_only = read_only
  )
  if (!base::isTRUE(read_only)) {
    DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS evidence")
    DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS raw")
    DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS model")
  }
  connection
}

#' Download a public national evidence extract
#'
#' @param source_id Identifier from [urps_national_source_registry()].
#' @param download_url Direct HTTPS file URL supplied by the publisher.
#' @param destination_dir Directory for the timestamped download.
#' @param expected_sha256 Optional publisher or preregistered SHA-256 hash.
#' @param overwrite Permit replacement of an identical timestamped path.
#'
#' @return Absolute downloaded-file path with SHA-256 attached.
#' @family data acquisition
#' @concept data
#' @export
download_urps_evidence_extract <- function(
    source_id,
    download_url,
    destination_dir,
    expected_sha256 = NULL,
    overwrite = FALSE) {
  registry_tbl <- urps_national_source_registry()
  if (!source_id %in% registry_tbl$source_id) {
    base::stop("Unknown `source_id`: ", source_id, call. = FALSE)
  }
  if (!base::grepl("^https://", download_url)) {
    base::stop("`download_url` must use HTTPS.", call. = FALSE)
  }
  if (!base::dir.exists(destination_dir)) {
    base::dir.create(destination_dir, recursive = TRUE)
  }
  url_path <- httr2::url_parse(download_url)$path
  source_name <- base::basename(url_path)
  if (!base::nzchar(source_name)) {
    base::stop("The download URL must end in a file name.", call. = FALSE)
  }
  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  is_csv_gz <- base::grepl("\\.csv\\.gz$", source_name,
    ignore.case = TRUE
  )
  extension <- if (is_csv_gz) "csv.gz" else tools::file_ext(source_name)
  stem <- if (is_csv_gz) {
    base::sub("\\.csv\\.gz$", "", source_name, ignore.case = TRUE)
  } else {
    tools::file_path_sans_ext(source_name)
  }
  timestamped_name <- if (base::nzchar(extension)) {
    base::paste0(stem, "_", timestamp, ".", extension)
  } else {
    base::paste0(source_name, "_", timestamp)
  }
  destination_path <- base::file.path(
    destination_dir,
    timestamped_name
  )
  if (base::file.exists(destination_path) && !base::isTRUE(overwrite)) {
    base::stop("Download destination already exists: ", destination_path)
  }
  base::message("Downloading `", source_id, "` from: ", download_url)
  base::message("Requested destination: ", destination_path)
  request <- httr2::request(download_url) |>
    httr2::req_user_agent("urpssim national evidence acquisition") |>
    httr2::req_retry(max_tries = 4L)
  httr2::req_perform(request, path = destination_path)
  observed_sha256 <- .urps_file_sha256(destination_path)
  if (!base::is.null(expected_sha256) &&
      !base::identical(
        base::tolower(observed_sha256),
        base::tolower(expected_sha256)
      )) {
    base::unlink(destination_path)
    base::stop(
      "SHA-256 mismatch; the downloaded file was removed.",
      call. = FALSE
    )
  }
  resolved_path <- base::normalizePath(destination_path, mustWork = TRUE)
  base::attr(resolved_path, "sha256") <- observed_sha256
  base::message("Exact saved file path: ", resolved_path)
  resolved_path
}

.urps_create_manifest <- function(connection) {
  DBI::dbExecute(
    connection,
    base::paste(
      "CREATE TABLE IF NOT EXISTS evidence.ingest_manifest (",
      "source_id VARCHAR, table_schema VARCHAR, table_name VARCHAR,",
      "source_path VARCHAR, source_sha256 VARCHAR, source_bytes BIGINT,",
      "ingested_at TIMESTAMP, row_count BIGINT, PRIMARY KEY",
      "(table_schema, table_name, source_sha256))"
    )
  )
}

.urps_write_registry <- function(connection) {
  registry_tbl <- urps_national_source_registry()
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "evidence", table = "source_registry"),
    registry_tbl,
    overwrite = TRUE
  )
  base::message("Registered ", base::nrow(registry_tbl), " source families.")
}

#' Load a CSV or Parquet extract into the evidence lake
#'
#' @param connection Writable DuckDB connection.
#' @param source_id Identifier from [urps_national_source_registry()].
#' @param source_path Local CSV, CSV.GZ, or Parquet file.
#' @param table_name Destination table name.
#' @param table_schema Destination schema; defaults to `raw`.
#' @param overwrite Replace an existing destination table.
#'
#' @return An ingest-manifest row.
#' @family data acquisition
#' @concept data
#' @export
ingest_urps_evidence_file <- function(
    connection,
    source_id,
    source_path,
    table_name,
    table_schema = "raw",
    overwrite = FALSE) {
  registry_tbl <- urps_national_source_registry()
  if (!source_id %in% registry_tbl$source_id) {
    base::stop("Unknown `source_id`: ", source_id, call. = FALSE)
  }
  if (!base::file.exists(source_path)) {
    base::stop("Evidence file does not exist: ", source_path, call. = FALSE)
  }
  table_name <- .urps_safe_name(table_name, "table_name")
  table_schema <- .urps_safe_name(table_schema, "table_schema")
  destination <- DBI::Id(schema = table_schema, table = table_name)
  if (DBI::dbExistsTable(connection, destination) && !base::isTRUE(overwrite)) {
    base::stop(
      "Destination exists; set `overwrite = TRUE`: ",
      table_schema, ".", table_name,
      call. = FALSE
    )
  }
  extension <- base::tolower(source_path)
  if (!base::grepl("\\.(csv|csv.gz|parquet)$", extension)) {
    base::stop("Only CSV, CSV.GZ, and Parquet files are supported.")
  }
  base::message(
    "Ingesting source `", source_id, "` from: ", source_path
  )
  destination_sql <- DBI::dbQuoteIdentifier(connection, destination)
  source_sql <- DBI::dbQuoteString(
    connection,
    base::normalizePath(source_path, mustWork = TRUE)
  )
  if (DBI::dbExistsTable(connection, destination)) {
    DBI::dbRemoveTable(connection, destination)
  }
  reader_sql <- if (base::grepl("\\.parquet$", extension)) {
    base::paste0("read_parquet(", source_sql, ")")
  } else {
    base::paste0(
      "read_csv_auto(", source_sql,
      ", header = true, sample_size = -1)"
    )
  }
  DBI::dbExecute(
    connection,
    base::paste0(
      "CREATE TABLE ", destination_sql,
      " AS SELECT * FROM ", reader_sql
    )
  )
  row_count <- DBI::dbGetQuery(
    connection,
    base::paste0(
      "SELECT COUNT(*) AS n FROM ",
      destination_sql
    )
  )$n[[1L]]
  .urps_create_manifest(connection)
  manifest_tbl <- tibble::tibble(
    source_id = source_id,
    table_schema = table_schema,
    table_name = table_name,
    source_path = base::normalizePath(source_path, mustWork = TRUE),
    source_sha256 = .urps_file_sha256(source_path),
    source_bytes = base::file.info(source_path)$size,
    ingested_at = base::Sys.time(),
    row_count = base::as.numeric(row_count)
  )
  DBI::dbExecute(
    connection,
    base::paste0(
      "DELETE FROM evidence.ingest_manifest WHERE table_schema = ? ",
      "AND table_name = ? AND source_sha256 = ?"
    ),
    params = base::list(
      manifest_tbl$table_schema[[1L]],
      manifest_tbl$table_name[[1L]],
      manifest_tbl$source_sha256[[1L]]
    )
  )
  DBI::dbAppendTable(
    connection,
    DBI::Id(schema = "evidence", table = "ingest_manifest"),
    manifest_tbl
  )
  base::message(
    "Loaded ", scales::comma(row_count), " rows into ",
    table_schema, ".", table_name, "."
  )
  manifest_tbl
}

.urps_existing_file <- function(project_root, relative_path) {
  candidate <- base::file.path(project_root, relative_path)
  if (base::file.exists(candidate)) candidate else NULL
}

.urps_bind_manifests <- function(manifest_rows) {
  present <- !base::vapply(
    manifest_rows,
    base::is.null,
    base::logical(1)
  )
  dplyr::bind_rows(manifest_rows[present])
}

#' Seed DuckDB with empirical files already maintained in the repository
#'
#' @param connection Writable DuckDB connection.
#' @param project_root Package repository root.
#' @param overwrite Replace canonical evidence tables.
#'
#' @return A combined ingest manifest for files that were present.
#' @family data acquisition
#' @concept data
#' @export
seed_urps_repository_evidence <- function(
    connection,
    project_root = ".",
    overwrite = TRUE) {
  .urps_write_registry(connection)
  specifications <- tibble::tribble(
    ~source_id, ~relative_path, ~table_name,
    "acs",
    "data-raw/spatial/acs5_2023_tract_female_by_ageband.csv",
    "acs_tract_female_2023",
    "training",
    "data-raw/calibration/nrmp_urps_entrants_series.csv",
    "nrmp_urps_entrants",
    "training",
    "data-raw/calibration/acgme_urps_fellows_series.csv",
    "acgme_urps_fellows",
    "nhanes", "data/anchors/ui_prevalence.csv",
    "nhanes_ui_prevalence"
  )
  base::message("Seeding evidence already maintained by the repository.")
  manifest_rows <- base::vector("list", base::nrow(specifications))
  for (index in base::seq_len(base::nrow(specifications))) {
    source_path <- .urps_existing_file(
      project_root,
      specifications$relative_path[[index]]
    )
    if (base::is.null(source_path)) {
      base::message(
        "Skipping absent repository evidence: ",
        specifications$relative_path[[index]]
      )
      next
    }
    destination <- DBI::Id(
      schema = "evidence",
      table = specifications$table_name[[index]]
    )
    if (DBI::dbExistsTable(connection, destination) &&
        base::isTRUE(overwrite)) {
      DBI::dbRemoveTable(connection, destination)
    }
    manifest_rows[[index]] <- ingest_urps_evidence_file(
      connection = connection,
      source_id = specifications$source_id[[index]],
      source_path = source_path,
      table_name = specifications$table_name[[index]],
      table_schema = "evidence",
      overwrite = FALSE
    )
  }
  .urps_bind_manifests(manifest_rows)
}

.urps_parameter_row <- function(
    parameter,
    estimate,
    source_id,
    source_table,
    reference_year,
    estimand,
    evidence_tier) {
  tibble::tibble(
    parameter = parameter,
    estimate = base::as.numeric(estimate),
    source_id = source_id,
    source_table = source_table,
    reference_year = base::as.integer(reference_year),
    estimand = estimand,
    evidence_tier = evidence_tier,
    derived_at = base::Sys.time()
  )
}

#' Derive simulation parameters from canonical evidence tables
#'
#' @param connection Writable DuckDB connection containing seeded evidence.
#' @param overwrite Replace `model.parameter_estimates`.
#'
#' @return Parameter table with provenance and evidence tier.
#' @family calibration
#' @concept calibration
#' @export
derive_urps_empirical_parameters <- function(
    connection,
    overwrite = TRUE) {
  parameter_rows <- base::list()
  acs_id <- DBI::Id(schema = "evidence", table = "acs_tract_female_2023")
  if (DBI::dbExistsTable(connection, acs_id)) {
    population_query <- base::paste(
      "SELECT SUM(female_20_39 + female_40_59 + female_60_64 +",
      "female_65_79 + female_80plus) AS female_20plus",
      "FROM evidence.acs_tract_female_2023"
    )
    population_n <- DBI::dbGetQuery(
      connection,
      population_query
    )$female_20plus[[1L]]
    parameter_rows[["population"]] <- .urps_parameter_row(
      parameter = "female_population_20plus",
      estimate = population_n,
      source_id = "acs",
      source_table = "evidence.acs_tract_female_2023",
      reference_year = 2023L,
      estimand = "US female population age 20 years and older",
      evidence_tier = "direct_population_estimate"
    )
  }
  nrmp_id <- DBI::Id(schema = "evidence", table = "nrmp_urps_entrants")
  if (DBI::dbExistsTable(connection, nrmp_id)) {
    entrant_query <- base::paste(
      "SELECT AVG(positions_filled) AS entrant_mean",
      "FROM evidence.nrmp_urps_entrants",
      "WHERE appointment_year BETWEEN 2021 AND 2025"
    )
    entrant_mean <- DBI::dbGetQuery(
      connection,
      entrant_query
    )$entrant_mean[[1L]]
    parameter_rows[["entrants"]] <- .urps_parameter_row(
      parameter = "annual_fellowship_entrants",
      estimate = entrant_mean,
      source_id = "training",
      source_table = "evidence.nrmp_urps_entrants",
      reference_year = 2025L,
      estimand = "Mean URPS positions filled, appointment years 2021-2025",
      evidence_tier = "direct_program_census"
    )
  }
  nhanes_id <- DBI::Id(
    schema = "evidence",
    table = "nhanes_ui_prevalence"
  )
  if (DBI::dbExistsTable(connection, nhanes_id)) {
    prevalence_query <- base::paste(
      "SELECT estimate, year FROM evidence.nhanes_ui_prevalence",
      "WHERE anchor_id = 'ui_prevalence'",
      "ORDER BY year DESC LIMIT 1"
    )
    prevalence_tbl <- DBI::dbGetQuery(connection, prevalence_query)
    if (base::nrow(prevalence_tbl) == 1L) {
      parameter_rows[["ui_prevalence"]] <- .urps_parameter_row(
        parameter = "moderate_severe_ui_prevalence",
        estimate = prevalence_tbl$estimate[[1L]],
        source_id = "nhanes",
        source_table = "evidence.nhanes_ui_prevalence",
        reference_year = prevalence_tbl$year[[1L]],
        estimand = base::paste(
          "Moderate-to-severe urinary incontinence among",
          "US nonpregnant women age 20 years and older"
        ),
        evidence_tier = "direct_population_survey"
      )
    }
  }
  parameter_tbl <- dplyr::bind_rows(parameter_rows)
  if (base::nrow(parameter_tbl) == 0L) {
    base::stop("No canonical evidence tables were available.", call. = FALSE)
  }
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "model", table = "parameter_estimates"),
    parameter_tbl,
    overwrite = overwrite
  )
  base::message(
    "Derived ", base::nrow(parameter_tbl),
    " empirical simulation parameters."
  )
  parameter_tbl
}

#' Read empirical parameters from the national evidence lake
#'
#' @param duckdb_path Path to the evidence DuckDB.
#'
#' @return A named numeric vector with the full provenance table attached as
#'   the `provenance` attribute.
#' @family calibration
#' @concept calibration
#' @export
read_urps_empirical_parameters <- function(duckdb_path) {
  connection <- open_urps_evidence_db(duckdb_path, read_only = TRUE)
  base::on.exit(.urps_disconnect(connection), add = TRUE)
  parameter_id <- DBI::Id(
    schema = "model",
    table = "parameter_estimates"
  )
  if (!DBI::dbExistsTable(connection, parameter_id)) {
    base::stop(
      base::paste(
        "DuckDB lacks `model.parameter_estimates`;",
        "build the evidence lake first."
      ),
      call. = FALSE
    )
  }
  parameter_tbl <- DBI::dbReadTable(connection, parameter_id)
  if (base::anyDuplicated(parameter_tbl$parameter)) {
    base::stop("Empirical parameter names must be unique.", call. = FALSE)
  }
  parameter_values <- stats::setNames(
    parameter_tbl$estimate,
    parameter_tbl$parameter
  )
  base::attr(parameter_values, "provenance") <- parameter_tbl
  base::message(
    "Read ", base::length(parameter_values),
    " empirical parameters from: ", duckdb_path
  )
  parameter_values
}

#' Audit national evidence readiness
#'
#' @param duckdb_path Path to the evidence DuckDB.
#'
#' @return One row per source family with loaded-file and row counts.
#' @family data acquisition
#' @concept data
#' @export
audit_urps_evidence_readiness <- function(duckdb_path) {
  connection <- open_urps_evidence_db(duckdb_path, read_only = TRUE)
  base::on.exit(.urps_disconnect(connection), add = TRUE)
  registry_tbl <- urps_national_source_registry()
  manifest_id <- DBI::Id(
    schema = "evidence",
    table = "ingest_manifest"
  )
  if (DBI::dbExistsTable(connection, manifest_id)) {
    manifest_tbl <- DBI::dbGetQuery(
      connection,
      base::paste(
        "SELECT source_id, COUNT(*) AS file_count,",
        "SUM(row_count) AS row_count",
        "FROM evidence.ingest_manifest GROUP BY source_id"
      )
    )
  } else {
    manifest_tbl <- tibble::tibble(
      source_id = base::character(),
      file_count = base::integer(),
      row_count = base::numeric()
    )
  }
  readiness_tbl <- registry_tbl |>
    dplyr::left_join(manifest_tbl, by = "source_id") |>
    dplyr::mutate(
      file_count = dplyr::coalesce(.data$file_count, 0L),
      row_count = dplyr::coalesce(.data$row_count, 0),
      readiness = dplyr::if_else(
        .data$file_count > 0L,
        "loaded",
        "not_loaded"
      )
    )
  base::message(
    "Evidence readiness: ",
    base::sum(readiness_tbl$readiness == "loaded"),
    "/", base::nrow(readiness_tbl), " source families loaded."
  )
  readiness_tbl
}

#' Build the repository-backed national evidence lake
#'
#' @param duckdb_path Destination DuckDB path.
#' @param project_root Package repository root.
#' @param overwrite Replace canonical evidence and parameter tables.
#'
#' @return A list containing the database path, manifest, parameter table, and
#'   source-readiness audit.
#' @family data acquisition
#' @concept data
#' @export
build_urps_national_evidence_lake <- function(
    duckdb_path,
    project_root = ".",
    overwrite = TRUE) {
  base::message("Starting national URPS evidence-lake build.")
  base::message("Project root: ", project_root)
  base::message("DuckDB destination: ", duckdb_path)
  connection <- open_urps_evidence_db(duckdb_path, read_only = FALSE)
  base::on.exit(.urps_disconnect(connection), add = TRUE)
  manifest_tbl <- seed_urps_repository_evidence(
    connection = connection,
    project_root = project_root,
    overwrite = overwrite
  )
  parameter_tbl <- derive_urps_empirical_parameters(
    connection = connection,
    overwrite = overwrite
  )
  resolved_path <- base::normalizePath(duckdb_path, mustWork = TRUE)
  base::message("Evidence-lake build complete: ", resolved_path)
  .urps_disconnect(connection)
  readiness_tbl <- audit_urps_evidence_readiness(resolved_path)
  base::list(
    duckdb_path = resolved_path,
    ingest_manifest = manifest_tbl,
    parameter_estimates = parameter_tbl,
    source_readiness = readiness_tbl
  )
}
