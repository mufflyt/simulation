#' Federal data sources for endogenous provider geography
#'
#' @return A tibble describing the six supported source families.
#' @export
endogenous_geography_source_registry <- function() {
  base::message("Building endogenous-geography source registry.")

  tibble::tribble(
    ~source_id, ~publisher, ~geography, ~role, ~landing_page,
    "irs_migration", "IRS SOI", "county pair-year",
    "annual population and adjusted-gross-income migration",
    "https://www.irs.gov/statistics/soi-tax-stats-migration-data",
    "acs_migration", "US Census Bureau", "county pair-period",
    "age- and sex-specific population migration",
    paste0(
      "https://www.census.gov/data/developers/data-sets/",
      "acs-migration-flows.html"
    ),
    "lodes", "US Census Bureau LEHD", "block pair-year",
    "empirical residence-to-work commuting catchments",
    "https://lehd.ces.census.gov/data/",
    "qcew", "US Bureau of Labor Statistics", "county-industry-year",
    "health-sector employment, establishments, and wages",
    "https://www.bls.gov/cew/downloadable-data-files.htm",
    "bea", "US Bureau of Economic Analysis", "county-year",
    "income, employment, and real economic opportunity",
    "https://apps.bea.gov/api/signup/",
    "ipeds", "US Department of Education NCES", "institution-year",
    "academic-market size and health-training environment",
    "https://nces.ed.gov/ipeds/use-the-data"
  ) |>
    dplyr::mutate(
      evidence_tier = "primary",
      required_for_canonical_run = TRUE
    )
}

#' Validate a pinned geography download manifest
#'
#' @param source_manifest Source-file manifest.
#' @return The validated manifest.
#' @export
validate_geography_source_manifest <- function(source_manifest) {
  base::message("Validating pinned geography source manifest.")

  required_columns <- c(
    "source_id", "release_id", "year_min", "year_max",
    "download_url", "local_file", "sha256", "table_name"
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

  supported_sources <- endogenous_geography_source_registry()$source_id
  unknown_sources <- base::setdiff(
    base::unique(source_manifest$source_id),
    supported_sources
  )
  if (base::length(unknown_sources) > 0L) {
    base::stop(
      "Unsupported source_id values: ",
      base::paste(unknown_sources, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::anyDuplicated(source_manifest$table_name) > 0L) {
    base::stop("table_name values must be unique.", call. = FALSE)
  }
  if (base::any(source_manifest$year_min > source_manifest$year_max)) {
    base::stop("year_min cannot exceed year_max.", call. = FALSE)
  }
  if (base::any(!base::grepl("^[a-f0-9]{64}$", source_manifest$sha256))) {
    base::stop(
      "Every source must have a lowercase 64-character SHA-256.",
      call. = FALSE
    )
  }
  if (base::any(!base::grepl(
    "^[A-Za-z][A-Za-z0-9_]*$",
    source_manifest$table_name
  ))) {
    base::stop("table_name values are not safe identifiers.",
      call. = FALSE
    )
  }

  base::message(
    "Validated ", scales::comma(base::nrow(source_manifest)),
    " pinned source files."
  )
  source_manifest
}

#' Download and verify pinned geography source files
#'
#' @param source_manifest Validated source-file manifest.
#' @param overwrite Whether verified existing files may be replaced.
#' @return Manifest with observed file sizes and hashes.
#' @export
download_geography_source_files <- function(
    source_manifest,
    overwrite = FALSE) {
  source_manifest <- validate_geography_source_manifest(source_manifest)
  base::message("Downloading missing endogenous-geography source files.")

  verified_manifest <- purrr::pmap_dfr(
    source_manifest,
    function(source_id, release_id, year_min, year_max,
             download_url, local_file, sha256, table_name, ...) {
      parent_directory <- base::dirname(local_file)
      if (!base::dir.exists(parent_directory)) {
        base::dir.create(parent_directory, recursive = TRUE)
      }

      file_exists <- base::file.exists(local_file)
      existing_hash <- if (file_exists) {
        digest::digest(file = local_file, algo = "sha256")
      } else {
        NA_character_
      }
      needs_download <- !file_exists ||
        (overwrite && !base::identical(existing_hash, sha256))

      if (file_exists && !overwrite && existing_hash != sha256) {
        base::stop(
          "Existing file has the wrong hash: ", local_file,
          call. = FALSE
        )
      }
      if (needs_download) {
        base::message("Downloading ", source_id, " release ", release_id, ".")
        temporary_file <- base::paste0(local_file, ".partial")
        request <- httr2::request(download_url) |>
          httr2::req_user_agent("urpssim geography data build") |>
          httr2::req_retry(max_tries = 4L)
        response <- httr2::req_perform(
          request,
          path = temporary_file
        )
        httr2::resp_check_status(response)
        downloaded_hash <- digest::digest(
          file = temporary_file,
          algo = "sha256"
        )
        if (downloaded_hash != sha256) {
          base::unlink(temporary_file)
          base::stop(
            "SHA-256 mismatch for ", source_id, " release ",
            release_id, ".",
            call. = FALSE
          )
        }
        base::file.rename(temporary_file, local_file)
      }

      observed_hash <- digest::digest(file = local_file, algo = "sha256")
      tibble::tibble(
        source_id = source_id,
        release_id = release_id,
        year_min = year_min,
        year_max = year_max,
        download_url = download_url,
        local_file = base::normalizePath(local_file),
        sha256 = sha256,
        observed_sha256 = observed_hash,
        file_size_bytes = base::file.info(local_file)$size,
        table_name = table_name,
        verified_at = base::format(base::Sys.time(), tz = "UTC")
      )
    }
  )

  base::message(
    "Verified ", scales::comma(base::nrow(verified_manifest)),
    " geography source files."
  )
  verified_manifest
}

#' Import verified geography sources into an existing DuckDB
#'
#' @param connection Open writable DuckDB connection.
#' @param verified_manifest Return value from
#'   [download_geography_source_files()].
#' @return Import audit tibble.
#' @export
ingest_geography_sources_duckdb <- function(
    connection,
    verified_manifest) {
  base::message("Importing verified geography files into DuckDB.")

  if (!DBI::dbIsValid(connection)) {
    base::stop("connection is not a valid open DBI connection.",
      call. = FALSE
    )
  }
  verified_manifest <- validate_geography_source_manifest(
    verified_manifest
  )
  missing_files <- verified_manifest$local_file[
    !base::file.exists(verified_manifest$local_file)
  ]
  if (base::length(missing_files) > 0L) {
    base::stop(
      "Verified source files are absent: ",
      base::paste(missing_files, collapse = ", "),
      call. = FALSE
    )
  }

  DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS geography_raw")
  DBI::dbExecute(connection, "CREATE SCHEMA IF NOT EXISTS provenance")

  import_audit <- purrr::pmap_dfr(
    verified_manifest,
    function(source_id, release_id, year_min, year_max,
             download_url, local_file, sha256, table_name, ...) {
      observed_hash <- digest::digest(file = local_file, algo = "sha256")
      if (observed_hash != sha256) {
        base::stop("Hash changed before import: ", local_file,
          call. = FALSE
        )
      }

      quoted_file <- DBI::dbQuoteString(connection, local_file)
      quoted_table <- DBI::dbQuoteIdentifier(connection, table_name)
      qualified_table <- base::paste0("geography_raw.", quoted_table)
      extension <- base::tolower(tools::file_ext(local_file))
      reader <- dplyr::case_when(
        extension == "parquet" ~ base::paste0(
          "read_parquet(", quoted_file, ")"
        ),
        extension %in% c("csv", "txt", "dat") ~ base::paste0(
          "read_csv_auto(", quoted_file,
          ", header = true, sample_size = -1)"
        ),
        TRUE ~ NA_character_
      )
      if (base::is.na(reader)) {
        base::stop(
          "Extract archives before ingestion; unsupported file: ",
          local_file,
          call. = FALSE
        )
      }

      DBI::dbExecute(
        connection,
        base::paste0(
          "CREATE OR REPLACE TABLE ", qualified_table,
          " AS SELECT * FROM ", reader
        )
      )
      row_count <- DBI::dbGetQuery(
        connection,
        base::paste0(
          "SELECT COUNT(*) AS n FROM ", qualified_table
        )
      )$n[[1]]
      if (row_count == 0) {
        base::stop("Imported table is empty: ", table_name,
          call. = FALSE
        )
      }

      base::message(
        "Imported ", source_id, " as ", table_name, " with ",
        scales::comma(row_count), " rows."
      )
      tibble::tibble(
        source_id = source_id,
        release_id = release_id,
        year_min = year_min,
        year_max = year_max,
        table_name = table_name,
        row_count = base::as.numeric(row_count),
        sha256 = sha256,
        imported_at = base::format(base::Sys.time(), tz = "UTC")
      )
    }
  )

  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = "provenance", table = "geography_imports"),
    import_audit,
    overwrite = TRUE
  )
  base::message("Geography imports and provenance are complete.")
  import_audit
}

#' Build the county-year market table from normalized source tables
#'
#' @param connection Open DuckDB connection.
#' @param start_year First required year.
#' @param end_year Last required year.
#' @return County-year market tibble.
#' @export
build_endogenous_geography_market_panel <- function(
    connection,
    start_year,
    end_year) {
  base::message(
    "Building county-year geography panel for ", start_year,
    "-", end_year, "."
  )

  required_tables <- c(
    "irs_county_year", "acs_county_year", "lodes_county_year",
    "qcew_county_year", "bea_county_year", "ipeds_county_year"
  )
  present_tables <- DBI::dbListTables(connection)
  missing_tables <- base::setdiff(required_tables, present_tables)
  if (base::length(missing_tables) > 0L) {
    base::stop(
      "Normalized geography tables are missing: ",
      base::paste(missing_tables, collapse = ", "),
      call. = FALSE
    )
  }

  panel_query <- "
    CREATE OR REPLACE TABLE geography_market_county_year AS
    SELECT
      i.county_fips,
      i.year,
      i.net_exemptions AS irs_net_people,
      i.net_agi AS irs_net_agi,
      a.net_female_movers,
      a.net_female_movers_65_plus,
      l.commuting_inflow,
      l.commuting_outflow,
      l.commuting_self_containment,
      q.health_employment,
      q.health_establishments,
      q.health_average_annual_pay,
      b.personal_income_per_capita,
      b.health_employment AS bea_health_employment,
      b.real_income_index,
      p.academic_institutions,
      p.health_professions_completions,
      p.institutional_employment
    FROM irs_county_year i
    LEFT JOIN acs_county_year a USING (county_fips, year)
    LEFT JOIN lodes_county_year l USING (county_fips, year)
    LEFT JOIN qcew_county_year q USING (county_fips, year)
    LEFT JOIN bea_county_year b USING (county_fips, year)
    LEFT JOIN ipeds_county_year p USING (county_fips, year)
    WHERE i.year BETWEEN ? AND ?
  "
  DBI::dbExecute(
    connection,
    panel_query,
    params = base::list(start_year, end_year)
  )

  panel_count <- DBI::dbGetQuery(
    connection,
    "SELECT COUNT(*) AS n FROM geography_market_county_year"
  )$n[[1]]
  if (panel_count == 0) {
    base::stop("County-year geography panel is empty.", call. = FALSE)
  }
  base::message(
    "Built geography market panel with ",
    scales::comma(panel_count), " county-years."
  )

  DBI::dbGetQuery(
    connection,
    "SELECT * FROM geography_market_county_year"
  ) |>
    tibble::as_tibble()
}
