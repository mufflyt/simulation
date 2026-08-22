#' Open the policy and migration evidence database
#'
#' Opens a DuckDB used to stage public policy and migration evidence. The
#' database stores normalized analytic tables and a source manifest. Existing
#' databases are preserved unless `overwrite` is explicitly requested.
#'
#' @param duckdb_path Destination DuckDB path.
#' @param overwrite Whether to replace an existing database.
#'
#' @return An open DuckDB connection. The caller must disconnect it.
#' @export
open_policy_migration_duckdb <- function(
    duckdb_path,
    overwrite = FALSE) {
  base::stopifnot(
    base::is.character(duckdb_path),
    base::length(duckdb_path) == 1L,
    !base::is.na(duckdb_path),
    base::nzchar(duckdb_path)
  )
  base::message("Policy evidence DuckDB path: ", duckdb_path)
  if (base::file.exists(duckdb_path) && base::isTRUE(overwrite)) {
    base::message("Removing existing policy evidence DuckDB.")
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
    source_vintage = character(),
    source_url = character(),
    local_path = character(),
    file_md5 = character(),
    row_count = integer(),
    model_use_allowed = logical(),
    ingested_at_utc = character()
  )
  if (!DBI::dbExistsTable(connection, "policy_source_manifest")) {
    DBI::dbWriteTable(
      connection,
      "policy_source_manifest",
      manifest,
      overwrite = TRUE
    )
  }
  base::message("Opened policy evidence DuckDB.")
  connection
}

#' Catalog public policy and migration evidence sources
#'
#' @return A tibble documenting source landing pages and ingestion policy.
#' @export
policy_migration_source_catalog <- function() {
  tibble::tribble(
    ~source_id, ~official_url, ~grain, ~automatic_download,
    ~model_use_default,
    "acs_pums",
    "https://www.census.gov/programs-surveys/acs/microdata/access.html",
    "person-year", FALSE, TRUE,
    "acs_migration_flows",
    paste0(
      "https://www.census.gov/data/developers/data-sets/",
      "acs-migration-flows.html"
    ),
    "origin-destination-year", FALSE, TRUE,
    "irs_soi_migration",
    "https://www.irs.gov/statistics/soi-tax-stats-migration-data",
    "origin-destination-year", FALSE, TRUE,
    "nppes_snapshot",
    "https://download.cms.gov/nppes/NPI_Files.html",
    "npi-snapshot", FALSE, TRUE,
    "lawatlas_reproductive_policy",
    "https://lawatlas.org/USabortionlaw",
    "state-effective-date-domain", FALSE, TRUE,
    "nrmp_urps_aggregate",
    paste0(
      "https://www.nrmp.org/fellowship-applicants/",
      "participating-fellowships/urogyn/"
    ),
    "national-appointment-year", FALSE, FALSE
  )
}

#' Download one explicitly selected public source file
#'
#' The source catalog intentionally does not guess changing bulk-file URLs.
#' Callers supply the exact official file URL selected from the landing page.
#'
#' @param source_url Exact HTTPS file URL.
#' @param destination Local destination path.
#' @param overwrite Whether an existing file may be replaced.
#'
#' @return The normalized destination path.
#' @export
download_policy_migration_file <- function(
    source_url,
    destination,
    overwrite = FALSE) {
  if (!stringr::str_detect(source_url, "^https://")) {
    base::stop("source_url must use HTTPS.", call. = FALSE)
  }
  if (base::file.exists(destination) && !base::isTRUE(overwrite)) {
    base::message("Using existing downloaded file: ", destination)
    return(base::normalizePath(destination, mustWork = TRUE))
  }
  parent <- base::dirname(destination)
  if (!base::dir.exists(parent)) {
    base::dir.create(parent, recursive = TRUE)
  }
  temporary <- base::paste0(destination, ".partial")
  base::message("Downloading official source to: ", destination)
  request <- httr2::request(source_url) |>
    httr2::req_retry(max_tries = 4L)
  httr2::req_perform(request, path = temporary)
  if (!base::file.exists(temporary) || base::file.info(temporary)$size <= 0) {
    base::stop("Downloaded file is empty.", call. = FALSE)
  }
  if (!base::file.rename(temporary, destination)) {
    base::stop("Could not atomically finalize download.", call. = FALSE)
  }
  base::message(
    "Downloaded ",
    scales::comma(base::file.info(destination)$size),
    " bytes."
  )
  base::normalizePath(destination, mustWork = TRUE)
}

.policy_require_columns <- function(table, required, source_name) {
  missing_columns <- base::setdiff(required, base::names(table))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      source_name,
      " is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  base::invisible(TRUE)
}

.policy_add_columns <- function(table, defaults) {
  for (column_name in base::names(defaults)) {
    if (!column_name %in% base::names(table)) {
      table[[column_name]] <- defaults[[column_name]]
    }
  }
  table
}

.policy_record_source <- function(
    connection,
    source_id,
    source_vintage,
    source_url,
    local_path,
    row_count,
    model_use_allowed) {
  checksum <- if (base::file.exists(local_path)) {
    base::unname(tools::md5sum(local_path))
  } else {
    NA_character_
  }
  entry <- tibble::tibble(
    source_id = source_id,
    source_vintage = base::as.character(source_vintage),
    source_url = source_url,
    local_path = local_path,
    file_md5 = checksum,
    row_count = base::as.integer(row_count),
    model_use_allowed = base::as.logical(model_use_allowed),
    ingested_at_utc = base::format(
      base::Sys.time(),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
  )
  DBI::dbAppendTable(connection, "policy_source_manifest", entry)
  base::message(
    "Recorded source ", source_id, " with ",
    scales::comma(row_count), " rows."
  )
  base::invisible(entry)
}

.policy_replace_table <- function(connection, table_name, table) {
  DBI::dbWriteTable(
    connection,
    table_name,
    tibble::as_tibble(table),
    overwrite = TRUE
  )
  base::message(
    "Wrote DuckDB table ", table_name, " with ",
    scales::comma(base::nrow(table)), " rows."
  )
  base::invisible(table_name)
}

.policy_upsert_table <- function(
    connection,
    table_name,
    table,
    keys) {
  incoming <- tibble::as_tibble(table)
  if (DBI::dbExistsTable(connection, table_name)) {
    existing <- dplyr::collect(dplyr::tbl(connection, table_name))
    retained <- dplyr::anti_join(existing, incoming, by = keys)
    incoming <- dplyr::bind_rows(retained, incoming)
  }
  .policy_replace_table(connection, table_name, incoming)
  base::invisible(table_name)
}

#' Ingest ACS PUMS migration microdata
#'
#' Reduces ACS person microdata to state-year, age-band, sex, and prior-state
#' weighted migration flows. Raw person records are not retained.
#'
#' @param connection Open policy DuckDB connection.
#' @param pums Person-level ACS PUMS table.
#' @param year ACS year.
#' @param source_path Optional downloaded file path for provenance.
#' @param source_url Official source URL.
#'
#' @return The normalized migration table, invisibly.
#' @export
ingest_acs_pums_migration <- function(
    connection,
    pums,
    year,
    source_path = NA_character_,
    source_url = paste0(
      "https://www.census.gov/programs-surveys/acs/microdata.html"
    )) {
  required <- c("AGEP", "SEX", "ST", "MIGSP", "PWGTP")
  .policy_require_columns(pums, required, "ACS PUMS")
  base::message("Aggregating ACS PUMS migration for ", year, ".")
  normalized <- pums |>
    dplyr::transmute(
      year = base::as.integer(year),
      destination_state_fips = stringr::str_pad(
        base::as.character(.data$ST), 2L, pad = "0"
      ),
      origin_state_fips = stringr::str_pad(
        base::as.character(.data$MIGSP), 2L, pad = "0"
      ),
      age = base::as.integer(.data$AGEP),
      sex = dplyr::case_when(
        base::as.integer(.data$SEX) == 2L ~ "female",
        base::as.integer(.data$SEX) == 1L ~ "male",
        TRUE ~ "unknown"
      ),
      person_weight = base::as.numeric(.data$PWGTP)
    ) |>
    dplyr::filter(
      !base::is.na(.data$age),
      !base::is.na(.data$person_weight),
      .data$person_weight >= 0
    ) |>
    dplyr::mutate(
      age_band = dplyr::case_when(
        .data$age < 45L ~ "00-44",
        .data$age < 55L ~ "45-54",
        .data$age < 65L ~ "55-64",
        .data$age < 75L ~ "65-74",
        TRUE ~ "75+"
      ),
      moved_interstate = !base::is.na(.data$origin_state_fips) &
        .data$origin_state_fips != .data$destination_state_fips &
        .data$origin_state_fips != "00"
    ) |>
    dplyr::group_by(
      .data$year,
      .data$destination_state_fips,
      .data$origin_state_fips,
      .data$sex,
      .data$age_band,
      .data$moved_interstate
    ) |>
    dplyr::summarise(
      weighted_people = base::sum(.data$person_weight),
      unweighted_records = dplyr::n(),
      .groups = "drop"
    )
  .policy_upsert_table(
    connection,
    "acs_pums_migration",
    normalized,
    c(
      "year", "destination_state_fips", "origin_state_fips", "sex",
      "age_band", "moved_interstate"
    )
  )
  .policy_record_source(
    connection,
    "acs_pums",
    year,
    source_url,
    source_path,
    base::nrow(normalized),
    TRUE
  )
  base::invisible(normalized)
}

#' Ingest Census migration flow estimates
#'
#' @param connection Open policy DuckDB connection.
#' @param flows Origin-destination flow table.
#' @param source_path Optional source file path.
#' @param source_url Official source URL.
#'
#' @return Normalized flow table, invisibly.
#' @export
ingest_acs_migration_flows <- function(
    connection,
    flows,
    source_path = NA_character_,
    source_url = paste0(
      "https://www.census.gov/data/developers/data-sets/",
      "acs-migration-flows.html"
    )) {
  required <- c(
    "year", "origin_state_fips", "destination_state_fips", "flow"
  )
  .policy_require_columns(flows, required, "ACS migration flows")
  flows <- .policy_add_columns(
    flows,
    list(margin_of_error = NA_real_)
  )
  normalized <- flows |>
    dplyr::transmute(
      year = base::as.integer(.data$year),
      origin_state_fips = stringr::str_pad(
        base::as.character(.data$origin_state_fips), 2L, pad = "0"
      ),
      destination_state_fips = stringr::str_pad(
        base::as.character(.data$destination_state_fips), 2L, pad = "0"
      ),
      flow = base::as.numeric(.data$flow),
      margin_of_error = dplyr::coalesce(
        base::as.numeric(.data$margin_of_error),
        NA_real_
      )
    ) |>
    dplyr::filter(!base::is.na(.data$flow), .data$flow >= 0)
  .policy_upsert_table(
    connection,
    "acs_state_migration_flows",
    normalized,
    c("year", "origin_state_fips", "destination_state_fips")
  )
  .policy_record_source(
    connection,
    "acs_migration_flows",
    base::paste(base::range(normalized$year), collapse = "-"),
    source_url,
    source_path,
    base::nrow(normalized),
    TRUE
  )
  base::invisible(normalized)
}

#' Ingest IRS migration validation flows
#'
#' @param connection Open policy DuckDB connection.
#' @param flows IRS state or county origin-destination table.
#' @param source_path Optional downloaded file path.
#' @param source_url Official IRS migration page.
#'
#' @return Normalized IRS flow table, invisibly.
#' @export
ingest_irs_migration <- function(
    connection,
    flows,
    source_path = NA_character_,
    source_url = paste0(
      "https://www.irs.gov/statistics/soi-tax-stats-migration-data"
    )) {
  required <- c(
    "year", "origin_state_fips", "destination_state_fips",
    "returns", "exemptions", "adjusted_gross_income"
  )
  .policy_require_columns(flows, required, "IRS migration")
  has_county <- all(c("origin_county_fips", "destination_county_fips") %in% base::names(flows))
  if (has_county) {
    normalized <- flows |>
      dplyr::transmute(
        year = base::as.integer(.data$year),
        origin_state_fips = stringr::str_pad(
          base::as.character(.data$origin_state_fips), 2L, pad = "0"
        ),
        destination_state_fips = stringr::str_pad(
          base::as.character(.data$destination_state_fips), 2L, pad = "0"
        ),
        origin_county_fips = stringr::str_pad(
          base::as.character(.data$origin_county_fips), 5L, pad = "0"
        ),
        destination_county_fips = stringr::str_pad(
          base::as.character(.data$destination_county_fips), 5L, pad = "0"
        ),
        returns = base::as.numeric(.data$returns),
        exemptions = base::as.numeric(.data$exemptions),
        adjusted_gross_income = base::as.numeric(
          .data$adjusted_gross_income
        )
      ) |>
      dplyr::filter(
        !base::is.na(.data$returns),
        .data$returns >= 0,
        .data$exemptions >= 0
      )
    key_cols <- c("year", "origin_county_fips", "destination_county_fips")
  } else {
    normalized <- flows |>
      dplyr::transmute(
        year = base::as.integer(.data$year),
        origin_state_fips = stringr::str_pad(
          base::as.character(.data$origin_state_fips), 2L, pad = "0"
        ),
        destination_state_fips = stringr::str_pad(
          base::as.character(.data$destination_state_fips), 2L, pad = "0"
        ),
        returns = base::as.numeric(.data$returns),
        exemptions = base::as.numeric(.data$exemptions),
        adjusted_gross_income = base::as.numeric(
          .data$adjusted_gross_income
        )
      ) |>
      dplyr::filter(
        !base::is.na(.data$returns),
        .data$returns >= 0,
        .data$exemptions >= 0
      )
    key_cols <- c("year", "origin_state_fips", "destination_state_fips")
  }
  .policy_upsert_table(
    connection,
    "irs_migration_flows",
    normalized,
    key_cols
  )
  .policy_record_source(
    connection,
    "irs_soi_migration",
    base::paste(base::range(normalized$year), collapse = "-"),
    source_url,
    source_path,
    base::nrow(normalized),
    TRUE
  )
  base::invisible(normalized)
}

#' Ingest a historical NPPES provider snapshot
#'
#' @param connection Open policy DuckDB connection.
#' @param providers NPPES provider table.
#' @param snapshot_date Snapshot date.
#' @param source_path Optional source file path.
#' @param source_url Official NPPES download page.
#' @param cohort_npi Optional validated URPS NPI cohort.
#' @param taxonomy_codes NPPES FPMRS taxonomy codes.
#'
#' @return Deduplicated provider-location snapshot, invisibly.
#' @export
ingest_nppes_snapshot <- function(
    connection,
    providers,
    snapshot_date,
    source_path = NA_character_,
    source_url = paste0(
      "https://download.cms.gov/nppes/NPI_Files.html"
    ),
    cohort_npi = NULL,
    taxonomy_codes = c("207VF0040X", "2088F0040X")) {
  required <- c("npi", "practice_state", "practice_postal_code")
  .policy_require_columns(providers, required, "NPPES snapshot")
  providers <- .policy_add_columns(
    providers,
    list(taxonomy = NA_character_, last_update_date = base::as.Date(NA))
  )
  snapshot <- providers |>
    dplyr::transmute(
      npi = base::as.character(.data$npi),
      snapshot_date = base::as.Date(snapshot_date),
      practice_state = stringr::str_to_upper(
        base::as.character(.data$practice_state)
      ),
      practice_postal_code = stringr::str_sub(
        stringr::str_replace_all(
          base::as.character(.data$practice_postal_code),
          "[^0-9]",
          ""
        ),
        1L,
        5L
      ),
      taxonomy = dplyr::coalesce(
        base::as.character(.data$taxonomy),
        NA_character_
      ),
      last_update_date = base::as.Date(.data$last_update_date)
    ) |>
    dplyr::filter(
      stringr::str_detect(.data$npi, "^[0-9]{10}$"),
      stringr::str_detect(.data$practice_state, "^[A-Z]{2}$")
    ) |>
    dplyr::distinct(
      .data$npi,
      .data$snapshot_date,
      .keep_all = TRUE
    )
  if (!base::is.null(cohort_npi)) {
    snapshot <- snapshot |>
      dplyr::filter(.data$npi %in% base::as.character(cohort_npi))
    base::message("Restricted NPPES snapshot to validated URPS cohort.")
  } else {
    snapshot <- snapshot |>
      dplyr::filter(.data$taxonomy %in% taxonomy_codes)
    base::message("Restricted NPPES snapshot to FPMRS taxonomies.")
  }
  existing <- if (DBI::dbExistsTable(connection, "nppes_snapshots")) {
    dplyr::collect(dplyr::tbl(connection, "nppes_snapshots"))
  } else {
    tibble::tibble()
  }
  combined <- dplyr::bind_rows(existing, snapshot) |>
    dplyr::arrange(.data$npi, .data$snapshot_date) |>
    dplyr::distinct(
      .data$npi,
      .data$snapshot_date,
      .keep_all = TRUE
    )
  .policy_replace_table(connection, "nppes_snapshots", combined)
  .policy_record_source(
    connection,
    "nppes_snapshot",
    base::as.character(snapshot_date),
    source_url,
    source_path,
    base::nrow(snapshot),
    TRUE
  )
  base::invisible(snapshot)
}

#' Ingest longitudinal state reproductive-health policies
#'
#' @param connection Open policy DuckDB connection.
#' @param policies State policy records with effective dates.
#' @param source_path Optional downloaded file path.
#' @param source_url LawAtlas source URL.
#'
#' @return Normalized policy table, invisibly.
#' @export
ingest_lawatlas_policies <- function(
    connection,
    policies,
    source_path = NA_character_,
    source_url = "https://lawatlas.org/USabortionlaw") {
  required <- c(
    "state", "effective_date", "policy_domain", "policy_value"
  )
  .policy_require_columns(policies, required, "LawAtlas policy data")
  policies <- .policy_add_columns(
    policies,
    list(end_date = base::as.Date(NA), enforceable = TRUE)
  )
  normalized <- policies |>
    dplyr::transmute(
      state = stringr::str_to_upper(base::as.character(.data$state)),
      effective_date = base::as.Date(.data$effective_date),
      end_date = base::as.Date(.data$end_date),
      policy_domain = stringr::str_to_lower(
        base::as.character(.data$policy_domain)
      ),
      policy_value = base::as.numeric(.data$policy_value),
      enforceable = dplyr::coalesce(
        base::as.logical(.data$enforceable),
        TRUE
      )
    ) |>
    dplyr::filter(
      stringr::str_detect(.data$state, "^[A-Z]{2}$"),
      !base::is.na(.data$effective_date),
      !base::is.na(.data$policy_value)
    )
  .policy_upsert_table(
    connection,
    "lawatlas_state_policies",
    normalized,
    c("state", "effective_date", "policy_domain")
  )
  .policy_record_source(
    connection,
    "lawatlas_reproductive_policy",
    base::paste(
      base::range(normalized$effective_date),
      collapse = "-"
    ),
    source_url,
    source_path,
    base::nrow(normalized),
    TRUE
  )
  base::invisible(normalized)
}

#' Ingest an aggregate URPS match series with permission controls
#'
#' Newer NRMP reports may prohibit algorithmic or machine-learning use without
#' written permission. Rows are stored for descriptive provenance but are
#' excluded from model-ready views unless `model_use_allowed` is explicitly
#' true based on the applicable license or written authorization.
#'
#' @param connection Open policy DuckDB connection.
#' @param match_series Aggregate URPS match table.
#' @param source_path Source file path.
#' @param model_use_allowed Documented permission for model fitting.
#' @param permission_reference Citation or authorization reference.
#'
#' @return Normalized match series, invisibly.
#' @export
ingest_nrmp_urps_series <- function(
    connection,
    match_series,
    source_path = NA_character_,
    model_use_allowed = FALSE,
    permission_reference = NA_character_) {
  required <- c("year", "positions", "filled")
  .policy_require_columns(match_series, required, "NRMP URPS series")
  match_series <- .policy_add_columns(
    match_series,
    list(applicants = NA_integer_)
  )
  if (base::isTRUE(model_use_allowed) &&
      (base::is.na(permission_reference) ||
        !base::nzchar(permission_reference))) {
    base::stop(
      "A permission_reference is required for NRMP model use.",
      call. = FALSE
    )
  }
  normalized <- match_series |>
    dplyr::transmute(
      year = base::as.integer(.data$year),
      positions = base::as.integer(.data$positions),
      filled = base::as.integer(.data$filled),
      applicants = base::as.integer(.data$applicants),
      fill_rate = dplyr::if_else(
        .data$positions > 0L,
        .data$filled / .data$positions,
        NA_real_
      ),
      model_use_allowed = base::isTRUE(model_use_allowed),
      permission_reference = permission_reference
    ) |>
    dplyr::filter(
      !base::is.na(.data$year),
      .data$positions >= 0L,
      .data$filled >= 0L,
      .data$filled <= .data$positions
    )
  .policy_replace_table(connection, "nrmp_urps_aggregate", normalized)
  model_ready <- normalized |>
    dplyr::filter(.data$model_use_allowed)
  .policy_replace_table(
    connection,
    "nrmp_urps_model_ready",
    model_ready
  )
  .policy_record_source(
    connection,
    "nrmp_urps_aggregate",
    base::paste(base::range(normalized$year), collapse = "-"),
    base::paste0(
      "https://www.nrmp.org/fellowship-applicants/",
      "participating-fellowships/urogyn/"
    ),
    source_path,
    base::nrow(normalized),
    model_use_allowed
  )
  if (!base::isTRUE(model_use_allowed)) {
    base::message(
      "Stored NRMP records for description only; model-ready rows: 0."
    )
  }
  base::invisible(normalized)
}

#' Ingest the repository-held URPS entrant series
#'
#' @param connection Open policy DuckDB connection.
#' @param source_path Existing reconciled repository CSV.
#' @param model_use_allowed Whether documented terms permit model fitting.
#' @param permission_reference Permission or provenance reference.
#'
#' @return Normalized series, invisibly.
#' @export
ingest_repository_nrmp_series <- function(
    connection,
    source_path = base::file.path(
      "data-raw", "calibration", "nrmp_urps_entrants_series.csv"
    ),
    model_use_allowed = FALSE,
    permission_reference = NA_character_) {
  if (!base::file.exists(source_path)) {
    base::stop("Repository NRMP series does not exist.", call. = FALSE)
  }
  base::message("Reading repository NRMP series: ", source_path)
  repository_series <- readr::read_csv(
    source_path,
    show_col_types = FALSE
  )
  .policy_require_columns(
    repository_series,
    c("appointment_year", "positions_offered", "positions_filled"),
    "Repository NRMP series"
  )
  normalized <- repository_series |>
    dplyr::transmute(
      year = base::as.integer(.data$appointment_year),
      positions = base::as.integer(.data$positions_offered),
      filled = base::as.integer(.data$positions_filled),
      applicants = NA_integer_
    )
  ingest_nrmp_urps_series(
    connection,
    normalized,
    source_path = source_path,
    model_use_allowed = model_use_allowed,
    permission_reference = permission_reference
  )
}
