# Free adequacy evidence in DuckDB -------------------------------------------

#' Registry of free external adequacy sources
#'
#' Sources 16 and 20 reuse the repository's existing BRFSS and ACS PUMS
#' acquisition paths. State fee schedules and licensing rosters are deliberately
#' manifest-driven because no authoritative national bulk endpoint exists.
#'
#' @return A tibble describing source, grain, role, and acquisition route.
#' @export
adequacy_free_source_registry <- function() {
  tibble::tribble(
    ~source_number, ~source_id, ~table_name, ~grain, ~model_role,
    ~acquisition, ~catalog_url, ~title_pattern,
    6L, "medicaid_mcpar", "raw_medicaid_mcpar", "state-plan-year",
    "Medicaid managed-care access", "data_json",
    "https://data.medicaid.gov/data.json",
    "Managed Care Program Annual Report|MCPAR",
    7L, "medicaid_naaar", "raw_medicaid_naaar", "state-plan-year",
    "Medicaid network adequacy", "data_json",
    "https://data.medicaid.gov/data.json",
    "Network Adequacy and Access Assurance|NAAAR",
    8L, "medicaid_fees", "raw_medicaid_fees", "state-cpt-year",
    "Medicaid fee ratio", "manifest", NA_character_, NA_character_,
    9L, "medicaid_enrollment", "raw_medicaid_enrollment", "state-month",
    "Population exposed to Medicaid barriers", "data_json",
    "https://data.medicaid.gov/data.json",
    "Performance Indicator|Medicaid.*Enrollment",
    10L, "cms_pos", "raw_cms_pos", "facility-quarter",
    "Facility availability", "data_json",
    "https://data.cms.gov/data.json",
    "Provider of Services File.*QIES",
    11L, "cms_hospital_owners", "raw_cms_hospital_owners",
    "hospital-owner-month", "Consolidation and ownership", "data_json",
    "https://data.cms.gov/data.json", "Hospital All Owners",
    12L, "cms_hcris", "raw_cms_hcris", "hospital-year",
    "Hospital capacity and financial viability", "manifest",
    NA_character_, NA_character_,
    14L, "nhis_access", "raw_nhis_access", "person-year",
    "Patient-reported delayed or forgone care", "manifest",
    NA_character_, NA_character_,
    16L, "brfss_access", "raw_brfss_access", "person-year",
    "Cost-related access and health burden", "existing",
    NA_character_, NA_character_,
    20L, "acs_pums", "raw_acs_pums", "person-year",
    "Insurance and transportation vulnerability", "existing",
    NA_character_, NA_character_,
    21L, "census_pulse_access", "raw_census_pulse_access",
    "person-wave", "Timely delayed-care pressure", "manifest",
    NA_character_, NA_character_,
    25L, "state_license", "raw_state_license", "provider-state-date",
    "Active-license corroboration", "manifest",
    NA_character_, NA_character_
  ) |>
    dplyr::mutate(
      source_status = dplyr::case_when(
        .data$acquisition == "existing" ~ "reuse_if_present",
        .data$acquisition == "data_json" ~ "public_catalog",
        TRUE ~ "manifest_required"
      ),
      absence_semantics = "missing_not_zero"
    )
}

#' Resolve a DCAT data.json source to a current downloadable resource
#'
#' @param catalog_url Official data.json catalog.
#' @param title_pattern Case-insensitive regular expression for dataset title.
#' @param preferred_formats Preferred resource formats in order.
#' @return One-row tibble with title, modified time, URL, and media type.
#' @export
resolve_public_catalog_resource <- function(
    catalog_url,
    title_pattern,
    preferred_formats = c("csv", "zip", "xlsx", "json")) {
  base::message("Reading official catalog: ", catalog_url)
  request <- httr2::request(catalog_url) |>
    httr2::req_user_agent("urpssim adequacy evidence acquisition") |>
    httr2::req_retry(max_tries = 3L) |>
    httr2::req_perform()
  payload <- httr2::resp_body_json(request, simplifyVector = FALSE)
  datasets <- payload$dataset
  if (base::is.null(datasets)) {
    base::stop("Catalog has no `dataset` collection: ", catalog_url,
               call. = FALSE)
  }
  matching <- base::Filter(function(item) {
    base::isTRUE(stringr::str_detect(
      item$title %||% "",
      stringr::regex(title_pattern, ignore_case = TRUE)
    ))
  }, datasets)
  if (base::length(matching) == 0L) {
    base::stop("No catalog dataset matched: ", title_pattern,
               call. = FALSE)
  }
  modified <- base::vapply(
    matching,
    function(item) item$modified %||% item$issued %||% "",
    FUN.VALUE = base::character(1)
  )
  selected <- matching[[base::order(modified, decreasing = TRUE)[[1L]]]]
  distributions <- selected$distribution
  if (base::is.null(distributions)) {
    base::stop("Matched dataset has no distributions: ", selected$title,
               call. = FALSE)
  }
  resource_tbl <- purrr::map_dfr(distributions, function(resource) {
    resource_url <- resource$downloadURL %||%
      resource$accessURL %||% NA_character_
    media_type <- base::tolower(resource$mediaType %||% resource$format %||% "")
    extension <- stringr::str_to_lower(
      tools::file_ext(base::sub("[?].*$", "", resource_url))
    )
    tibble::tibble(
      resource_url = resource_url,
      media_type = media_type,
      extension = extension
    )
  }) |>
    dplyr::filter(!base::is.na(.data$resource_url)) |>
    dplyr::mutate(
      preference = base::match(.data$extension, preferred_formats),
      preference = dplyr::if_else(
        base::is.na(.data$preference),
        base::length(preferred_formats) + 1L,
        .data$preference
      )
    ) |>
    dplyr::arrange(.data$preference)
  if (base::nrow(resource_tbl) == 0L) {
    base::stop("Matched dataset has no downloadable resource: ",
               selected$title, call. = FALSE)
  }
  resource_tbl |>
    dplyr::slice(1L) |>
    dplyr::transmute(
      dataset_title = selected$title,
      dataset_modified = selected$modified %||%
        selected$issued %||% NA_character_,
      resource_url = .data$resource_url,
      media_type = .data$media_type,
      extension = .data$extension
    )
}

#' Download one public source with a timestamped provenance record
#'
#' @param resource_url Direct official download URL.
#' @param source_id Stable registry identifier.
#' @param cache_dir Download-cache directory.
#' @param expected_sha256 Optional expected SHA-256.
#' @return One-row provenance tibble.
#' @export
download_adequacy_source <- function(
    resource_url,
    source_id,
    cache_dir = base::file.path("data-raw", "adequacy_sources"),
    expected_sha256 = NULL) {
  base::dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  extension <- tools::file_ext(base::sub("[?].*$", "", resource_url))
  if (!base::nzchar(extension)) extension <- "bin"
  stamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  destination <- base::file.path(
    cache_dir,
    base::paste0(source_id, "_", stamp, ".", extension)
  )
  base::message("Downloading ", source_id, " to: ", destination)
  response <- httr2::request(resource_url) |>
    httr2::req_user_agent("urpssim adequacy evidence acquisition") |>
    httr2::req_retry(max_tries = 3L) |>
    httr2::req_perform(path = destination)
  status <- httr2::resp_status(response)
  if (status < 200L || status >= 300L || !base::file.exists(destination)) {
    base::stop("Download failed for ", source_id, call. = FALSE)
  }
  sha256 <- digest::digest(file = destination, algo = "sha256")
  if (!base::is.null(expected_sha256) &&
      !base::identical(sha256, expected_sha256)) {
    base::stop("SHA-256 mismatch for ", source_id, call. = FALSE)
  }
  file_info <- base::file.info(destination)
  provenance_tbl <- tibble::tibble(
    source_id = source_id,
    source_url = resource_url,
    local_path = base::normalizePath(destination, mustWork = TRUE),
    retrieved_at_utc = base::format(
      base::Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
    ),
    file_size_bytes = base::as.numeric(file_info$size),
    sha256 = sha256,
    status = "downloaded"
  )
  base::message(
    "Downloaded ", source_id, ": ",
    base::format(provenance_tbl$file_size_bytes, big.mark = ","),
    " bytes; SHA-256 ", sha256
  )
  provenance_tbl
}

#' Ingest an external file into the adequacy DuckDB
#'
#' @param con Writable DuckDB connection.
#' @param path Existing CSV, TSV, Parquet, Excel, RDS, or RData file.
#' @param table_name Destination table.
#' @param overwrite Whether to replace an existing staging table.
#' @return One-row ingestion audit tibble.
#' @export
ingest_adequacy_file <- function(
    con,
    path,
    table_name,
    overwrite = FALSE) {
  if (!base::file.exists(path)) {
    base::stop("Source file does not exist: ", path, call. = FALSE)
  }
  if (!base::grepl("^[A-Za-z][A-Za-z0-9_]*$", table_name)) {
    base::stop("Unsafe DuckDB table name: ", table_name, call. = FALSE)
  }
  if (DBI::dbExistsTable(con, table_name) && !base::isTRUE(overwrite)) {
    row_count <- DBI::dbGetQuery(
      con,
      base::paste0("SELECT COUNT(*) AS n FROM ", table_name)
    )$n[[1L]]
    base::message("Reusing DuckDB table ", table_name, " with ",
                  base::format(row_count, big.mark = ","), " rows.")
    return(tibble::tibble(
      table_name = table_name,
      local_path = base::normalizePath(path, mustWork = TRUE),
      row_count = base::as.numeric(row_count),
      status = "reused"
    ))
  }
  extension <- base::tolower(tools::file_ext(path))
  base::message("Reading ", path, " into DuckDB table ", table_name, ".")
  source_tbl <- switch(
    extension,
    csv = readr::read_csv(path, show_col_types = FALSE, progress = FALSE),
    tsv = readr::read_tsv(path, show_col_types = FALSE, progress = FALSE),
    parquet = arrow::read_parquet(path),
    xlsx = readxl::read_excel(path),
    xls = readxl::read_excel(path),
    rds = base::readRDS(path),
    rdata = {
      loaded_names <- base::load(path, envir = temporary_env <- base::new.env())
      if (base::length(loaded_names) != 1L) {
        base::stop("RData must contain exactly one object: ", path,
                   call. = FALSE)
      }
      temporary_env[[loaded_names[[1L]]]]
    },
    base::stop("Unsupported ingestion extension: ", extension,
               call. = FALSE)
  )
  source_tbl <- tibble::as_tibble(source_tbl)
  if (base::nrow(source_tbl) == 0L) {
    base::stop("Refusing to ingest an empty source: ", path,
               call. = FALSE)
  }
  DBI::dbWriteTable(
    con,
    table_name,
    source_tbl,
    overwrite = overwrite,
    temporary = FALSE
  )
  row_count <- DBI::dbGetQuery(
    con,
    base::paste0("SELECT COUNT(*) AS n FROM ", table_name)
  )$n[[1L]]
  base::message("Ingested ", base::format(row_count, big.mark = ","),
                " rows into ", table_name, ".")
  tibble::tibble(
    table_name = table_name,
    local_path = base::normalizePath(path, mustWork = TRUE),
    row_count = base::as.numeric(row_count),
    status = "ingested"
  )
}

#' Load selected free adequacy sources into DuckDB
#'
#' @param db_path Existing or new DuckDB path. Parent directory must exist.
#' @param source_manifest Optional tibble with `source_id` and `local_path`.
#' @param download_catalog_sources Whether to resolve and download DCAT sources.
#' @param cache_dir Download-cache directory.
#' @param overwrite Whether to replace existing source tables.
#' @param strict Whether every selected source must be present.
#' @return A list containing the ingestion audit and database path.
#' @export
load_adequacy_sources_duckdb <- function(
    db_path,
    source_manifest = NULL,
    download_catalog_sources = TRUE,
    cache_dir = base::file.path("data-raw", "adequacy_sources"),
    overwrite = FALSE,
    strict = FALSE) {
  parent_dir <- base::dirname(db_path)
  if (!base::dir.exists(parent_dir)) {
    base::stop("DuckDB parent directory does not exist: ", parent_dir,
               call. = FALSE)
  }
  registry_tbl <- adequacy_free_source_registry()
  if (base::is.null(source_manifest)) {
    source_manifest <- tibble::tibble(
      source_id = base::character(),
      local_path = base::character()
    )
  }
  required_manifest_cols <- c("source_id", "local_path")
  if (!base::all(required_manifest_cols %in% base::names(source_manifest))) {
    base::stop("`source_manifest` requires source_id and local_path.",
               call. = FALSE)
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path,
                        read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "adequacy_source_registry", registry_tbl,
                    overwrite = TRUE)

  audit_rows <- base::list()
  for (row_index in base::seq_len(base::nrow(registry_tbl))) {
    specification <- registry_tbl[row_index, , drop = FALSE]
    source_id <- specification$source_id[[1L]]
    table_name <- specification$table_name[[1L]]
    manifest_row <- source_manifest |>
      dplyr::filter(.data$source_id == source_id)
    local_path <- if (base::nrow(manifest_row) == 1L) {
      manifest_row$local_path[[1L]]
    } else {
      NA_character_
    }

    if ((base::is.na(local_path) || !base::file.exists(local_path)) &&
        base::isTRUE(download_catalog_sources) &&
        specification$acquisition[[1L]] == "data_json") {
      catalog_hit <- base::tryCatch(
        resolve_public_catalog_resource(
          specification$catalog_url[[1L]],
          specification$title_pattern[[1L]]
        ),
        error = function(condition) {
          base::message("Catalog resolution unavailable for ", source_id,
                        ": ", base::conditionMessage(condition))
          NULL
        }
      )
      if (!base::is.null(catalog_hit)) {
        download_record <- download_adequacy_source(
          catalog_hit$resource_url[[1L]],
          source_id,
          cache_dir = cache_dir
        )
        local_path <- download_record$local_path[[1L]]
      }
    }

    if (!base::is.na(local_path) && base::file.exists(local_path)) {
      audit_rows[[source_id]] <- ingest_adequacy_file(
        con,
        local_path,
        table_name,
        overwrite = overwrite
      ) |>
        dplyr::mutate(source_id = source_id, .before = 1L)
    } else {
      base::message("Source unavailable; recording missing, not zero: ",
                    source_id)
      audit_rows[[source_id]] <- tibble::tibble(
        source_id = source_id,
        table_name = table_name,
        local_path = NA_character_,
        row_count = NA_real_,
        status = "missing"
      )
    }
  }
  audit_tbl <- dplyr::bind_rows(audit_rows) |>
    dplyr::left_join(
      registry_tbl |>
        dplyr::select("source_id", "model_role", "absence_semantics"),
      by = "source_id"
    ) |>
    dplyr::mutate(
      ingested_at_utc = base::format(
        base::Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
      )
    )
  DBI::dbWriteTable(con, "adequacy_source_ingest_audit", audit_tbl,
                    overwrite = TRUE)
  missing_ids <- audit_tbl$source_id[audit_tbl$status == "missing"]
  if (base::isTRUE(strict) && base::length(missing_ids) > 0L) {
    base::stop("Required adequacy sources are missing: ",
               base::paste(missing_ids, collapse = ", "),
               call. = FALSE)
  }
  base::message(
    "Adequacy DuckDB load complete: ",
    base::sum(audit_tbl$status != "missing"), " available; ",
    base::sum(audit_tbl$status == "missing"), " missing."
  )
  base::list(
    db_path = base::normalizePath(db_path, mustWork = TRUE),
    audit = audit_tbl
  )
}

#' Build standardized geographic adequacy features in DuckDB
#'
#' @param db_path Adequacy DuckDB.
#' @param feature_spec Tibble with `table_name`, `geography_col`, `value_col`,
#'   `feature_name`, and `aggregation`. Aggregation is `mean`, `sum`, or
#'   `weighted_mean`; the latter also requires `weight_col`.
#' @param overwrite Whether to replace `adequacy_geographic_features`.
#' @return Geographic feature tibble.
#' @export
build_adequacy_geographic_features <- function(
    db_path,
    feature_spec,
    overwrite = TRUE) {
  required_cols <- c(
    "table_name", "geography_col", "value_col", "feature_name",
    "aggregation"
  )
  if (!base::all(required_cols %in% base::names(feature_spec))) {
    base::stop(
      "`feature_spec` is missing: ",
      base::paste(
        base::setdiff(required_cols, base::names(feature_spec)),
        collapse = ", "
      ),
      call. = FALSE
    )
  }
  if (base::anyDuplicated(feature_spec$feature_name) > 0L) {
    base::stop("Feature names must be unique.", call. = FALSE)
  }
  allowed_aggregations <- c("mean", "sum", "weighted_mean")
  invalid_aggregations <- base::setdiff(
    base::unique(feature_spec$aggregation),
    allowed_aggregations
  )
  if (base::length(invalid_aggregations) > 0L) {
    base::stop("Unsupported aggregation: ",
               base::paste(invalid_aggregations, collapse = ", "),
               call. = FALSE)
  }
  if (!base::file.exists(db_path)) {
    base::stop("Adequacy DuckDB does not exist: ", db_path, call. = FALSE)
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path,
                        read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  available_tables <- DBI::dbListTables(con)
  feature_rows <- base::list()

  for (row_index in base::seq_len(base::nrow(feature_spec))) {
    specification <- feature_spec[row_index, , drop = FALSE]
    table_name <- specification$table_name[[1L]]
    feature_name <- specification$feature_name[[1L]]
    if (!table_name %in% available_tables) {
      base::message("Feature source table missing; skipping: ", table_name)
      next
    }
    table_fields <- DBI::dbListFields(con, table_name)
    needed_fields <- c(
      specification$geography_col[[1L]],
      specification$value_col[[1L]]
    )
    if (specification$aggregation[[1L]] == "weighted_mean") {
      if (!"weight_col" %in% base::names(specification) ||
          base::is.na(specification$weight_col[[1L]])) {
        base::stop("Weighted mean requires `weight_col` for ", feature_name,
                   call. = FALSE)
      }
      needed_fields <- c(needed_fields, specification$weight_col[[1L]])
    }
    missing_fields <- base::setdiff(needed_fields, table_fields)
    if (base::length(missing_fields) > 0L) {
      base::stop("Source fields absent in ", table_name, ": ",
                 base::paste(missing_fields, collapse = ", "),
                 call. = FALSE)
    }
    quote_id <- function(identifier) {
      base::as.character(DBI::dbQuoteIdentifier(con, identifier))
    }
    geography_sql <- quote_id(specification$geography_col[[1L]])
    value_sql <- quote_id(specification$value_col[[1L]])
    aggregation_sql <- switch(
      specification$aggregation[[1L]],
      mean = base::paste0("AVG(TRY_CAST(", value_sql, " AS DOUBLE))"),
      sum = base::paste0("SUM(TRY_CAST(", value_sql, " AS DOUBLE))"),
      weighted_mean = {
        weight_sql <- quote_id(specification$weight_col[[1L]])
        base::paste0(
          "SUM(TRY_CAST(", value_sql, " AS DOUBLE) * TRY_CAST(",
          weight_sql, " AS DOUBLE)) / NULLIF(SUM(TRY_CAST(", weight_sql,
          " AS DOUBLE)), 0)"
        )
      }
    )
    query <- base::paste0(
      "SELECT CAST(", geography_sql, " AS VARCHAR) AS geography, ",
      aggregation_sql, " AS feature_value FROM ", quote_id(table_name),
      " WHERE ", geography_sql, " IS NOT NULL GROUP BY 1"
    )
    base::message("Building feature ", feature_name, " from ", table_name,
                  ".")
    feature_rows[[feature_name]] <- DBI::dbGetQuery(con, query) |>
      tibble::as_tibble() |>
      dplyr::rename(!!feature_name := "feature_value")
  }

  if (base::length(feature_rows) == 0L) {
    base::stop("No geographic features could be built.", call. = FALSE)
  }
  geographic_features <- purrr::reduce(
    feature_rows,
    dplyr::full_join,
    by = "geography"
  ) |>
    dplyr::arrange(.data$geography)
  DBI::dbWriteTable(
    con,
    "adequacy_geographic_features",
    geographic_features,
    overwrite = overwrite
  )
  base::message(
    "Built adequacy_geographic_features: ",
    base::format(base::nrow(geographic_features), big.mark = ","),
    " geographies and ", base::ncol(geographic_features) - 1L,
    " external features."
  )
  geographic_features
}

#' Join standardized DuckDB evidence to an adequacy calibration table
#'
#' @param calibration_tbl Core geographic calibration table.
#' @param db_path Adequacy DuckDB created by [load_adequacy_sources_duckdb()].
#' @param feature_table Standardized table containing `geography` and optional
#'   state or county adequacy features.
#' @return Calibration table with external evidence and source-coverage fields.
#' @export
augment_adequacy_from_duckdb <- function(
    calibration_tbl,
    db_path,
    feature_table = "adequacy_geographic_features") {
  if (!base::file.exists(db_path)) {
    base::stop("Adequacy DuckDB does not exist: ", db_path, call. = FALSE)
  }
  if (!"geography" %in% base::names(calibration_tbl)) {
    base::stop("`calibration_tbl` requires `geography`.", call. = FALSE)
  }
  handle <- open_research_db(
    path = db_path,
    required_tables = c("adequacy_source_ingest_audit", feature_table),
    what = "adequacy evidence DuckDB"
  )
  on.exit(DBI::dbDisconnect(handle$con, shutdown = TRUE), add = TRUE)
  feature_tbl <- DBI::dbReadTable(handle$con, feature_table) |>
    tibble::as_tibble()
  if (!"geography" %in% base::names(feature_tbl)) {
    base::stop("Feature table requires `geography`.", call. = FALSE)
  }
  if (base::anyDuplicated(feature_tbl$geography) > 0L) {
    base::stop("Feature table must have one row per geography.",
               call. = FALSE)
  }
  augmented_tbl <- calibration_tbl |>
    dplyr::left_join(feature_tbl, by = "geography")
  evidence_cols <- base::setdiff(
    base::names(feature_tbl),
    "geography"
  )
  augmented_tbl |>
    dplyr::mutate(
      external_evidence_n = base::rowSums(
        !base::is.na(dplyr::pick(dplyr::all_of(evidence_cols)))
      ),
      external_evidence_complete = .data$external_evidence_n ==
        base::length(evidence_cols)
    )
}

#' Fit an evidence-informed geographic appointment-access model
#'
#' This is an empirical binomial model, not a latent Bayesian model. It uses
#' mystery-caller appointment counts as the outcome and external indicators as
#' predictors. Missing predictors receive an explicit missingness indicator;
#' the filled value has no standalone interpretation.
#'
#' @param calibration_tbl Geographic table with appointment counts and optional
#'   external evidence from [augment_adequacy_from_duckdb()].
#' @param predictor_names Candidate numeric predictors. Unavailable or constant
#'   predictors are logged and omitted.
#' @param population_col Population weighting column.
#' @param bootstrap_reps Number of geographic bootstrap replicates.
#' @param seed Random seed.
#' @return Model, coefficients, geographic estimates, national draws, and an
#'   evidence-coverage audit.
#' @export
fit_empirical_adequacy_glm <- function(
    calibration_tbl,
    predictor_names = c(
      "wait_days",
      "medicaid_fee_ratio",
      "medicaid_enrollment_share",
      "managed_care_access_score",
      "network_adequacy_score",
      "facility_per_100k",
      "hospital_concentration",
      "hcris_operating_margin",
      "nhis_delayed_care_pct",
      "brfss_cost_barrier_pct",
      "pums_uninsured_pct",
      "pums_no_vehicle_pct",
      "pulse_delayed_care_pct",
      "active_license_per_100k"
    ),
    population_col = "female_population",
    bootstrap_reps = 500L,
    seed = 20260821L) {
  required_cols <- c(
    "geography", "appointments_offered", "appointment_attempts",
    population_col
  )
  missing_cols <- base::setdiff(required_cols, base::names(calibration_tbl))
  if (base::length(missing_cols) > 0L) {
    base::stop("Calibration table is missing: ",
               base::paste(missing_cols, collapse = ", "),
               call. = FALSE)
  }
  offered <- base::as.integer(calibration_tbl$appointments_offered)
  attempts <- base::as.integer(calibration_tbl$appointment_attempts)
  if (base::any(base::is.na(offered)) ||
      base::any(base::is.na(attempts)) ||
      base::any(attempts <= 0L) ||
      base::any(offered < 0L | offered > attempts)) {
    base::stop("Appointment counts require 0 <= offered <= attempts and ",
               "attempts > 0.", call. = FALSE)
  }
  population <- base::as.numeric(calibration_tbl[[population_col]])
  if (base::any(!base::is.finite(population)) ||
      base::any(population <= 0)) {
    base::stop("Population weights must be positive and finite.",
               call. = FALSE)
  }

  available_names <- base::intersect(
    predictor_names,
    base::names(calibration_tbl)
  )
  model_frame <- tibble::tibble(
    geography = base::as.character(calibration_tbl$geography),
    offered = offered,
    not_offered = attempts - offered,
    population = population
  )
  retained_names <- base::character()
  coverage_rows <- base::list()

  for (predictor_name in available_names) {
    predictor_value <- base::as.numeric(calibration_tbl[[predictor_name]])
    observed <- base::is.finite(predictor_value)
    observed_n <- base::sum(observed)
    unique_n <- base::length(base::unique(predictor_value[observed]))
    coverage_rows[[predictor_name]] <- tibble::tibble(
      predictor = predictor_name,
      observed_n = observed_n,
      missing_n = base::length(predictor_value) - observed_n,
      observed_pct = observed_n / base::length(predictor_value),
      unique_n = unique_n,
      used = observed_n >= 5L && unique_n >= 2L
    )
    if (observed_n < 5L || unique_n < 2L) {
      base::message("Omitting unavailable or constant predictor: ",
                    predictor_name)
      next
    }
    center <- stats::median(predictor_value[observed])
    spread <- stats::mad(predictor_value[observed], center = center)
    if (!base::is.finite(spread) || spread <= 0) {
      spread <- stats::sd(predictor_value[observed])
    }
    if (!base::is.finite(spread) || spread <= 0) spread <- 1
    filled_value <- predictor_value
    filled_value[!observed] <- center
    safe_name <- base::make.names(predictor_name)
    model_frame[[safe_name]] <- (filled_value - center) / spread
    missing_name <- base::paste0(safe_name, "_missing")
    model_frame[[missing_name]] <- base::as.integer(!observed)
    retained_names <- c(retained_names, safe_name)
    if (base::any(!observed)) {
      retained_names <- c(retained_names, missing_name)
    }
  }
  if (base::length(retained_names) == 0L) {
    base::stop("No external predictor had at least five observations and ",
               "two unique values.", call. = FALSE)
  }
  model_formula <- stats::as.formula(
    base::paste(
      "cbind(offered, not_offered) ~",
      base::paste(retained_names, collapse = " + ")
    )
  )
  base::message("Fitting empirical adequacy model with predictors: ",
                base::paste(retained_names, collapse = ", "))
  access_fit <- stats::glm(
    model_formula,
    family = stats::binomial(),
    weights = base::rep(1, base::nrow(model_frame)),
    data = model_frame
  )
  predicted_adequacy <- stats::predict(
    access_fit,
    newdata = model_frame,
    type = "response"
  )

  base::set.seed(seed)
  bootstrap_national <- base::rep(NA_real_, bootstrap_reps)
  geography_n <- base::nrow(model_frame)
  for (replicate_index in base::seq_len(bootstrap_reps)) {
    sampled_index <- base::sample.int(
      geography_n,
      size = geography_n,
      replace = TRUE
    )
    bootstrap_frame <- model_frame[sampled_index, , drop = FALSE]
    bootstrap_fit <- base::tryCatch(
      stats::glm(
        model_formula,
        family = stats::binomial(),
        data = bootstrap_frame
      ),
      error = function(condition) NULL
    )
    if (base::is.null(bootstrap_fit) ||
        !base::isTRUE(bootstrap_fit$converged)) {
      next
    }
    replicate_prediction <- base::tryCatch(
      stats::predict(
        bootstrap_fit,
        newdata = model_frame,
        type = "response"
      ),
      error = function(condition) base::rep(NA_real_, geography_n)
    )
    if (base::all(base::is.finite(replicate_prediction))) {
      bootstrap_national[[replicate_index]] <- stats::weighted.mean(
        replicate_prediction,
        population
      )
    }
  }
  bootstrap_national <- bootstrap_national[
    base::is.finite(bootstrap_national)
  ]
  if (base::length(bootstrap_national) < 0.80 * bootstrap_reps) {
    base::stop("Fewer than 80% of geographic bootstrap fits converged.",
               call. = FALSE)
  }

  national_adequacy <- stats::weighted.mean(
    predicted_adequacy,
    population
  )
  national_summary <- tibble::tibble(
    adequacy_mean = national_adequacy,
    adequacy_sd = stats::sd(bootstrap_national),
    adequacy_median = stats::median(bootstrap_national),
    adequacy_p25 = stats::quantile(bootstrap_national, 0.25),
    adequacy_p75 = stats::quantile(bootstrap_national, 0.75),
    adequacy_p025 = stats::quantile(bootstrap_national, 0.025),
    adequacy_p975 = stats::quantile(bootstrap_national, 0.975)
  )
  summary_sentence <- base::sprintf(
    paste0(
      "Population-weighted appointment adequacy was %.1f%% ",
      "(SD %.1f%%; median %.1f%%, p25 %.1f%%, p75 %.1f%%)."
    ),
    100 * national_summary$adequacy_mean,
    100 * national_summary$adequacy_sd,
    100 * national_summary$adequacy_median,
    100 * national_summary$adequacy_p25,
    100 * national_summary$adequacy_p75
  )
  base::message(summary_sentence)
  coefficient_matrix <- base::summary(access_fit)$coefficients
  confidence_matrix <- stats::confint.default(access_fit)
  coefficient_tbl <- tibble::tibble(
    term = base::rownames(coefficient_matrix),
    estimate = coefficient_matrix[, "Estimate"],
    std_error = coefficient_matrix[, "Std. Error"],
    statistic = coefficient_matrix[, "z value"],
    p_value = coefficient_matrix[, "Pr(>|z|)"],
    conf_low = confidence_matrix[, 1L],
    conf_high = confidence_matrix[, 2L]
  )
  geographic_summary <- model_frame |>
    dplyr::transmute(
      geography = .data$geography,
      adequacy_mean = predicted_adequacy,
      female_population = .data$population
    )
  coverage_tbl <- dplyr::bind_rows(coverage_rows)

  base::list(
    fit = access_fit,
    coefficients = coefficient_tbl,
    geographic_summary = geographic_summary,
    national_summary = national_summary,
    national_draws = tibble::tibble(adequacy = bootstrap_national),
    evidence_coverage = coverage_tbl,
    summary_sentence = summary_sentence,
    method = "binomial_glm_with_geographic_bootstrap"
  )
}
