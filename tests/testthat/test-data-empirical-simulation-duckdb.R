testthat::test_that("manifest validation rejects duplicate sources", {
  source_manifest <- empirical_source_manifest()
  duplicated_manifest <- dplyr::bind_rows(
    source_manifest,
    source_manifest[1, ]
  )
  testthat::expect_error(
    validate_empirical_manifest(duplicated_manifest),
    "must be unique"
  )
})

testthat::test_that("missing required files fail closed", {
  source_manifest <- empirical_source_manifest()[1, ] |>
    dplyr::mutate(
      local_path = base::tempfile(fileext = ".csv"),
      download_url = NA_character_
    )
  testthat::expect_error(
    resolve_empirical_source(
      source_record = source_manifest,
      raw_directory = base::tempdir(),
      download_missing = FALSE
    ),
    "Required source is absent"
  )
})

testthat::test_that("CSV ingestion records checksum and row count", {
  source_path <- base::tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tibble(NPI = base::c("1", "2"), service_count = 1:2),
    source_path
  )
  database_path <- base::tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = database_path)
  base::on.exit(
    DBI::dbDisconnect(connection, shutdown = TRUE),
    add = TRUE
  )
  initialize_empirical_registry(connection)
  source_record <- tibble::tibble(
    source_id = "fixture",
    table_name = "fixture_raw",
    local_path = source_path,
    download_url = NA_character_,
    release = "test",
    format = "csv",
    required = TRUE
  )
  ingest_empirical_source(
    connection = connection,
    source_record = source_record,
    source_path = source_path,
    overwrite = FALSE
  )
  registry_record <- DBI::dbReadTable(
    connection,
    "empirical_source_registry"
  )
  testthat::expect_equal(registry_record$row_count, 2)
  testthat::expect_match(registry_record$sha256, "^[a-f0-9]{64}$")
})

testthat::test_that("model tables require all six sources", {
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  base::on.exit(
    DBI::dbDisconnect(connection, shutdown = TRUE),
    add = TRUE
  )
  testthat::expect_error(
    build_empirical_model_tables(connection),
    "missing"
  )
})
