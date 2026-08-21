test_that("geography source registry contains all selected sources", {
  registry <- endogenous_geography_source_registry()

  expect_setequal(
    registry$source_id,
    c(
      "irs_migration", "acs_migration", "lodes", "qcew",
      "bea", "ipeds"
    )
  )
  expect_true(all(registry$evidence_tier == "primary"))
})

test_that("source manifests fail closed without hashes", {
  manifest <- tibble::tibble(
    source_id = "irs_migration",
    release_id = "2022-2023",
    year_min = 2022L,
    year_max = 2023L,
    download_url = "https://example.test/irs.csv",
    local_file = "irs.csv",
    sha256 = "not-a-hash",
    table_name = "irs_raw"
  )

  expect_error(
    validate_geography_source_manifest(manifest),
    "SHA-256"
  )
})

test_that("unknown source families are refused", {
  manifest <- tibble::tibble(
    source_id = "invented_source",
    release_id = "one",
    year_min = 2020L,
    year_max = 2021L,
    download_url = "https://example.test/file.csv",
    local_file = "file.csv",
    sha256 = paste(rep("a", 64L), collapse = ""),
    table_name = "invented_raw"
  )

  expect_error(
    validate_geography_source_manifest(manifest),
    "Unsupported source_id"
  )
})

test_that("DuckDB import records row counts and provenance", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  database_path <- tempfile(fileext = ".duckdb")
  source_path <- tempfile(fileext = ".csv")
  on.exit(unlink(c(database_path, source_path)), add = TRUE)
  readr::write_csv(
    tibble::tibble(county_fips = "08031", year = 2023L),
    source_path
  )
  source_hash <- digest::digest(file = source_path, algo = "sha256")
  manifest <- tibble::tibble(
    source_id = "irs_migration",
    release_id = "fixture",
    year_min = 2023L,
    year_max = 2023L,
    download_url = "https://example.test/fixture.csv",
    local_file = source_path,
    sha256 = source_hash,
    observed_sha256 = source_hash,
    file_size_bytes = file.info(source_path)$size,
    table_name = "irs_fixture",
    verified_at = "2026-08-21 UTC"
  )
  connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = database_path)
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  audit <- ingest_geography_sources_duckdb(connection, manifest)

  expect_equal(audit$row_count, 1)
  expect_true("geography_imports" %in% DBI::dbListTables(connection))
  expect_equal(
    DBI::dbGetQuery(
      connection,
      "SELECT COUNT(*) AS n FROM geography_raw.irs_fixture"
    )$n[[1]],
    1
  )
})
