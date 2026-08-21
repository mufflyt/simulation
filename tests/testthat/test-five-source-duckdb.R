test_that("five-source manifest rejects missing columns", {
  incomplete_manifest <- tibble::tibble(source_id = "nppes")

  expect_error(
    validate_five_source_manifest(incomplete_manifest),
    "Source manifest is missing"
  )
})

test_that("five-source manifest rejects missing required files", {
  source_manifest <- tibble::tibble(
    source_id = "nppes",
    year = 2026L,
    file_path = tempfile(fileext = ".csv"),
    file_format = "csv",
    required = TRUE,
    source_url = "https://download.cms.gov/nppes/NPI_Files.html",
    sha256 = ""
  )

  expect_error(
    validate_five_source_manifest(source_manifest),
    "Required empirical files are missing"
  )
})

test_that("five-source DuckDB preserves source years and provenance", {
  skip_if_not_installed("duckdb")
  source_path <- withr::local_tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tibble(
      npi = c("1000000001", "1000000002"),
      hcpcs_code = c("57288", "57160"),
      service_n = c(14, 22)
    ),
    source_path
  )
  database_path <- withr::local_tempfile(fileext = ".duckdb")
  source_manifest <- tibble::tibble(
    source_id = "cms_provider_service",
    year = 2023L,
    file_path = source_path,
    file_format = "csv",
    required = TRUE,
    source_url = "https://data.cms.gov/",
    sha256 = digest::digest(
      source_path,
      algo = "sha256",
      file = TRUE,
      serialize = FALSE
    )
  )

  ingestion_summary <- build_five_source_duckdb(
    source_manifest = source_manifest,
    duckdb_path = database_path
  )

  expect_equal(ingestion_summary$row_n, 2)
  expect_equal(ingestion_summary$status, "ingested")
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = TRUE
  )
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  observed_panel <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT source_year, COUNT(*) AS row_n",
      "FROM model.cms_provider_service_all",
      "GROUP BY source_year"
    )
  )
  expect_equal(observed_panel$source_year, 2023L)
  expect_equal(observed_panel$row_n, 2)
})

test_that("checksum mismatch fails closed", {
  skip_if_not_installed("duckdb")
  source_path <- withr::local_tempfile(fileext = ".csv")
  readr::write_csv(tibble::tibble(npi = "1000000001"), source_path)
  source_manifest <- tibble::tibble(
    source_id = "nppes",
    year = 2026L,
    file_path = source_path,
    file_format = "csv",
    required = TRUE,
    source_url = "https://download.cms.gov/nppes/NPI_Files.html",
    sha256 = base::paste(base::rep("0", 64), collapse = "")
  )

  expect_error(
    build_five_source_duckdb(
      source_manifest,
      withr::local_tempfile(fileext = ".duckdb")
    ),
    "Checksum mismatch"
  )
})
