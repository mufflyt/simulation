test_that("open_ipums_lodes_duckdb creates database and manifest", {
  db_path <- tempfile(fileext = ".duckdb")
  con <- open_ipums_lodes_duckdb(db_path, overwrite = TRUE)
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(db_path)
  }, add = TRUE)

  expect_true(DBI::dbExistsTable(con, "ipums_lodes_manifest"))
})

test_that("ipums_lodes_source_catalog returns valid metadata", {
  cat_tbl <- ipums_lodes_source_catalog()
  expect_s3_class(cat_tbl, "tbl_df")
  expect_true(all(c("source_id", "official_name", "url", "grain") %in% names(cat_tbl)))
  expect_true("ipums_usa" %in% cat_tbl$source_id)
  expect_true("lodes_od" %in% cat_tbl$source_id)
})

test_that("ingest_ipums_microdata stages IPUMS records into DuckDB", {
  db_path <- tempfile(fileext = ".duckdb")
  con <- open_ipums_lodes_duckdb(db_path, overwrite = TRUE)
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(db_path)
  }, add = TRUE)

  mock_ipums <- tibble::tibble(
    year = c(2021L, 2022L, 2023L),
    age = c(35L, 42L, 58L),
    sex = c("female", "female", "female"),
    statefip = c("25", "36", "48"),
    perwt = c(100, 120, 95)
  )

  res <- ingest_ipums_microdata(con, mock_ipums, source_id = "ipums_usa")
  expect_equal(res$table_name, "raw_ipums_usa")
  expect_equal(res$row_count, 3L)
  expect_true(DBI::dbExistsTable(con, "raw_ipums_usa"))
})

test_that("ingest_lodes_commute_flows stages LEHD commute matrices into DuckDB", {
  db_path <- tempfile(fileext = ".duckdb")
  con <- open_ipums_lodes_duckdb(db_path, overwrite = TRUE)
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(db_path)
  }, add = TRUE)

  mock_lodes <- tibble::tibble(
    w_geocode = c("250170001001001", "360610002001002"),
    h_geocode = c("250170002001005", "360610003001004"),
    S000 = c(45, 120),
    CNS16 = c(15, 40)
  )

  res <- ingest_lodes_commute_flows(con, mock_lodes, vintage = "2021")
  expect_equal(res$table_name, "raw_lodes_od")
  expect_equal(res$row_count, 2L)
  expect_true(DBI::dbExistsTable(con, "raw_lodes_od"))
})
