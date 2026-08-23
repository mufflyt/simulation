test_that("ingest_all_12_infrastructure_tables_to_duckdb writes valid schemas and non-empty tables to DuckDB", {
  .skip_unless_cms_service_share_data()
  tmp_db <- tempfile(fileext = ".duckdb")
  on.exit(unlink(tmp_db), add = TRUE)

  summary_res <- ingest_all_12_infrastructure_tables_to_duckdb(
    duckdb_path = tmp_db,
    n_draws = 10L,
    schema = "evidence"
  )

  expect_type(summary_res, "list")

  con <- open_urps_evidence_db(tmp_db, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE, after = FALSE)

  tables <- DBI::dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'evidence'")$table_name

  expect_true("service_registry" %in% tables)
  expect_true("provider_taxonomy_registry" %in% tables)
  expect_true("cms_service_shares_suppression" %in% tables)
  expect_true("chia_setting_shares" %in% tables)
  expect_true("compositional_share_draws" %in% tables)
  expect_true("allocated_provider_workload" %in% tables)

  # Check non-empty count
  count_res <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM evidence.service_registry")$n
  expect_equal(count_res, 20L)
})
