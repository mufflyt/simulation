testthat::test_that("source registry contains all eight evidence families", {
  registry <- patient_destination_source_registry()
  testthat::expect_equal(base::nrow(registry), 8L)
  testthat::expect_equal(base::sum(registry$revealed_choice), 1L)
  testthat::expect_identical(
    registry$source_id[registry$revealed_choice],
    "patient_od"
  )
})

testthat::test_that("DuckDB ingestion records hashes and row counts", {
  testthat::skip_if_not_installed("duckdb")
  temporary_database <- base::tempfile(fileext = ".duckdb")
  temporary_source <- base::tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tibble(origin_id = c("A", "B"), value = c(1, 2)),
    temporary_source
  )
  connection <- connect_patient_choice_duckdb(temporary_database)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  initialize_patient_choice_catalog(connection)
  audit_row <- ingest_patient_choice_file(
    connection,
    path = temporary_source,
    table = "test_origins",
    source_id = "acs",
    overwrite = TRUE
  )
  testthat::expect_equal(audit_row$row_count, 2)
  testthat::expect_match(audit_row$source_sha256, "^[0-9a-f]{64}$")
})

testthat::test_that("aggregate data cannot authorize choice estimation", {
  testthat::skip_if_not_installed("duckdb")
  temporary_database <- base::tempfile(fileext = ".duckdb")
  connection <- connect_patient_choice_duckdb(temporary_database)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  initialize_patient_choice_catalog(connection)
  evidence <- validate_patient_choice_evidence(connection)
  testthat::expect_false(evidence$estimation_allowed)
  testthat::expect_match(evidence$reason, "No observed")
})

testthat::test_that("travel barriers increase travel-time disutility inputs", {
  choice_fixture <- tibble::tibble(
    origin_id = "A",
    destination_id = "X",
    travel_time_min = 60,
    no_vehicle_share = 0.20,
    disability_share = 0.10,
    poverty_share = 0.30,
    transportation_barrier_share = 0.05,
    rural_share = 0.40,
    lagged_service_volume = 99
  )
  enriched_choices <- add_patient_travel_barrier_features(choice_fixture)
  testthat::expect_equal(enriched_choices$travel_no_vehicle, 12)
  testthat::expect_equal(enriched_choices$travel_disability, 6)
  testthat::expect_equal(enriched_choices$log_lagged_volume,
                         base::log(100))
})
