testthat::test_that("national source registry covers all ten source families", {
  registry_tbl <- urps_national_source_registry()

  testthat::expect_s3_class(registry_tbl, "data.frame")
  testthat::expect_equal(base::nrow(registry_tbl), 10L)
  testthat::expect_false(base::anyDuplicated(registry_tbl$source_id) > 0L)
  testthat::expect_true(base::all(base::nzchar(registry_tbl$landing_url)))
})

testthat::test_that("repository evidence builds a provenance-complete DuckDB", {
  testthat::skip_if_not_installed("duckdb")
  testthat::skip_if_not_installed("DBI")
  project_root <- testthat::test_path("..", "..")
  duckdb_path <- base::file.path(
    base::tempdir(),
    base::paste0(
      "urps_evidence_test_",
      base::sample.int(1000000L, 1L),
      ".duckdb"
    )
  )

  evidence_bundle <- build_urps_national_evidence_lake(
    duckdb_path = duckdb_path,
    project_root = project_root,
    overwrite = TRUE
  )

  testthat::expect_true(base::file.exists(evidence_bundle$duckdb_path))
  testthat::expect_setequal(
    evidence_bundle$ingest_manifest$source_id,
    base::c("acs", "training", "nhanes")
  )
  testthat::expect_true(
    base::all(
      base::c(
        "female_population_20plus",
        "annual_fellowship_entrants",
        "moderate_severe_ui_prevalence"
      ) %in% evidence_bundle$parameter_estimates$parameter
    )
  )
  testthat::expect_true(
    base::all(base::nzchar(
      evidence_bundle$parameter_estimates$estimand
    ))
  )
})

testthat::test_that("runner uses empirical values and records provenance", {
  provenance_tbl <- tibble::tibble(
    parameter = base::c(
      "female_population_20plus",
      "annual_fellowship_entrants"
    ),
    estimate = base::c(100000000, 66),
    source_id = base::c("acs", "training")
  )
  parameter_values <- stats::setNames(
    provenance_tbl$estimate,
    provenance_tbl$parameter
  )
  base::attr(parameter_values, "provenance") <- provenance_tbl

  simulation_bundle <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 1L,
    empirical_parameters = parameter_values,
    save_outputs = FALSE
  )

  expected_population <- 100000000 * (1.006^2)
  testthat::expect_equal(
    simulation_bundle$audit_ledger_tbl$population[[1L]],
    expected_population
  )
  testthat::expect_equal(
    simulation_bundle$simulation_config$fellowship_entrants,
    66
  )
  testthat::expect_identical(
    simulation_bundle$empirical_parameter_provenance,
    provenance_tbl
  )
})

testthat::test_that("evidence ingestion fails closed on unknown sources", {
  testthat::skip_if_not_installed("duckdb")
  duckdb_path <- base::file.path(
    base::tempdir(),
    base::paste0(
      "urps_evidence_reject_",
      base::sample.int(1000000L, 1L),
      ".duckdb"
    )
  )
  connection <- open_urps_evidence_db(duckdb_path)
  base::on.exit(
    DBI::dbDisconnect(connection, shutdown = TRUE),
    add = TRUE
  )
  fixture_path <- testthat::test_path(
    "..", "..", "data-raw", "calibration",
    "nrmp_urps_entrants_series.csv"
  )

  testthat::expect_error(
    ingest_urps_evidence_file(
      connection = connection,
      source_id = "not_a_source",
      source_path = fixture_path,
      table_name = "rejected"
    ),
    "Unknown `source_id`"
  )
})
