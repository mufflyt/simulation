test_that("run_end_to_end_simulation executes 8 coupled annual steps cleanly", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2027L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_s3_class(sim_res$audit_ledger_tbl, "tbl_df")
  expect_equal(nrow(sim_res$audit_ledger_tbl), 3L) # 2025, 2026, 2027

  # Patient-flow conservation identity: served + unserved == appointment_requests
  audit <- sim_res$audit_ledger_tbl
  expect_equal(
    audit$served_patients_n + audit$unserved_delayed_n,
    audit$appointment_requests_n,
    tolerance = 1e-6
  )

  # Check HRR 306 spatial balance
  expect_s3_class(sim_res$annual_hrr_balance, "tbl_df")
  expect_equal(nrow(sim_res$annual_hrr_balance), 3L * 306L)
})

test_that("default policy_migration_scenario is a zero-behavior-change identity", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_null(sim_res$policy_migration_summary_tbl)
  expect_s3_class(sim_res$policy_migration_diagnostics, "tbl_df")
  expect_true(all(!sim_res$policy_migration_diagnostics$policy_migration_active))
  expect_equal(
    sim_res$policy_migration_diagnostics$demand_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$policy_migration_diagnostics$provider_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$policy_migration_diagnostics$application_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$simulation_config$policy_migration_scenario,
    "baseline"
  )
})

test_that("a non-baseline scenario against an empty policy DB degrades gracefully", {
  database_path <- base::tempfile(fileext = ".duckdb")

  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    policy_migration_scenario = "combined_stress",
    policy_evidence_db = database_path
  )

  expect_s3_class(sim_res$policy_migration_summary_tbl, "tbl_df")
  expect_true(all(sim_res$policy_migration_diagnostics$policy_migration_active))
  expect_false(sim_res$policy_migration_diagnostics$relocation_empirical[[1]])
  expect_equal(
    sim_res$policy_migration_diagnostics$relocation_method[[1]],
    "declared_scenario_prior"
  )
  # No evidence ingested: coalesced-to-zero migration/policy signals leave
  # the demand multiplier at the identity value.
  expect_equal(
    sim_res$policy_migration_diagnostics$demand_multiplier,
    rep(1, 2L)
  )
})

test_that("real policy evidence moves the demand multiplier off 1.0", {
  database_path <- base::tempfile(fileext = ".duckdb")
  connection <- open_policy_migration_duckdb(database_path)

  # Strong, opposite-signed legislative climate in two states so the
  # evidence panel carries real cross-state variation.
  lawatlas <- tibble::tibble(
    state = base::rep(base::c("FL", "CO"), each = 2L),
    effective_date = base::as.Date("2024-01-01"),
    end_date = base::as.Date(base::c(
      "2026-12-31", "2026-12-31", "2026-12-31", "2026-12-31"
    )),
    policy_domain = "reproductive_health",
    policy_value = base::rep(base::c(2, -2), each = 2L)
  )
  ingest_lawatlas_policies(connection, lawatlas)

  # Symmetric-flow ACS PUMS migration so the demand channel is driven by
  # the legislative-climate channel specifically, not migration noise.
  pums <- tibble::tibble(
    AGEP = base::rep(base::c(60, 70), times = 10L),
    SEX = 2,
    ST = base::rep(base::c("12", "08"), times = 10L),
    MIGSP = base::rep(base::c("08", "12"), times = 10L),
    PWGTP = 100
  )
  ingest_acs_pums_migration(connection, pums, year = 2025L)
  ingest_acs_pums_migration(connection, pums, year = 2026L)
  DBI::dbDisconnect(connection, shutdown = TRUE)

  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    policy_migration_scenario = "combined_stress",
    policy_evidence_db = database_path
  )

  expect_true(base::any(
    sim_res$policy_migration_diagnostics$provider_multiplier != 1
  ))
  expect_true(base::any(
    sim_res$policy_migration_diagnostics$application_multiplier != 1
  ))
})
