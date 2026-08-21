testthat::test_that("six evidence arms normalize into DuckDB", {
  testthat::skip_if_not_installed("duckdb")
  database_path <- base::tempfile(fileext = ".duckdb")
  connection <- open_policy_migration_duckdb(database_path)
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))

  pums <- tibble::tibble(
    AGEP = c(68L, 70L, 72L, 40L),
    SEX = c(2L, 2L, 2L, 2L),
    ST = c(12L, 12L, 8L, 12L),
    MIGSP = c(36L, 12L, 12L, 36L),
    PWGTP = c(100, 200, 50, 500)
  )
  ingest_acs_pums_migration(connection, pums, 2024L)
  pums_table <- dplyr::collect(dplyr::tbl(
    connection, "acs_pums_migration"
  ))
  testthat::expect_equal(
    base::sum(pums_table$weighted_people),
    850
  )

  acs_flows <- tibble::tibble(
    year = 2024L,
    origin_state_fips = c("36", "12"),
    destination_state_fips = c("12", "36"),
    flow = c(1000, 200),
    margin_of_error = c(100, 40)
  )
  ingest_acs_migration_flows(connection, acs_flows)

  irs_flows <- tibble::tibble(
    year = 2024L,
    origin_state_fips = c("36", "12"),
    destination_state_fips = c("12", "36"),
    returns = c(500, 100),
    exemptions = c(800, 150),
    adjusted_gross_income = c(40000000, 5000000)
  )
  ingest_irs_migration(connection, irs_flows)

  for (snapshot_index in base::seq_len(3L)) {
    providers <- tibble::tibble(
      npi = "1234567890",
      practice_state = c("NY", "FL", "FL")[[snapshot_index]],
      practice_postal_code = c("10001", "33101", "33101")[[
        snapshot_index
      ]],
      taxonomy = "207VF0040X"
    )
    ingest_nppes_snapshot(
      connection,
      providers,
      base::as.Date(c(
        "2023-01-01", "2024-01-01", "2024-06-01"
      )[[snapshot_index]])
    )
  }

  policies <- tibble::tibble(
    state = "FL",
    effective_date = base::as.Date("2023-01-01"),
    policy_domain = "ban",
    policy_value = 1
  )
  ingest_lawatlas_policies(connection, policies)

  nrmp <- tibble::tibble(
    year = 2024L,
    positions = 60L,
    filled = 58L
  )
  ingest_nrmp_urps_series(connection, nrmp)
  model_ready <- dplyr::collect(dplyr::tbl(
    connection, "nrmp_urps_model_ready"
  ))
  testthat::expect_equal(base::nrow(model_ready), 0L)

  crosswalk <- tibble::tibble(
    state = c("FL", "NY", "CO"),
    state_fips = c("12", "36", "08")
  )
  evidence <- build_policy_migration_evidence(
    connection,
    crosswalk
  )
  florida <- evidence |>
    dplyr::filter(.data$state == "FL", .data$year == 2024L)
  testthat::expect_equal(florida$older_female_net_migration, 50)
  testthat::expect_equal(florida$acs_net_migration, 800)
  testthat::expect_equal(florida$irs_net_exemptions, 650)
  testthat::expect_equal(florida$provider_net_moves, 1)
  testthat::expect_true(florida$migration_direction_agrees)
})

testthat::test_that("NRMP use fails closed without permission evidence", {
  testthat::skip_if_not_installed("duckdb")
  database_path <- base::tempfile(fileext = ".duckdb")
  connection <- open_policy_migration_duckdb(database_path)
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  match_series <- tibble::tibble(
    year = 2024L,
    positions = 60L,
    filled = 58L
  )
  testthat::expect_error(
    ingest_nrmp_urps_series(
      connection,
      match_series,
      model_use_allowed = TRUE
    ),
    "permission_reference"
  )
})

testthat::test_that("scenario engine preserves baseline and seed", {
  baseline <- tibble::tibble(
    state = "FL",
    year = 2024L,
    female_older_population = 1000,
    pfd_demand = 100,
    provider_fte = 10,
    fellowship_applications = 5
  )
  evidence <- tibble::tibble(
    state = "FL",
    year = 2024L,
    older_female_net_migration = 100,
    legislative_climate = 1
  )
  first_run <- simulate_policy_migration_scenarios(
    baseline,
    evidence,
    scenario = c("baseline", "migration_stress"),
    draws = 10L,
    seed = 44L
  )
  second_run <- simulate_policy_migration_scenarios(
    baseline,
    evidence,
    scenario = c("baseline", "migration_stress"),
    draws = 10L,
    seed = 44L
  )
  testthat::expect_equal(first_run, second_run)
  identity_rows <- first_run |>
    dplyr::filter(.data$scenario == "baseline")
  testthat::expect_equal(identity_rows$pfd_demand_scenario,
    identity_rows$pfd_demand
  )
  stressed <- first_run |>
    dplyr::filter(.data$scenario == "migration_stress")
  testthat::expect_true(base::all(stressed$migration_multiplier >= 1.25))
  testthat::expect_true(base::all(stressed$migration_multiplier <= 1.35))
})
