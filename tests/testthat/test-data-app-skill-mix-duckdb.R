.app_evidence_fixture <- function() {
  duckdb_path <- tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
  DBI::dbExecute(connection, "CREATE SCHEMA credentials")
  part_b_rows <- tibble::tribble(
    ~data_year, ~HCPCS_Cd, ~Tot_Srvcs, ~Rndrng_Prvdr_Type,
    ~Rndrng_NPI, ~Rndrng_Prvdr_State_Abrvtn,
    2023L, "57160", 40, "Nurse Practitioner", "100", "CO",
    2023L, "57160", 60, "Obstetrics & Gynecology", "200", "CO",
    2023L, "57288", 10, "Physician Assistant", "300", "CO",
    2023L, "57288", 90, "Urology", "400", "CO",
    2023L, "99999", 500, "Nurse Practitioner", "100", "CO"
  )
  DBI::dbWriteTable(
    connection,
    "medicare_part_b_by_service_all_years",
    part_b_rows
  )
  nppes_2023 <- tibble::tribble(
    ~npi, ~taxonomy_code, ~state,
    "100", "363L00000X", "CO",
    "200", "207VF0040X", "CO",
    "300", "363A00000X", "CO",
    "400", "2088F0040X", "CO"
  )
  DBI::dbWriteTable(
    connection,
    DBI::Id(
      schema = "credentials",
      table = "temporal_nppes_2023_fixed"
    ),
    nppes_2023
  )
  DBI::dbDisconnect(connection, shutdown = TRUE)

  dac_path <- tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tribble(
      ~NPI, ~org_pac_id, ~org_nm, ~State,
      "100", "P1", "Practice One", "CO",
      "200", "P1", "Practice One", "CO",
      "300", "P1", "Practice One", "CO",
      "400", "P2", "Practice Two", "CO"
    ),
    dac_path
  )
  list(duckdb_path = duckdb_path, dac_path = dac_path)
}

test_that("inventory finds existing Part B and longitudinal NPPES", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))

  inventory <- inventory_app_evidence_duckdb(
    fixture_values$duckdb_path
  )

  expect_true("cms_part_b" %in% inventory$source_family)
  expect_true("nppes_longitudinal" %in% inventory$source_family)
})

test_that("builder materializes only URPS-relevant Medicare summaries", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))

  row_counts <- build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )
  expect_setequal(
    row_counts$table,
    c(
      "urps_hcpcs_crosswalk",
      "medicare_app_service_share",
      "nppes_provider_year",
      "doctors_clinicians_affiliations",
      "practice_supervision_pools",
      "evidence_provenance"
    )
  )

  evidence_bundle <- read_app_skill_mix_evidence(
    fixture_values$duckdb_path
  )
  expect_false("99999" %in% evidence_bundle$service_evidence$service)
  expect_true(all(evidence_bundle$service_evidence$is_lower_bound))
})

test_that("Medicare APP billed shares are calculated correctly", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))
  build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )
  evidence_bundle <- read_app_skill_mix_evidence(
    fixture_values$duckdb_path
  )
  pessary_np <- evidence_bundle$service_evidence |>
    dplyr::filter(
      .data$service == "pessary_care",
      .data$provider_type == "nurse_practitioner"
    )
  sling_pa <- evidence_bundle$service_evidence |>
    dplyr::filter(
      .data$service == "sling_procedure",
      .data$provider_type == "physician_assistant"
    )

  expect_equal(pessary_np$billed_service_share, 0.40)
  expect_equal(sling_pa$billed_service_share, 0.10)
})

test_that("NPPES taxonomy improves provider classification", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))
  build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    fixture_values$duckdb_path,
    read_only = TRUE
  )
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  classifications <- DBI::dbGetQuery(
    connection,
    "SELECT npi, provider_type FROM app_evidence.nppes_provider_year"
  )

  expect_equal(
    classifications$provider_type[classifications$npi == "200"],
    "fpmrs_obgyn"
  )
  expect_equal(
    classifications$provider_type[classifications$npi == "400"],
    "fpmrs_urology"
  )
})

test_that("Doctors and Clinicians builds practice supervision pools", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))
  build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )
  evidence_bundle <- read_app_skill_mix_evidence(
    fixture_values$duckdb_path
  )
  practice_one <- evidence_bundle$practice_pools |>
    dplyr::filter(.data$practice_id == "P1")

  expect_equal(practice_one$app_headcount, 2)
  expect_equal(practice_one$physician_headcount, 1)
  expect_equal(practice_one$observed_app_physician_ratio, 2)
})

test_that("positive billing evidence never makes absence ineligible", {
  productivity <- tibble::tribble(
    ~service, ~provider_type, ~clinically_eligible,
    "pessary_care", "nurse_practitioner", TRUE,
    "sling_procedure", "physician_assistant", FALSE,
    "urodynamics", "nurse_practitioner", TRUE
  )
  service_evidence <- tibble::tribble(
    ~service, ~provider_type, ~billed_services,
    ~billed_service_share, ~billing_npis, ~is_lower_bound,
    "pessary_care", "nurse_practitioner", 40, 0.40, 2, TRUE,
    "sling_procedure", "physician_assistant", 10, 0.10, 1, TRUE
  )
  enhanced <- augment_app_productivity_evidence(
    productivity,
    service_evidence
  )

  expect_identical(
    enhanced$clinical_eligibility_preserved,
    productivity$clinically_eligible
  )
  expect_true(enhanced$positive_claims_evidence[[1]])
  expect_false(enhanced$positive_claims_evidence[[2]])
  expect_false(enhanced$positive_claims_evidence[[3]])
})

test_that("builder fails closed instead of silently overwriting evidence", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))
  build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )

  expect_error(
    build_app_skill_mix_evidence_duckdb(
      duckdb_path = fixture_values$duckdb_path,
      doctors_clinicians_path = fixture_values$dac_path
    ),
    "already exist"
  )
})

test_that("year filters cannot be used for SQL injection", {
  fixture_values <- .app_evidence_fixture()
  withr::defer(unlink(fixture_values$duckdb_path))
  withr::defer(unlink(fixture_values$dac_path))
  build_app_skill_mix_evidence_duckdb(
    duckdb_path = fixture_values$duckdb_path,
    doctors_clinicians_path = fixture_values$dac_path
  )

  expect_error(
    read_app_skill_mix_evidence(
      fixture_values$duckdb_path,
      years = "2023); DROP TABLE x; --"
    ),
    "finite calendar years"
  )
})

test_that("practice benchmark is bounded by the independent policy ceiling", {
  practice_pools <- tibble::tibble(
    app_headcount = c(0, 1, 2, 8),
    physician_headcount = c(1, 1, 1, 2),
    observed_app_physician_ratio = c(0, 1, 2, 4)
  )
  scenario_table <- estimate_app_ratio_scenario(
    practice_pools,
    probability = 0.75,
    policy_ceiling = 3
  )

  expect_lte(scenario_table$scenario_ratio, 3)
  expect_equal(scenario_table$practices, 4L)
  expect_match(scenario_table$estimand, "not FTE")
  expect_match(scenario_table$estimand, "not a legal limit")
})
