test_that("career evidence warehouse initializes all required tables", {
  database_path <- tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = FALSE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  expect_true(initialize_provider_career_duckdb(connection))
  table_rows <- DBI::dbGetQuery(
    connection,
    "SELECT table_schema, table_name
     FROM information_schema.tables
     WHERE table_schema IN ('career', 'career_raw', 'career_meta')"
  )
  expect_setequal(
    table_rows$table_name,
    c(
      "source_manifest",
      "provider_identity",
      "cms_opt_out",
      "cms_pecos",
      "cms_part_d",
      "cms_revoked",
      "irs_form_990",
      "clinical_trials",
      "orcid_affiliation"
    )
  )
})

test_that("provider evidence preserves distinct activity explanations", {
  database_path <- tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = FALSE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  initialize_provider_career_duckdb(connection)

  identities <- tibble::tibble(
    provider_id = c("P1", "P2", "P3"),
    npi = c("1000000001", "1000000002", "1000000003"),
    orcid = c("0000-0001", "0000-0002", NA_character_),
    normalized_name = c("alex one", "blair two", "casey three"),
    organization_name = c("alpha health", "beta health", "gamma health"),
    identity_tier = c(1L, 1L, 3L),
    identity_verified = c(TRUE, TRUE, FALSE)
  )
  register_provider_career_identities(
    connection,
    identities,
    overwrite = TRUE
  )

  opt_out_rows <- tibble::tibble(
    npi = "1000000001",
    effective_date = as.Date("2024-01-01"),
    end_date = as.Date(NA),
    specialty = "Obstetrics/Gynecology",
    source_year = 2024L
  )
  ingest_provider_career_source(
    connection,
    "cms_opt_out",
    opt_out_rows
  )
  pecos_rows <- tibble::tibble(
    npi = "1000000001",
    enrollment_id = "E1",
    enrollment_type = "individual",
    specialty = "Obstetrics/Gynecology",
    organization_name = "alpha health",
    state = "CO",
    enrollment_date = as.Date("2020-01-01"),
    source_year = 2025L
  )
  ingest_provider_career_source(
    connection,
    "cms_pecos",
    pecos_rows
  )

  part_d_rows <- tibble::tibble(
    npi = "1000000002",
    source_year = 2025L,
    total_claim_count = 120,
    total_30_day_fills = 135,
    total_drug_cost = 10000
  )
  ingest_provider_career_source(
    connection,
    "cms_part_d",
    part_d_rows
  )

  revoked_rows <- tibble::tibble(
    npi = "1000000003",
    revocation_date = as.Date("2025-01-01"),
    reinstatement_date = as.Date(NA),
    revocation_reason = "fixture",
    state = "CO"
  )
  ingest_provider_career_source(
    connection,
    "cms_revoked",
    revoked_rows
  )

  evidence_panel <- build_provider_career_evidence_panel(
    connection,
    years = 2025L,
    collect = TRUE
  )
  classified_panel <- classify_provider_career_evidence(evidence_panel)

  opt_out_provider <- classified_panel |>
    dplyr::filter(.data$provider_id == "P1")
  prescribing_provider <- classified_panel |>
    dplyr::filter(.data$provider_id == "P2")
  revoked_provider <- classified_panel |>
    dplyr::filter(.data$provider_id == "P3")

  expect_true(as.logical(opt_out_provider$medicare_opt_out))
  expect_true(as.logical(opt_out_provider$pecos_enrolled))
  expect_true(opt_out_provider$unresolved)
  expect_false(opt_out_provider$retirement_ascertained)
  expect_true(prescribing_provider$clinical_activity_ascertained)
  expect_true(prescribing_provider$unresolved)
  expect_equal(revoked_provider$provisional_state, "inactive")
  expect_false(revoked_provider$retirement_ascertained)
})

test_that("unverified name matches cannot assign leadership", {
  database_path <- tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = FALSE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  initialize_provider_career_duckdb(connection)

  identities <- tibble::tibble(
    provider_id = c("VERIFIED", "UNVERIFIED"),
    npi = c("1000000011", "1000000012"),
    orcid = c("0000-0011", "0000-0012"),
    normalized_name = c("same name", "same name"),
    organization_name = c("same hospital", "same hospital"),
    identity_tier = c(1L, 3L),
    identity_verified = c(TRUE, FALSE)
  )
  register_provider_career_identities(
    connection,
    identities,
    overwrite = TRUE
  )
  form_990_rows <- tibble::tibble(
    normalized_name = "same name",
    organization_name = "same hospital",
    organization_ein = "123456789",
    tax_year = 2025L,
    role_title = "Chief Medical Officer",
    compensation = 400000
  )
  ingest_provider_career_source(
    connection,
    "irs_form_990",
    form_990_rows
  )
  trial_rows <- tibble::tibble(
    normalized_name = "same name",
    organization_name = "same hospital",
    nct_id = "NCT00000001",
    source_year = 2025L,
    investigator_role = "PRINCIPAL_INVESTIGATOR",
    overall_status = "RECRUITING"
  )
  ingest_provider_career_source(
    connection,
    "clinical_trials",
    trial_rows
  )
  orcid_rows <- tibble::tibble(
    orcid = "0000-0011",
    organization_name = "same hospital",
    start_year = 2020L,
    end_year = NA_integer_,
    role_title = "Professor and Division Director",
    affiliation_type = "employments"
  )
  ingest_provider_career_source(
    connection,
    "orcid",
    orcid_rows
  )

  evidence_panel <- build_provider_career_evidence_panel(
    connection,
    years = 2025L,
    collect = TRUE
  )
  expect_equal(
    evidence_panel$form_990_executive[
      evidence_panel$provider_id == "VERIFIED"
    ],
    1
  )
  expect_equal(
    evidence_panel$form_990_executive[
      evidence_panel$provider_id == "UNVERIFIED"
    ],
    0
  )
  expect_equal(
    evidence_panel$principal_investigator[
      evidence_panel$provider_id == "VERIFIED"
    ],
    1
  )
  expect_equal(
    evidence_panel$principal_investigator[
      evidence_panel$provider_id == "UNVERIFIED"
    ],
    0
  )
  expect_equal(
    evidence_panel$orcid_academic[
      evidence_panel$provider_id == "VERIFIED"
    ],
    1
  )
})

test_that("source audit never interprets absent files as zero evidence", {
  database_path <- tempfile(fileext = ".duckdb")
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = FALSE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  source_audit <- audit_provider_career_sources(connection)
  expect_true(all(!source_audit$available))
  expect_true(base::all(base::grepl(
    "must not be interpreted",
    source_audit$interpretation
  )))
})

test_that("verified retirement outranks indirect public evidence", {
  provider_year_panel <- tibble::tibble(
    provider_id = c("P1", "P2", "P3"),
    year = c(2025L, 2025L, 2025L),
    clinical_fte = c(0, 1.0, NA_real_),
    verified_state = c("retired", NA_character_, NA_character_)
  )
  evidence_panel <- tibble::tibble(
    provider_id = c("P1", "P2", "P3"),
    year = c(2025L, 2025L, 2025L),
    medicare_opt_out = c(0L, 0L, 1L),
    pecos_enrolled = c(0L, 1L, 0L),
    part_d_observed = c(1L, 1L, 0L),
    medicare_revoked = c(0L, 0L, 0L),
    form_990_executive = c(0L, 0L, 0L),
    principal_investigator = c(0L, 0L, 0L),
    orcid_academic = c(0L, 0L, 0L),
    orcid_leadership = c(0L, 0L, 0L),
    leadership_signal = c(0L, 0L, 0L),
    academic_signal = c(0L, 0L, 0L),
    administrative_inactivity_signal = c(0L, 0L, 0L)
  )
  enriched_panel <- merge_provider_career_evidence(
    provider_year_panel,
    evidence_panel
  )
  expect_equal(enriched_panel$career_state[[1L]], "retired")
  expect_equal(enriched_panel$career_state[[2L]], "full_time_clinical")
  expect_true(is.na(enriched_panel$career_state[[3L]]))
  expect_true(enriched_panel$retirement_ascertained[[1L]])
})
