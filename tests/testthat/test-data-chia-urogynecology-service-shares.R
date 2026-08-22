test_that("CHIA reader fails closed and reads a classified event table", {
  db_path <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix")
  events <- tibble::tibble(
    encounter_id = c("a", "b"),
    year = c(2023L, 2023L),
    rendering_npi = c("1111111111", "2222222222"),
    service = c("sling_procedure", "sling_procedure"),
    payer_group = c("Commercial", "Medicaid"),
    setting = c("inpatient", "inpatient"),
    service_events = c(1, 1)
  )
  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "chia_casemix", table = "urogynecology_service_events"),
    events
  )

  observed <- read_chia_service_share_events(con)
  expect_equal(observed, events)
  expect_error(
    read_chia_service_share_events(con, table = "missing_table"),
    "does not exist"
  )
})


test_that("CHIA evidence recognizes both URPS taxonomy branches", {
  events <- tibble::tribble(
    ~encounter_id, ~year, ~rendering_npi, ~service, ~payer_group,
    ~setting, ~service_events,
    "a", 2023L, "1111111111", "sling_procedure", "Commercial",
    "inpatient", 20,
    "b", 2023L, "2222222222", "sling_procedure", "Commercial",
    "inpatient", 30,
    "c", 2023L, "3333333333", "sling_procedure", "Commercial",
    "inpatient", 50
  )
  taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "2222222222", "2088F0040X", TRUE,
    "3333333333", "207V00000X", TRUE
  )

  evidence <- build_chia_service_share_evidence(events, taxonomy)
  shares <- evidence$provider_shares
  expect_equal(
    base::sum(shares$provider_share),
    1,
    tolerance = 1e-12
  )
  urps <- shares |>
    dplyr::filter(.data$provider_group == "urps")
  expect_equal(urps$service_events, 50)
  expect_equal(urps$provider_share, 0.50)

  physician <- evidence$physician_share
  expect_equal(physician$urps_events, 50)
  expect_equal(physician$physician_events, 100)
  expect_equal(physician$urps_given_physician, 0.50)
})


test_that("frozen roster membership overrides non-URPS taxonomy", {
  events <- tibble::tibble(
    encounter_id = "a",
    year = 2023L,
    rendering_npi = "1111111111",
    service = "pessary_care",
    payer_group = "Medicare",
    setting = "outpatient",
    service_events = 5
  )
  taxonomy <- tibble::tibble(
    rendering_npi = "1111111111",
    taxonomy_code = "207V00000X",
    is_primary = TRUE
  )
  roster <- tibble::tibble(npi = "1111111111")

  evidence <- build_chia_service_share_evidence(
    events,
    taxonomy,
    urps_roster = roster
  )

  expect_equal(evidence$provider_shares$provider_group, "urps")
  expect_equal(evidence$diagnostics$roster_override_events, 5)
})


test_that("CHIA versus CMS comparison widens transport SD with disagreement", {
  chia_close <- base::list(
    physician_share = tibble::tibble(
      service = "sling_procedure",
      year = 2023L,
      payer_group = "Commercial",
      setting = "inpatient",
      urps_events = 50,
      physician_events = 100,
      urps_given_physician = 0.50
    )
  )
  chia_far <- base::list(
    physician_share = tibble::tibble(
      service = "sling_procedure",
      year = 2023L,
      payer_group = "Commercial",
      setting = "inpatient",
      urps_events = 90,
      physician_events = 100,
      urps_given_physician = 0.90
    )
  )
  cms <- base::list(
    service_bounds = tibble::tibble(
      service = "sling_procedure",
      lower_bound = 0.40,
      upper_bound = 0.60
    )
  )

  close_cmp <- compare_chia_to_cms_service_share_evidence(
    chia_close,
    cms,
    baseline_transport_sd = 0.05
  )
  far_cmp <- compare_chia_to_cms_service_share_evidence(
    chia_far,
    cms,
    baseline_transport_sd = 0.05
  )

  expect_equal(close_cmp$distance_to_cms_interval, 0)
  expect_equal(close_cmp$transport_sd, 0.05)
  expect_gt(far_cmp$distance_to_cms_interval, 0)
  expect_gt(far_cmp$transport_sd, close_cmp$transport_sd)
})
