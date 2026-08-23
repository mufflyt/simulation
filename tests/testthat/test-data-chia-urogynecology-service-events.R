# tests/testthat/test-data-chia-urogynecology-service-events.R
# Synthetic-fixture coverage for R/data-chia_procedure_family.R and
# R/data-chia_urogynecology_service_events.R -- the builder that assembles
# chia_casemix.urogynecology_service_events, which read_chia_service_share_events()
# (R/data-chia_urogynecology_service_shares.R) requires and, until this work,
# nothing in the repo built.

test_that(".chia_resolve_payer_group() maps every code actually observed in the real database", {
  # Every PrimaryPayerType code confirmed present in FY2015-2018 (verified
  # directly against the live database while building this pipeline), plus
  # the blank/sentinel/typo values that also occur. None may resolve to NA.
  observed <- c(
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "B", "C", "D", "E", "F", "H", "J", "K", "N", "Q", "T", "U", "Z",
    "", "-", "c"
  )
  resolved <- .chia_resolve_payer_group(observed)
  expect_false(any(is.na(resolved)))
  expect_equal(.chia_resolve_payer_group("1"), "Self-pay")
  expect_equal(.chia_resolve_payer_group(c("3", "F")), c("Medicare", "Medicare"))
  expect_equal(.chia_resolve_payer_group(c("4", "B")), c("Medicaid", "Medicaid"))
  expect_equal(
    .chia_resolve_payer_group(c("6", "7", "8", "C", "D", "E", "J", "K")),
    rep("Commercial", 8)
  )
  # Unmapped/rare codes go to a documented catch-all, never silently dropped.
  expect_equal(.chia_resolve_payer_group(c("0", "Z", "", "c")), rep("Other/Public", 4))
})

test_that("the procedure_family -> service crosswalk excludes revision_removal and genitourinary_fistula", {
  expect_equal(unname(.CHIA_PROCEDURE_FAMILY_TO_SERVICE["sui_sling"]), "sling_procedure")
  expect_equal(
    unname(.CHIA_PROCEDURE_FAMILY_TO_SERVICE[c(
      "pop_hysterectomy", "apical_abdominal_mesh", "colpocleisis",
      "transvaginal_mesh_pop", "vaginal_native_tissue_pop_repair"
    )]),
    rep("prolapse_procedure", 5)
  )
  expect_false("revision_removal" %in% names(.CHIA_PROCEDURE_FAMILY_TO_SERVICE))
  expect_false("genitourinary_fistula" %in% names(.CHIA_PROCEDURE_FAMILY_TO_SERVICE))
})

test_that(".chia_suppress_small_cells() nulls rendering_npi below the floor and conserves total volume", {
  events <- tibble::tibble(
    encounter_id = as.character(1:12),
    year = 2018L,
    rendering_npi = c(rep("1111111111", 3), rep("2222222222", 9)),
    service = "sling_procedure",
    payer_group = "Commercial",
    setting = "inpatient"
  )
  out <- .chia_suppress_small_cells(events, min_cell_size = 5L)
  # 3 events (below floor 5) get their NPI nulled and collapse into one row;
  # 9 events (>= floor) keep their NPI as a separate row.
  suppressed_row <- out[is.na(out$rendering_npi), ]
  kept_row <- out[!is.na(out$rendering_npi) & out$rendering_npi == "2222222222", ]
  expect_equal(nrow(suppressed_row), 1L)
  expect_equal(suppressed_row$service_events, 3)
  expect_equal(nrow(kept_row), 1L)
  expect_equal(kept_row$service_events, 9)
  # total volume is conserved regardless of suppression
  expect_equal(sum(out$service_events), 12)
})

.build_service_events_fixture_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix")
  DBI::dbExecute(con, "CREATE SCHEMA chia_provider")

  physicians <- tibble::tibble(
    license = c(100001, 100002),
    NPI     = c("1000000001", "1000000002")
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_provider", table = "borim_stdrel_npi_straight_from_cd"), physicians)

  # 15 sling-eligible discharges (procedure_family = sui_sling) across two
  # physicians, enough that the default min_cell_size (11) leaves one NPI's
  # cell suppressed and the other's intact.
  n_p1 <- 3L
  n_p2 <- 12L
  discharge <- tibble::tibble(
    RecordType20ID = 1:(n_p1 + n_p2),
    `_data_year` = 2018L,
    PrimaryPayerType = "3"
  )
  canonical <- tibble::tibble(
    RecordType20ID = 1:(n_p1 + n_p2),
    `_data_year` = 2018L,
    procedure_family = "sui_sling"
  )
  physician_link <- tibble::tibble(
    RecordType20ID = 1:(n_p1 + n_p2),
    `_data_year` = 2018L,
    borim_license = c(rep(100001, n_p1), rep(100002, n_p2))
  )

  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_discharge_all_years"), discharge)
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_discharge_canonical"), canonical)
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_discharge_physician"), physician_link)
  con
}

test_that("build_chia_urogynecology_service_events() produces a table read_chia_service_share_events() accepts", {
  con <- .build_service_events_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  build_chia_urogynecology_service_events(con, min_cell_size = 11L)
  observed <- read_chia_service_share_events(con)

  expect_true(all(c(
    "encounter_id", "year", "rendering_npi", "service",
    "payer_group", "setting", "service_events"
  ) %in% names(observed)))
  expect_equal(sum(observed$service_events), 15)
  expect_true(all(observed$service == "sling_procedure"))
  expect_true(all(observed$setting == "inpatient"))
  # the 3-case physician's cell is below the default floor of 11 and is nulled
  expect_true(any(is.na(observed$rendering_npi)))
  # the 12-case physician's cell is at/above the floor and keeps its NPI
  expect_true("1000000002" %in% observed$rendering_npi)
})

test_that("build_chia_urogynecology_service_events() emits nothing for excluded procedure families", {
  con <- .build_service_events_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "UPDATE chia_casemix.v_hdd_discharge_canonical SET procedure_family = 'revision_removal'")

  build_chia_urogynecology_service_events(con)
  observed <- read_chia_service_share_events(con)
  expect_equal(nrow(observed), 0L)
})
