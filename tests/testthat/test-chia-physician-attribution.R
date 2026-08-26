# tests/testthat/test-chia-physician-attribution.R
# Synthetic-fixture coverage for R/data-chia_physician_attribution.R.
#
# The real CHIA database is 10GB+ on a personal external drive and cannot run
# in CI (see scripts/chia/build_physician_attribution.R's header for the
# empirical findings this fixture is built to pin). This mirrors the existing
# mock-DuckDB pattern in test-chia-real-vs-fixture.R ("DuckDB schema-faithful
# query path executes accurately on mock DuckDB connection"): a small,
# hand-built in-memory database matching the real schema shape, exercising
# specifically the two findings this session's work turned up:
#   1. a raw-operative ICD code gets downgraded when most of its performers
#      are a non-surgical specialty (the empirical specialty-share fix), but
#      NOT when it has too little attributed volume to trust the signal;
#   2. newborn (AdmissionType = '4') discharges must be excluded from the
#      specialty-share SIGNAL itself, not just the final case count -- else a
#      genuinely-surgical newborn-adjacent code (e.g. circumcision) gets
#      wrongly downgraded because the delivering, non-surgical-specialty
#      physician dominates its performer mix.

.physician_cols <- function(principal) {
  cols <- as.list(rep(NA_character_, 15))
  names(cols) <- c("OperatingPhysicianPrincipal", paste0("OperatingPhysicianSignificant", 1:14))
  cols$OperatingPhysicianPrincipal <- as.character(principal)
  cols
}

.build_fixture_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix")
  DBI::dbExecute(con, "CREATE SCHEMA chia_provider")

  physicians <- tibble::tibble(
    license     = c(100001, 100002, 100003, 100004),
    NPI         = c("1000000001", "1000000002", "1000000003", "1000000004"),
    specialty_1 = c("General Surgery", "Internal Medicine",
                    "Obstetrics and Gynecology", "Family Medicine")
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_provider",
                                 table = "borim_stdrel_npi_straight_from_cd"),
                    physicians)

  rt <- 0L
  discharge_rows <- list()
  procedure_rows <- list()

  add_case <- function(year, admission_type, code, physician_license, age = 45L) {
    rt <<- rt + 1L
    discharge_rows[[length(discharge_rows) + 1L]] <<- c(
      list(RecordType20ID = rt, `_data_year` = year, AdmissionType = admission_type,
           AgeLDS = age, SexLDS = "F", Race1 = "White", IdOrgSite = "SITE1",
           IdOrgFiler = "FILER1", TotalChargesSpecial = 1000),
      .physician_cols(physician_license)
    )
    procedure_rows[[length(procedure_rows) + 1L]] <<- list(
      RecordType20ID = rt, `_data_year` = year, AssociatedIndicator = 1L, ProcedureCode = code
    )
  }

  # AdmissionType values used for non-newborn cases below -- deliberately
  # excludes '4' (newborn), which is reserved for the circumcision fixture.
  adm_cycle <- c("1", "2", "3", "5")

  # FY2015 (ICD-9-CM): code 4701, category 47 (in the 01-86 operative range),
  # performed entirely by the surgical physician (license 100001) -- stays
  # operative both from the raw coding-system rule AND the specialty-share
  # check (n=25 >= min_n, 100% surgical).
  for (i in 1:25) add_case(2015L, adm_cycle[(i %% 4) + 1L], "4701", 100001)

  # FY2015: code 3801, also category 38 (in range), but performed entirely by
  # the non-surgical physician (license 100002) -- raw rule says operative,
  # specialty-share check (n=25 >= min_n, 0% surgical) downgrades it.
  for (i in 1:25) add_case(2015L, adm_cycle[(i %% 4) + 1L], "3801", 100002)

  # FY2015: code 5501, category 55 (in range), performed entirely by the
  # non-surgical physician but only 5 times -- below min_n=20, so the
  # specialty-share check must NOT override the raw classification: stays
  # operative despite being 0% surgical among its (too few) occurrences.
  for (i in 1:5) add_case(2015L, adm_cycle[(i %% 4) + 1L], "5501", 100002)

  # FY2016 (ICD-10-PCS): 0VTTXZZ (circumcision, section 0 = operative by the
  # raw rule) performed on 22 NEWBORNS (AdmissionType = '4', age 0) entirely
  # by the Family Medicine physician (license 100004, non-surgical keyword).
  # THE REGRESSION THIS PINS: if newborns were included in the specialty-share
  # signal, this code would look 0% surgical with n=22 >= min_n and get
  # wrongly downgraded (this happened during development -- see commit
  # 5a117d7). With newborns correctly excluded from the signal, this code has
  # zero NON-newborn attributed occurrences (n=0 < min_n), so the raw
  # classification (operative) is kept.
  for (i in 1:22) add_case(2016L, "4", "0VTTXZZ", 100004, age = 0L)

  # FY2016: a genuinely adult, non-newborn operative case by the surgical
  # physician, to confirm the ICD-10-PCS section-0 rule and non-newborn path
  # both still work normally.
  for (i in 1:3) add_case(2016L, "1", "0TB03ZZ", 100001)

  discharge_df <- dplyr::bind_rows(lapply(discharge_rows, tibble::as_tibble))
  procedure_df <- dplyr::bind_rows(lapply(procedure_rows, tibble::as_tibble))

  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_discharge_all_years"), discharge_df)
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_procedurecode_all_years"), procedure_df)

  # Empty but schema-shaped: build_chia_hdd_diagnosis_long_view() (part of
  # build_chia_physician_attribution()'s chain as of the procedure_family
  # work) is a thin rename over this view and DuckDB validates referenced
  # tables at CREATE VIEW time, so it must exist even when no test here
  # exercises diagnosis-qualified families.
  diagnosis_df <- tibble::tibble(
    RecordType20ID = integer(0), `_data_year` = integer(0),
    DiagnosisCode = character(0)
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "v_hdd_diagnosiscode_all_years"), diagnosis_df)

  con
}

test_that("build_chia_physician_attribution builds the full view chain without error", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_no_error(build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list())))
})

test_that("a high-volume, surgical-dominant code stays operative", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))
  cls <- DBI::dbGetQuery(con, "SELECT DISTINCT procedure_class FROM chia_casemix.v_hdd_discharge_procedure WHERE principal_procedure = '4701'")
  expect_equal(cls$procedure_class, "operative")
})

test_that("a high-volume, non-surgical-dominant code is downgraded", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))
  cls <- DBI::dbGetQuery(con, "SELECT DISTINCT procedure_class FROM chia_casemix.v_hdd_discharge_procedure WHERE principal_procedure = '3801'")
  expect_equal(cls$procedure_class, "non_operative")
})

test_that("a low-volume non-surgical-dominant code is NOT downgraded (below min_n)", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))
  cls <- DBI::dbGetQuery(con, "SELECT DISTINCT procedure_class FROM chia_casemix.v_hdd_discharge_procedure WHERE principal_procedure = '5501'")
  expect_equal(cls$procedure_class, "operative")
})

test_that("newborn discharges are excluded from the specialty-share signal, not just the case count", {
  # THE REGRESSION THIS PINS (see file header): without the newborn exclusion,
  # 0VTTXZZ here would be 0% surgical (all 22 occurrences are the Family
  # Medicine physician) and get wrongly downgraded to non_operative.
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))

  share <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.v_procedure_specialty_share WHERE code = '0VTTXZZ'")
  expect_equal(nrow(share), 0)  # zero NON-newborn attributed occurrences

  cls <- DBI::dbGetQuery(con, "SELECT DISTINCT procedure_class FROM chia_casemix.v_hdd_discharge_procedure WHERE principal_procedure = '0VTTXZZ'")
  expect_equal(cls$procedure_class, "operative")
})

test_that("surgeon_year_volume splits newborn_cases from operative_cases correctly", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))

  syv <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.surgeon_year_volume WHERE npi = '1000000004'")
  expect_equal(nrow(syv), 1L)
  expect_equal(syv$fy, 2016)
  expect_equal(syv$operative_cases, 0L)
  expect_equal(syv$newborn_cases, 22L)

  surgeon <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.surgeon_year_volume WHERE npi = '1000000001' AND fy = 2015")
  expect_equal(surgeon$operative_cases, 25L)
  expect_equal(surgeon$newborn_cases, 0L)
})

test_that("surgeon_year_volume is unique on (npi, fy)", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))
  dup <- DBI::dbGetQuery(con, "SELECT count(*) n FROM (SELECT npi, fy FROM chia_casemix.surgeon_year_volume GROUP BY 1,2 HAVING count(*)>1)")
  expect_equal(dup$n, 0)
})

test_that("the pre-2015 sentinel is never counted as a principal procedure", {
  con <- .build_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  # A discharge with no recorded procedure (the CHIA '-' sentinel).
  DBI::dbExecute(con, "INSERT INTO chia_casemix.v_hdd_procedurecode_all_years
                       VALUES (9001, 2015, 1, '-')")
  build_chia_physician_attribution(con, procedure_family_config = list(families = list(), diagnosis_qualifiers = list()))
  bad <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_hdd_discharge_procedure WHERE principal_procedure IN ('-', '')")
  expect_equal(bad$n, 0)
})
