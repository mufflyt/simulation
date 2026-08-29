# tests/testthat/test-data-chia-ood-urogynecology-service-events.R
# Synthetic-fixture coverage for R/data-chia_ood_observation_normalization.R
# and R/data-chia_ood_urogynecology_service_events.R -- the OOD-based sibling
# of test-data-chia-urogynecology-service-events.R, covering the six URPS
# services CHIA HDD cannot see. See
# docs/superpowers/plans/2026-08-28-chia-ood-outpatient-urps-service-events.md.

test_that(".chia_ood_classify_source_of_payment() resolves Medicare/Medicaid/Commercial/Other-Public and never guesses Self-pay", {
  defs <- c(
    "Medicare",
    "Medicare HMO - Fallon Senior Plan",
    "AARP/Medigap supplement",
    "Medicaid (includes MassHealth)",
    "Medicaid Managed Care - Fallon Community Health Plan",
    "Network Health (Cambridge Health Alliance MCD Program)",
    "Aetna Life Insurance",
    "Blue Care Elect",
    "Other Commercial Insurance (not listed elsewhere)",
    "Worker's Compensation",
    "Auto Insurance",
    "Free Care",
    "Foundation",
    "CommCare: BMC HealthNet Plan/Commonwealth Care - Plan Type I",
    "None (Valid only for secondary source of payment)"
  )
  got <- .chia_ood_classify_source_of_payment(defs)
  expect_equal(got[1:3], rep("Medicare", 3))
  expect_equal(got[4:6], rep("Medicaid", 3))
  expect_equal(got[7:9], rep("Commercial", 3))
  expect_equal(got[10:15], rep("Other/Public", 6))
  expect_false(any(got == "Self-pay"))
  expect_false(any(is.na(got)))
})

test_that(".chia_ood_resolve_payer_group() reads the real shipped lookup table and never returns NA", {
  lut <- .chia_ood_source_of_payment_table()
  expect_gt(nrow(lut), 100L)  # real table has 157 real codes
  expect_true(all(c("source_pay_code", "definition", "payer_group") %in% names(lut)))
  expect_true(all(lut$payer_group %in% c("Medicare", "Medicaid", "Commercial", "Other/Public")))
  # "121" is the real, verified Medicare code in the shipped lookup
  expect_equal(.chia_ood_resolve_payer_group("121"), "Medicare")
  # "103" is the real, verified Medicaid (incl. MassHealth) code
  expect_equal(.chia_ood_resolve_payer_group("103"), "Medicaid")
  # an unmapped/unknown code falls to the documented catch-all, never NA
  expect_equal(.chia_ood_resolve_payer_group("999999"), "Other/Public")
  expect_false(any(is.na(.chia_ood_resolve_payer_group(c("121", "103", "999999", NA_character_)))))
})

test_that(".chia_ood_resolve_payer_group() distinguishes missing input (Unknown) from a real-but-unmapped code (Other/Public)", {
  # Real bug caught 2026-08-28: a first version silently folded NA/blank
  # PrimarySourceOfPayment into "Other/Public" via the same fallback used
  # for a genuinely-known-but-unmapped code, misrepresenting "we don't know
  # the payer" as a real classified category -- inflated Other/Public to
  # 84.6% of total OOD volume in the real database. This must stay
  # distinguished: a missing code is NOT evidence of a Worker's-Comp/
  # Free-Care/CommCare-type payer.
  got <- .chia_ood_resolve_payer_group(c(NA_character_, "", "  ", "999999", "121"))
  expect_equal(got, c("Unknown", "Unknown", "Unknown", "Other/Public", "Medicare"))
  expect_false(any(is.na(got)))
})

test_that("build_chia_ood_observation_normalized_view() coalesces CPT1-5 and CPTCode1-5 across the column-name era split", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix")

  # one row shaped like the pre-2015 era (CPT1-5 populated, CPTCode1-5 absent
  # from that table entirely -- verified real shape), one shaped like 2015+
  pre2015 <- tibble::tibble(
    RecordType01ID = 1L, `_data_year` = 2014L,
    CPT1 = "57160", CPT2 = NA_character_, CPT3 = NA_character_,
    CPT4 = NA_character_, CPT5 = NA_character_,
    PrincipalProcedureCode = NA_character_,
    PhysicianNumber = "100001", PrimarySourceOfPayment = "121"
  )
  post2015 <- tibble::tibble(
    RecordType01ID = 2L, `_data_year` = 2016L,
    CPTCode1 = "52000", CPTCode2 = NA_character_, CPTCode3 = NA_character_,
    CPTCode4 = NA_character_, CPTCode5 = NA_character_,
    PrincipalProcedureCode = NA_character_,
    PhysicianNumber = "100002", PrimarySourceOfPayment = "103"
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "ood_observation_2014"), pre2015)
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "ood_observation_2016"), post2015)
  DBI::dbExecute(con, "
    CREATE VIEW chia_casemix.v_ood_observation_all_years AS
    (SELECT * FROM chia_casemix.ood_observation_2014)
    UNION ALL BY NAME
    (SELECT * FROM chia_casemix.ood_observation_2016)
  ")

  build_chia_ood_observation_normalized_view(con)
  out <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.v_ood_observation_cpt_normalized ORDER BY RecordType01ID")

  expect_equal(out$cpt_1, c("57160", "52000"))
  expect_equal(out$`_cpt_column_era`, c("CPT1-5", "CPTCode1-5"))
})

.build_ood_service_events_fixture_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(con, "CREATE SCHEMA chia_casemix")
  DBI::dbExecute(con, "CREATE SCHEMA chia_provider")

  physicians <- tibble::tibble(
    license = c(100001, 100002),
    NPI     = c("1000000001", "1000000002")
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_provider", table = "borim_stdrel_npi_straight_from_cd"), physicians)

  # 15 pessary-care (CPT 57160) observation encounters across two physicians
  # and two years (one in 2016, in-window; one in 2013, out-of-window for the
  # physician-attributed table but should still count in the volume table),
  # enough that the default min_cell_size (11) suppresses one physician's
  # cell in the attributed table.
  n_p1 <- 3L
  n_p2 <- 12L
  n <- n_p1 + n_p2
  obs2016 <- tibble::tibble(
    RecordType01ID = 1:n, `_data_year` = 2016L,
    CPTCode1 = "57160", CPTCode2 = NA_character_, CPTCode3 = NA_character_,
    CPTCode4 = NA_character_, CPTCode5 = NA_character_,
    PrincipalProcedureCode = NA_character_,
    PhysicianNumber = as.character(c(rep(100001, n_p1), rep(100002, n_p2))),
    PrimarySourceOfPayment = "121"  # Medicare
  )
  obs2013 <- tibble::tibble(
    RecordType01ID = (n + 1):(n + 5), `_data_year` = 2013L,
    CPT1 = "57160", CPT2 = NA_character_, CPT3 = NA_character_,
    CPT4 = NA_character_, CPT5 = NA_character_,
    PrincipalProcedureCode = NA_character_,
    PhysicianNumber = "999999",  # deliberately unmatched to the crosswalk
    PrimarySourceOfPayment = "103"  # Medicaid
  )
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "ood_observation_2016"), obs2016)
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_casemix", table = "ood_observation_2013"), obs2013)
  DBI::dbExecute(con, "
    CREATE VIEW chia_casemix.v_ood_observation_all_years AS
    (SELECT * FROM chia_casemix.ood_observation_2016)
    UNION ALL BY NAME
    (SELECT * FROM chia_casemix.ood_observation_2013)
  ")
  build_chia_ood_observation_normalized_view(con)
  build_chia_ood_cpt_service_view(con)
  con
}

test_that("build_chia_ood_urogynecology_service_events() is FY2015-2018-scoped, small-cell-suppressed, and never Self-pay", {
  con <- .build_ood_service_events_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  build_chia_ood_urogynecology_service_events(con, years = 2015:2018, min_cell_size = 11L)
  out <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.ood_urogynecology_service_events")

  # only the 2016 (in-window) rows appear -- the 2013 rows are excluded
  expect_equal(sum(out$service_events), 15)
  expect_true(all(out$service == "pessary_care"))
  expect_true(all(out$setting == "outpatient_observation"))
  expect_true(all(out$payer_group == "Medicare"))
  expect_false(any(out$payer_group == "Self-pay"))
  # the 3-case physician's cell is below the default floor and is nulled
  expect_true(any(is.na(out$rendering_npi)))
  # the 12-case physician's cell is at/above the floor and keeps its NPI
  expect_true("1000000002" %in% out$rendering_npi)
})

test_that("build_chia_ood_urogynecology_service_volume() covers the full 2004-2018 range with no rendering_npi column", {
  con <- .build_ood_service_events_fixture_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  build_chia_ood_urogynecology_service_volume(con, years = 2004:2018)
  out <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.ood_urogynecology_service_volume_2004_2018")

  # BOTH the 2016 and the out-of-attribution-window 2013 rows appear
  expect_equal(sum(out$service_events), 20)
  expect_true(2013L %in% out$year)
  expect_true(2016L %in% out$year)
  expect_false("rendering_npi" %in% names(out))
  expect_true(all(out$setting == "outpatient_observation"))
  expect_setequal(out$payer_group, c("Medicare", "Medicaid"))
})
