# Thin Medicare Part B DuckDB reader (R/data-medicare_part_b). Fixture DuckDB
# only; no external drive, no network. The reader does NOT aggregate or map
# services -- it hands CMS-canonical-named claim rows to the canonical
# aggregate_medicare_realized_care(). Tests cover the reader plus the integration.

.pb_fixture <- function(rows, table = "medicare_part_b_by_service_all_years") {
  path <- tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), path)
  DBI::dbWriteTable(conn, table, rows)
  DBI::dbDisconnect(conn, shutdown = TRUE)
  path
}

.full_rows <- function() {
  tibble::tibble(
    data_year          = c(2017L, 2017L, 2018L, 2017L, 2017L),
    HCPCS_Cd           = c("57288", "57288", "57288", "52000", "57160"),
    Rndrng_Prvdr_Type  = c("Obstetrics & Gynecology", "Urology", "Obstetrics & Gynecology", "Urology", "Obstetrics & Gynecology"),
    Plc_Of_Srvc        = c("F", "O", "F", "O", "O"),
    Rndrng_NPI         = c("N1", "N2", "N1", "N2", "N3"),
    Tot_Srvcs          = c(100, 50, 90, 500, 30),
    Tot_Benes          = c(95, 48, 88, 400, NA_real_),
    Tot_Bene_Day_Srvcs = c(99, 50, 90, 500, 30),
    Rndrng_Prvdr_State_Abrvtn = c("CO", "CO", "CO", "UT", "CO"))
}

test_that("reader pulls only requested codes and renames to CMS-canonical names", {
  path <- .pb_fixture(.full_rows()); on.exit(unlink(path), add = TRUE)
  rc <- read_part_b_claims(c("57288", "52000"), duckdb_path = path)
  expect_setequal(unique(rc$HCPCS_Cd), c("57288", "52000"))
  # Plc_Of_Srvc aliased to the name the canonical aggregator expects
  expect_true("Place_Of_Srvc" %in% names(rc))
  expect_true(all(c("year", "HCPCS_Cd", "Tot_Srvcs", "Tot_Benes", "Tot_Bene_Day_Srvcs",
                    "Rndrng_Prvdr_Type", "Rndrng_NPI", "Rndrng_Prvdr_State_Abrvtn") %in% names(rc)))
  expect_equal(nrow(rc), 4L)   # excludes the 57160 row
})

test_that("year filter restricts the reader", {
  path <- .pb_fixture(.full_rows()); on.exit(unlink(path), add = TRUE)
  rc <- read_part_b_claims("57288", duckdb_path = path, years = 2018L)
  expect_true(all(rc$year == 2018L))
  expect_equal(nrow(rc), 1L)
})

test_that("reader degrades gracefully when optional columns are absent", {
  minimal <- tibble::tibble(data_year = c(2019L, 2019L),
                            HCPCS_Cd = c("57288", "52000"), Tot_Srvcs = c(11, 22))
  path <- .pb_fixture(minimal); on.exit(unlink(path), add = TRUE)
  rc <- read_part_b_claims(c("57288", "52000"), duckdb_path = path)
  expect_true(all(c("year", "HCPCS_Cd", "Tot_Srvcs") %in% names(rc)))
  expect_false("Tot_Benes" %in% names(rc))
})

test_that("reader fails loudly on missing file and missing table", {
  expect_error(read_part_b_claims("57288", duckdb_path = tempfile(fileext = ".duckdb")),
               "not found", ignore.case = TRUE)
  path <- .pb_fixture(.full_rows(), table = "other_table"); on.exit(unlink(path), add = TRUE)
  expect_error(read_part_b_claims("57288", duckdb_path = path), "absent", ignore.case = TRUE)
})

test_that("reader + canonical aggregator produce per-service utilization with all three measures", {
  path <- .pb_fixture(.full_rows()); on.exit(unlink(path), add = TRUE)
  xwalk  <- urps_medicare_service_crosswalk()
  claims <- read_part_b_claims(c("57288", "52000", "57160"), duckdb_path = path)
  agg <- aggregate_medicare_realized_care(
    claims, crosswalk = xwalk, year = "year",
    state = NULL, provider_type = NULL, place_of_service = NULL, npi = "Rndrng_NPI")

  sling17 <- agg[agg$service == "sling_procedure" & agg$year == 2017L, ]
  expect_equal(sling17$billed_services, 150)              # 100 + 50
  expect_equal(sling17$billed_beneficiaries_sum, 143)     # 95 + 48 (summed, double-counts)
  expect_equal(sling17$billed_bene_day_services, 149)     # 99 + 50
  expect_equal(sling17$billing_npis, 2L)
  # three measures genuinely distinct
  expect_false(sling17$billed_services == sling17$billed_beneficiaries_sum)

  # pessary 2017 has a suppressed beneficiary cell -> flagged, not summed as zero silently
  pess <- agg[agg$service == "pessary_care" & agg$year == 2017L, ]
  expect_equal(pess$rows_with_na_benes, 1)
  expect_equal(pess$billed_services, 30)
})
