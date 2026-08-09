# Medicare Part B extraction (R/data-medicare_part_b). Fixture DuckDB only; no
# external drive, no network. Verifies: correct aggregation, the three CMS
# measures kept DISTINCT, year/provider/POS filtering + grouping, code-group
# labeling, missingness flag, graceful degradation when optional columns are
# absent, and provenance.

# Build a temp DuckDB with a CMS-like by-Provider-and-Service table.
.pb_fixture <- function(rows, table = "medicare_part_b_by_service_all_years") {
  path <- tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), path)
  DBI::dbWriteTable(conn, table, rows)
  DBI::dbDisconnect(conn, shutdown = TRUE)
  path
}

.full_rows <- function() {
  tibble::tibble(
    data_year         = c(2017L, 2017L, 2017L, 2017L, 2018L, 2018L, 2017L),
    HCPCS_Cd          = c("57288", "57288", "57288", "57240", "57288", "52000", "57160"),
    Rndrng_Prvdr_Type = c("Obstetrics & Gynecology", "Urology", "Urology",
                          "Obstetrics & Gynecology", "Obstetrics & Gynecology",
                          "Urology", "Obstetrics & Gynecology"),
    Plc_Of_Srvc       = c("F", "F", "O", "F", "F", "O", "O"),
    Rndrng_NPI        = c("N1", "N2", "N3", "N1", "N1", "N2", "N4"),
    Tot_Srvcs         = c(100, 40, 10, 20, 90, 500, 30),
    Tot_Benes         = c(95, 38, 10, 20, 88, 400, NA_real_),
    Tot_Bene_Day_Srvcs = c(99, 40, 10, 20, 90, 500, 30)
  )
}

test_that("aggregates services per year/code and keeps the three CMS measures distinct", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path)

  sling17 <- out[out$code_group == "sling" & out$year == 2017L, ]
  expect_equal(nrow(sling17), 1L)
  expect_equal(sling17$tot_srvcs, 150)            # 100 + 40 + 10
  expect_equal(sling17$tot_benes_sum, 143)        # 95 + 38 + 10 (summed, double-counts)
  expect_equal(sling17$tot_bene_day_srvcs, 149)   # 99 + 40 + 10
  # the three measures are genuinely different numbers
  expect_false(sling17$tot_srvcs == sling17$tot_benes_sum)
  expect_false(sling17$tot_benes_sum == sling17$tot_bene_day_srvcs)
  expect_equal(sling17$n_provider_rows, 3L)
  expect_equal(sling17$n_distinct_npi, 3L)
})

test_that("code groups are labeled from the code map", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path)
  expect_equal(out$code_group[out$hcpcs == "57240"][1], "prolapse_repair")
  expect_equal(out$code_group[out$hcpcs == "52000"][1], "cystoscopy")
  expect_equal(out$code_group[out$hcpcs == "57160"][1], "pessary")
})

test_that("year filter restricts output", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path, years = 2018L)
  expect_true(all(out$year == 2018L))
  expect_equal(out$tot_srvcs[out$code_group == "sling"], 90)
})

test_that("provider_type filter restricts AND groups by specialty", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path, provider_type = "Urology")
  expect_true("rndrng_prvdr_type" %in% names(out))
  expect_true(all(out$rndrng_prvdr_type == "Urology"))
  expect_equal(sum(out$tot_srvcs[out$code_group == "sling" & out$year == 2017L]), 50) # 40 + 10
})

test_that("place_of_service filter restricts AND groups by POS", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path, place_of_service = "O")
  expect_true("place_of_service" %in% names(out))
  expect_true(all(out$place_of_service == "O"))
  expect_equal(out$tot_srvcs[out$code_group == "sling" & out$year == 2017L], 10)
})

test_that("beneficiary suppression/missingness is made explicit", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path)
  pessary <- out[out$code_group == "pessary" & out$year == 2017L, ]
  expect_equal(pessary$rows_with_na_benes, 1)
})

test_that("provenance is attached with the fields a downstream user needs", {
  path <- .pb_fixture(.full_rows())
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path)
  prov <- attr(out, "provenance")
  expect_true(is.list(prov))
  for (f in c("source", "cms_dataset", "duckdb_path", "source_md5", "years",
              "extraction_date", "code_group_version", "field_dictionary", "caveat")) {
    expect_true(f %in% names(prov))
  }
  expect_equal(prov$code_group_version, PART_B_CODE_GROUP_VERSION)
  expect_match(prov$caveat, "not a national all-payer", ignore.case = TRUE)
})

test_that("optional columns absent -> graceful NA, services still computed", {
  minimal <- tibble::tibble(
    data_year = c(2019L, 2019L),
    HCPCS_Cd  = c("57288", "52000"),
    Tot_Srvcs = c(11, 22)
  )
  path <- .pb_fixture(minimal)
  on.exit(unlink(path), add = TRUE)
  out <- extract_part_b_utilization(duckdb_path = path)
  expect_equal(out$tot_srvcs[out$hcpcs == "57288"], 11)
  expect_true(all(is.na(out$tot_benes_sum)))
  expect_true(all(is.na(out$tot_bene_day_srvcs)))
  expect_true(all(is.na(out$n_distinct_npi)))
})

test_that("missing file and missing table fail loudly", {
  expect_error(extract_part_b_utilization(duckdb_path = tempfile(fileext = ".duckdb")),
               "not found", ignore.case = TRUE)
  path <- .pb_fixture(.full_rows(), table = "some_other_table")
  on.exit(unlink(path), add = TRUE)
  expect_error(extract_part_b_utilization(duckdb_path = path), "absent", ignore.case = TRUE)
})
