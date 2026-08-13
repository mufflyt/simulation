# CHIA + BORIM + CADR empirical capacity evidence. These tests drive the linkage
# -> physician-year -> summary chain and the resolution gate on in-memory
# fixtures, bypassing the external file readers. The gate is the point: workload
# evidence alone never resolves adequacy; only a validated access fit does.

sd_chia_extract <- function() {
  records <- tibble::tibble(
    attending_npi = c("1234567893", "1234567893", "9876543210", "LIC-55555"),
    year = c("2019", "2020", "2019", "2019"),
    discharge_id = c("d1", "d2", "d3", "d4"),
    dx1 = c("N814", "6256", "N393", "N81"),   # pop, ui, ui, pop
    proc1 = c("6851", "", "", "685"),           # hysterectomy on d1 and d4
    .source_file = "chia_test.csv",
    .source_row = 1:4
  )
  list(
    records = records,
    schema = list(
      provider_columns = "attending_npi",
      year_column = "year",
      encounter_id_column = "discharge_id",
      diagnosis_columns = "dx1",
      procedure_columns = "proc1"
    )
  )
}

sd_borim_bridge <- function() {
  tibble::tibble(
    key_value = "LIC55555",
    key_source = "license",
    npi = "5555555555"
  )
}

sd_roster <- function() {
  data.frame(
    npi = c("1234567893", "9876543210", "5555555555"),
    stringsAsFactors = FALSE
  )
}

test_that("identify_chia_provider_columns ranks NPI/identifier fields", {
  cols <- identify_chia_provider_columns(
    c("ATTENDING_NPI", "patient_name", "DIAG1", "operating_license")
  )
  expect_true("ATTENDING_NPI" %in% cols$column)
  expect_equal(cols$column[[1]], "ATTENDING_NPI")   # highest score
  expect_false("patient_name" %in% cols$column)     # negatively scored, dropped
})

test_that("link_chia_to_urps links direct NPI and BORIM identifiers to the roster", {
  linked <- link_chia_to_urps(
    chia_extract = sd_chia_extract(),
    borim_bridge = sd_borim_bridge(),
    urps_roster = sd_roster()
  )
  expect_true(all(!is.na(linked$npi)))
  expect_setequal(unique(linked$npi), c("1234567893", "9876543210", "5555555555"))
  # Both link methods exercised: a direct CHIA NPI and a BORIM-mapped license.
  expect_true(all(c("chia_npi_direct", "borim_identifier_to_npi") %in%
                    linked$npi_link_method))
  # Pelvic-floor flags fired: d1/d4 are POP, d2/d3 are UI; d1/d4 add hysterectomy.
  expect_true(any(linked$pop_hysterectomy))
})

test_that("physician-year summary and workload distribution are well-formed", {
  linked <- link_chia_to_urps(sd_chia_extract(), sd_borim_bridge(), sd_roster())
  py <- chia_urps_physician_year(linked)
  expect_true(all(c("npi", "year", "n_attributed_discharges") %in% names(py)))
  # NPI 1234567893 appears in two distinct years.
  expect_equal(sum(py$npi == "1234567893"), 2L)

  summary_tbl <- summarize_chia_urps_workload(py)
  expect_true(all(c("metric", "mean", "median", "n_unique_npis") %in%
                    names(summary_tbl)))
  expect_equal(unique(summary_tbl$n_unique_npis), 3L)
})

test_that("empirical_capacity_status stays unresolved without a validated access fit", {
  bundle <- list(
    chia_physician_year = tibble::tibble(npi = "1234567893", year = 2019L),
    cadr_workload = tibble::tibble(pathway = "prolapse"),
    lizeth_anchor = NULL
  )
  # Workload evidence present, but no access fit -> not identified.
  unresolved <- empirical_capacity_status(bundle)
  expect_false(unresolved$resolved)
  expect_equal(unresolved$tier, "empirical_support_not_identified")
  expect_true(is.na(unresolved$adequacy))

  # A validated access fit resolves it; the bundle did not by itself.
  resolved <- empirical_capacity_status(
    bundle,
    access_fit = list(adequacy = 0.82, lower = 0.74, upper = 0.91,
                      method = "lizeth_wait_inverse_v1", validation_passed = TRUE)
  )
  expect_true(resolved$resolved)
  expect_equal(resolved$tier, "calibrated_empirical_access")
  expect_equal(resolved$adequacy, 0.82)
})
