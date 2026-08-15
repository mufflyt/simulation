# The transport model's job is as much refusal as estimation. These tests assert
# it refuses, because a transport function that silently defaults its missing
# factor to 1 reports a Massachusetts inpatient count as a national total.

.root <- function(...) file.path("..", "..", ...)
.db   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"

test_that("CHIA transport refuses to produce an all-setting volume without a share", {
  skip_if_not(file.exists(.db))
  r <- suppressMessages(transport_chia_to_national(db = .db, census_path = .root("data-raw","census","np2023_d1_mid.csv")))
  expect_true(is.finite(r$estimate$national_inpatient))
  expect_true(is.na(r$estimate$national_all_setting))
  expect_equal(r$estimate$evidence_status, "incomplete_transport_inpatient_only")
})

test_that("an unsourced setting share is rejected", {
  skip_if_not(file.exists(.db))
  expect_error(
    suppressMessages(transport_chia_to_national(db = .db, census_path = .root("data-raw","census","np2023_d1_mid.csv"), inpatient_share = 0.12)),
    "without inpatient_share_source")
  expect_error(
    suppressMessages(transport_chia_to_national(db = .db, census_path = .root("data-raw","census","np2023_d1_mid.csv"), inpatient_share = 1.4, inpatient_share_source = "x")),
    "must be in")
})

test_that("CADR transport refuses without a Medicare share", {
  skip_if_not(file.exists(.root("scripts","cadr","outputs","workload_per_treated_patient.csv")))
  r <- suppressMessages(transport_cadr_to_national(cadr_path = .root("scripts","cadr","outputs","workload_per_treated_patient.csv")))
  expect_true(is.na(r$estimate$national_all_payer_per_year))
  expect_equal(r$estimate$evidence_status, "incomplete_transport_medicare_only")
  # 5,566 is a COHORT total, never an annual figure
  expect_equal(r$estimate$medicare_cohort_episodes, 5566)
  expect_lt(r$estimate$medicare_per_year, 1000)
})

test_that("neither transport result is ever scalar-eligible", {
  skip_if_not(file.exists(.db))
  a <- suppressMessages(transport_chia_to_national(
    db = .db, census_path = .root("data-raw","census","np2023_d1_mid.csv"),
    inpatient_share = 0.12, inpatient_share_source = "test"))
  expect_false(a$estimate$production_scalar_eligible)
})

test_that("the missing factor spans an order of magnitude", {
  s <- transport_setting_share_sensitivity(16959)
  expect_gt(max(s$national_all_setting) / min(s$national_all_setting), 9)
})
