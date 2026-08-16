# The transport model's job is as much refusal as estimation. These tests assert
# it refuses, because a transport function that silently defaults its missing
# factor to 1 reports a Massachusetts inpatient count as a national total.

.root <- function(...) file.path("..", "..", ...)
.db   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
# transport_chia_to_national() needs BOTH the CHIA database and the census
# denominator. Guarding only .db meant that on a machine where the external
# drive is mounted, the skip did not fire and the census read then failed --
# so R CMD check errored locally while passing on a runner that has neither.
# Two dependencies, two guards.
.census <- file.path("..", "..", "data-raw", "census", "np2023_d1_mid.csv")

test_that("CHIA transport refuses to produce an all-setting volume without a share", {
  skip_if_not(file.exists(.db))
  skip_if_not(file.exists(.census))
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
  skip_if_not(file.exists(.census))
  a <- suppressMessages(transport_chia_to_national(
    db = .db, census_path = .root("data-raw","census","np2023_d1_mid.csv"),
    inpatient_share = 0.12, inpatient_share_source = "test"))
  expect_false(a$estimate$production_scalar_eligible)
})

test_that("the missing factor spans an order of magnitude", {
  s <- transport_setting_share_sensitivity(16959)
  expect_gt(max(s$national_all_setting) / min(s$national_all_setting), 9)
})

test_that("all three families resolve, and sling refuses for a stated reason", {
  skip_if_not(file.exists(.db))
  pop <- suppressMessages(chia_ma_age_specific_rates(db = .db, family = "pop_hysterectomy"))
  all <- suppressMessages(chia_ma_age_specific_rates(db = .db, family = "all_hysterectomy"))
  expect_equal(nrow(pop), 4L)
  expect_equal(nrow(all), 4L)
  # POP-indication is a strict subset of all hysterectomy
  expect_lt(sum(pop$cases), sum(all$cases))
  # POP peaks in the 65-79 band; all-hysterectomy does not
  expect_equal(pop$age_band[which.max(pop$rate_per_100k)], "65-79")

  # Sling has left the inpatient setting: 0 cases in FY2018, so no rate exists.
  expect_error(
    suppressMessages(chia_ma_age_specific_rates(db = .db, family = "sui_sling")),
    "left the inpatient setting")
})

test_that("the retired ICD-9 codes are counted", {
  skip_if_not(file.exists(.db))
  # 684/686/687 were withdrawn in the October 2006 ICD-9 update and are absent
  # from ref.icd9cm_procedure (v32). Omitting them undercuts FY2004-2006.
  con <- DBI::dbConnect(duckdb::duckdb(), .db, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  n <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n FROM chia_casemix.v_cohort_female_adult
    WHERE _data_year BETWEEN 2004 AND 2006
      AND principal_procedure IN ('684','686','687')")$n
  expect_gt(n, 10000)   # ~17.5k in the cohort; they are not a rounding error
})

test_that("the free sling route works without HCUP", {
  skip_if_not(file.exists(.root("data-raw","cms_psps","MUP_PHY_R26_P05_V10_D24_Geo.csv")))
  skip_if_not(file.exists(.db))
  puf <- suppressMessages(cms_puf_national_volume(
    path = .root("data-raw","cms_psps","MUP_PHY_R26_P05_V10_D24_Geo.csv")))
  expect_gt(sum(puf$services), 20000)          # ~25k Medicare FFS slings

  age <- suppressMessages(chia_sling_age_distribution(db = .db))
  p65 <- attr(age, "p_65plus")
  expect_gt(p65, 0.25); expect_lt(p65, 0.60)   # sling is NOT a geriatric-only procedure
})

test_that("the sling route refuses without the FFS share", {
  skip_if_not(file.exists(.root("data-raw","cms_psps","MUP_PHY_R26_P05_V10_D24_Geo.csv")))
  skip_if_not(file.exists(.db))
  r <- suppressMessages(transport_sling_via_medicare(
    db = .db, puf_path = .root("data-raw","cms_psps","MUP_PHY_R26_P05_V10_D24_Geo.csv")))
  expect_true(is.na(r$estimate$national_all_payer))
  expect_equal(r$estimate$evidence_status, "incomplete_transport_needs_ffs_share")
  expect_error(
    suppressMessages(transport_sling_via_medicare(
      db = .db, puf_path = .root("data-raw","cms_psps","MUP_PHY_R26_P05_V10_D24_Geo.csv"),
      ffs_share_of_medicare = 0.5)),
    "without ffs_share_source")
})
