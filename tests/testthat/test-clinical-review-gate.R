# The gate exists because `production_scalar_eligible: false` is a convention,
# and conventions get flipped to unblock a render. These tests assert it is a
# precondition the calibration path cannot route around.

.spec <- function(...) {
  base <- list(anchor_id = "test_anchor", production_scalar_eligible = TRUE,
               clinical_review_status = "approved",
               clinical_reviewer = "A Reviewer",
               clinical_review_date = "2026-08-14")
  utils::modifyList(base, list(...))
}

test_that("a fully reviewed anchor passes", {
  expect_true(suppressMessages(assert_production_scalar_eligible(.spec())))
})

test_that("eligibility alone is not enough without an approved review", {
  expect_error(suppressMessages(assert_production_scalar_eligible(
    .spec(clinical_review_status = "needs_clinical_review"))),
    "Clinical review is not approved")
})

test_that("an approved review cannot be anonymous or undated", {
  expect_error(suppressMessages(assert_production_scalar_eligible(
    .spec(clinical_reviewer = ""))), "Clinical reviewer is missing")
  expect_error(suppressMessages(assert_production_scalar_eligible(
    .spec(clinical_review_date = ""))), "Clinical review date is missing")
})

test_that("a missing field fails loudly rather than defaulting", {
  s <- .spec(); s$clinical_reviewer <- NULL
  expect_error(suppressMessages(assert_production_scalar_eligible(s)),
               "missing: clinical_reviewer")
})

test_that("flipping only the boolean does not unblock an unreviewed anchor", {
  # The failure mode this guards: somebody sets eligible = TRUE to unblock a
  # render without a review having happened.
  expect_error(suppressMessages(assert_production_scalar_eligible(
    .spec(production_scalar_eligible = TRUE,
          clinical_review_status = "needs_clinical_review"))),
    "Clinical review is not approved")
})

test_that("every CHIA procedure family is currently blocked", {
  skip_if_not(file.exists("../../config/chia_urps_inpatient_codes.yml"))
  st <- suppressMessages(clinical_review_status(
    calibration_config = "../../config/calibration_targets.yml",
    family_config = "../../config/chia_urps_inpatient_codes.yml"))
  fams <- st[st$kind == "procedure_family", ]
  expect_gt(nrow(fams), 0)
  expect_true(all(fams$blocked))
})
