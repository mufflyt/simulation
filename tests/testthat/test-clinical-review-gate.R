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

test_that("every CHIA procedure family carries an attributable review decision", {
  # This deliberately does NOT assert that families are blocked. They were
  # blocked when written and are now approved, so a point-in-time assertion
  # would have to be flipped every time a review lands -- which trains people to
  # edit the gate's tests. The durable invariant is that the decision is
  # RECORDED and ATTRIBUTABLE, and that blocked is the exact complement of
  # approved. Whether a given family is approved is the reviewer's call, not
  # this test's.
  skip_if_not(file.exists("../../config/chia_urps_inpatient_codes.yml"))
  st <- suppressMessages(clinical_review_status(
    calibration_config = "../../config/calibration_targets.yml",
    family_config = "../../config/chia_urps_inpatient_codes.yml"))
  fams <- st[st$kind == "procedure_family", ]
  expect_gt(nrow(fams), 0)
  expect_true(all(fams$review_status %in% c("approved", "needs_clinical_review",
                                            "not_recorded", "unknown")))
  expect_identical(fams$blocked, fams$review_status != "approved")
  # anything approved must name a reviewer; an anonymous approval is not one
  approved <- fams[fams$review_status == "approved", ]
  if (nrow(approved) > 0) expect_true(all(nzchar(approved$reviewer)))
})

test_that("revision/removal is mutually exclusive with incident sling", {
  skip_if_not(file.exists("../../config/chia_urps_inpatient_codes.yml"))
  fam <- yaml::read_yaml("../../config/chia_urps_inpatient_codes.yml")$families

  sling_primary  <- c(fam$sui_sling$icd9cm$exact, fam$sui_sling$icd10pcs$prefix)
  revision_codes <- c(fam$revision_removal$icd9cm$exact,
                      fam$revision_removal$icd10pcs$prefix)

  # the semantic negative test: incident placement must never contain a
  # revision or removal code, in either direction
  expect_false(any(sling_primary %in% revision_codes))
  expect_false(any(revision_codes %in% sling_primary))

  # and no ICD-10 prefix of one may be a prefix of the other
  expect_false(any(vapply(fam$sui_sling$icd10pcs$prefix, function(a)
    any(startsWith(fam$revision_removal$icd10pcs$prefix, a)), logical(1))))
  expect_false(any(vapply(fam$revision_removal$icd10pcs$prefix, function(a)
    any(startsWith(fam$sui_sling$icd10pcs$prefix, a)), logical(1))))

  expect_false(isTRUE(fam$revision_removal$incident_sling_eligible))
})

test_that("each approved anchor passes on its own named review", {
  # The original form asserted sling and prolapse still ERRORED. Both have since
  # been reviewed and approved, so that assertion now encodes a stale snapshot
  # rather than a rule. What must stay true is anchor-INDEPENDENCE: each anchor
  # is judged on its own clinical_review block, so approving one never unblocks
  # another and a procedure-family blocker never reaches the NAMCS office anchor.
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  cfg <- yaml::read_yaml("../../config/calibration_targets.yml")
  for (nm in names(cfg$anchors)) {
    a <- cfg$anchors[[nm]]
    if (identical(a$clinical_review$status, "approved")) {
      expect_true(suppressMessages(assert_anchor_reviewed(a)), info = nm)
      expect_true(nzchar(a$clinical_review$reviewer), info = nm)
      expect_true(nzchar(a$clinical_review$date), info = nm)
    } else {
      expect_error(suppressMessages(assert_anchor_reviewed(a)),
                   "not approved", info = nm)
    }
  }
})

test_that("the gate still blocks, on a fixture rather than on live config", {
  # Blocking behaviour is tested against synthetic anchors so it cannot decay as
  # the real config gets approved. This is what the two stale expect_error()
  # assertions were actually protecting.
  unreviewed <- list(clinical_review = list(
    status = "needs_clinical_review",
    blockers = c("missing_sui_diagnosis_qualifier"),
    scope = "sling identification"))
  expect_error(suppressMessages(assert_anchor_reviewed(unreviewed)),
               "missing_sui_diagnosis_qualifier")
  expect_error(suppressMessages(assert_anchor_reviewed(unreviewed)), "not approved")

  # an approval with no reviewer or no date is not an approval
  expect_error(suppressMessages(assert_anchor_reviewed(list(clinical_review = list(
    status = "approved", reviewer = "", date = "2026-08-15")))),
    "no named reviewer")
  expect_error(suppressMessages(assert_anchor_reviewed(list(clinical_review = list(
    status = "approved", reviewer = "T Muffly", date = "")))),
    "no date")
  # and an anchor with no review block at all must not sail through
  expect_error(suppressMessages(assert_anchor_reviewed(list())),
               "no clinical-review specification")
})
