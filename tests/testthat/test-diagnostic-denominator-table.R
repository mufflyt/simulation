# THE NEGATIVE CONTROLS ARE THE POINT OF THIS FILE.
#
# The scientific distinction being encoded is that the canonical entry rate's
# denominator is the UPSTREAM eligible prevalent stock (prevalence x
# p_eligible), and NOT any quantity carrying recognition, seeking, referral or
# treatment. `pathway_stage_entrants()` sets conservative-stage `entering` to
# `treated[[cond]]` unchanged, and `.lifecourse_treated()` builds `treated` as
# prevalence x recognition x p_seek x p_referral x p_eligible x p_treated -- so
# `79787 / entering` would count recognition, seeking and referral twice.
#
# That reasoning currently lives in a code comment and in a session. Comments do
# not fail builds. These tests do.

test_that("the upstream denominator guard accepts only the declared upstream stock", {
  expect_true(assert_upstream_denominator(
    DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
    "acs5_2023_sex_by_age_state;pfd_prevalence_by_band(UI);p_eligible"
  ))

  # An undeclared or differently-declared estimand is refused even when the
  # source string is innocent: declaration is required, not inferred, because a
  # numeric vector carries no evidence of how it was built.
  expect_error(
    assert_upstream_denominator("prevalent_stock", "acs5_2023;p_eligible"),
    "only 'upstream_eligible_prevalent_stock' is admissible"
  )
  expect_error(
    assert_upstream_denominator(DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG, ""),
    "denominator_source must be a non-empty string"
  )
})

test_that("NEGATIVE CONTROL: downstream quantities are rejected as denominators", {
  # Each of these is a real name from the production pipeline. Every one of them
  # is numerically plausible as a denominator and scientifically wrong.
  downstream <- c(
    "treated_national",
    ".lifecourse_treated(pop, pathway)",
    "treated",
    "pathway_stage_entrants()$entering",
    "entering",
    "conservative_stage_volume",
    "pathway_service_volumes(treated, year)",
    "stage_volume for new_consultation",
    "care_seeking rate from MEPS",
    "p_referral x p_treated product",
    "recognition-adjusted prevalence"
  )
  for (src in downstream) {
    expect_error(
      assert_upstream_denominator(DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG, src),
      "downstream of the entry process",
      info = paste0("'", src, "' must be refused as a denominator: it carries ",
                    "recognition/seeking/referral/treatment, which the entry ",
                    "rate already absorbs.")
    )
  }
})

test_that("the table is built, shaped, and carries required provenance columns", {
  skip_if_not(nzchar(system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                                 package = "urpssim")),
              "ACS 2023 population file not installed")
  tbl <- build_diagnostic_denominator_table(2023L)

  required <- c("condition", "year", "age_band", "payer_coverage",
                "population_n", "prevalent_n", "eligible_prevalent_n",
                "practice_new_fpmrs_n", "practice_new_ratio",
                "numerator_source", "denominator_source",
                "numerator_estimand", "denominator_estimand",
                "status", "missing_reason")
  expect_true(all(required %in% names(tbl)),
              info = paste("missing:", paste(setdiff(required, names(tbl)),
                                             collapse = ", ")))
  expect_true(isTRUE(attr(tbl, "diagnostic_only")))
  # Every row declares the upstream tag; nothing may claim another provenance.
  expect_true(all(tbl$denominator_estimand == DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG))
})

test_that("the Medicare FFS denominator stays MISSING rather than falling back", {
  skip_if_not(nzchar(system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                                 package = "urpssim")),
              "ACS 2023 population file not installed")
  tbl <- build_diagnostic_denominator_table(2023L)
  ffs <- tbl[tbl$payer_coverage == "medicare_ffs", ]
  expect_gt(nrow(ffs), 0L)

  # THE CENTRAL ASSERTION. No CMS enrolment file exists, so every Medicare FFS
  # denominator must be NA -- never quietly filled from the all-payer national
  # population, which is the exact substitution that would look like an answer
  # while answering a different question.
  expect_true(all(is.na(ffs$population_n)))
  expect_true(all(is.na(ffs$prevalent_n)))
  expect_true(all(is.na(ffs$eligible_prevalent_n)))
  expect_true(all(is.na(ffs$practice_new_ratio)))
  expect_true(all(ffs$status == "BLOCKED"))
  expect_true(all(ffs$missing_reason == "missing_cms_ffs_enrollment_denominator"))

  # And it must not have silently inherited the all-payer value for its band.
  allp <- tbl[tbl$payer_coverage == "all_payers" & tbl$condition == "ui" &
                tbl$age_band == "65-79", ]
  expect_true(is.finite(allp$eligible_prevalent_n[[1]]))
  ffs_ui <- ffs[!is.na(ffs$condition) & ffs$condition == "ui" &
                  ffs$age_band == "65-79", ]
  expect_true(is.na(ffs_ui$eligible_prevalent_n[[1]]))
})

test_that("the 79,787 aggregate is preserved and never allocated across conditions", {
  agg <- medicare_ffs_practice_new_fpmrs_2023()
  expect_equal(agg$practice_new_fpmrs_n, 79787L)
  expect_false(agg$condition_split_available)
  expect_equal(agg$condition_split_blocked_by,
               "part_b_puf_carries_no_diagnosis_field")

  skip_if_not(nzchar(system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                                 package = "urpssim")),
              "ACS 2023 population file not installed")
  tbl <- build_diagnostic_denominator_table(2023L)
  carrying <- tbl[!is.na(tbl$practice_new_fpmrs_n), ]

  # Exactly ONE row carries the count, and its condition is NA. If it were split
  # three ways -- by prevalence share or anything else -- three condition-
  # specific numbers would exist where the source supports one aggregate, and
  # the canonical blocker needs three INDEPENDENTLY estimated rates.
  expect_equal(nrow(carrying), 1L)
  expect_true(is.na(carrying$condition[[1]]))
  expect_equal(carrying$practice_new_fpmrs_n[[1]], 79787)
  expect_true(all(is.na(tbl$practice_new_fpmrs_n[!is.na(tbl$condition)])))
})

test_that("the named Medicare ratio returns NA with a machine-readable reason", {
  r <- medicare_ffs_practice_new_fpmrs_ratio_65plus_2023()
  expect_true(is.na(r$ratio))
  expect_true(is.na(r$eligible_prevalent_n))
  expect_equal(r$status, "BLOCKED")
  expect_equal(r$missing_reason, "missing_cms_ffs_enrollment_denominator")
  # The numerator is real and stays visible; only the ratio is unresolvable.
  expect_equal(r$practice_new_fpmrs_n, 79787L)
})

test_that("the diagnostic quantity is not named like a canonical parameter", {
  # Naming is load-bearing. `per_entering` and `annual_first_urps_entry_rate`
  # are canonical parameters with settled definitions; this is a practice-new
  # consultation ratio in one payer stratum. A shared name is how a diagnostic
  # gets adopted as an estimate by inheritance.
  nm <- "medicare_ffs_practice_new_fpmrs_ratio_65plus_2023"
  expect_true(exists(nm))
  expect_false(grepl("per_entering", nm, fixed = TRUE))
  expect_false(grepl("annual_first_urps_entry_rate", nm, fixed = TRUE))

  cols <- names(medicare_ffs_practice_new_fpmrs_ratio_65plus_2023())
  expect_false("per_entering" %in% cols)
  expect_false("annual_first_urps_entry_rate" %in% cols)
})

test_that("the diagnostic table does not touch production per_entering", {
  # Readiness stays BLOCKED and the shipped values stay put. This file is
  # diagnostic infrastructure; if it ever moved a production parameter that
  # would be a scientific change wearing a diagnostic label.
  pw <- condition_service_pathway()
  nc <- pw[pw$service == "new_consultation" & pw$stage == "conservative", ]
  expect_true(all(nc$per_entering == 1.00),
              info = paste("The shipped conservative-stage per_entering must",
                           "remain 1.00 and refusing. Resolving it is a data",
                           "task, not a diagnostic-table task."))
})
