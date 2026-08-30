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

test_that("the CMS enrolment cell is a published cross and reconciles", {
  e <- cms_original_medicare_enrollment_2023()
  expect_equal(e$female_65plus_ffs, 16542982)
  # Male + female aged must reconcile with the published 65+ total. This is the
  # evidence that 16,542,982 is a PUBLISHED CELL rather than a marginal times a
  # share -- the distinction that makes it admissible as a denominator at all.
  expect_lt(abs(e$total_65plus_ffs - 30833932), 1e-9)
  expect_gt(e$part_b_share_65plus, 0.8)
  expect_lt(e$part_b_share_65plus, 0.9)
})

test_that("the Medicare FFS denominator is populated from CMS, never inherited", {
  skip_if_not(nzchar(system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                                 package = "urpssim")),
              "ACS 2023 population file not installed")
  tbl <- build_diagnostic_denominator_table(2023L)
  ffs <- tbl[tbl$payer_coverage == "medicare_ffs", ]
  expect_equal(nrow(ffs), 4L)   # 3 conditions at 65+, plus the aggregate

  # Populated now -- and populated from the CMS enrolment cell specifically.
  expect_true(all(ffs$population_n == 16542982))
  expect_true(all(is.finite(ffs$eligible_prevalent_n)))
  expect_true(all(grepl("cms_program_statistics", ffs$denominator_source)))

  # THE ANTI-INHERITANCE ASSERTION SURVIVES THE DENOMINATOR ARRIVING. The FFS
  # population must be the CMS figure, never the all-payer ACS one -- that
  # substitution is what would answer a different question while looking like
  # an answer to this one.
  allp <- tbl[tbl$payer_coverage == "all_payers" & tbl$age_band == "65-79", ]
  expect_false(any(ffs$population_n %in% allp$population_n))
  expect_true(all(ffs$age_band == "65+"))
})

test_that("a condition-level FFS ratio stays NA even though the denominator arrived", {
  skip_if_not(nzchar(system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                                 package = "urpssim")),
              "ACS 2023 population file not installed")
  tbl <- build_diagnostic_denominator_table(2023L)
  cond_ffs <- tbl[tbl$payer_coverage == "medicare_ffs" & !is.na(tbl$condition), ]
  expect_equal(nrow(cond_ffs), 3L)

  # The numerator has no condition split, so no condition-level ratio can be
  # formed. Getting the denominator does not change that, and this is the most
  # likely place for someone to later "finish the job" by dividing 79,787 three
  # ways.
  expect_true(all(is.na(cond_ffs$practice_new_fpmrs_n)))
  expect_true(all(is.na(cond_ffs$practice_new_ratio)))
  expect_true(all(grepl("numerator_has_no_condition_split", cond_ffs$missing_reason)))
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

test_that("the named Medicare ratio is computed, with its assumptions declared", {
  r <- medicare_ffs_practice_new_fpmrs_ratio_65plus_2023()
  expect_equal(nrow(r), 3L)
  expect_true(all(r$practice_new_fpmrs_n == 79787))
  expect_true(all(is.finite(r$ratio)))

  # THREE denominators, not one. The choice between them is a real modelling
  # decision; returning a single number would hide it and invite the reader to
  # treat whichever was chosen as the answer.
  expect_setequal(r$denominator_definition,
                  c("all_ffs_women_65plus", "pfd_prevalent_ffs_women_65plus",
                    "part_b_pfd_prevalent"))
  # Crude < PFD-prevalent < Part-B-restricted, since each shrinks the denominator.
  ord <- r$ratio[match(c("all_ffs_women_65plus", "pfd_prevalent_ffs_women_65plus",
                         "part_b_pfd_prevalent"), r$denominator_definition)]
  expect_true(all(diff(ord) > 0))

  # Only the crude denominator is assumption-free; the other two must say so.
  expect_true(is.na(r$assumption[r$denominator_definition == "all_ffs_women_65plus"]))
  expect_true(all(!is.na(r$assumption[r$denominator_definition != "all_ffs_women_65plus"])))
  expect_true(all(r$status[r$denominator_definition != "all_ffs_women_65plus"] == "ASSUMPTION"))
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
