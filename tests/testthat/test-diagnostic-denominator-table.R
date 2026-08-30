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
  expect_equal(agg$practice_new_fpmrs_services, 79787L)
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

test_that("the service rate is computed with four denominators, correctly ranked", {
  r <- medicare_ffs_practice_new_fpmrs_ratio_65plus_2023()
  expect_equal(nrow(r), 4L)
  expect_true(all(r$practice_new_fpmrs_services == 79787))
  expect_true(all(is.finite(r$services_per_1000)))

  # THE PART B ROW IS THE PRIMARY ONE, and it must exist. Numerator and
  # denominator sit on the same coverage footing, and no disease definition is
  # imposed -- which matters because the numerator carries no diagnosis.
  expect_true("all_part_b_female_65plus" %in% r$denominator_definition)
  expect_equal(r$interpretation[r$denominator_definition == "all_part_b_female_65plus"],
               "primary")

  # The disease-conditioned rows are EXPLORATORY, not estimand-aligned. An
  # earlier version called the 12.05 figure estimand-aligned while its
  # denominator included women without Part B -- i.e. a coverage universe the
  # numerator cannot arise from.
  disease <- r$denominator_definition %in%
    c("disease_stock_aligned_coverage_unrestricted", "coverage_aligned_partb_disease")
  expect_true(all(r$interpretation[disease] == "exploratory"))
  expect_true(all(grepl("numerator_has_no_diagnosis", r$assumption[disease])))

  # Each restriction shrinks the denominator, so rates rise monotonically.
  ord <- r$services_per_1000[match(
    c("all_ffs_women_65plus", "all_part_b_female_65plus",
      "disease_stock_aligned_coverage_unrestricted", "coverage_aligned_partb_disease"),
    r$denominator_definition)]
  expect_true(all(diff(ord) > 0))
  expect_true(is.na(r$assumption[r$denominator_definition == "all_ffs_women_65plus"]))
})

test_that("the PUF numerator is never described as unique women or a probability", {
  # PUF beneficiary counts are computed WITHIN provider/service cells, so their
  # near-equality with service counts (79,785 vs 79,787) shows only that a woman
  # rarely gets the same new-patient code twice from the same provider -- which
  # the billing rules already require. It says nothing about the same woman
  # appearing under a different NPI or code. 1,322 cells over 794 NPIs leaves
  # ample room, and the PUF supplies no key to detect it.
  #
  # The coincidence is seductive, which is why this is a test and not a comment.
  agg <- medicare_ffs_practice_new_fpmrs_2023()
  expect_false(agg$beneficiary_deduplication_possible)
  expect_equal(agg$deduplication_blocked_by,
               "puf_bene_counts_are_within_provider_service_cells")
  # Both raw quantities are kept separate; neither is collapsed into a "count".
  expect_equal(agg$practice_new_fpmrs_services, 79787L)
  expect_equal(agg$summed_bene_cells, 79785L)
  expect_true(agg$n_cells > agg$n_roster_npis_billing)

  # Rate columns must be named as SERVICE rates, never per-woman probabilities.
  r <- medicare_ffs_practice_new_fpmrs_ratio_65plus_2023()
  expect_true("services_per_1000" %in% names(r))
  expect_false(any(grepl("unique_women|unique_benef|probability|per_woman",
                         names(r), ignore.case = TRUE)))
  expect_false(any(grepl("unique_women|unique_benef", names(agg), ignore.case = TRUE)))

  # And the prose must not reintroduce it. The shipped documentation is the
  # surface where "79,787 women" is most likely to reappear.
  doc <- file.path(.repo_root(), "docs", "DIAGNOSTIC_DENOMINATOR_STATUS.md")
  skip_if_not(file.exists(doc), "status doc not present")
  # Strip double-quoted spans: the document must stay free to QUOTE the wrong
  # claim in order to reject it. Same exclusion as the APCD guard, and for the
  # same reason -- a guard its own correction prose trips is a guard that
  # forbids recording the correction.
  txt <- tolower(paste(readLines(doc, warn = FALSE), collapse = " "))
  txt <- gsub('"[^"]*"', " ", txt)
  # Phrases are matched loosely on purpose: the first draft of this guard
  # banned "of prevalent women have a new" and the document said "of prevalent
  # women 65+ have a new", so the guard passed over the exact sentence it was
  # written to catch. Anchor on the shortest distinctive fragment.
  for (phrase in c("unique women", "79,787 women", "unique beneficiaries",
                   "prevalent women", "% of women", "one per person")) {
    expect_false(grepl(phrase, txt, fixed = TRUE),
                 info = paste0("'", phrase, "' describes the PUF numerator as ",
                               "deduplicated people, which it is not."))
  }
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
