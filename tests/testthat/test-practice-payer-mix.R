# testthat runs from tests/testthat/, not the repo root, so
# load_namcs_pooled()'s CWD-relative default only resolves when invoked
# interactively from the root. Matches the existing fallback idiom used in
# test-namcs-demand-calibration.R.
.pooled_namcs_path <- function() {
  if (file.exists("data-raw/namcs/namcs_pooled_2015_2019.rds")) {
    "data-raw/namcs/namcs_pooled_2015_2019.rds"
  } else {
    file.path("..", "..", "data-raw", "namcs", "namcs_pooled_2015_2019.rds")
  }
}
# LAZY, AND SKIPPING RATHER THAN ERRORING.
#
# This was an eager top-level load_namcs_pooled(). A missing microdata file
# therefore did not skip these tests -- it made the whole FILE unsourceable,
# and testthat reported one error before a single test_that() ran. Since
# data-raw/ is .Rbuildignore'd that was guaranteed under R CMD check and in
# the nightly, and the failure read as a broken package rather than as absent
# input.
#
# Unlike the practice-economics and end-to-end tests, these are ABOUT the
# derivation from microdata -- the reliability floor, the PATWT weighting, the
# provenance attribute. The vendored aggregate is that derivation's OUTPUT, so
# feeding it back in would assert nothing. Skipping is the honest outcome.
.pooled_namcs <- local({
  cached <- NULL
  function() {
    # Uses helper-setup.R's shared guard rather than a private message, so the
    # skip reason matches the pattern already declared in tests/skip-budget.csv.
    # An undeclared skip reason is a hard failure in scripts/ci/check_suite.R --
    # deliberately, since that is what a gate going dark looks like.
    .skip_unless_namcs_pooled_data()
    if (is.null(cached)) cached <<- load_namcs_pooled(.pooled_namcs_path())
    cached
  }
})

testthat::test_that("namcs_urps_payer_mix returns four shares summing to 1", {
  mix <- namcs_urps_payer_mix(.pooled_namcs())

  testthat::expect_setequal(
    mix$payer_tier,
    c("Medicare", "Medicaid", "Private", "Uninsured")
  )
  testthat::expect_equal(sum(mix$share), 1, tolerance = 1e-8)
  testthat::expect_true(all(mix$share >= 0))
  testthat::expect_true(all(mix$n_unweighted > 0))

  provenance <- attr(mix, "provenance")
  testthat::expect_true(!is.null(provenance))
  testthat::expect_match(provenance$source, "NAMCS")
})

testthat::test_that("namcs_urps_payer_mix flags cells below the NCHS reliability floor", {
  mix <- namcs_urps_payer_mix(.pooled_namcs())
  uninsured <- mix[mix$payer_tier == "Uninsured", ]

  # This is a real property of the 2015-2019 pooled NAMCS URPS-visit sample:
  # only a single unweighted record codes PAYTYPER as uninsured, which is
  # below the package's NCHS reliability floor (NAMCS_MIN_RECORDS = 30).
  testthat::expect_lt(uninsured$n_unweighted, NAMCS_MIN_RECORDS)
  testthat::expect_false(uninsured$reliable)

  reliable_tiers <- mix[mix$payer_tier %in% c("Medicare", "Medicaid", "Private"), ]
  testthat::expect_true(all(reliable_tiers$reliable))
})

testthat::test_that("ahrq_3prd_medicare_medicaid_ratio reads the vendored summary", {
  crosscheck <- ahrq_3prd_medicare_medicaid_ratio()

  testthat::expect_equal(nrow(crosscheck), 1L)
  testthat::expect_true(
    crosscheck$medicare_share_of_government_claims > 0 &&
      crosscheck$medicare_share_of_government_claims < 1
  )
  testthat::expect_equal(crosscheck$n_zip3, 325L)
  testthat::expect_gt(crosscheck$total_medicare_claims_permonth, 0)
  testthat::expect_gt(crosscheck$total_medicaid_claims_permonth, 0)
})

testthat::test_that("practice_payer_mix_defaults is NAMCS-derived, not blended with 3P-RD/CHIA", {
  namcs_mix <- namcs_urps_payer_mix(.pooled_namcs())
  defaults <- practice_payer_mix_defaults(namcs_mix, include_crosscheck = TRUE)

  private_row <- namcs_mix[namcs_mix$payer_tier == "Private", ]
  testthat::expect_equal(defaults$commercial_share, private_row$share)

  medicare_row <- namcs_mix[namcs_mix$payer_tier == "Medicare", ]
  testthat::expect_equal(defaults$medicare_share, medicare_row$share)

  total <- defaults$medicare_share + defaults$medicaid_share +
    defaults$commercial_share + defaults$self_pay_share
  testthat::expect_equal(total, 1, tolerance = 1e-8)
  testthat::expect_false(defaults$self_pay_reliable)

  # The 3P-RD ratio (always reachable -- vendored) must be attached for
  # comparison but must NOT have moved medicare_share/medicaid_share away
  # from the NAMCS-only values above -- confirms the explicit "cross-check
  # only, never blended" design. CHIA depends on a live external DuckDB that
  # is not guaranteed to be mounted in every environment (e.g. CI), so it is
  # asserted only when practice_payer_mix_defaults() actually reached it.
  crosschecks <- attr(defaults, "crosschecks")
  testthat::expect_true(!is.null(crosschecks))
  testthat::expect_true("ahrq_3prd" %in% names(crosschecks))
  namcs_medicare_of_government <-
    defaults$medicare_share / (defaults$medicare_share + defaults$medicaid_share)
  testthat::expect_false(isTRUE(all.equal(
    crosschecks$ahrq_3prd$medicare_share_of_government_claims,
    namcs_medicare_of_government
  )))
  if ("chia" %in% names(crosschecks)) {
    pooled_row <- crosschecks$chia[crosschecks$chia$data_year == "pooled", ]
    testthat::expect_false(isTRUE(all.equal(
      pooled_row$medicare_share_of_government_discharges,
      namcs_medicare_of_government
    )))
  }
})

testthat::test_that("practice_payer_mix_defaults can omit the crosschecks attribute", {
  # Deliberately does NOT pass namcs_mix: this asserts a property of
  # practice_payer_mix_defaults() itself, so it must exercise the resolution
  # path -- microdata when present, vendored aggregate otherwise -- and run
  # everywhere rather than skipping wherever data-raw/ is absent.
  defaults <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  testthat::expect_null(attr(defaults, "crosschecks"))
})

testthat::test_that("chia_medicare_medicaid_ratio reads the live CHIA DuckDB when available", {
  chia_path <- .chia_duckdb_default()
  # Wording deliberately matches the pattern already declared in
  # tests/skip-budget.csv. This skip is not new -- it has always been here --
  # but until the file-level load_namcs_pooled() above was made lazy, the whole
  # file errored before reaching it, so scripts/ci/check_suite.R never saw it
  # and it sat undeclared. Making the file sourceable surfaced it.
  testthat::skip_if(
    is.na(chia_path) || !file.exists(chia_path),
    "CHIA case-mix database not attached"
  )

  crosscheck <- chia_medicare_medicaid_ratio()
  testthat::expect_true("pooled" %in% crosscheck$data_year)
  pooled_row <- crosscheck[crosscheck$data_year == "pooled", ]
  testthat::expect_true(
    pooled_row$medicare_share_of_government_discharges > 0 &&
      pooled_row$medicare_share_of_government_discharges < 1
  )
  testthat::expect_gt(pooled_row$medicare_n, 0)
  testthat::expect_gt(pooled_row$medicaid_n, 0)
})

testthat::test_that("practice_payer_mix_defaults feeds simulate_practice_economics cleanly", {
  # As above: this is about the handoff into simulate_practice_economics(),
  # not about the NAMCS derivation, so it takes the resolved default.
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)

  practice_tbl <- tibble::tibble(
    practice_id = "P1",
    year = 2026L,
    clinical_fte = 1,
    annual_wrvu = 10000,
    medicare_share = mix$medicare_share,
    medicaid_share = mix$medicaid_share,
    commercial_share = mix$commercial_share,
    self_pay_share = mix$self_pay_share,
    practice_setting = "independent",
    app_fte = 0.2
  )

  result <- simulate_practice_economics(practice_tbl, draws = 100L)
  testthat::expect_equal(nrow(result$summary), 1L)
  testthat::expect_true(is.finite(result$summary$median_operating_margin))
})
