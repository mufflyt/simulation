# Contract tests for R/39-severity_sandvik.R
#
# Written before the module (TDD). Sandvik's index is a published
# instrument with a fixed arithmetic; these tests lock that arithmetic
# so no future refactor can quietly redefine "severe".

sandvik_input_fixture <- function() {
  tibble::tibble(
    swan_id = c(101L, 102L, 103L, 104L, 105L),
    visit = rep(0L, 5L),
    leakage_ever = c(TRUE, TRUE, TRUE, TRUE, FALSE),
    frequency_level = c(1L, 2L, 4L, 4L, NA_integer_),
    amount_level = c(1L, 2L, 3L, 4L, NA_integer_),
    urge_amount_level = rep(NA_integer_, 5L),
    amount_scope = rep("overall", 5L)
  )
}

test_that("the index is frequency times amount on the Sandvik scale", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )

  # SWAN amount 4 (wet floor) collapses onto Sandvik amount 3 by default.
  testthat::expect_equal(
    severity_scored_panel$sandvik_index,
    c(1L, 4L, 12L, 12L, NA_integer_)
  )
})

test_that("the index only ever takes the eight attainable products", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )

  attainable_products <- c(1L, 2L, 3L, 4L, 6L, 8L, 9L, 12L)
  testthat::expect_true(all(
    severity_scored_panel$sandvik_index %in%
      c(attainable_products, NA_integer_)
  ))
})

test_that("published cutpoints map onto the four named categories", {
  every_combination <- tidyr::expand_grid(
    frequency_level = 1:4,
    amount_level = 1:3
  )
  every_combination$swan_id <- seq_len(nrow(every_combination))
  every_combination$visit <- 0L
  every_combination$leakage_ever <- TRUE
  every_combination$urge_amount_level <- NA_integer_
  every_combination$amount_scope <- "overall"

  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = every_combination,
    verbose = FALSE
  )

  testthat::expect_equal(
    as.character(
      severity_scored_panel$sandvik_category[
        severity_scored_panel$sandvik_index == 1L
      ]
    ),
    "slight"
  )
  testthat::expect_true(all(
    as.character(severity_scored_panel$sandvik_category[
      severity_scored_panel$sandvik_index %in% c(3L, 4L, 6L)
    ]) == "moderate"
  ))
  testthat::expect_true(all(
    as.character(severity_scored_panel$sandvik_category[
      severity_scored_panel$sandvik_index %in% c(8L, 9L)
    ]) == "severe"
  ))
  testthat::expect_equal(
    as.character(
      severity_scored_panel$sandvik_category[
        severity_scored_panel$sandvik_index == 12L
      ]
    ),
    "very_severe"
  )
})

test_that("the category is an ordered factor so comparisons are meaningful", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )

  testthat::expect_s3_class(
    severity_scored_panel$sandvik_category,
    "ordered"
  )
  testthat::expect_equal(
    levels(severity_scored_panel$sandvik_category),
    c("slight", "moderate", "severe", "very_severe")
  )
})

test_that("continent women score NA, not zero", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )

  continent_row <- severity_scored_panel[
    !severity_scored_panel$leakage_ever,
  ]
  testthat::expect_true(is.na(continent_row$sandvik_index))
  testthat::expect_true(is.na(continent_row$sandvik_category))
  testthat::expect_false(continent_row$surgical_threshold_met)
})

test_that("the wet-floor collapse is an argument, not a hard-coded rule", {
  four_level_crosswalk <- c(`1` = 1L, `2` = 2L, `3` = 3L, `4` = 3L)
  conservative_crosswalk <- c(`1` = 1L, `2` = 2L, `3` = 2L, `4` = 3L)

  default_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    amount_crosswalk = four_level_crosswalk,
    verbose = FALSE
  )
  alternative_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    amount_crosswalk = conservative_crosswalk,
    verbose = FALSE
  )

  testthat::expect_false(
    identical(default_panel$sandvik_index,
              alternative_panel$sandvik_index)
  )
})

test_that("an unmapped amount level errors rather than silently NA-ing", {
  panel_with_stray_level <- sandvik_input_fixture()
  panel_with_stray_level$amount_level[1] <- 9L

  testthat::expect_error(
    score_sandvik_severity(
      swan_incontinence_panel = panel_with_stray_level,
      verbose = FALSE
    ),
    regexp = "unmapped"
  )
})

test_that("the surgical threshold is configurable and defaults to severe", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )
  testthat::expect_equal(
    severity_scored_panel$surgical_threshold_met,
    c(FALSE, FALSE, TRUE, TRUE, FALSE)
  )

  permissive_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    surgical_threshold = "moderate",
    verbose = FALSE
  )
  testthat::expect_equal(
    permissive_panel$surgical_threshold_met,
    c(FALSE, TRUE, TRUE, TRUE, FALSE)
  )
})

test_that("calibration provenance rides along on the returned object", {
  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = sandvik_input_fixture(),
    verbose = FALSE
  )

  severity_provenance <- attr(severity_scored_panel, "severity_provenance")
  testthat::expect_type(severity_provenance, "list")
  testthat::expect_equal(
    severity_provenance$instrument_tier,
    "calibrated"
  )
  testthat::expect_equal(
    severity_provenance$amount_crosswalk_tier,
    "derived_by_analogy"
  )
})

test_that("stress-specific amounts are refused for an overall severity claim", {
  stress_scoped_panel <- sandvik_input_fixture()
  stress_scoped_panel$amount_scope <- "stress_specific"

  testthat::expect_warning(
    score_sandvik_severity(
      swan_incontinence_panel = stress_scoped_panel,
      verbose = FALSE
    ),
    regexp = "stress_specific"
  )
})

test_that("PROPERTY: severity is monotone in both Sandvik inputs", {
  every_combination <- tidyr::expand_grid(
    frequency_level = 1:4,
    amount_level = 1:3
  )
  every_combination$swan_id <- seq_len(nrow(every_combination))
  every_combination$visit <- 0L
  every_combination$leakage_ever <- TRUE
  every_combination$urge_amount_level <- NA_integer_
  every_combination$amount_scope <- "overall"

  severity_scored_panel <- score_sandvik_severity(
    swan_incontinence_panel = every_combination,
    verbose = FALSE
  )

  holding_amount_fixed <- severity_scored_panel[
    severity_scored_panel$amount_level == 2L,
  ]
  holding_amount_fixed <- holding_amount_fixed[
    order(holding_amount_fixed$frequency_level),
  ]
  testthat::expect_false(
    is.unsorted(holding_amount_fixed$sandvik_index)
  )
})
