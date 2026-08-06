# Contract tests for R/data-swan_incontinence_panel.R
#
# Written before the module (TDD). These tests define what
# build_swan_incontinence_panel() must guarantee. Every fixture is
# constructed in memory, so the file runs in < 1 second and needs no
# SIMULATION_DATA_ROOT.

make_wide_visit_frame <- function(swan_ids,
                                  ever_column,
                                  frequency_column,
                                  amount_column,
                                  ever_values,
                                  frequency_values,
                                  amount_values) {
  wide_visit_frame <- tibble::tibble(SWANID = swan_ids)
  wide_visit_frame[[ever_column]] <- ever_values
  wide_visit_frame[[frequency_column]] <- frequency_values
  if (!is.na(amount_column)) {
    wide_visit_frame[[amount_column]] <- amount_values
  }
  wide_visit_frame
}

visit_00_fixture <- function() {
  make_wide_visit_frame(
    swan_ids = c(101L, 102L, 103L),
    ever_column = "INVOLEA0",
    frequency_column = "DAYSLEA0",
    amount_column = "AMTLEAK0",
    ever_values = c("(2) Yes", "(1) No", "(2) Yes"),
    frequency_values = c("(3) 1-3 times/week", "(-1) Not applicable",
                         "(4) Every day"),
    amount_values = c("(1) Drop", "(-1) Not applicable", "(4) Wet floor")
  )
}

visit_07_fixture <- function() {
  make_wide_visit_frame(
    swan_ids = c(101L, 102L, 103L),
    ever_column = "LEKINVO7",
    frequency_column = "LEKDAYS7",
    amount_column = "LEKAMNT7",
    ever_values = c("(2) Yes", "(2) Yes", "(1) No"),
    frequency_values = c("(2) 1-3 times/month", "(4) Every day",
                         "(-1) Not applicable"),
    amount_values = c("(2) Small amount", "(3) Large amount",
                      "(-1) Not applicable")
  )
}

test_that("a single verified visit reshapes to one row per participant", {
  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`0` = visit_00_fixture()),
    verbose = FALSE
  )

  testthat::expect_s3_class(swan_incontinence_panel, "tbl_df")
  testthat::expect_equal(nrow(swan_incontinence_panel), 3L)
  testthat::expect_true(all(
    c("swan_id", "visit", "leakage_ever", "frequency_level",
      "amount_level", "amount_scope") %in%
      names(swan_incontinence_panel)
  ))
  testthat::expect_equal(unique(swan_incontinence_panel$visit), 0L)
})

test_that("parenthetical SWAN labels are stripped to integer codes", {
  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`0` = visit_00_fixture()),
    verbose = FALSE
  )

  testthat::expect_equal(
    swan_incontinence_panel$frequency_level,
    c(3L, NA_integer_, 4L)
  )
  testthat::expect_equal(
    swan_incontinence_panel$amount_level,
    c(1L, NA_integer_, 4L)
  )
  testthat::expect_equal(
    swan_incontinence_panel$leakage_ever,
    c(TRUE, FALSE, TRUE)
  )
})

test_that("negative SWAN sentinel codes become NA, never real levels", {
  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`0` = visit_00_fixture()),
    verbose = FALSE
  )

  testthat::expect_false(any(
    swan_incontinence_panel$frequency_level < 0L,
    na.rm = TRUE
  ))
  testthat::expect_false(any(
    swan_incontinence_panel$amount_level < 0L,
    na.rm = TRUE
  ))
})

test_that("stacking visits does not duplicate participant-visit rows", {
  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(
      `0` = visit_00_fixture(),
      `7` = visit_07_fixture()
    ),
    verbose = FALSE
  )

  participant_visit_keys <- paste(
    swan_incontinence_panel$swan_id,
    swan_incontinence_panel$visit
  )
  testthat::expect_equal(
    length(participant_visit_keys),
    length(unique(participant_visit_keys))
  )
  testthat::expect_equal(nrow(swan_incontinence_panel), 6L)
})

test_that("an unverified visit fails loudly instead of guessing names", {
  unverified_visit_frame <- make_wide_visit_frame(
    swan_ids = 101L,
    ever_column = "INVOLEA3",
    frequency_column = "DAYSLEA3",
    amount_column = "AMTLEAK3",
    ever_values = "(2) Yes",
    frequency_values = "(3) 1-3 times/week",
    amount_values = "(1) Drop"
  )

  testthat::expect_error(
    build_swan_incontinence_panel(
      swan_visit_frames = list(`3` = unverified_visit_frame),
      verbose = FALSE
    ),
    regexp = "unverified"
  )
})

test_that("visit 5 is admitted but carries no amount item", {
  visit_05_frame <- tibble::tibble(
    SWANID = c(101L, 102L),
    INVOLEA5 = c("(2) Yes", "(2) Yes"),
    DAYSLEA5 = c("(3) 1-3 times/week", "(4) Every day")
  )

  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`5` = visit_05_frame),
    verbose = FALSE
  )

  testthat::expect_true(all(is.na(swan_incontinence_panel$amount_level)))
  testthat::expect_equal(
    unique(swan_incontinence_panel$amount_scope),
    "none"
  )
})

test_that("HALL OF SHAME: visit 10 mixes DAYSLEA with LEKAMNT", {
  # Visit 10 is the one visit that pairs the early frequency name with
  # the late amount name. A map that assumes the prefixes move together
  # silently drops the amount item here and every visit-10 woman scores
  # as non-severe. Fixture named after the failure.
  mixed_naming_visit_10 <- readr::read_csv(
    testthat::test_path("fixtures",
                        "mixed_naming_visit10_hall_of_shame.csv"),
    show_col_types = FALSE
  )

  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`10` = mixed_naming_visit_10),
    verbose = FALSE
  )

  testthat::expect_false(all(is.na(swan_incontinence_panel$amount_level)))
  testthat::expect_equal(
    swan_incontinence_panel$amount_level,
    c(1L, 3L, 4L)
  )
})

test_that("stress-specific amount scope is recorded, not silently pooled", {
  visit_12_frame <- tibble::tibble(
    SWANID = c(101L, 102L),
    INVOLEA12 = c("(2) Yes", "(2) Yes"),
    LEKDAYS12 = c("(3) 1-3 times/week", "(4) Every day"),
    LEKAMNT12 = c("(2) Small amount", "(3) Large amount"),
    URGEAMT12 = c("(1) Drop", "(4) Wet floor")
  )

  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`12` = visit_12_frame),
    verbose = FALSE
  )

  testthat::expect_equal(
    unique(swan_incontinence_panel$amount_scope),
    "stress_specific"
  )
  testthat::expect_equal(
    swan_incontinence_panel$urge_amount_level,
    c(1L, 4L)
  )
})

test_that("a missing SWANID column is rejected at the door", {
  frame_without_identifier <- tibble::tibble(
    ARCHID = 101L,
    INVOLEA0 = "(2) Yes",
    DAYSLEA0 = "(3) 1-3 times/week",
    AMTLEAK0 = "(1) Drop"
  )

  testthat::expect_error(
    build_swan_incontinence_panel(
      swan_visit_frames = list(`0` = frame_without_identifier),
      verbose = FALSE
    ),
    regexp = "SWANID"
  )
})

test_that("verbose changes only the console, never the panel contents", {
  quiet_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(`0` = visit_00_fixture()),
    verbose = FALSE
  )
  testthat::expect_message(
    loud_panel <- build_swan_incontinence_panel(
      swan_visit_frames = list(`0` = visit_00_fixture()),
      verbose = TRUE
    ),
    regexp = "swan-panel"
  )

  # Provenance carries a build timestamp, so compare the data itself.
  attr(quiet_panel, "swan_panel_provenance") <- NULL
  attr(loud_panel, "swan_panel_provenance") <- NULL
  testthat::expect_equal(quiet_panel, loud_panel)
})

test_that("PROPERTY: every returned row has a non-missing swan_id and visit", {
  swan_incontinence_panel <- build_swan_incontinence_panel(
    swan_visit_frames = list(
      `0` = visit_00_fixture(),
      `7` = visit_07_fixture()
    ),
    verbose = FALSE
  )

  testthat::expect_false(any(is.na(swan_incontinence_panel$swan_id)))
  testthat::expect_false(any(is.na(swan_incontinence_panel$visit)))
})
