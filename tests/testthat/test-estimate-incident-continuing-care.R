# Unit tests for incident vs continuing care parameter pipeline

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("estimate_incident_continuing_care.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_incident_continuing_care.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("care_engagement_params returns requires_source for unanchored utilization parameters", {
  params <- care_engagement_params()
  expect_s3_class(params, "data.frame")
  expect_equal(nrow(params), 4L)

  expect_equal(sum(params$calibration_status == "requires_source"), 3L)
  inc <- dplyr::filter(params, parameter == "incident_share")
  expect_true(is.na(inc$value))

  fy <- dplyr::filter(params, parameter == "first_year_followup_rate")
  expect_true(is.na(fy$value))
})

test_that("split_care_engagement decomposes stock into incident and continuing care", {
  stock <- FROZEN_CARE_ENGAGED
  split <- split_care_engagement(stock, incident_share = 0.3104)

  expect_s3_class(split, "data.frame")
  expect_equal(nrow(split), 3L)
  expect_true(all(c("condition", "care_engaged", "newly_entering_care", "continuing_care") %in% names(split)))

  # Check conservation identity
  expect_equal(unname(split$newly_entering_care + split$continuing_care), unname(split$care_engaged))

  # Check condition-specific incident shares
  ui_row <- dplyr::filter(split, condition == "ui")
  expect_equal(unname(ui_row$newly_entering_care / ui_row$care_engaged), 0.3420)

  pop_row <- dplyr::filter(split, condition == "pop")
  expect_equal(unname(pop_row$newly_entering_care / pop_row$care_engaged), 0.2850)
})

test_that("assert_care_engagement_gates reports Gate 4 while parameters require source", {
  split <- split_care_engagement(FROZEN_CARE_ENGAGED, incident_share = 0.3104)
  visits <- care_engagement_visits(split, first_year_followup_rate = 1.4820, annual_followup_rate = 1.1250)

  gates <- assert_care_engagement_gates(split, visits)
  expect_false(gates$passed[4])
})
