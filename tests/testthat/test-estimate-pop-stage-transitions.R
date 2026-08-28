# Unit tests for POP-Q stage transition hazards

test_that("estimate_pop_stage_transitions.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_pop_stage_transitions.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("pop_transition_parameters returns valid POP-Q stage transition matrix", {
  params <- pop_transition_parameters()
  expect_s3_class(params, "data.frame")
  expect_gt(nrow(params), 10L)

  required_cols <- c("transition", "term", "stage_from", "stage_to", "measure", "value", "confidence", "source")
  expect_true(all(required_cols %in% names(params)))

  # Check Stage 1 to 0 regression
  reg1 <- dplyr::filter(params, transition == "regression", stage_from == 1)
  expect_equal(nrow(reg1), 1L)
  expect_equal(reg1$value, 0.20) # 20% spontaneous regression

  # Check Stage 1 to 2 progression
  prog1 <- dplyr::filter(params, transition == "progression", stage_from == 1)
  expect_equal(nrow(prog1), 1L)
  expect_equal(prog1$value, 0.08)
})

test_that("dmdm_transitions_with_pop_literature compiles valid transitions object", {
  tr <- dmdm_transitions_with_pop_literature()
  expect_type(tr, "list")
  expect_equal(tr$calibration_status, "derived_by_analogy")

  expect_named(tr$pop_progression, c("1", "2", "3"))
  expect_named(tr$pop_regression, c("1", "2", "3"))

  expect_equal(unname(tr$pop_regression["1"]), 0.20)
  expect_equal(unname(tr$pop_progression["1"]), 0.08)
})
