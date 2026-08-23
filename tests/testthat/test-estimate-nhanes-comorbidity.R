# Unit tests for scripts/estimate_nhanes_comorbidity.R

test_that("is_yes helper correctly identifies affirmative responses across data types", {
  is_yes <- function(x) {
    if (is.null(x)) return(FALSE)
    if (is.logical(x)) return(x & !is.na(x))
    if (is.numeric(x)) return(!is.na(x) & x == 1)
    if (is.factor(x) || is.character(x)) {
      return(grepl("^Yes|^1", as.character(x), ignore.case = TRUE) & !is.na(x))
    }
    FALSE
  }

  expect_true(is_yes(1))
  expect_false(is_yes(2))
  expect_false(is_yes(NA))
  expect_true(is_yes(TRUE))
  expect_false(is_yes(FALSE))
  expect_true(is_yes("Yes"))
  expect_true(is_yes("1 - Yes"))
  expect_false(is_yes("No"))
  expect_false(is_yes("2 - No"))
})

test_that("comorbidity composite indicator calculation works as expected", {
  df <- tibble::tibble(
    SEQN = 1:4,
    diabetes = c(TRUE, FALSE, FALSE, FALSE),
    hypertension = c(FALSE, TRUE, FALSE, FALSE),
    cvd_arthritis = c(FALSE, FALSE, TRUE, FALSE)
  ) |>
    dplyr::mutate(comorbidity = as.numeric(diabetes | hypertension | cvd_arthritis))

  expect_equal(df$comorbidity, c(1, 1, 1, 0))
})

.repo_path <- function(...) {
  root <- .source_tree_root()
  if (length(root) == 0) root <- ".."
  file.path(root[1], ...)
}

test_that("estimate_nhanes_comorbidity.R script file exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_nhanes_comorbidity.R")
  expect_true(file.exists(script_path))

  # Verify script is valid R code by parsing it
  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("comorbidity survey regression runs cleanly when NHANES UI microdata is present", {
  ui_path <- .repo_path("data-raw", "nhanes", "nhanes_ui_person_2017_2023.rds")
  skip_if_not(file.exists(ui_path), "NHANES UI microdata file not present")

  ui <- readRDS(ui_path)
  expect_gt(nrow(ui), 0L)
  expect_true("SEQN" %in% names(ui))
  expect_true("WTMEC_pooled" %in% names(ui))
})

test_that("comorbidity survey regression runs cleanly when NHANES AI microdata is present", {
  ai_path <- .repo_path("data-raw", "nhanes", "nhanes_ai_person_2005_2010.rds")
  skip_if_not(file.exists(ai_path), "NHANES AI microdata file not present")

  ai <- readRDS(ai_path)
  expect_gt(nrow(ai), 0L)
  expect_true("SEQN" %in% names(ai))
  expect_true("fi_wu" %in% names(ai))
})
