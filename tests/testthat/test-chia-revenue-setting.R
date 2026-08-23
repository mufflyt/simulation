# Unit tests for build_chia_ub04_setting_evidence()

.repo_path <- function(...) {
  root <- .source_tree_root()
  if (length(root) == 0) root <- ".."
  file.path(root[1], ...)
}

test_that("R/data-chia_revenue_setting.R script exists and has valid syntax", {
  script_path <- .repo_path("R", "data-chia_revenue_setting.R")
  skip_if_not(file.exists(script_path), "Source script absent under R CMD check")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("build_chia_ub04_setting_evidence processes encounter evidence cleanly", {
  cohort_tbl <- tibble::tibble(
    encounter_id = c("E1", "E2", "E3"),
    year = c(2015L, 2016L, 2017L),
    procedure_family = c("pop_hysterectomy", "pop_hysterectomy", "prolapse_repair"),
    los_days = c(2, 2, 1)
  )

  service_tbl <- tibble::tibble(
    encounter_id = c("E1", "E1", "E2", "E3"),
    revenue_code = c("0110", "0360", "0360", "0490")
  )

  res <- build_chia_ub04_setting_evidence(
    cohort_tbl = cohort_tbl,
    service_tbl = service_tbl,
    encounter_id_col = "encounter_id",
    year_col = "year",
    procedure_family_col = "procedure_family",
    revenue_code_col = "revenue_code",
    los_col = "los_days",
    save_encounter_level = FALSE
  )

  expect_type(res, "list")
  expect_true(all(c("encounter_evidence", "annual_trends", "setting_patterns", "revenue_families", "audit") %in% names(res)))
  expect_equal(nrow(res$encounter_evidence), 3L)
})
