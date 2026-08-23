# Unit tests for export_access_surface (spatial access contract exporter)

.repo_path <- function(...) {
  root <- .source_tree_root()
  if (length(root) == 0) root <- ".."
  file.path(root[1], ...)
}

test_that("export_access_surface requires valid columns and fails closed on unvalidated status", {
  tmp_dir <- tempfile("access_export_test")
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  bad_df <- tibble::tibble(foo = 1:5)
  expect_error(export_access_surface(bad_df, tmp_dir), "missing column")

  good_df <- tibble::tibble(
    demand_id = c("25025000100", "25025000200"),
    access = c(1.25, 0.85),
    population = c(5000, 4200)
  )

  # Fail-closed gate: uncalibrated status without override throws error
  expect_error(
    export_access_surface(good_df, tmp_dir, calibration_status = "uncalibrated_illustrative"),
    "calibration_status is"
  )
})

test_that("export_access_surface exports valid CSV and manifest JSON when validated or overridden", {
  tmp_dir <- tempfile("access_export_success_test")
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  good_df <- tibble::tibble(
    demand_id = c("25025000100", "25025000200"),
    access = c(1.25, 0.85),
    population = c(5000, 4200)
  )

  # With override
  res <- export_access_surface(
    good_df,
    tmp_dir,
    calibration_status = "uncalibrated_illustrative",
    allow_unvalidated = TRUE,
    verbose = FALSE
  )

  expect_type(res, "list")
  expect_true(file.exists(res$csv_path))
  expect_true(file.exists(res$manifest_path))

  csv_data <- readr::read_csv(res$csv_path, show_col_types = FALSE)
  expect_equal(nrow(csv_data), 2L)
  expect_true(all(c("demand_id", "access", "population", "sigma", "wait_scale", "calibration_status") %in% names(csv_data)))

  # With validated status
  res_val <- export_access_surface(
    good_df,
    tmp_dir,
    calibration_status = "fitted_and_geographically_validated",
    verbose = FALSE
  )

  expect_true(file.exists(res_val$csv_path))
})
