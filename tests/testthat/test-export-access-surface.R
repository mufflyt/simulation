# Tests for export_access_surface() (R/reporting-export_demand_contract.R): the
# upstream half of cliff's Module D v2 seam. Base-R + a temp dir, so the schema,
# provenance stamping, and the validation gate are testable without the E2SFCA /
# isochrone stack.

fake_access_result <- function() {
  list(
    resolved = TRUE,
    access = data.frame(
      demand_id = c("01001020100", "56045951100"),
      access = c(12.5, 3.2),
      n_providers = c(4L, 1L),
      population = c(400, 120),
      access_scaled = c(3125, 2666.7),
      stringsAsFactors = FALSE
    )
  )
}
val_capacity <- list(calibration_status = "fitted_and_geographically_validated",
                     resolved = TRUE)
a_sigma_fit  <- list(sigma = 55, wait_scale = 20,
                     calibration_status = "fitted_to_lizeth_wait_response")

test_that("export_access_surface writes a versioned CSV + manifest with provenance", {
  dir <- withr::local_tempdir()
  res <- export_access_surface(
    fake_access_result(), output_directory = dir,
    sigma_fit = a_sigma_fit, capacity = val_capacity,
    isochrone_run_id = "20260508_ec2_r6i", verbose = FALSE)

  expect_true(file.exists(res$csv_path))
  expect_true(file.exists(res$manifest_path))
  expect_match(basename(res$csv_path), "^access_surface_v0\\.1\\.0\\.csv$")

  df <- utils::read.csv(res$csv_path, stringsAsFactors = FALSE)
  # the columns cliff's read_access_surface() needs, plus provenance
  expect_true(all(c("demand_id", "access", "population",
                    "sigma", "wait_scale", "isochrone_run_id",
                    "calibration_status") %in% names(df)))
  expect_equal(nrow(df), 2L)
  expect_equal(unique(df$sigma), 55)
  expect_equal(unique(df$wait_scale), 20)
  expect_equal(unique(df$isochrone_run_id), "20260508_ec2_r6i")
  expect_equal(unique(df$calibration_status),
               "fitted_and_geographically_validated")
})

test_that("capacity status is preferred over sigma_fit status", {
  res <- export_access_surface(
    fake_access_result(), output_directory = withr::local_tempdir(),
    sigma_fit = a_sigma_fit, capacity = val_capacity,
    isochrone_run_id = "run", verbose = FALSE)
  expect_equal(unique(res$data$calibration_status),
               "fitted_and_geographically_validated")   # not the sigma_fit status
})

test_that("an un-validated surface is refused unless allow_unvalidated = TRUE", {
  dir <- withr::local_tempdir()
  # sigma_fit status is 'fitted_to_lizeth_wait_response' (not validated) and no capacity
  expect_error(
    export_access_surface(fake_access_result(), output_directory = dir,
                          sigma_fit = a_sigma_fit, verbose = FALSE),
    "not .*validated|allow_unvalidated"
  )
  # override emits it, stamped with the honest (weaker) status
  res <- export_access_surface(fake_access_result(), output_directory = dir,
                               sigma_fit = a_sigma_fit, allow_unvalidated = TRUE,
                               verbose = FALSE)
  expect_equal(unique(res$data$calibration_status),
               "fitted_to_lizeth_wait_response")
})

test_that("export_access_surface accepts a bare access data frame", {
  df_in <- fake_access_result()$access
  res <- export_access_surface(df_in, output_directory = withr::local_tempdir(),
                               calibration_status = "calibrated", verbose = FALSE)
  expect_equal(nrow(res$data), 2L)
})

test_that("export_access_surface validates inputs and resolution", {
  expect_error(
    export_access_surface(list(resolved = FALSE, reason = "no membership"),
                          output_directory = withr::local_tempdir(),
                          allow_unvalidated = TRUE, verbose = FALSE),
    "unresolved"
  )
  expect_error(
    export_access_surface(data.frame(demand_id = "01001020100", access = 1),
                          output_directory = withr::local_tempdir(),
                          allow_unvalidated = TRUE, verbose = FALSE),
    "population"
  )
})
