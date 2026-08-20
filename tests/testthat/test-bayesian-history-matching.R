test_that("compute_implausibility_metric calculates correct I(x) value", {
  val <- compute_implausibility_metric(
    simulator_mean = 1200,
    simulator_sd = 100,
    observed = 1000,
    observation_se = 50,
    discrepancy_sd = 100
  )

  # (1200 - 1000) / sqrt(100^2 + 50^2 + 100^2) = 200 / sqrt(10000 + 2500 + 10000) = 200 / sqrt(22500) = 200 / 150 = 1.333333
  expect_equal(round(val, 4), 1.3333)
})

test_that("generate_lhs_parameter_design creates bounded parameter settings", {
  defaults <- load_default_history_matching_inputs()
  design <- generate_lhs_parameter_design(defaults$parameter_spec, n_samples = 50L)

  expect_s3_class(design, "tbl_df")
  expect_equal(nrow(design), 50L)
  expect_named(design, c("care_seeking_rate", "annual_exit_hazard", "graduate_entry_rate"))
  expect_true(all(design$care_seeking_rate >= 0.10 & design$care_seeking_rate <= 0.60))
})

test_that("calibrate_bayesian_history_matching executes waves and projects 2025-2050", {
  defaults <- load_default_history_matching_inputs()

  mock_simulator <- function(parameters, years, seed) {
    # Simple deterministic response function for mock simulator
    csr <- parameters$care_seeking_rate
    aeh <- parameters$annual_exit_hazard
    ger <- parameters$graduate_entry_rate

    dplyr::bind_rows(
      tibble::tibble(year = years, metric = "ui_visits", value = 100000 + csr * 50000),
      tibble::tibble(year = years, metric = "sling_services", value = 15000 + ger * 100)
    )
  }

  res <- calibrate_bayesian_history_matching(
    parameter_spec = defaults$parameter_spec,
    benchmark_table = defaults$benchmark_table,
    workforce_simulator = mock_simulator,
    n_waves = 2L,
    initial_samples = 40L,
    n_posterior_draws = 10L,
    save_directory = NULL
  )

  expect_type(res, "list")
  expect_named(res, c("wave_history", "posterior_parameters", "projections", "projection_summary", "ess", "saved_files"))
  expect_s3_class(res$posterior_parameters, "tbl_df")
  expect_s3_class(res$projection_summary, "tbl_df")
  expect_true(res$ess > 0)
  expect_true(all(res$projection_summary$year %in% 2025:2050))
})

test_that("build_urps_prior_specification constructs 10-parameter literature prior table", {
  spec <- build_urps_prior_specification()

  expect_s3_class(spec, "tbl_df")
  expect_equal(nrow(spec), 10L)
  expect_true("identifiability" %in% names(spec))
  expect_true("nuisance_informative" %in% spec$identifiability)
})
