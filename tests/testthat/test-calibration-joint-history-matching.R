test_that("calibrate_joint_history_matching validates parameter specs and target tables", {
  param_spec <- tibble::tribble(
    ~parameter, ~lower, ~upper, ~transform,
    "theta1", 0.1, 0.9, "identity",
    "theta2", 1.0, 5.0, "log"
  )

  targets <- tibble::tribble(
    ~target_id, ~metric, ~year, ~observed, ~observation_sd, ~discrepancy_sd,
    "T1", "prevalence", 2023L, 0.25, 0.02, 0.01,
    "T2", "volume", 2023L, 1000.0, 50.0, 25.0
  )

  mock_simulator <- function(params) {
    tibble::tribble(
      ~metric, ~year, ~simulated,
      "prevalence", 2023L, params[["theta1"]] * 0.3,
      "volume", 2023L, params[["theta2"]] * 250.0
    )
  }

  res <- calibrate_joint_history_matching(
    parameter_spec = param_spec,
    historical_targets = targets,
    simulator = mock_simulator,
    initial_runs = 20L,
    candidates_per_wave = 500L,
    max_waves = 2L,
    cutoff = 3.0,
    seed = 20260820L
  )

  expect_named(res, c(
    "ensemble_type", "non_implausible_ensemble", "target_scores",
    "parameter_runs", "simulation_runs", "target_diagnostics",
    "wave_diagnostics", "emulator_diagnostics", "metadata"
  ))
  expect_equal(res$ensemble_type, "non_implausible_ensemble")
  expect_false(res$metadata$posterior_sample)
})
