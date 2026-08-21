test_that("calibrate_joint_history_matching runs wave-based history matching with GP emulators", {
  skip_if_not_installed("DiceKriging")
  skip_if_not_installed("lhs")

  param_spec <- tibble::tribble(
    ~parameter, ~lower, ~upper, ~transform,
    "p1", 0.1, 1.0, "identity",
    "p2", 10.0, 50.0, "identity"
  )

  hist_targets <- tibble::tribble(
    ~target_id, ~metric, ~year, ~observed, ~observation_sd, ~discrepancy_sd,
    "t1", "workforce_count", 2025, 100.0, 5.0, 2.0
  )

  mock_simulator <- function(params) {
    # Simple deterministic simulator response
    val <- params[["p1"]] * 100 + params[["p2"]]
    tibble::tribble(
      ~metric, ~year, ~simulated,
      "workforce_count", 2025, val
    )
  }

  res <- calibrate_joint_history_matching(
    parameter_spec = param_spec,
    historical_targets = hist_targets,
    simulator = mock_simulator,
    initial_runs = 15L,
    candidates_per_wave = 200L,
    max_waves = 2L,
    cutoff = 3.0,
    new_runs_per_wave = 5L,
    seed = 42L
  )

  expect_named(res, c("ensemble_type", "non_implausible_ensemble", "target_scores",
                      "parameter_runs", "simulation_runs", "target_diagnostics",
                      "wave_diagnostics", "emulator_diagnostics", "metadata"))
  expect_equal(res$ensemble_type, "non_implausible_ensemble")
  expect_false(res$metadata$posterior_sample)
  expect_gt(nrow(res$wave_diagnostics), 0)
})
