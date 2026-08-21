test_that("run_joint_history_matcher_sbc executes known-truth replicates and returns diagnostics", {
  # Mock simple calibration function
  mock_calibrator <- function(historical_targets, seed, cutoff = 3) {
    base::set.seed(seed)
    # Generate 50 mock non-implausible parameter draws
    draws <- tibble::tibble(
      entrant_rate = stats::rnorm(50, mean = 50, sd = 5),
      annual_exit_hazard = stats::rnorm(50, mean = 0.04, sd = 0.005)
    )
    list(non_implausible_parameters = draws)
  }

  adapter <- make_joint_history_matcher_adapter(
    calibration_function = mock_calibrator,
    fixed_arguments = list(cutoff = 3)
  )

  prior_sampler <- function(n, seed) {
    base::set.seed(seed)
    tibble::tibble(
      entrant_rate = stats::rnorm(n, 50, 5),
      annual_exit_hazard = stats::rnorm(n, 0.04, 0.005)
    )
  }

  simulator <- function(params, seed) {
    base::set.seed(seed)
    tibble::tibble(
      target_headcount = 1000 + params$entrant_rate * 5 - params$annual_exit_hazard * 1000
    )
  }

  res <- run_joint_history_matcher_sbc(
    n_replicates = 20L,
    prior_sampler = prior_sampler,
    simulator = simulator,
    calibrator_adapter = adapter,
    parameter_names = c("entrant_rate", "annual_exit_hazard"),
    n_bins = 5L,
    seed = 123L,
    save_directory = NULL
  )

  expect_true(is.list(res))
  expect_named(res, c(
    "ranks", "rank_summary", "coverage_summary", "failures",
    "truths", "targets", "rank_plot", "ecdf_plot", "saved_paths"
  ))
  expect_equal(nrow(res$ranks), 40L) # 20 replicates x 2 parameters
  expect_equal(unique(res$ranks$rank_kind), "history_matching_diagnostic")
  expect_true("passes_ks" %in% names(res$rank_summary))
  expect_true("coverage" %in% names(res$coverage_summary))
})

test_that("run_joint_history_matcher_sbc distinguishes weighted SBC from unweighted diagnostics", {
  mock_weighted_calibrator <- function(historical_targets, seed) {
    base::set.seed(seed)
    draws <- tibble::tibble(
      p1 = stats::rnorm(30, 10, 2)
    )
    weights <- rep(1 / 30, 30)
    list(draws = draws, weights = weights)
  }

  adapter <- make_joint_history_matcher_adapter(
    calibration_function = mock_weighted_calibrator,
    ensemble_extractor = function(fit) fit$draws,
    weight_extractor = function(fit) fit$weights
  )

  prior_sampler <- function(n, seed) {
    base::set.seed(seed)
    tibble::tibble(p1 = stats::rnorm(n, 10, 2))
  }

  simulator <- function(params, seed) {
    tibble::tibble(t1 = params$p1 + 1)
  }

  res <- run_joint_history_matcher_sbc(
    n_replicates = 20L,
    prior_sampler = prior_sampler,
    simulator = simulator,
    calibrator_adapter = adapter,
    parameter_names = "p1",
    seed = 999L
  )

  expect_equal(unique(res$ranks$rank_kind), "weighted_sbc")
})
