#' Simulation-based calibration for joint history matching
#'
#' Runs a known-truth synthetic experiment around
#' `calibrate_joint_history_matching()`. History matching produces a
#' non-implausible set, not necessarily posterior draws. Consequently, raw
#' ensemble ranks are diagnostic ranks. They become formal SBC ranks only when
#' `calibrator_adapter` returns valid posterior weights or posterior draws.
#'
#' Required packages: dplyr, ggplot2, purrr, rlang, tibble, tidyr.
#' Optional package: future.apply for parallel execution.
#'
#' @param n_replicates Number of simulated datasets. Use at least 500.
#' @param prior_sampler Function `(n, seed)` returning one row per draw.
#' @param simulator Function `(parameters, seed)` returning a named target row.
#' @param calibrator_adapter Function `(targets, seed)` returning a list with
#'   `ensemble`, and optionally `weights`, `converged`, and `metadata`.
#' @param parameter_names Names of parameters to rank.
#' @param n_bins Number of rank histogram bins.
#' @param seed Master random seed.
#' @param parallel Whether to use `future.apply::future_lapply()`.
#' @param save_directory Directory for timestamped CSV and PNG files. Set to
#'   `NULL` to skip saving.
#' @param alpha Family-wise unadjusted diagnostic threshold.
#'
#' @return A named list containing ranks, summaries, failures, plots, and paths.
#' @family calibration
#' @concept calibration
#' @export
run_joint_history_matcher_sbc <- function(
    n_replicates = 500L,
    prior_sampler,
    simulator,
    calibrator_adapter,
    parameter_names,
    n_bins = 10L,
    seed = 20260821L,
    parallel = FALSE,
    save_directory = NULL,
    alpha = 0.05) {
  base::message("SBC: validating inputs.")
  check_sbc_inputs(
    n_replicates = n_replicates,
    prior_sampler = prior_sampler,
    simulator = simulator,
    calibrator_adapter = calibrator_adapter,
    parameter_names = parameter_names,
    n_bins = n_bins,
    seed = seed,
    alpha = alpha
  )

  replicate_ids <- base::seq_len(base::as.integer(n_replicates))
  replicate_seeds <- make_sbc_seeds(seed, n_replicates)
  base::message(
    "SBC: running ", base::format(n_replicates, big.mark = ","),
    " known-truth replicates."
  )

  worker <- function(index) {
    run_one_sbc_replicate(
      replicate_id = replicate_ids[[index]],
      replicate_seed = replicate_seeds[[index]],
      prior_sampler = prior_sampler,
      simulator = simulator,
      calibrator_adapter = calibrator_adapter,
      parameter_names = parameter_names
    )
  }

  if (base::isTRUE(parallel)) {
    if (!base::requireNamespace("future.apply", quietly = TRUE)) {
      base::stop("Install future.apply or set parallel = FALSE.")
    }
    base::message("SBC: executing with reproducible future RNG streams.")
    replicate_runs <- future.apply::future_lapply(
      X = base::seq_along(replicate_ids),
      FUN = worker,
      future.seed = seed
    )
  } else {
    replicate_runs <- base::lapply(base::seq_along(replicate_ids), worker)
  }

  failures <- purrr::map_dfr(replicate_runs, "failure")
  rank_draws <- purrr::map_dfr(replicate_runs, "ranks")
  truth_draws <- purrr::map_dfr(replicate_runs, "truth")
  target_draws <- purrr::map_dfr(replicate_runs, "targets")

  if (base::nrow(rank_draws) == 0L) {
    base::stop("All SBC replicates failed; inspect the failure table.")
  }

  base::message("SBC: computing uniformity and coverage diagnostics.")
  rank_summary <- summarise_sbc_ranks(
    rank_draws = rank_draws,
    n_bins = n_bins,
    alpha = alpha
  )
  coverage_summary <- summarise_sbc_coverage(rank_draws)
  rank_plot <- plot_sbc_rank_histograms(rank_draws, n_bins)
  ecdf_plot <- plot_sbc_ecdf(rank_draws)

  saved_paths <- tibble::tibble()
  if (!base::is.null(save_directory)) {
    saved_paths <- save_sbc_artifacts(
      rank_draws = rank_draws,
      rank_summary = rank_summary,
      coverage_summary = coverage_summary,
      failures = failures,
      rank_plot = rank_plot,
      ecdf_plot = ecdf_plot,
      save_directory = save_directory
    )
  }

  completed <- dplyr::n_distinct(rank_draws$replicate_id)
  failed <- base::nrow(failures)
  base::message(
    "SBC: completed ", base::format(completed, big.mark = ","),
    " replicates; ", base::format(failed, big.mark = ","), " failed."
  )

  base::list(
    ranks = rank_draws,
    rank_summary = rank_summary,
    coverage_summary = coverage_summary,
    failures = failures,
    truths = truth_draws,
    targets = target_draws,
    rank_plot = rank_plot,
    ecdf_plot = ecdf_plot,
    saved_paths = saved_paths
  )
}

check_sbc_inputs <- function(n_replicates, prior_sampler, simulator,
                             calibrator_adapter, parameter_names, n_bins,
                             seed, alpha) {
  if (!base::is.numeric(n_replicates) || n_replicates < 20L) {
    base::stop("n_replicates must be at least 20.")
  }
  function_flags <- base::vapply(
    base::list(prior_sampler, simulator, calibrator_adapter),
    base::is.function,
    logical(1)
  )
  if (!base::all(function_flags)) {
    base::stop("Sampler, simulator, and adapter must all be functions.")
  }
  if (!base::is.character(parameter_names) ||
      base::length(parameter_names) == 0L) {
    base::stop("parameter_names must be a non-empty character vector.")
  }
  if (!base::is.numeric(n_bins) || n_bins < 2L) {
    base::stop("n_bins must be at least 2.")
  }
  if (!base::is.numeric(seed) || base::length(seed) != 1L) {
    base::stop("seed must be one number.")
  }
  if (!base::is.numeric(alpha) || alpha <= 0 || alpha >= 1) {
    base::stop("alpha must be between zero and one.")
  }
  base::invisible(TRUE)
}

make_sbc_seeds <- function(seed, n_replicates) {
  base::set.seed(seed)
  base::sample.int(.Machine$integer.max, n_replicates, replace = FALSE)
}

run_one_sbc_replicate <- function(replicate_id, replicate_seed,
                                  prior_sampler, simulator,
                                  calibrator_adapter, parameter_names) {
  tryCatch(
    {
      truth_seed <- replicate_seed
      simulation_seed <- base::as.integer((replicate_seed + 1L) %%
        .Machine$integer.max)
      calibration_seed <- base::as.integer((replicate_seed + 2L) %%
        .Machine$integer.max)

      truth <- tibble::as_tibble(prior_sampler(1L, truth_seed))
      require_sbc_columns(truth, parameter_names, "prior draw")
      if (base::nrow(truth) != 1L) {
        base::stop("prior_sampler(1, seed) must return exactly one row.")
      }

      targets <- tibble::as_tibble(simulator(truth, simulation_seed))
      calibration <- calibrator_adapter(targets, calibration_seed)
      validate_calibration_result(calibration, parameter_names)
      ensemble <- tibble::as_tibble(calibration$ensemble)
      weights <- calibration$weights %||%
        base::rep(1 / base::nrow(ensemble), base::nrow(ensemble))
      rank_kind <- if (base::is.null(calibration$weights)) {
        "history_matching_diagnostic"
      } else {
        "weighted_sbc"
      }

      ranks <- purrr::map_dfr(
        parameter_names,
        function(parameter_name) {
          calculate_randomized_rank(
            truth_value = truth[[parameter_name]][[1]],
            draws = ensemble[[parameter_name]],
            weights = weights,
            seed = calibration_seed +
              base::match(parameter_name, parameter_names)
          ) |>
            dplyr::mutate(
              replicate_id = replicate_id,
              parameter = parameter_name,
              rank_kind = rank_kind,
              ensemble_size = base::nrow(ensemble),
              .before = 1
            )
        }
      )

      list(
        ranks = ranks,
        truth = dplyr::mutate(truth, replicate_id = replicate_id,
                              .before = 1),
        targets = dplyr::mutate(targets, replicate_id = replicate_id,
                                .before = 1),
        failure = tibble::tibble()
      )
    },
    error = function(error_condition) {
      base::message(
        "SBC: replicate ", replicate_id, " failed: ",
        base::conditionMessage(error_condition)
      )
      list(
        ranks = tibble::tibble(),
        truth = tibble::tibble(),
        targets = tibble::tibble(),
        failure = tibble::tibble(
          replicate_id = replicate_id,
          seed = replicate_seed,
          message = base::conditionMessage(error_condition)
        )
      )
    }
  )
}

`%||%` <- function(left, right) {
  if (base::is.null(left)) right else left
}

require_sbc_columns <- function(table, columns, label) {
  missing_columns <- base::setdiff(columns, base::names(table))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      label, " is missing: ", base::paste(missing_columns, collapse = ", ")
    )
  }
  base::invisible(TRUE)
}

validate_calibration_result <- function(calibration, parameter_names) {
  if (!base::is.list(calibration) || base::is.null(calibration$ensemble)) {
    base::stop("Adapter must return list(ensemble = ...).")
  }
  ensemble <- tibble::as_tibble(calibration$ensemble)
  require_sbc_columns(ensemble, parameter_names, "calibration ensemble")
  if (base::nrow(ensemble) < 2L) {
    base::stop("Calibration ensemble must contain at least two draws.")
  }
  if (!base::is.null(calibration$converged) &&
      !base::isTRUE(calibration$converged)) {
    base::stop("Calibration adapter reported non-convergence.")
  }
  if (!base::is.null(calibration$weights)) {
    weights <- calibration$weights
    if (base::length(weights) != base::nrow(ensemble) ||
        base::any(!base::is.finite(weights)) || base::any(weights < 0) ||
        base::sum(weights) <= 0) {
      base::stop("weights must be finite, nonnegative, and match ensemble.")
    }
  }
  base::invisible(TRUE)
}

calculate_randomized_rank <- function(truth_value, draws, weights, seed) {
  valid <- base::is.finite(draws) & base::is.finite(weights)
  draws <- draws[valid]
  weights <- weights[valid]
  weights <- weights / base::sum(weights)
  if (!base::is.finite(truth_value) || base::length(draws) < 2L) {
    base::stop("Truth and at least two ensemble draws must be finite.")
  }

  lower_mass <- base::sum(weights[draws < truth_value])
  equal_mass <- base::sum(weights[draws == truth_value])
  base::set.seed(base::as.integer(seed %% .Machine$integer.max))
  randomized_quantile <- lower_mass +
    stats::runif(1L, min = 0, max = equal_mass)
  if (equal_mass == 0) {
    randomized_quantile <- lower_mass
  }
  randomized_quantile <- base::min(
    base::max(randomized_quantile, 0), 1
  )

  weighted_mean <- stats::weighted.mean(draws, weights)
  weighted_variance <- base::sum(
    weights * (draws - weighted_mean)^2
  )
  tibble::tibble(
    rank_quantile = randomized_quantile,
    truth = truth_value,
    ensemble_mean = weighted_mean,
    ensemble_sd = base::sqrt(weighted_variance),
    interval_50_lower = weighted_quantile(draws, weights, 0.25),
    interval_50_upper = weighted_quantile(draws, weights, 0.75),
    interval_90_lower = weighted_quantile(draws, weights, 0.05),
    interval_90_upper = weighted_quantile(draws, weights, 0.95),
    interval_95_lower = weighted_quantile(draws, weights, 0.025),
    interval_95_upper = weighted_quantile(draws, weights, 0.975)
  )
}

weighted_quantile <- function(values, weights, probability) {
  ordering <- base::order(values)
  sorted_values <- values[ordering]
  cumulative_weights <- base::cumsum(weights[ordering]) /
    base::sum(weights[ordering])
  sorted_values[[base::which(cumulative_weights >= probability)[[1]]]]
}

summarise_sbc_ranks <- function(rank_draws, n_bins, alpha) {
  rank_draws |>
    dplyr::group_by(parameter, rank_kind) |>
    dplyr::group_modify(
      function(group_rows, group_key) {
        sample_size <- base::nrow(group_rows)
        ks_test <- stats::ks.test(
          group_rows$rank_quantile,
          "punif",
          exact = FALSE
        )
        breaks <- base::seq(0, 1, length.out = n_bins + 1L)
        observed <- base::tabulate(
          base::findInterval(
            group_rows$rank_quantile,
            breaks,
            all.inside = TRUE
          ),
          nbins = n_bins
        )
        bin_probabilities <- base::rep(1 / n_bins, n_bins)
        chi_test <- stats::chisq.test(
          x = observed,
          p = bin_probabilities
        )
        tibble::tibble(
          n = sample_size,
          mean_rank = base::mean(group_rows$rank_quantile),
          rank_sd = stats::sd(group_rows$rank_quantile),
          ks_statistic = base::unname(ks_test$statistic),
          ks_p_value = ks_test$p.value,
          chi_square = base::unname(chi_test$statistic),
          chi_square_p_value = chi_test$p.value,
          nominal_alpha = alpha
        )
      }
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      ks_p_adjusted = stats::p.adjust(ks_p_value, method = "holm"),
      chi_square_p_adjusted = stats::p.adjust(
        chi_square_p_value,
        method = "holm"
      ),
      passes_ks = ks_p_adjusted >= alpha,
      passes_chi_square = chi_square_p_adjusted >= alpha
    )
}

summarise_sbc_coverage <- function(rank_draws) {
  rank_draws |>
    dplyr::mutate(
      covered_50 = truth >= interval_50_lower &
        truth <= interval_50_upper,
      covered_90 = truth >= interval_90_lower &
        truth <= interval_90_upper,
      covered_95 = truth >= interval_95_lower &
        truth <= interval_95_upper
    ) |>
    tidyr::pivot_longer(
      cols = dplyr::starts_with("covered_"),
      names_to = "interval",
      values_to = "covered"
    ) |>
    dplyr::group_by(parameter, rank_kind, interval) |>
    dplyr::summarise(
      n = dplyr::n(),
      coverage = base::mean(covered),
      standard_error = base::sqrt(coverage * (1 - coverage) / n),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      nominal_coverage = base::as.numeric(
        base::sub("covered_", "", interval)
      ) / 100,
      coverage_difference = coverage - nominal_coverage
    )
}

plot_sbc_rank_histograms <- function(rank_draws, n_bins = 10L) {
  ggplot2::ggplot(rank_draws, ggplot2::aes(x = rank_quantile)) +
    ggplot2::geom_histogram(
      breaks = base::seq(0, 1, length.out = n_bins + 1L),
      color = "white",
      fill = "#2C7FB8"
    ) +
    ggplot2::facet_wrap(ggplot2::vars(parameter), scales = "free_y") +
    ggplot2::labs(
      x = "Randomized rank quantile",
      y = "Replicates",
      title = "Simulation-based calibration rank histograms"
    ) +
    ggplot2::theme_minimal(base_size = 11)
}

plot_sbc_ecdf <- function(rank_draws) {
  ggplot2::ggplot(
    rank_draws,
    ggplot2::aes(x = rank_quantile, color = parameter)
  ) +
    ggplot2::stat_ecdf(linewidth = 0.7) +
    ggplot2::geom_abline(
      slope = 1,
      intercept = 0,
      color = "grey30",
      linetype = "dashed"
    ) +
    ggplot2::coord_equal() +
    ggplot2::labs(
      x = "Randomized rank quantile",
      y = "Empirical cumulative probability",
      title = "SBC empirical CDF versus Uniform(0, 1)"
    ) +
    ggplot2::theme_minimal(base_size = 11)
}

save_sbc_artifacts <- function(rank_draws, rank_summary, coverage_summary,
                               failures, rank_plot, ecdf_plot,
                               save_directory) {
  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_directory, recursive = TRUE, showWarnings = FALSE)
  file_specs <- tibble::tribble(
    ~artifact, ~path,
    "ranks", base::file.path(
      save_directory, base::paste0("sbc_ranks_", timestamp, ".csv")
    ),
    "rank_summary", base::file.path(
      save_directory,
      base::paste0("sbc_rank_summary_", timestamp, ".csv")
    ),
    "coverage_summary", base::file.path(
      save_directory,
      base::paste0("sbc_coverage_summary_", timestamp, ".csv")
    ),
    "failures", base::file.path(
      save_directory, base::paste0("sbc_failures_", timestamp, ".csv")
    ),
    "rank_plot", base::file.path(
      save_directory,
      base::paste0("sbc_rank_histograms_", timestamp, ".png")
    ),
    "ecdf_plot", base::file.path(
      save_directory, base::paste0("sbc_ecdf_", timestamp, ".png")
    )
  )

  utils::write.csv(rank_draws, file_specs$path[[1]], row.names = FALSE)
  utils::write.csv(rank_summary, file_specs$path[[2]], row.names = FALSE)
  utils::write.csv(
    coverage_summary,
    file_specs$path[[3]],
    row.names = FALSE
  )
  utils::write.csv(failures, file_specs$path[[4]], row.names = FALSE)
  ggplot2::ggsave(file_specs$path[[5]], rank_plot, width = 10, height = 7)
  ggplot2::ggsave(file_specs$path[[6]], ecdf_plot, width = 8, height = 7)
  purrr::walk(file_specs$path, ~ base::message("SBC: saved ", .x))
  file_specs
}

#' Example prior for a four-parameter workforce DGP
#' @family calibration
#' @concept calibration
#' @export
example_sbc_prior <- function(n, seed) {
  base::set.seed(seed)
  tibble::tibble(
    entrant_rate = stats::rlnorm(n, base::log(50), 0.15),
    annual_exit_hazard = stats::rbeta(n, 3, 70),
    productivity_growth = stats::rnorm(n, 0.005, 0.006),
    care_seeking_rate = stats::rbeta(n, 18, 7)
  )
}

#' Example synthetic target simulator
#' @family calibration
#' @concept calibration
#' @export
example_sbc_simulator <- function(parameters, seed) {
  base::set.seed(seed)
  tibble::tibble(
    headcount_2024 = stats::rnorm(
      1,
      1050 + 8 * parameters$entrant_rate -
        3000 * parameters$annual_exit_hazard,
      20
    ),
    clinical_fte_2024 = stats::rnorm(
      1,
      850 + 4 * parameters$entrant_rate -
        1200 * parameters$annual_exit_hazard,
      16
    ),
    claims_volume_2024 = stats::rnorm(
      1,
      800000 * parameters$care_seeking_rate *
        (1 + parameters$productivity_growth),
      15000
    ),
    median_wait_days_2024 = stats::rnorm(
      1,
      55 - 25 * parameters$productivity_growth -
        0.15 * parameters$entrant_rate,
      4
    )
  )
}

#' Build an adapter around calibrate_joint_history_matching()
#'
#' Change argument names and extraction functions once, rather than changing
#' the SBC engine. If `weight_extractor` is NULL, ranks are explicitly labeled
#' as history-matching diagnostics rather than formal posterior SBC.
#'
#' @family calibration
#' @concept calibration
#' @export
make_joint_history_matcher_adapter <- function(
    calibration_function = calibrate_joint_history_matching,
    fixed_arguments = list(),
    ensemble_extractor = function(fit) fit$non_implausible_parameters,
    weight_extractor = NULL,
    convergence_extractor = function(fit) TRUE) {
  base::force(calibration_function)
  base::force(fixed_arguments)
  base::force(ensemble_extractor)
  base::force(weight_extractor)
  base::force(convergence_extractor)

  function(targets, seed) {
    base::message("SBC adapter: calibrating synthetic targets.")
    call_arguments <- base::c(
      base::list(historical_targets = targets, seed = seed),
      fixed_arguments
    )
    fit <- base::do.call(calibration_function, call_arguments)
    extracted_weights <- if (base::is.null(weight_extractor)) {
      NULL
    } else {
      weight_extractor(fit)
    }
    base::list(
      ensemble = ensemble_extractor(fit),
      weights = extracted_weights,
      converged = convergence_extractor(fit)
    )
  }
}
