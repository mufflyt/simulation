# Joint History Matching Calibration Engine ------------------------------
#
# Identifies parameter combinations consistent with multiple historical targets using
# Gaussian-process emulators and wave-based history matching.
# Explicitly labeled as a non_implausible_ensemble, NOT a posterior distribution sample.

#' Joint Bayesian history matching with Gaussian-process emulators
#'
#' Identifies parameter combinations consistent with multiple historical
#' targets. This function does not estimate a posterior distribution.
#'
#' @param parameter_spec One row per parameter with columns `parameter`,
#'   `lower`, `upper`, and optional `transform` (`identity`, `log`, `logit`).
#' @param historical_targets Target table with columns `target_id`, `metric`,
#'   `year`, `observed`, `observation_sd`, and `discrepancy_sd`.
#' @param simulator Function accepting one named parameter vector and returning
#'   columns `metric`, `year`, and `simulated`.
#' @param initial_runs Number of full-model runs in the initial design.
#' @param candidates_per_wave Emulator candidates evaluated in each wave.
#' @param max_waves Maximum number of history-matching waves.
#' @param cutoff Maximum acceptable implausibility, commonly 3.
#' @param max_implausibility_rank Rank used for the overall implausibility.
#'   Use 1 for the maximum, or 2/3 for robust early waves.
#' @param new_runs_per_wave Full simulator evaluations added per wave.
#' @param seed Random seed.
#' @param save_dir Directory for timestamped artifacts; `NULL` does not save.
#'
#' @return A named list containing the non-implausible ensemble, all simulator
#'   runs, target diagnostics, wave diagnostics, emulator diagnostics, and
#'   metadata. The ensemble is not a posterior sample.
#' @family calibration
#' @concept calibration
#' @export
calibrate_joint_history_matching <- function(
    parameter_spec,
    historical_targets,
    simulator,
    initial_runs = NULL,
    candidates_per_wave = 50000L,
    max_waves = 6L,
    cutoff = 3,
    max_implausibility_rank = 2L,
    new_runs_per_wave = NULL,
    seed = 20260820L,
    save_dir = NULL) {
  required_packages <- c("DiceKriging", "dplyr", "lhs", "purrr", "scales",
                         "tibble", "tidyr")
  missing_packages <- required_packages[
    !base::vapply(required_packages, requireNamespace, logical(1),
                  quietly = TRUE)
  ]
  if (base::length(missing_packages) > 0L) {
    base::stop(
      "Install required packages: ",
      base::paste(missing_packages, collapse = ", "),
      call. = FALSE
    )
  }

  base::message("Joint history matching: validating inputs.")
  parameter_spec <- tibble::as_tibble(parameter_spec)
  historical_targets <- tibble::as_tibble(historical_targets)
  parameter_columns <- c("parameter", "lower", "upper")
  target_columns <- c(
    "target_id", "metric", "year", "observed", "observation_sd",
    "discrepancy_sd"
  )
  check_columns(parameter_spec, parameter_columns, "parameter_spec")
  check_columns(historical_targets, target_columns, "historical_targets")
  if (!base::is.function(simulator)) {
    base::stop("simulator must be a function.", call. = FALSE)
  }
  if (base::anyDuplicated(parameter_spec$parameter) > 0L) {
    base::stop("parameter names must be unique.", call. = FALSE)
  }
  if (base::anyDuplicated(historical_targets$target_id) > 0L) {
    base::stop("target_id values must be unique.", call. = FALSE)
  }
  if (base::any(parameter_spec$lower >= parameter_spec$upper)) {
    base::stop("Every lower bound must be below its upper bound.",
               call. = FALSE)
  }
  if (base::any(historical_targets$observation_sd < 0) ||
      base::any(historical_targets$discrepancy_sd < 0)) {
    base::stop("Target uncertainty standard deviations must be nonnegative.",
               call. = FALSE)
  }
  if (!"transform" %in% base::names(parameter_spec)) {
    parameter_spec$transform <- "identity"
  }
  parameter_spec <- parameter_spec |>
    dplyr::mutate(
      transform = dplyr::coalesce(.data$transform, "identity")
    )
  allowed_transforms <- c("identity", "log", "logit")
  if (base::any(!parameter_spec$transform %in% allowed_transforms)) {
    base::stop("transform must be identity, log, or logit.", call. = FALSE)
  }
  invalid_log <- parameter_spec$transform == "log" &
    parameter_spec$lower <= 0
  invalid_logit <- parameter_spec$transform == "logit" &
    (parameter_spec$lower <= 0 | parameter_spec$upper >= 1)
  if (base::any(invalid_log)) {
    base::stop("Log-transformed parameters require lower > 0.",
               call. = FALSE)
  }
  if (base::any(invalid_logit)) {
    base::stop("Logit-transformed parameters require bounds inside (0, 1).",
               call. = FALSE)
  }

  parameter_count <- base::nrow(parameter_spec)
  initial_runs <- if (base::is.null(initial_runs)) base::max(10L * parameter_count, 200L) else initial_runs
  new_runs_per_wave <- if (base::is.null(new_runs_per_wave)) base::max(2L * parameter_count, 50L) else new_runs_per_wave
  base::message(
    "Inputs: ", parameter_count, " parameters; ",
    base::nrow(historical_targets), " joint historical targets; ",
    initial_runs, " initial simulator runs."
  )
  base::set.seed(seed)

  base::message("Wave 0: generating Latin-hypercube design.")
  design_unit <- lhs::randomLHS(initial_runs, parameter_count)
  base::colnames(design_unit) <- parameter_spec$parameter
  parameter_runs <- scale_unit_design(design_unit, parameter_spec) |>
    tibble::as_tibble() |>
    dplyr::mutate(run_id = dplyr::row_number(), wave_added = 0L) |>
    dplyr::relocate(.data$run_id, .data$wave_added)

  base::message("Wave 0: running the full simulator.")
  simulation_runs <- run_simulator_design(
    parameter_runs,
    parameter_spec,
    historical_targets,
    simulator
  )
  wave_diagnostics <- tibble::tibble()
  candidate_scores <- tibble::tibble()
  emulator_diagnostics <- tibble::tibble()

  for (wave_index in base::seq_len(max_waves)) {
    base::message("Wave ", wave_index, ": fitting target-specific GPs.")
    training_wide <- simulation_runs |>
      dplyr::select(.data$run_id, .data$target_id, .data$simulated) |>
      tidyr::pivot_wider(
        names_from = .data$target_id,
        values_from = .data$simulated
      ) |>
      dplyr::inner_join(parameter_runs, by = "run_id")
    emulator_bundle <- fit_target_emulators(
      training_wide,
      parameter_spec,
      historical_targets
    )
    emulator_diagnostics <- dplyr::bind_rows(
      emulator_diagnostics,
      emulator_bundle$diagnostics |>
        dplyr::mutate(wave = wave_index)
    )

    base::message("Wave ", wave_index, ": scoring joint candidates.")
    candidate_unit <- lhs::randomLHS(
      candidates_per_wave,
      parameter_count
    )
    base::colnames(candidate_unit) <- parameter_spec$parameter
    candidate_parameters <- scale_unit_design(
      candidate_unit,
      parameter_spec
    ) |>
      tibble::as_tibble() |>
      dplyr::mutate(candidate_id = dplyr::row_number()) |>
      dplyr::relocate(.data$candidate_id)
    candidate_scores <- score_joint_implausibility(
      candidate_parameters,
      emulator_bundle$models,
      parameter_spec,
      historical_targets,
      max_implausibility_rank
    ) |>
      dplyr::mutate(
        wave = wave_index,
        non_implausible = .data$overall_implausibility <= cutoff
      )
    retained_parameters <- candidate_scores |>
      dplyr::filter(.data$non_implausible) |>
      dplyr::select(
        .data$candidate_id,
        dplyr::all_of(parameter_spec$parameter),
        .data$overall_implausibility
      )
    retained_fraction <- base::nrow(retained_parameters) /
      candidates_per_wave
    wave_diagnostics <- dplyr::bind_rows(
      wave_diagnostics,
      tibble::tibble(
        wave = wave_index,
        simulator_runs = base::nrow(parameter_runs),
        candidate_count = candidates_per_wave,
        retained_count = base::nrow(retained_parameters),
        retained_fraction = retained_fraction,
        cutoff = cutoff,
        implausibility_rank = max_implausibility_rank
      )
    )
    base::message(
      "Wave ", wave_index, ": retained ",
      scales::comma(base::nrow(retained_parameters)), " of ",
      scales::comma(candidates_per_wave), " candidates (",
      scales::percent(retained_fraction, accuracy = 0.1), ")."
    )
    if (base::nrow(retained_parameters) == 0L) {
      base::warning(
        "No candidates survived. Review target conflict, discrepancy, and ",
        "emulator fit before tightening the cutoff.",
        call. = FALSE
      )
      break
    }
    if (wave_index == max_waves) {
      break
    }

    selected_count <- base::min(
      new_runs_per_wave,
      base::nrow(retained_parameters)
    )
    selected_candidates <- select_maximin_points(
      retained_parameters,
      parameter_spec,
      selected_count
    ) |>
      dplyr::select(dplyr::all_of(parameter_spec$parameter)) |>
      dplyr::mutate(
        run_id = base::max(parameter_runs$run_id) + dplyr::row_number(),
        wave_added = wave_index
      ) |>
      dplyr::relocate(.data$run_id, .data$wave_added)
    base::message(
      "Wave ", wave_index, ": adding ", selected_count,
      " space-filling simulator runs."
    )
    added_simulations <- run_simulator_design(
      selected_candidates,
      parameter_spec,
      historical_targets,
      simulator
    )
    parameter_runs <- dplyr::bind_rows(
      parameter_runs,
      selected_candidates
    )
    simulation_runs <- dplyr::bind_rows(
      simulation_runs,
      added_simulations
    )
  }

  base::message("Creating target-level diagnostics.")
  target_diagnostics <- candidate_scores |>
    dplyr::filter(.data$non_implausible) |>
    dplyr::select(.data$candidate_id, .data$target_scores) |>
    tidyr::unnest(.data$target_scores) |>
    dplyr::group_by(.data$target_id, .data$metric, .data$year) |>
    dplyr::summarise(
      observed = dplyr::first(.data$observed),
      predicted_mean = base::mean(.data$emulator_mean),
      predicted_sd = stats::sd(.data$emulator_mean),
      predicted_p25 = stats::quantile(.data$emulator_mean, 0.25),
      predicted_median = stats::median(.data$emulator_mean),
      predicted_p75 = stats::quantile(.data$emulator_mean, 0.75),
      mean_implausibility = base::mean(.data$implausibility),
      max_implausibility = base::max(.data$implausibility),
      .groups = "drop"
    )
  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  analysis_bundle <- base::list(
    ensemble_type = "non_implausible_ensemble",
    non_implausible_ensemble = candidate_scores |>
      dplyr::filter(.data$non_implausible) |>
      dplyr::select(-.data$target_scores),
    target_scores = candidate_scores |>
      dplyr::filter(.data$non_implausible) |>
      dplyr::select(.data$candidate_id, .data$target_scores) |>
      tidyr::unnest(.data$target_scores),
    parameter_runs = parameter_runs,
    simulation_runs = simulation_runs,
    target_diagnostics = target_diagnostics,
    wave_diagnostics = wave_diagnostics,
    emulator_diagnostics = emulator_diagnostics,
    metadata = base::list(
      method = "Bayesian history matching with GP emulators",
      interpretation = paste(
        "Parameter vectors not ruled out by the specified observations,",
        "uncertainties, discrepancy terms, emulator uncertainty, and cutoff."
      ),
      posterior_sample = FALSE,
      cutoff = cutoff,
      seed = seed,
      created_utc = timestamp
    )
  )
  if (!base::is.null(save_dir)) {
    base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
    saved_path <- base::file.path(
      save_dir,
      base::paste0("joint_history_matching_", timestamp, ".rds")
    )
    base::saveRDS(analysis_bundle, saved_path)
    base::message("Saved joint calibration artifact: ", saved_path)
  }
  base::message(
    "Complete: returning a non-implausible ensemble, not a posterior."
  )
  analysis_bundle
}



check_columns <- function(table_input, required, object_name) {
  absent <- base::setdiff(required, base::names(table_input))
  if (base::length(absent) > 0L) {
    base::stop(
      object_name, " is missing: ", base::paste(absent, collapse = ", "),
      call. = FALSE
    )
  }
  base::invisible(TRUE)
}

scale_unit_design <- function(unit_matrix, parameter_spec) {
  scaled_matrix <- unit_matrix
  for (column_index in base::seq_len(base::nrow(parameter_spec))) {
    lower_bound <- parameter_spec$lower[[column_index]]
    upper_bound <- parameter_spec$upper[[column_index]]
    scaled_matrix[, column_index] <- lower_bound +
      unit_matrix[, column_index] * (upper_bound - lower_bound)
  }
  scaled_matrix
}

transform_parameters <- function(parameter_table, parameter_spec) {
  transformed_table <- parameter_table |>
    dplyr::select(dplyr::all_of(parameter_spec$parameter))
  for (parameter_index in base::seq_len(base::nrow(parameter_spec))) {
    parameter_name <- parameter_spec$parameter[[parameter_index]]
    transform_name <- parameter_spec$transform[[parameter_index]]
    parameter_value <- transformed_table[[parameter_name]]
    transformed_table[[parameter_name]] <- base::switch(
      transform_name,
      identity = parameter_value,
      log = base::log(parameter_value),
      logit = stats::qlogis(parameter_value)
    )
  }
  base::as.matrix(transformed_table)
}

run_simulator_design <- function(parameter_runs, parameter_spec,
                                  historical_targets, simulator) {
  purrr::map_dfr(
    base::seq_len(base::nrow(parameter_runs)),
    function(run_index) {
      run_row <- parameter_runs[run_index, , drop = FALSE]
      parameter_vector <- base::unlist(
        run_row[parameter_spec$parameter],
        use.names = TRUE
      )
      base::message("Simulator run ", run_row$run_id, ".")
      simulated_targets <- simulator(parameter_vector) |>
        tibble::as_tibble()
      check_columns(
        simulated_targets,
        c("metric", "year", "simulated"),
        "simulator return"
      )
      joined_targets <- historical_targets |>
        dplyr::select(.data$target_id, .data$metric, .data$year) |>
        dplyr::left_join(
          simulated_targets |>
            dplyr::select(.data$metric, .data$year, .data$simulated),
          by = c("metric", "year")
        )
      if (base::anyNA(joined_targets$simulated)) {
        base::stop(
          "The simulator did not return every requested metric-year target.",
          call. = FALSE
        )
      }
      joined_targets |>
        dplyr::mutate(run_id = run_row$run_id) |>
        dplyr::relocate(.data$run_id)
    }
  )
}

fit_target_emulators <- function(training_wide, parameter_spec,
                                  historical_targets) {
  training_matrix <- transform_parameters(training_wide, parameter_spec)
  fitted_models <- purrr::map(
    historical_targets$target_id,
    function(target_name) {
      DiceKriging::km(
        design = training_matrix,
        response = training_wide[[target_name]],
        covtype = "matern5_2",
        nugget.estim = TRUE,
        control = base::list(trace = FALSE)
      )
    }
  )
  base::names(fitted_models) <- historical_targets$target_id
  diagnostics <- purrr::map_dfr(
    historical_targets$target_id,
    function(target_name) {
      fitted_values <- stats::predict(
        fitted_models[[target_name]],
        newdata = training_matrix,
        type = "UK",
        checkNames = FALSE
      )$mean
      residuals <- training_wide[[target_name]] - fitted_values
      tibble::tibble(
        target_id = target_name,
        training_rmse = base::sqrt(base::mean(residuals^2)),
        training_mae = base::mean(base::abs(residuals)),
        training_runs = base::nrow(training_wide)
      )
    }
  )
  base::list(models = fitted_models, diagnostics = diagnostics)
}

score_joint_implausibility <- function(candidate_parameters, fitted_models,
                                        parameter_spec, historical_targets,
                                        implausibility_rank) {
  candidate_matrix <- transform_parameters(
    candidate_parameters,
    parameter_spec
  )
  target_score_table <- purrr::map_dfr(
    base::seq_len(base::nrow(historical_targets)),
    function(target_index) {
      target_row <- historical_targets[target_index, , drop = FALSE]
      prediction <- stats::predict(
        fitted_models[[target_row$target_id]],
        newdata = candidate_matrix,
        type = "UK",
        checkNames = FALSE
      )
      total_variance <- prediction$sd^2 + target_row$observation_sd^2 +
        target_row$discrepancy_sd^2
      tibble::tibble(
        candidate_id = candidate_parameters$candidate_id,
        target_id = target_row$target_id,
        metric = target_row$metric,
        year = target_row$year,
        observed = target_row$observed,
        emulator_mean = prediction$mean,
        emulator_sd = prediction$sd,
        implausibility = base::abs(
          prediction$mean - target_row$observed
        ) / base::sqrt(total_variance)
      )
    }
  )
  overall_scores <- target_score_table |>
    dplyr::group_by(.data$candidate_id) |>
    dplyr::summarise(
      overall_implausibility = dplyr::nth(
        base::sort(.data$implausibility, decreasing = TRUE),
        implausibility_rank,
        default = base::max(.data$implausibility)
      ),
      target_scores = base::list(dplyr::pick(dplyr::everything())),
      .groups = "drop"
    )
  candidate_parameters |>
    dplyr::left_join(overall_scores, by = "candidate_id")
}

select_maximin_points <- function(candidate_parameters, parameter_spec,
                                   selected_count) {
  scaled_candidates <- candidate_parameters |>
    dplyr::select(dplyr::all_of(parameter_spec$parameter)) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        ~ (.x - base::min(.x)) /
          base::max(base::max(.x) - base::min(.x), .Machine$double.eps)
      )
    ) |>
    base::as.matrix()
  selected_indices <- base::integer(selected_count)
  selected_indices[[1L]] <- base::which.min(
    candidate_parameters$overall_implausibility
  )
  minimum_distance <- base::rep(Inf, base::nrow(candidate_parameters))
  if (selected_count > 1L) {
    for (selection_index in 2:selected_count) {
      latest_index <- selected_indices[[selection_index - 1L]]
      latest_distance <- base::sqrt(
        base::rowSums(
          (scaled_candidates - scaled_candidates[latest_index, ])^2
        )
      )
      minimum_distance <- base::pmin(minimum_distance, latest_distance)
      minimum_distance[selected_indices[base::seq_len(selection_index - 1L)]] <-
        -Inf
      selected_indices[[selection_index]] <- base::which.max(minimum_distance)
    }
  }
  candidate_parameters[selected_indices, , drop = FALSE]
}
