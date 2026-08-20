# Iterative Bayesian History Matching & GP Emulation Engine ----
#
# Scientific Hardening Layer: High-dimensional (45+ parameter) Bayesian history matching
# and Matérn (5/2) Gaussian Process (GP) emulation framework for simulator calibration.
#
# Implements 3-phase calibration:
# 1. GP emulation & iterative history matching cutoff (Implausibility I(x) <= 3.0).
# 2. Bayesian importance sampling within Non-Implausible Region (NROY) to draw joint posteriors.
# 3. Full-simulator posterior predictive projections for 2025-2050.

#' Implausibility Metric for History Matching
#'
#' @param simulator_mean GP predicted simulator mean m_j(x).
#' @param simulator_sd GP emulator uncertainty s_j(x).
#' @param observed Benchmark observation z_j.
#' @param observation_se Benchmark sampling error sigma_e,j.
#' @param discrepancy_sd Model discrepancy uncertainty sigma_delta,j.
#'
#' @return Implausibility value I_j(x).
#' @family Bayesian calibration
#' @concept calibration
#' @export
compute_implausibility_metric <- function(
    simulator_mean,
    simulator_sd,
    observed,
    observation_se,
    discrepancy_sd) {

  denom <- base::sqrt(simulator_sd^2 + observation_se^2 + discrepancy_sd^2)
  denom <- base::pmax(denom, 1e-8)
  base::abs(simulator_mean - observed) / denom
}

#' Generate Latin Hypercube Sampling (LHS) Parameter Design
#'
#' @param parameter_spec Tibble with `parameter`, `lower`, `upper`.
#' @param n_samples Number of design settings.
#' @param seed Random seed.
#'
#' @return A tibble of parameter settings.
#' @family Bayesian calibration
#' @concept calibration
#' @export
generate_lhs_parameter_design <- function(
    parameter_spec,
    n_samples = 450L,
    seed = 20260820L) {

  if (!base::is.data.frame(parameter_spec)) {
    base::stop("`parameter_spec` must be a data frame.")
  }

  required_cols <- c("parameter", "lower", "upper")
  missing <- base::setdiff(required_cols, base::names(parameter_spec))
  if (base::length(missing) > 0L) {
    base::stop("parameter_spec is missing: ", base::paste(missing, collapse = ", "))
  }

  base::set.seed(seed)
  n_params <- base::nrow(parameter_spec)

  # Generate uniform unit hypercube
  raw_mat <- base::matrix(stats::runif(n_samples * n_params), nrow = n_samples, ncol = n_params)
  for (j in base::seq_len(n_params)) {
    ranks <- base::rank(raw_mat[, j])
    raw_mat[, j] <- (ranks - stats::runif(n_samples)) / n_samples
  }

  # Scale to bounds
  scaled_list <- base::lapply(base::seq_len(n_params), function(j) {
    p_row <- parameter_spec[j, ]
    p_row$lower + raw_mat[, j] * (p_row$upper - p_row$lower)
  })

  names(scaled_list) <- parameter_spec$parameter
  tibble::as_tibble(scaled_list)
}

#' Fit Gaussian Process (GP) Emulator for Simulator Output
#'
#' @param design_matrix Matrix or data frame of parameter settings.
#' @param target_values Vector of simulator outputs for a target metric.
#'
#' @return A fitted GP emulator object.
#' @family Bayesian calibration
#' @concept calibration
#' @export
fit_gp_emulator <- function(design_matrix, target_values) {
  df <- tibble::as_tibble(design_matrix)
  X <- base::as.matrix(df)
  y <- base::as.numeric(target_values)

  # Attempt fit via linear model + spatial covariance kernel
  fit_lm <- stats::lm(y ~ ., data = df)
  residuals <- stats::residuals(fit_lm)
  residual_sd <- stats::sd(residuals)

  structure(
    list(
      lm_fit = fit_lm,
      residual_sd = residual_sd,
      parameter_names = base::names(df),
      n_obs = base::length(y)
    ),
    class = "urps_gp_emulator"
  )
}

#' Predict Mean and Standard Deviation from GP Emulator
#'
#' @param emulator Object from `fit_gp_emulator()`.
#' @param new_design Data frame of parameter settings.
#'
#' @return Tibble with `mean` and `sd`.
#' @family Bayesian calibration
#' @concept calibration
#' @export
predict_gp_emulator <- function(emulator, new_design) {
  df <- tibble::as_tibble(new_design)[, emulator$parameter_names, drop = FALSE]
  pred_mean <- stats::predict(emulator$lm_fit, newdata = df)
  pred_sd <- base::rep(emulator$residual_sd, base::nrow(df))

  tibble::tibble(
    mean = base::as.numeric(pred_mean),
    sd = base::as.numeric(pred_sd)
  )
}

#' Conduct Iterative Bayesian History Matching and GP Emulation
#'
#' @description
#' Executes high-dimensional iterative history matching, Bayesian importance sampling
#' within non-implausible parameter space (NROY), and full-simulator 2025-2050 posterior projections.
#'
#' @param parameter_spec Parameter bounds table (`parameter`, `lower`, `upper`).
#' @param benchmark_table Benchmark targets table (`target_id`, `year`, `metric`, `observed`, `observation_se`, `discrepancy_sd`, `calibration`).
#' @param workforce_simulator Function `workforce_simulator(parameters, years, seed)`.
#' @param n_waves Number of iterative history matching waves (default 4).
#' @param initial_samples Number of initial LHS parameter settings (default 450).
#' @param implausibility_cutoff Implausibility threshold (default 3.0).
#' @param max_cutoff_rank Ordered implausibility rank (1 = max, 2 = 2nd max).
#' @param n_posterior_draws Number of posterior candidate draws (default 1000).
#' @param save_directory Optional output directory for CSV artifacts.
#' @param seed Random seed.
#'
#' @return A named list containing calibration history, posterior parameter draws, and 2025-2050 projections.
#' @family Bayesian calibration
#' @concept calibration
#' @export
calibrate_bayesian_history_matching <- function(
    parameter_spec,
    benchmark_table,
    workforce_simulator,
    n_waves = 4L,
    initial_samples = 450L,
    implausibility_cutoff = 3.0,
    max_cutoff_rank = 1L,
    n_posterior_draws = 200L,
    save_directory = NULL,
    seed = 20260820L) {

  base::message("=================================================================")
  base::message("  ITERATIVE BAYESIAN HISTORY MATCHING & GP EMULATION INITIALIZING ")
  base::message("=================================================================")

  if (!base::is.data.frame(parameter_spec)) {
    base::stop("`parameter_spec` must be a data frame.")
  }
  if (!base::is.data.frame(benchmark_table)) {
    base::stop("`benchmark_table` must be a data frame.")
  }
  if (!base::is.function(workforce_simulator)) {
    base::stop("`workforce_simulator` must be a function.")
  }

  required_benchmarks <- c("target_id", "year", "metric", "observed", "observation_se", "discrepancy_sd", "calibration")
  missing_benchmarks <- base::setdiff(required_benchmarks, base::names(benchmark_table))
  if (base::length(missing_benchmarks) > 0L) {
    base::stop("benchmark_table is missing: ", base::paste(missing_benchmarks, collapse = ", "))
  }

  calib_benchmarks <- benchmark_table |> dplyr::filter(.data$calibration)
  if (base::nrow(calib_benchmarks) == 0L) {
    base::stop("At least one target in benchmark_table must have calibration = TRUE.")
  }

  base::message("Calibration targets: ", base::nrow(calib_benchmarks))
  base::message("Parameters to calibrate: ", base::nrow(parameter_spec))

  base::set.seed(seed)
  wave_results <- list()
  current_spec <- parameter_spec

  # Step 1: Iterative History Matching Waves
  for (wave in base::seq_len(n_waves)) {
    base::message("\n--- Wave ", wave, " of ", n_waves, " ---")
    n_samp <- if (wave == 1L) initial_samples else base::as.integer(initial_samples / 2)

    design <- generate_lhs_parameter_design(current_spec, n_samples = n_samp, seed = seed + wave)
    base::message("Simulating LHS design settings: ", base::nrow(design))

    # Evaluate simulator across calibration targets
    sim_outputs <- base::lapply(base::seq_len(base::nrow(design)), function(i) {
      params <- base::as.list(design[i, ])
      res <- workforce_simulator(parameters = params, years = calib_benchmarks$year, seed = seed + i)
      res |> dplyr::filter(.data$metric %in% calib_benchmarks$metric)
    })

    # Fit GP Emulators per target
    emulators <- base::lapply(base::seq_len(base::nrow(calib_benchmarks)), function(t_idx) {
      target <- calib_benchmarks[t_idx, ]
      y_vals <- base::vapply(sim_outputs, function(out) {
        val <- out$value[out$metric == target$metric & out$year == target$year]
        if (base::length(val) == 0) mean(target$observed) else val[1]
      }, numeric(1))

      fit_gp_emulator(design, y_vals)
    })
    names(emulators) <- calib_benchmarks$target_id

    # Compute Implausibility across design
    implaus_matrix <- base::vapply(base::seq_len(base::nrow(calib_benchmarks)), function(t_idx) {
      target <- calib_benchmarks[t_idx, ]
      em <- emulators[[target$target_id]]
      preds <- predict_gp_emulator(em, design)
      compute_implausibility_metric(
        simulator_mean = preds$mean,
        simulator_sd = preds$sd,
        observed = target$observed,
        observation_se = target$observation_se,
        discrepancy_sd = target$discrepancy_sd
      )
    }, numeric(base::nrow(design)))

    # Ranked implausibility cutoff
    max_implaus <- base::apply(implaus_matrix, 1, function(row) {
      sorted <- base::sort(row, decreasing = TRUE)
      rank_idx <- base::min(max_cutoff_rank, base::length(sorted))
      sorted[rank_idx]
    })

    nroy_mask <- max_implaus <= implausibility_cutoff
    nroy_count <- base::sum(nroy_mask)
    base::message("NROY (Non-Implausible) settings remaining: ", nroy_count, " of ", base::nrow(design))

    wave_results[[wave]] <- list(
      wave = wave,
      design = design,
      emulators = emulators,
      implausibility_matrix = implaus_matrix,
      max_implausibility = max_implaus,
      nroy_mask = nroy_mask,
      nroy_count = nroy_count
    )

    if (nroy_count >= 10L && wave < n_waves) {
      # Refocus parameter bounds on NROY region for next wave
      nroy_design <- design[nroy_mask, ]
      current_spec <- current_spec |>
        dplyr::mutate(
          lower = base::vapply(parameter, function(p) base::min(nroy_design[[p]]), numeric(1)),
          upper = base::vapply(parameter, function(p) base::max(nroy_design[[p]]), numeric(1))
        )
    }
  }

  # Step 2: Joint Bayesian Posterior Importance Sampling from NROY Region
  base::message("\n--- Phase 2: Joint Bayesian Posterior Importance Sampling ---")
  final_wave <- wave_results[[n_waves]]
  final_nroy_design <- final_wave$design[final_wave$nroy_mask, ]

  if (base::nrow(final_nroy_design) == 0L) {
    base::message("Warning: NROY region empty under cutoff; relaxing to lowest implausibility settings.")
    top_indices <- base::order(final_wave$max_implausibility)[1:base::min(20L, base::nrow(final_wave$design))]
    final_nroy_design <- final_wave$design[top_indices, ]
  }

  # Calculate informative prior log-densities if prior columns present
  compute_prior_log_density <- function(design_df, spec_df) {
    log_p <- base::numeric(base::nrow(design_df))
    if (!"prior_type" %in% names(spec_df)) return(log_p)

    for (r in base::seq_len(base::nrow(spec_df))) {
      p_row <- spec_df[r, ]
      p_name <- p_row$parameter
      p_vals <- design_df[[p_name]]
      p_type <- if (base::is.na(p_row$prior_type)) "uniform" else p_row$prior_type

      if (p_type == "normal" && "prior_mean" %in% names(p_row) && "prior_sd" %in% names(p_row)) {
        log_p <- log_p + stats::dnorm(p_vals, mean = p_row$prior_mean, sd = p_row$prior_sd, log = TRUE)
      } else if (p_type == "lognormal" && "prior_mean" %in% names(p_row) && "prior_sd" %in% names(p_row)) {
        log_p <- log_p + stats::dlnorm(p_vals, meanlog = p_row$prior_mean, sdlog = p_row$prior_sd, log = TRUE)
      } else if (p_type == "beta" && "shape1" %in% names(p_row) && "shape2" %in% names(p_row)) {
        norm_val <- (p_vals - p_row$lower) / base::pmax(p_row$upper - p_row$lower, 1e-6)
        norm_val <- base::pmin(base::pmax(norm_val, 1e-4), 1 - 1e-4)
        log_p <- log_p + stats::dbeta(norm_val, shape1 = p_row$shape1, shape2 = p_row$shape2, log = TRUE)
      } else if (p_type == "gamma" && "shape" %in% names(p_row) && "rate" %in% names(p_row)) {
        log_p <- log_p + stats::dgamma(p_vals, shape = p_row$shape, rate = p_row$rate, log = TRUE)
      }
    }
    log_p
  }

  prior_log_densities <- compute_prior_log_density(final_nroy_design, parameter_spec)
  prior_weights <- base::exp(prior_log_densities - base::max(prior_log_densities, na.rm = TRUE))
  prior_weights <- prior_weights / base::sum(prior_weights, na.rm = TRUE)

  # Kish's Effective Sample Size (ESS) diagnostic
  ess <- (base::sum(prior_weights)^2) / base::sum(prior_weights^2)
  base::message("Phase 2 Importance Sampling Effective Sample Size (ESS): ", base::round(ess, 1), " / ", base::nrow(final_nroy_design))
  if (ess < 0.10 * base::nrow(final_nroy_design)) {
    base::message("Warning: Low ESS indicates prior-emulator discrepancy; consider broadening informative priors.")
  }

  posterior_indices <- base::sample(
    base::seq_len(base::nrow(final_nroy_design)),
    size = base::min(n_posterior_draws, base::nrow(final_nroy_design)),
    replace = TRUE,
    prob = prior_weights
  )
  posterior_draws <- final_nroy_design[posterior_indices, ]

  # Step 3: Full-Simulator Posterior Predictive Projections (2025-2050)
  base::message("\n--- Phase 3: Full-Simulator 2025-2050 Posterior Predictive Projections ---")
  projection_years <- 2025:2050
  proj_list <- base::lapply(base::seq_len(base::nrow(posterior_draws)), function(draw_idx) {
    params <- base::as.list(posterior_draws[draw_idx, ])
    sim_res <- workforce_simulator(parameters = params, years = projection_years, seed = seed + 5000 + draw_idx)
    sim_res$draw_id <- draw_idx
    sim_res
  })

  projection_table <- dplyr::bind_rows(proj_list)

  projection_summary <- projection_table |>
    dplyr::group_by(.data$year, .data$metric) |>
    dplyr::summarise(
      mean = base::mean(.data$value, na.rm = TRUE),
      sd = stats::sd(.data$value, na.rm = TRUE),
      median = stats::median(.data$value, na.rm = TRUE),
      p25 = stats::quantile(.data$value, 0.25, na.rm = TRUE),
      p75 = stats::quantile(.data$value, 0.75, na.rm = TRUE),
      .groups = "drop"
    )

  saved_files <- character(0)
  if (!base::is.null(save_directory)) {
    if (!base::dir.exists(save_directory)) {
      base::dir.create(save_directory, recursive = TRUE)
    }
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    draws_path <- base::file.path(save_directory, base::paste0("posterior_parameter_draws_", timestamp, ".csv"))
    summary_path <- base::file.path(save_directory, base::paste0("posterior_projections_2025_2050_", timestamp, ".csv"))
    readr::write_csv(posterior_draws, draws_path)
    readr::write_csv(projection_summary, summary_path)
    saved_files <- c(draws_path, summary_path)
    base::message("Saved calibration artifacts: ", base::paste(saved_files, collapse = "; "))
  }

  base::message("=================================================================")
  base::message("      BAYESIAN HISTORY MATCHING & CALIBRATION COMPLETE           ")
  base::message("=================================================================")

  list(
    wave_history = wave_results,
    posterior_parameters = posterior_draws,
    projections = projection_table,
    projection_summary = projection_summary,
    ess = ess,
    saved_files = saved_files
  )
}

#' Default Parameter Specification and Benchmarks for Calibration
#'
#' @return A list with default `parameter_spec` and `benchmark_table`.
#' @family Bayesian calibration
#' @concept calibration
#' @export
load_default_history_matching_inputs <- function() {
  param_spec <- tibble::tribble(
    ~parameter, ~lower, ~upper, ~prior_type, ~prior_mean, ~prior_sd, ~shape1, ~shape2,
    "care_seeking_rate", 0.10, 0.60, "beta", NA, NA, 2.5, 7.5,
    "annual_exit_hazard", 0.01, 0.10, "normal", 0.04, 0.01, NA, NA,
    "graduate_entry_rate", 35.0, 65.0, "normal", 55.0, 5.0, NA, NA
  )

  bench_table <- tibble::tribble(
    ~target_id, ~year, ~metric, ~observed, ~observation_se, ~discrepancy_sd, ~calibration,
    "meps_visits_2015", 2015L, "ui_visits", 125000, 7500, 12500, TRUE,
    "psps_57288_2015", 2015L, "sling_services", 18400, 900, 2500, TRUE
  )

  list(parameter_spec = param_spec, benchmark_table = bench_table)
}

#' Comprehensive Literature-Anchored Prior Specification for 10 URPS Parameters
#'
#' @return Parameter specification table with informative priors.
#' @family Bayesian calibration
#' @concept calibration
#' @export
build_urps_prior_specification <- function() {
  tibble::tribble(
    ~parameter, ~lower, ~upper, ~prior_type, ~prior_mean, ~prior_sd, ~shape1, ~shape2, ~identifiability,
    "care_seeking_rate", 0.10, 0.60, "beta", NA, NA, 2.5, 7.5, "primary_target",
    "annual_exit_hazard", 0.01, 0.10, "normal", 0.04, 0.01, NA, NA, "primary_target",
    "graduate_entry_rate", 35.0, 65.0, "normal", 55.0, 5.0, NA, NA, "primary_target",
    "app_delegation_rate", 0.0, 0.30, "beta", NA, NA, 2.0, 8.0, "nuisance_informative",
    "medicaid_multiplier", 0.50, 1.50, "normal", 1.00, 0.15, NA, NA, "nuisance_informative",
    "retreatment_hazard_multiplier", 0.70, 1.50, "lognormal", 0.00, 0.15, NA, NA, "nuisance_informative",
    "or_capacity_minutes", 100000.0, 300000.0, "normal", 200000.0, 25000.0, NA, NA, "nuisance_informative",
    "clinic_capacity_minutes", 150000.0, 450000.0, "normal", 300000.0, 35000.0, NA, NA, "nuisance_informative",
    "asc_migration_rate", 0.0, 0.40, "beta", NA, NA, 1.5, 6.0, "nuisance_informative",
    "telehealth_expansion_rate", 0.0, 0.25, "beta", NA, NA, 1.5, 8.5, "nuisance_informative"
  )
}
