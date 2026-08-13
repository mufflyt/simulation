# Lizeth inverse adequacy calibration ----------------------------------------
#
# Inverts the tested clear_access() forward map against observed Lizeth wait
# quantiles.
#
# Primary estimand:
#
#   catchment adequacy A_i = accessible capacity_i / demand_i
#
# Catchment adequacy is modeled as lognormal:
#
#   log(A_i) = mu + sigma * z_i
#
# where z_i is a fixed deterministic catchment score. The optimizer estimates
# mu and sigma by matching simulated wait p25 / median / p75 to Lizeth.
#
# IMPORTANT:
#
# wait_scale is fixed in the primary analysis. Jointly fitting wait_scale and
# adequacy creates a strong scale-identification problem because both move
# waits: low adequacy + small wait_scale mimics higher adequacy + larger
# wait_scale, giving an excellent objective and a meaningless level. Joint
# fitting is a sensitivity analysis only, never the primary fit.
#
# Conventions: the package declares neither scales nor lubridate, so
# thousands/percent formatting for messages uses base R -- .lizeth_comma()
# (defined in calibration-lizeth_access_anchor.R) and the local .lizeth_percent()
# below. The statistics are unchanged.

.lizeth_percent <- function(x, accuracy = 0.1) {
  digits <- base::max(0L, base::as.integer(base::round(-base::log10(accuracy))))
  base::paste0(
    base::formatC(x * 100, format = "f", digits = digits),
    "%"
  )
}

#' Weighted quantile without additional package dependencies
#'
#' @param x Numeric values.
#' @param w Nonnegative weights.
#' @param probs Probabilities in [0, 1].
#'
#' @return Numeric vector of weighted quantiles.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
urps_weighted_quantile <- function(
    x,
    w,
    probs = c(0.25, 0.50, 0.75)) {
  base::message(
    "Computing weighted quantiles at probabilities: ",
    base::paste(probs, collapse = ", ")
  )
  keep <- base::is.finite(x) &
    base::is.finite(w) &
    w > 0
  x_use <- x[keep]
  w_use <- w[keep]
  if (base::length(x_use) == 0L) {
    base::stop(
      "No finite observations with positive weight.",
      call. = FALSE
    )
  }
  if (base::any(probs < 0 | probs > 1)) {
    base::stop(
      "`probs` must lie between 0 and 1.",
      call. = FALSE
    )
  }
  order_index <- base::order(x_use)
  x_sorted <- x_use[order_index]
  w_sorted <- w_use[order_index]
  cumulative_weight <- base::cumsum(w_sorted) /
    base::sum(w_sorted)
  quantiles <- purrr::map_dbl(
    probs,
    \(probability) {
      position <- base::which(
        cumulative_weight >= probability
      )[[1]]
      x_sorted[[position]]
    }
  )
  base::names(quantiles) <- base::paste0(
    "p",
    probs * 100
  )
  quantiles
}

#' Extract Lizeth wait-time calibration targets
#'
#' @param lizeth_calls Call-level Lizeth analytic records.
#' @param wait_col Wait-time variable.
#' @param obtained_col Appointment-obtained variable.
#'
#' @return Named numeric vector with p25, p50 and p75.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @importFrom rlang .data
#' @export
lizeth_wait_targets <- function(
    lizeth_calls,
    wait_col = "wait_days",
    obtained_col = "appointment_obtained") {
  base::message("Extracting Lizeth wait-time calibration targets.")
  required_names <- c(
    wait_col,
    obtained_col
  )
  missing_names <- base::setdiff(
    required_names,
    base::names(lizeth_calls)
  )
  if (base::length(missing_names) > 0L) {
    base::stop(
      "Missing Lizeth variable(s): ",
      base::paste(missing_names, collapse = ", "),
      call. = FALSE
    )
  }
  wait_values <- lizeth_calls |>
    dplyr::filter(
      .data[[obtained_col]] %in% TRUE,
      base::is.finite(.data[[wait_col]]),
      .data[[wait_col]] >= 0
    ) |>
    dplyr::pull(.data[[wait_col]])
  if (base::length(wait_values) < 20L) {
    base::stop(
      "Fewer than 20 observed appointment waits are available.",
      call. = FALSE
    )
  }
  wait_quantiles <- stats::quantile(
    wait_values,
    probs = c(0.25, 0.50, 0.75),
    na.rm = TRUE,
    names = FALSE,
    type = 7
  )
  targets <- c(
    p25 = wait_quantiles[[1]],
    p50 = wait_quantiles[[2]],
    p75 = wait_quantiles[[3]]
  )
  base::message(
    "Lizeth targets: p25=",
    base::format(targets[["p25"]], digits = 4),
    ", median=",
    base::format(targets[["p50"]], digits = 4),
    ", p75=",
    base::format(targets[["p75"]], digits = 4),
    " days."
  )
  targets
}

#' Create deterministic catchment heterogeneity scores
#'
#' The same scores are reused at every optimizer evaluation. This is critical:
#' stochastic draws inside the objective would make the optimization noisy.
#'
#' @param catchments Base-year catchment table.
#' @param adequacy_col Optional existing adequacy variable used only to preserve
#'   geographic rank.
#'
#' @return Numeric standard-normal catchment scores.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
urps_catchment_scores <- function(
    catchments,
    adequacy_col = NULL) {
  base::message("Constructing deterministic catchment scores.")
  n_catchments <- base::nrow(catchments)
  if (n_catchments < 3L) {
    base::stop(
      "At least three catchments are required.",
      call. = FALSE
    )
  }
  probability_grid <- (
    base::seq_len(n_catchments) - 0.5
  ) / n_catchments
  normal_scores <- stats::qnorm(probability_grid)
  if (
    !base::is.null(adequacy_col) &&
      adequacy_col %in% base::names(catchments)
  ) {
    adequacy_values <- catchments[[adequacy_col]]
    rank_index <- base::order(
      base::order(
        adequacy_values,
        na.last = TRUE
      ),
      na.last = TRUE
    )
    normal_scores <- normal_scores[rank_index]
  }
  normal_scores <- base::as.numeric(
    base::scale(normal_scores)
  )
  base::message(
    "Created scores for ",
    .lizeth_comma(n_catchments),
    " catchments."
  )
  normal_scores
}

#' Generate a catchment-level adequacy distribution
#'
#' @param mean_adequacy Target arithmetic mean adequacy.
#' @param log_sd Between-catchment SD on the log-adequacy scale.
#' @param z Fixed catchment standard-normal scores.
#' @param weights Catchment weights used to force the weighted arithmetic mean
#'   to equal mean_adequacy.
#'
#' @return Numeric catchment adequacy vector.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
urps_adequacy_distribution <- function(
    mean_adequacy,
    log_sd,
    z,
    weights = base::rep(1, base::length(z))) {
  if (
    !base::is.finite(mean_adequacy) ||
      mean_adequacy <= 0
  ) {
    base::stop(
      "`mean_adequacy` must be positive and finite.",
      call. = FALSE
    )
  }
  if (
    !base::is.finite(log_sd) ||
      log_sd < 0
  ) {
    base::stop(
      "`log_sd` must be nonnegative and finite.",
      call. = FALSE
    )
  }
  if (
    base::length(z) != base::length(weights)
  ) {
    base::stop(
      "`z` and `weights` must have equal length.",
      call. = FALSE
    )
  }
  raw_adequacy <- base::exp(
    log_sd * z
  )
  weighted_raw_mean <- stats::weighted.mean(
    raw_adequacy,
    w = weights,
    na.rm = TRUE
  )
  adequacy <- raw_adequacy *
    mean_adequacy /
    weighted_raw_mean
  adequacy
}

#' Forward map from an adequacy distribution to national wait quantiles
#'
#' @param catchments Base-year catchments containing demand_workload.
#' @param mean_adequacy Arithmetic mean catchment adequacy.
#' @param log_sd SD of log catchment adequacy.
#' @param z Fixed catchment scores.
#' @param wait_scale Fixed clear_access() wait scale.
#' @param weight_col Catchment weight for national wait quantiles. NULL uses
#'   demand_workload.
#' @param wait_ceiling Finite ceiling for saturated catchments.
#'
#' @return List containing quantiles and catchment-level forward predictions.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @importFrom rlang .data
#' @export
forward_lizeth_adequacy <- function(
    catchments,
    mean_adequacy,
    log_sd,
    z,
    wait_scale,
    weight_col = NULL,
    wait_ceiling = 365) {
  required_names <- "demand_workload"
  if (!required_names %in% base::names(catchments)) {
    base::stop(
      "`catchments` must contain demand_workload.",
      call. = FALSE
    )
  }
  if (base::length(z) != base::nrow(catchments)) {
    base::stop(
      "`z` must have one value per catchment.",
      call. = FALSE
    )
  }
  if (base::is.null(weight_col)) {
    catchment_weights <- catchments$demand_workload
  } else {
    if (!weight_col %in% base::names(catchments)) {
      base::stop(
        "Weight column not found: ",
        weight_col,
        call. = FALSE
      )
    }
    catchment_weights <- catchments[[weight_col]]
  }
  adequacy_values <- urps_adequacy_distribution(
    mean_adequacy = mean_adequacy,
    log_sd = log_sd,
    z = z,
    weights = catchment_weights
  )
  forward_catchments <- catchments |>
    dplyr::mutate(
      fitted_adequacy = adequacy_values,
      accessible_capacity =
        demand_workload * fitted_adequacy
    )
  cleared_catchments <- clear_access(
    catchments = forward_catchments,
    wait_scale = wait_scale,
    wait_ceiling = wait_ceiling,
    status = "inverse_fit_candidate"
  )
  wait_quantiles <- urps_weighted_quantile(
    x = cleared_catchments$wait_time,
    w = catchment_weights,
    probs = c(0.25, 0.50, 0.75)
  )
  base::names(wait_quantiles) <- c(
    "p25",
    "p50",
    "p75"
  )
  base::list(
    quantiles = wait_quantiles,
    catchments = cleared_catchments
  )
}

#' Quantile-matching loss for the Lizeth inverse fit
#'
#' Uses squared error on log1p wait time so that the p75 does not dominate
#' simply because it is numerically larger than the p25.
#'
#' @param parameters Optimizer parameters: log mean adequacy and log SD.
#' @param catchments Base-year catchments.
#' @param targets Lizeth wait targets.
#' @param z Fixed catchment scores.
#' @param wait_scale Fixed forward-map wait scale.
#' @param target_weights Relative weights for p25, p50 and p75.
#' @param weight_col Catchment aggregation weight.
#' @param wait_ceiling Saturation ceiling.
#'
#' @return Numeric scalar objective.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
lizeth_adequacy_loss <- function(
    parameters,
    catchments,
    targets,
    z,
    wait_scale,
    target_weights = c(
      p25 = 1,
      p50 = 2,
      p75 = 1
    ),
    weight_col = NULL,
    wait_ceiling = 365) {
  mean_adequacy <- base::exp(
    parameters[[1]]
  )
  log_sd <- base::exp(
    parameters[[2]]
  )
  forward_fit <- forward_lizeth_adequacy(
    catchments = catchments,
    mean_adequacy = mean_adequacy,
    log_sd = log_sd,
    z = z,
    wait_scale = wait_scale,
    weight_col = weight_col,
    wait_ceiling = wait_ceiling
  )
  predicted <- forward_fit$quantiles[
    base::names(targets)
  ]
  target_weights <- target_weights[
    base::names(targets)
  ]
  residuals <- base::log1p(predicted) -
    base::log1p(targets)
  loss <- base::sum(
    target_weights * residuals^2
  )
  if (!base::is.finite(loss)) {
    return(.Machine$double.xmax / 100)
  }
  loss
}

#' Inverse-fit URPS base-year adequacy to Lizeth wait times
#'
#' @param catchments Base-year access-clearing catchments.
#' @param lizeth_calls Call-level Lizeth records.
#' @param wait_scale Fixed wait-scale parameter for clear_access().
#' @param initial_mean Starting national adequacy.
#' @param initial_log_sd Starting heterogeneity on log scale.
#' @param reference_adequacy Existing borrowed stand-in.
#' @param weight_col Catchment weighting variable. NULL uses demand_workload.
#' @param preserve_rank_col Optional existing catchment adequacy variable whose
#'   rank is preserved while fitting the new distribution.
#' @param wait_ceiling Wait ceiling passed to clear_access().
#'
#' @return Named list of fitted parameters, fit diagnostics and catchments.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
fit_lizeth_inverse_adequacy <- function(
    catchments,
    lizeth_calls,
    wait_scale,
    initial_mean = 0.948,
    initial_log_sd = 0.20,
    reference_adequacy = 0.948,
    weight_col = NULL,
    preserve_rank_col = NULL,
    wait_ceiling = 365) {
  base::message("Starting Lizeth inverse adequacy fit.")
  base::message(
    "Input catchments: ",
    .lizeth_comma(base::nrow(catchments))
  )
  base::message(
    "Input Lizeth calls: ",
    .lizeth_comma(base::nrow(lizeth_calls))
  )
  base::message(
    "Fixed wait scale: ",
    base::format(wait_scale, digits = 5)
  )
  targets <- lizeth_wait_targets(
    lizeth_calls = lizeth_calls
  )
  base::message(
    "Reference adequacy: ",
    .lizeth_percent(
      reference_adequacy,
      accuracy = 0.1
    )
  )
  z <- urps_catchment_scores(
    catchments = catchments,
    adequacy_col = preserve_rank_col
  )
  starting_parameters <- c(
    log_mean = base::log(initial_mean),
    log_log_sd = base::log(initial_log_sd)
  )
  lower_bounds <- c(
    log_mean = base::log(0.30),
    log_log_sd = base::log(0.005)
  )
  upper_bounds <- c(
    log_mean = base::log(2.00),
    log_log_sd = base::log(1.50)
  )
  base::message(
    "Optimizing national adequacy level and catchment heterogeneity."
  )
  optimization <- stats::optim(
    par = starting_parameters,
    fn = lizeth_adequacy_loss,
    catchments = catchments,
    targets = targets,
    z = z,
    wait_scale = wait_scale,
    weight_col = weight_col,
    wait_ceiling = wait_ceiling,
    method = "L-BFGS-B",
    lower = lower_bounds,
    upper = upper_bounds,
    control = base::list(
      factr = 1e7,
      pgtol = 1e-8,
      maxit = 1000
    )
  )
  fitted_mean <- base::exp(
    optimization$par[[1]]
  )
  fitted_log_sd <- base::exp(
    optimization$par[[2]]
  )
  base::message(
    "Optimization convergence code: ",
    optimization$convergence
  )
  base::message(
    "Fitted mean adequacy: ",
    .lizeth_percent(
      fitted_mean,
      accuracy = 0.1
    )
  )
  base::message(
    "Fitted log-SD: ",
    base::format(
      fitted_log_sd,
      digits = 4
    )
  )
  fitted_forward <- forward_lizeth_adequacy(
    catchments = catchments,
    mean_adequacy = fitted_mean,
    log_sd = fitted_log_sd,
    z = z,
    wait_scale = wait_scale,
    weight_col = weight_col,
    wait_ceiling = wait_ceiling
  )
  reference_forward <- forward_lizeth_adequacy(
    catchments = catchments,
    mean_adequacy = reference_adequacy,
    log_sd = fitted_log_sd,
    z = z,
    wait_scale = wait_scale,
    weight_col = weight_col,
    wait_ceiling = wait_ceiling
  )
  comparison <- tibble::tibble(
    quantile = base::names(targets),
    observed_days = base::as.numeric(targets),
    fitted_days = base::as.numeric(
      fitted_forward$quantiles
    ),
    reference_days = base::as.numeric(
      reference_forward$quantiles
    )
  ) |>
    dplyr::mutate(
      fitted_error_days =
        fitted_days - observed_days,
      reference_error_days =
        reference_days - observed_days
    )
  adequacy_delta <- fitted_mean -
    reference_adequacy
  adequacy_ratio <- fitted_mean /
    reference_adequacy
  direction <- dplyr::case_when(
    adequacy_delta < 0 ~ "lower",
    adequacy_delta > 0 ~ "higher",
    TRUE ~ "equal"
  )
  summary_sentence <- base::sprintf(
    base::paste(
      "Inverse calibration to Lizeth wait-time p25, median, and p75",
      "estimated mean base-year URPS adequacy at %s, %s than the",
      "0.948 reference by %s (%sx the reference)."
    ),
    .lizeth_percent(
      fitted_mean,
      accuracy = 0.1
    ),
    direction,
    .lizeth_percent(
      base::abs(adequacy_delta),
      accuracy = 0.1
    ),
    base::formatC(
      adequacy_ratio,
      format = "f",
      digits = 2
    )
  )
  base::message(summary_sentence)
  base::list(
    mean_adequacy = fitted_mean,
    log_sd = fitted_log_sd,
    reference_adequacy = reference_adequacy,
    difference_from_reference = adequacy_delta,
    ratio_to_reference = adequacy_ratio,
    targets = targets,
    fitted_quantiles = fitted_forward$quantiles,
    reference_quantiles = reference_forward$quantiles,
    comparison = comparison,
    catchments = fitted_forward$catchments,
    optimization = optimization,
    wait_scale = wait_scale,
    calibration_status = "fitted_to_lizeth_wait_distribution",
    summary_sentence = summary_sentence
  )
}

#' Bootstrap the Lizeth inverse adequacy fit by physician
#'
#' Resampling is clustered at NPI because multiple mystery calls were made to
#' the same physician. Resampling individual calls would understate sampling
#' uncertainty.
#'
#' @param catchments Base-year catchments.
#' @param lizeth_calls Call-level Lizeth records containing npi.
#' @param wait_scale Fixed wait scale.
#' @param n_boot Number of cluster-bootstrap replicates.
#' @param seed Random seed.
#' @param ... Additional arguments passed to fit_lizeth_inverse_adequacy().
#'
#' @return List containing point estimate, bootstrap draws and intervals.
#'
#' @concept calibration
#' @family Lizeth inverse adequacy
#' @export
bootstrap_lizeth_inverse_adequacy <- function(
    catchments,
    lizeth_calls,
    wait_scale,
    n_boot = 1000L,
    seed = 20260812L,
    ...) {
  base::message("Starting physician-cluster bootstrap.")
  base::message("Bootstrap replicates: ", .lizeth_comma(n_boot))
  base::message("Random seed: ", seed)
  if (!"npi" %in% base::names(lizeth_calls)) {
    base::stop(
      "Lizeth calls must contain `npi` for cluster bootstrap.",
      call. = FALSE
    )
  }
  point_estimate <- fit_lizeth_inverse_adequacy(
    catchments = catchments,
    lizeth_calls = lizeth_calls,
    wait_scale = wait_scale,
    ...
  )
  npi_values <- lizeth_calls |>
    dplyr::filter(!base::is.na(npi)) |>
    dplyr::distinct(npi) |>
    dplyr::pull(npi)
  base::set.seed(seed)
  bootstrap_draws <- purrr::map_dfr(
    base::seq_len(n_boot),
    \(bootstrap_index) {
      if (
        bootstrap_index == 1L ||
          bootstrap_index %% 50L == 0L
      ) {
        base::message(
          "Bootstrap replicate ",
          .lizeth_comma(bootstrap_index),
          " of ",
          .lizeth_comma(n_boot),
          "."
        )
      }
      sampled_npis <- base::sample(
        npi_values,
        size = base::length(npi_values),
        replace = TRUE
      )
      sampled_calls <- tibble::tibble(
        bootstrap_cluster = base::seq_along(sampled_npis),
        npi = sampled_npis
      ) |>
        dplyr::left_join(
          lizeth_calls,
          by = "npi",
          relationship = "many-to-many"
        )
      fit_attempt <- base::tryCatch(
        fit_lizeth_inverse_adequacy(
          catchments = catchments,
          lizeth_calls = sampled_calls,
          wait_scale = wait_scale,
          ...
        ),
        error = \(condition) {
          base::message(
            "Bootstrap replicate ",
            bootstrap_index,
            " failed: ",
            base::conditionMessage(condition)
          )
          NULL
        }
      )
      if (base::is.null(fit_attempt)) {
        return(
          tibble::tibble(
            bootstrap_index = bootstrap_index,
            mean_adequacy = NA_real_,
            log_sd = NA_real_,
            convergence = NA_integer_
          )
        )
      }
      tibble::tibble(
        bootstrap_index = bootstrap_index,
        mean_adequacy = fit_attempt$mean_adequacy,
        log_sd = fit_attempt$log_sd,
        convergence =
          fit_attempt$optimization$convergence
      )
    }
  )
  successful_draws <- bootstrap_draws |>
    dplyr::filter(
      convergence == 0L,
      base::is.finite(mean_adequacy)
    )
  base::message(
    "Successful bootstrap fits: ",
    .lizeth_comma(base::nrow(successful_draws)),
    " / ",
    .lizeth_comma(n_boot)
  )
  adequacy_interval <- stats::quantile(
    successful_draws$mean_adequacy,
    probs = c(0.025, 0.25, 0.50, 0.75, 0.975),
    na.rm = TRUE,
    names = FALSE
  )
  interval_table <- tibble::tibble(
    estimate = point_estimate$mean_adequacy,
    p2_5 = adequacy_interval[[1]],
    p25 = adequacy_interval[[2]],
    median = adequacy_interval[[3]],
    p75 = adequacy_interval[[4]],
    p97_5 = adequacy_interval[[5]]
  )
  reference_probability <- base::mean(
    successful_draws$mean_adequacy <
      point_estimate$reference_adequacy,
    na.rm = TRUE
  )
  summary_sentence <- base::sprintf(
    base::paste(
      "The Lizeth inverse fit estimated mean URPS adequacy at %s;",
      "the physician-cluster bootstrap median was %s",
      "(p25 %s, p75 %s; 95%% interval %s-%s), and %.1f%% of",
      "bootstrap estimates were below the 0.948 reference."
    ),
    .lizeth_percent(
      point_estimate$mean_adequacy,
      accuracy = 0.1
    ),
    .lizeth_percent(
      interval_table$median,
      accuracy = 0.1
    ),
    .lizeth_percent(
      interval_table$p25,
      accuracy = 0.1
    ),
    .lizeth_percent(
      interval_table$p75,
      accuracy = 0.1
    ),
    .lizeth_percent(
      interval_table$p2_5,
      accuracy = 0.1
    ),
    .lizeth_percent(
      interval_table$p97_5,
      accuracy = 0.1
    ),
    100 * reference_probability
  )
  base::message(summary_sentence)
  base::list(
    point_estimate = point_estimate,
    bootstrap_draws = bootstrap_draws,
    successful_draws = successful_draws,
    interval = interval_table,
    probability_below_reference = reference_probability,
    summary_sentence = summary_sentence
  )
}
