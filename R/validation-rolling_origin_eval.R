#' Evaluate probabilistic forecasts with rolling origins
#'
#' Fits a user-supplied forecasting function at successive historical origins
#' and scores 1-, 3-, 5-, and 10-year-ahead predictive draws. The forecast
#' function must accept `training_panel`, `forecast_years`, `id_cols`,
#' `year_col`, `value_col`, `n_draws`, and `seed`. It must return one row per
#' predictive draw with the ID columns, forecast year, `draw`, and
#' `prediction`.
#'
#' @param observed_panel Historical observed outcomes.
#' @param forecast_fun Function that fits and predicts from one origin.
#' @param id_cols Character vector identifying observed outcome series.
#' @param model_cols Character vector identifying competing forecast arms. These
#'   columns are required only in predictive draws, not in observed outcomes.
#' @param year_col Name of integer year column.
#' @param value_col Name of observed outcome column.
#' @param horizons Positive forecast horizons in years.
#' @param first_origin Earliest training-set endpoint. Default uses the first
#'   year allowing `min_train_years` observations.
#' @param last_origin Latest training-set endpoint. Default is the latest year
#'   with observations at the longest requested horizon.
#' @param min_train_years Minimum number of distinct training years.
#' @param n_draws Number of predictive draws requested from `forecast_fun`.
#' @param interval_level Central predictive interval level.
#' @param log_score_method Either kernel-density or Gaussian log score.
#' @param density_floor Minimum predictive density used before taking logs.
#' @param seed Reproducibility seed.
#' @param evidence_registry Optional provenance and estimand registry.
#' @param save_path Optional path for an `.rds` evaluation bundle.
#'
#' @return A named list containing origin-level scores, horizon summaries,
#'   forecast draws, a calibration test, and a dynamic summary sentence.
#' @export
evaluate_rolling_origin_forecasts <- function(
    observed_panel,
    forecast_fun,
    id_cols,
    model_cols = character(),
    year_col = "year",
    value_col = "observed",
    horizons = c(1L, 3L, 5L, 10L),
    first_origin = NULL,
    last_origin = NULL,
    min_train_years = 5L,
    n_draws = 2000L,
    interval_level = 0.95,
    log_score_method = c("kernel", "gaussian"),
    density_floor = 1e-12,
    seed = 20260821L,
    evidence_registry = NULL,
    save_path = NULL) {
  log_score_method <- base::match.arg(log_score_method)
  base::message("Starting multi-horizon rolling-origin evaluation.")
  base::message(
    "Inputs: ", base::nrow(observed_panel), " rows; horizons = ",
    base::paste(horizons, collapse = ", "), "; draws = ", n_draws, "."
  )

  required_cols <- base::unique(c(id_cols, year_col, value_col))
  missing_cols <- base::setdiff(required_cols, base::names(observed_panel))
  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Missing columns: ", base::paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }
  if (!base::is.function(forecast_fun)) {
    base::stop("`forecast_fun` must be a function.", call. = FALSE)
  }
  if (base::any(horizons < 1L) || base::any(horizons %% 1 != 0)) {
    base::stop("`horizons` must contain positive integers.", call. = FALSE)
  }
  if (interval_level <= 0 || interval_level >= 1) {
    base::stop("`interval_level` must be between 0 and 1.", call. = FALSE)
  }
  if (n_draws < 2L) {
    base::stop("`n_draws` must be at least 2.", call. = FALSE)
  }

  clean_panel <- observed_panel |>
    dplyr::select(dplyr::all_of(required_cols)) |>
    dplyr::filter(
      !base::is.na(.data[[year_col]]),
      !base::is.na(.data[[value_col]])
    ) |>
    dplyr::mutate(
      "{year_col}" := base::as.integer(.data[[year_col]]),
      "{value_col}" := base::as.numeric(.data[[value_col]])
    )
  duplicate_keys <- clean_panel |>
    dplyr::count(dplyr::across(dplyr::all_of(c(id_cols, year_col)))) |>
    dplyr::filter(.data$n > 1L)
  if (base::nrow(duplicate_keys) > 0L) {
    base::stop("Observed series-year keys must be unique.", call. = FALSE)
  }
  observed_years <- base::sort(base::unique(clean_panel[[year_col]]))
  if (base::length(observed_years) < min_train_years + 1L) {
    base::stop("There are too few historical years for backtesting.",
      call. = FALSE
    )
  }
  if (base::is.null(first_origin)) {
    first_origin <- observed_years[[min_train_years]]
  }
  if (base::is.null(last_origin)) {
    last_origin <- base::max(observed_years) - base::min(horizons)
  }
  origins <- observed_years[
    observed_years >= first_origin & observed_years <= last_origin
  ]
  if (base::length(origins) == 0L) {
    base::stop("No eligible forecast origins were found.", call. = FALSE)
  }
  base::message("Validated and cleaned the historical panel.")

  score_one_origin <- function(origin_index) {
    origin_year <- origins[[origin_index]]
    target_years <- origin_year + horizons
    available_targets <- base::intersect(target_years, observed_years)
    if (base::length(available_targets) == 0L) {
      return(NULL)
    }
    training_panel <- clean_panel |>
      dplyr::filter(.data[[year_col]] <= origin_year)
    base::message(
      "Origin ", origin_year, ": training through ", origin_year,
      " and forecasting ", base::paste(available_targets, collapse = ", "),
      "."
    )
    draw_panel <- forecast_fun(
      training_panel = training_panel,
      forecast_years = available_targets,
      id_cols = id_cols,
      year_col = year_col,
      value_col = value_col,
      n_draws = n_draws,
      seed = seed + origin_index
    )
    required_draw_cols <- c(
      id_cols,
      model_cols,
      year_col,
      "draw",
      "prediction"
    )
    absent_draw_cols <- base::setdiff(
      required_draw_cols,
      base::names(draw_panel)
    )
    if (base::length(absent_draw_cols) > 0L) {
      base::stop(
        "Forecast output is missing: ",
        base::paste(absent_draw_cols, collapse = ", "),
        call. = FALSE
      )
    }
    scored_draws <- draw_panel |>
      dplyr::select(dplyr::all_of(required_draw_cols)) |>
      dplyr::mutate(
        origin = origin_year,
        horizon = .data[[year_col]] - origin_year
      ) |>
      dplyr::filter(.data$horizon %in% horizons) |>
      dplyr::inner_join(
        clean_panel |>
          dplyr::rename(observed = dplyr::all_of(value_col)),
        by = c(id_cols, year_col)
      )
    if (base::nrow(scored_draws) == 0L) {
      base::stop("Forecasts did not match any observed targets.",
        call. = FALSE
      )
    }
    scored_draws
  }

  draw_evaluations <- purrr::map_dfr(
    base::seq_along(origins),
    score_one_origin
  )
  base::message("Joined predictive draws to held-out observations.")

  grouping_cols <- c(
    id_cols,
    model_cols,
    "origin",
    "horizon",
    year_col
  )
  alpha <- (1 - interval_level) / 2
  score_panel <- draw_evaluations |>
    dplyr::group_by(dplyr::across(dplyr::all_of(grouping_cols))) |>
    dplyr::summarise(
      observed = dplyr::first(.data$observed),
      n_draws = dplyr::n_distinct(.data$draw),
      forecast_mean = base::mean(.data$prediction),
      forecast_sd = stats::sd(.data$prediction),
      forecast_median = stats::median(.data$prediction),
      p25 = stats::quantile(.data$prediction, 0.25, names = FALSE),
      p75 = stats::quantile(.data$prediction, 0.75, names = FALSE),
      lower = stats::quantile(.data$prediction, alpha, names = FALSE),
      upper = stats::quantile(
        .data$prediction,
        1 - alpha,
        names = FALSE
      ),
      crps = empirical_crps(.data$prediction, .data$observed[[1L]]),
      log_predictive_score = predictive_log_score(
        .data$prediction,
        .data$observed[[1L]],
        method = log_score_method,
        density_floor = density_floor
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      covered = .data$observed >= .data$lower &
        .data$observed <= .data$upper,
      error = .data$forecast_mean - .data$observed,
      absolute_error = base::abs(.data$error)
    )
  base::message("Computed CRPS, coverage, and log predictive scores.")

  horizon_summary <- score_panel |>
    dplyr::group_by(
      dplyr::across(dplyr::all_of(c(model_cols, "horizon")))
    ) |>
    dplyr::summarise(
      n_forecasts = dplyr::n(),
      n_origins = dplyr::n_distinct(.data$origin),
      crps_mean = base::mean(.data$crps),
      crps_sd = stats::sd(.data$crps),
      crps_median = stats::median(.data$crps),
      crps_p25 = stats::quantile(.data$crps, 0.25, names = FALSE),
      crps_p75 = stats::quantile(.data$crps, 0.75, names = FALSE),
      coverage = base::mean(.data$covered),
      coverage_lower = binomial_interval(
        base::sum(.data$covered),
        dplyr::n()
      )$lower,
      coverage_upper = binomial_interval(
        base::sum(.data$covered),
        dplyr::n()
      )$upper,
      coverage_p_value = stats::binom.test(
        base::sum(.data$covered),
        dplyr::n(),
        p = interval_level
      )$p.value,
      log_score_mean = base::mean(.data$log_predictive_score),
      log_score_sd = stats::sd(.data$log_predictive_score),
      bias_mean = base::mean(.data$error),
      bias_sd = stats::sd(.data$error),
      mae_mean = base::mean(.data$absolute_error),
      mae_sd = stats::sd(.data$absolute_error),
      .groups = "drop"
    ) |>
    dplyr::arrange(.data$horizon)

  calibration_test <- assess_horizon_change(score_panel, model_cols)
  summary_sentence <- build_backtest_summary(
    score_panel,
    horizon_summary,
    calibration_test,
    model_cols,
    interval_level
  )
  evaluation_bundle <- base::list(
    scores = score_panel,
    horizon_summary = horizon_summary,
    forecast_draws = draw_evaluations,
    calibration_test = calibration_test,
    summary_sentence = summary_sentence,
    settings = base::list(
      horizons = horizons,
      origins = origins,
      interval_level = interval_level,
      log_score_method = log_score_method,
      seed = seed,
      evidence_registry = evidence_registry
    )
  )
  if (!base::is.null(save_path)) {
    base::dir.create(base::dirname(save_path), recursive = TRUE,
      showWarnings = FALSE
    )
    base::saveRDS(evaluation_bundle, save_path)
    base::message("Saved evaluation bundle to: ",
      base::normalizePath(save_path, mustWork = TRUE)
    )
  }
  base::message("Completed rolling-origin evaluation: ", summary_sentence)
  evaluation_bundle
}

#' Calculate empirical CRPS from predictive draws
#' @keywords internal
empirical_crps <- function(draws, observed) {
  finite_draws <- base::sort(draws[base::is.finite(draws)])
  draw_count <- base::length(finite_draws)
  if (draw_count < 2L || !base::is.finite(observed)) {
    return(NA_real_)
  }
  ranks <- base::seq_len(draw_count)
  pairwise_term <- base::sum(
    (2 * ranks - draw_count - 1) * finite_draws
  ) / draw_count^2
  base::mean(base::abs(finite_draws - observed)) - pairwise_term
}

#' Calculate a predictive log score from draws
#' @keywords internal
predictive_log_score <- function(
    draws,
    observed,
    method = c("kernel", "gaussian"),
    density_floor = 1e-12) {
  method <- base::match.arg(method)
  finite_draws <- draws[base::is.finite(draws)]
  if (base::length(finite_draws) < 2L || !base::is.finite(observed)) {
    return(NA_real_)
  }
  draw_sd <- stats::sd(finite_draws)
  if (!base::is.finite(draw_sd) || draw_sd <= 0) {
    predictive_density <- if (base::all(finite_draws == observed)) {
      1 / density_floor
    } else {
      density_floor
    }
  } else if (method == "gaussian") {
    predictive_density <- stats::dnorm(
      observed,
      mean = base::mean(finite_draws),
      sd = draw_sd
    )
  } else {
    bandwidth <- stats::bw.nrd0(finite_draws)
    if (!base::is.finite(bandwidth) || bandwidth <= 0) {
      bandwidth <- draw_sd * 0.1
    }
    predictive_density <- base::mean(
      stats::dnorm(observed, mean = finite_draws, sd = bandwidth)
    )
  }
  base::log(base::max(predictive_density, density_floor))
}

#' Test whether CRPS changes from short to long horizons
#' @keywords internal
assess_horizon_change <- function(score_panel, model_cols = character()) {
  available_horizons <- base::sort(base::unique(score_panel$horizon))
  shortest <- available_horizons[[1L]]
  longest <- available_horizons[[base::length(available_horizons)]]
  if (shortest == longest) {
    return(tibble::tibble(
      shortest_horizon = shortest,
      longest_horizon = longest,
      mean_change = 0,
      p_value = NA_real_,
      direction = "unchanged"
    ))
  }
  comparison_panel <- score_panel |>
    dplyr::filter(.data$horizon %in% c(shortest, longest)) |>
    dplyr::group_by(
      dplyr::across(
        dplyr::all_of(c(model_cols, "origin", "horizon"))
      )
    ) |>
    dplyr::summarise(crps = base::mean(.data$crps), .groups = "drop") |>
    tidyr::pivot_wider(
      names_from = "horizon",
      values_from = "crps",
      names_prefix = "h_"
    )
  short_col <- base::paste0("h_", shortest)
  long_col <- base::paste0("h_", longest)
  paired_panel <- comparison_panel |>
    dplyr::filter(
      base::is.finite(.data[[short_col]]),
      base::is.finite(.data[[long_col]])
    )
  paired_panel |>
    dplyr::group_by(dplyr::across(dplyr::all_of(model_cols))) |>
    dplyr::summarise(
      shortest_horizon = shortest,
      longest_horizon = longest,
      n_paired_origins = dplyr::n(),
      mean_change = base::mean(
        .data[[long_col]] - .data[[short_col]]
      ),
      p_value = paired_wilcox_p(
        .data[[long_col]],
        .data[[short_col]]
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      direction = dplyr::case_when(
        .data$mean_change > 0 ~ "worsened",
        .data$mean_change < 0 ~ "improved",
        TRUE ~ "was unchanged"
      )
    )
}

#' Calculate a Wilson binomial confidence interval
#' @keywords internal
binomial_interval <- function(successes, trials, level = 0.95) {
  if (trials < 1L) {
    return(base::list(lower = NA_real_, upper = NA_real_))
  }
  z_value <- stats::qnorm(1 - (1 - level) / 2)
  proportion <- successes / trials
  denominator <- 1 + z_value^2 / trials
  center <- (
    proportion + z_value^2 / (2 * trials)
  ) / denominator
  half_width <- z_value * base::sqrt(
    proportion * (1 - proportion) / trials +
      z_value^2 / (4 * trials^2)
  ) / denominator
  base::list(
    lower = base::max(0, center - half_width),
    upper = base::min(1, center + half_width)
  )
}

#' Calculate a paired Wilcoxon p-value when estimable
#' @keywords internal
paired_wilcox_p <- function(long_scores, short_scores) {
  if (base::length(long_scores) < 2L) {
    return(NA_real_)
  }
  if (base::all(long_scores == short_scores)) {
    return(1)
  }
  stats::wilcox.test(
    long_scores,
    short_scores,
    paired = TRUE,
    exact = FALSE
  )$p.value
}

#' Format a p-value for a narrative summary
#' @keywords internal
format_p_value <- function(p_value) {
  if (base::is.na(p_value)) {
    return("not estimable")
  }
  if (p_value < 0.001) {
    return("<0.001")
  }
  base::formatC(p_value, format = "f", digits = 3)
}

#' Filter grouped metrics to one model key
#' @keywords internal
filter_to_key <- function(metric_panel, key_row, model_cols) {
  if (base::length(model_cols) == 0L) {
    return(metric_panel)
  }
  keep <- base::rep(TRUE, base::nrow(metric_panel))
  for (column in model_cols) {
    keep <- keep & metric_panel[[column]] == key_row[[column]][[1L]]
  }
  metric_panel[keep, , drop = FALSE]
}

#' Evidence and estimand registry for URPS rolling-origin validation
#'
#' Records the target definitions already enforced in `urpssim`. It prevents
#' national active-workforce claims from being justified with a cumulative
#' certification series or Massachusetts inpatient-only CHIA utilization.
#'
#' @return A tibble describing evidence, allowed use, and limitations.
#' @export
urps_backtest_evidence_registry <- function() {
  tibble::tribble(
    ~evidence_id, ~source, ~years, ~geography, ~measure,
    ~allowed_role, ~key_limitation, ~reference,
    "URPS_V3_NATIONAL",
    "mufflyaccess URPS contract v3.0.0",
    "through 2023",
    "United States",
    "ABOG+ABU board-certified cumulative stock",
    "entrant and cumulative-certification validation",
    "Departures are not ascertained; not active stock net of attrition",
    "https://github.com/mufflyt/simulation",
    "URPS_V3_CONUS",
    "mufflyaccess URPS contract v3.0.0",
    "through 2023",
    "CONUS",
    "ABOG+ABU board-certified cumulative stock",
    "CONUS sensitivity analysis",
    "The 2023 count is 1,303, not the national target of 1,306",
    "https://github.com/mufflyt/simulation",
    "URPS_V2_RETIRED",
    "mufflyaccess URPS contract v2.1.0",
    "through 2023",
    "United States and CONUS",
    "Primary-board-certification-year stock",
    "historical reconciliation only",
    "Retired targets 1,332 and 1,329 use a different year basis",
    "https://github.com/mufflyt/simulation",
    "NRMP_SMS",
    "NRMP Specialties Matching Service reports",
    "2017\u20132025 appointment years",
    "United States",
    "filled fellowship positions",
    "entrant-flow forecasting when published by the origin",
    "Publication year, not appointment year, controls availability",
    "https://www.nrmp.org/match-data-analytics/",
    "CHIA_D6",
    "Massachusetts CHIA case-mix inpatient discharge series",
    "2010\u20132018",
    "Massachusetts",
    "all-payer inpatient URPS surgery",
    "regional inpatient utilization validation",
    "Cannot validate ambulatory volume, national demand, wait, or FTE",
    "https://www.chiamass.gov/case-mix-data/",
    "ROLLING_ORIGIN",
    "Tashman 2000",
    "methodological",
    "not applicable",
    "multiple out-of-sample origins",
    "forecast evaluation design",
    "Training and evaluation windows must preserve time order",
    "https://doi.org/10.1016/S0169-2070(00)00065-0",
    "PROPER_SCORES",
    "Gneiting and Raftery 2007",
    "methodological",
    "not applicable",
    "CRPS and logarithmic score",
    "probabilistic forecast evaluation",
    "CRPS is minimized; the log predictive score is maximized",
    "https://doi.org/10.1198/016214506000001437"
  )
}

#' Build a forecast adapter around the existing URPS backtest runner
#'
#' The adapter runs the existing leakage-audited engine once per origin through
#' the longest available horizon, then exposes every Monte Carlo headcount as a
#' predictive draw. The observed target is used only by the runner's contract
#' check and scoring; it is not supplied to model fitting.
#'
#' @param run_backtest_fun Usually [run_backtest()].
#' @param target_count_fun Function accepting a year and returning the exact
#'   contract-matched observed count.
#' @param acknowledge_no_attrition Explicit acknowledgement passed to the
#'   existing runner.
#' @return A forecasting function accepted by
#'   [evaluate_rolling_origin_forecasts()].
#' @export
make_urps_run_backtest_forecaster <- function(
    run_backtest_fun = run_backtest,
    target_count_fun = function(year) {
      mufflyaccess::urps_count(
        year,
        geography = "national",
        include_urology = TRUE
      )
    },
    acknowledge_no_attrition = TRUE) {
  function(
      training_panel,
      forecast_years,
      id_cols,
      year_col,
      value_col,
      n_draws,
      seed) {
    origin_year <- base::max(training_panel[[year_col]])
    target_year <- base::max(forecast_years)
    expected_target <- target_count_fun(target_year)
    base::message(
      "URPS adapter: origin ", origin_year, ", target ", target_year,
      ", contract count ",
      base::format(expected_target, big.mark = ","), "."
    )
    backtest_bundle <- run_backtest_fun(
      cutoff_year = origin_year,
      target_year = target_year,
      n_iterations = n_draws,
      seed = seed,
      acknowledge_no_attrition = acknowledge_no_attrition,
      expected_target = expected_target
    )
    backtest_bundle$iterations |>
      dplyr::filter(.data$year %in% forecast_years) |>
      dplyr::transmute(
        arm = .data$arm,
        apply_attrition = .data$apply_attrition,
        year = base::as.integer(.data$year),
        draw = base::as.integer(.data$iteration),
        prediction = base::as.numeric(.data$headcount)
      )
  }
}

#' Build a dynamic human-readable backtest summary
#' @keywords internal
build_backtest_summary <- function(
    score_panel,
    horizon_summary,
    calibration_test,
    model_cols,
    interval_level) {
  first_year <- base::min(score_panel$origin)
  last_year <- base::max(score_panel$origin + score_panel$horizon)
  sentence_keys <- if (base::length(model_cols) == 0L) {
    tibble::tibble(.summary_group = "Overall")
  } else {
    calibration_test |>
      dplyr::distinct(dplyr::across(dplyr::all_of(model_cols))) |>
      tidyr::unite(
        ".summary_group",
        dplyr::all_of(model_cols),
        sep = " / ",
        remove = FALSE
      )
  }
  purrr::map_chr(base::seq_len(base::nrow(sentence_keys)), function(index) {
    key_row <- sentence_keys[index, , drop = FALSE]
    test_row <- filter_to_key(calibration_test, key_row, model_cols)
    summary_rows <- filter_to_key(horizon_summary, key_row, model_cols)
    shortest <- test_row$shortest_horizon[[1L]]
    longest <- test_row$longest_horizon[[1L]]
    direction <- test_row$direction[[1L]]
    p_value <- test_row$p_value[[1L]]
    p_text <- format_p_value(p_value)
    coverage_text <- summary_rows |>
      dplyr::mutate(
        label = base::paste0(
          .data$horizon,
          "-year ",
          base::formatC(
            100 * .data$coverage,
            format = "f",
            digits = 1
          ),
          "%"
        )
      ) |>
      dplyr::pull(.data$label) |>
      base::paste(collapse = ", ")
    base::paste0(
      key_row$.summary_group[[1L]], ": across ",
      base::format(first_year, big.mark = ","), "\u2013",
      base::format(last_year, big.mark = ","), ", mean CRPS ",
      direction, " from the ", shortest, "-year to ", longest,
      "-year horizon (p=", p_text, "); ",
      base::formatC(100 * interval_level, format = "f", digits = 1),
      "% interval coverage was ", coverage_text, "."
    )
  })
}
