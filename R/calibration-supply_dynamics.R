# Forecast-calibration layer for the supply microsimulation.
#
# The shipped supply engine held two processes fixed that a back-test showed it
# should not: entrants were drawn around a single timeless rate, and the
# retirement hazard carried no coefficient uncertainty at all (hazard_cv = 0).
# The back-test's failure was one-sided -- every arm UNDER-predicted -- and its
# 95% intervals were 6.5-8.2x too narrow, so widening intervals alone could
# never fix it. Two things had to change together:
#
#   1. Entrants must be allowed to change LEVEL AND SLOPE after an empirical
#      regime break, not oscillate around one mean.
#   2. The retirement/departure hazard must be a FITTED empirical curve whose
#      coefficient covariance is resampled on every Monte Carlo draw. Do not use
#      hazard_cv = 0 anymore -- the CV is now read off the fitted hazard.
#
# calibrate_urps_supply_dynamics() estimates both processes with uncertainty;
# advance_urps_supply_one_year() steps one draw forward; and
# backtest_urps_supply_calibration() hindcasts and scores. The fourth function,
# decompose_urps_forecast_miss(), implements the methodological refinement the
# back-test demands: a leakage-free "what could the model have known in year Y"
# fit against an oracle post-hoc fit on the complete series, so the miss can be
# split into an unforeseeable regime break versus a deficient entrant model.
#
# These functions are deliberately NAMED distinctly from the existing
# fit_entrant_regime_model()/build_urps_exit_hazard()/draw_entrant_paths() stack
# (R/supply-entrant_regime.R, R/supply-retirement_hazard.R). That stack fits the
# certification-contract entrant series and a Gompertz exit hazard; this layer
# is the count-GLM + age-spline formulation used for the forecast-calibration
# experiment and its train/oracle decomposition. They are complementary, not
# duplicates.

# Quantile helper: unnamed 2.5/25/50/75/97.5 percentiles of a numeric vector.
.sd_quantiles <- function(x) {
  q <- stats::quantile(x, probs = c(0.025, 0.25, 0.5, 0.75, 0.975), names = FALSE)
  stats::setNames(as.list(q), c("p025", "p25", "p50", "p75", "p975"))
}

#' Calibrate URPS supply dynamics from entrant and departure data
#'
#' @description
#' Estimates, with uncertainty, the two supply processes the forecast back-test
#' found were held fixed: (1) an empirical entrant process allowing a recent
#' regime shift in level and slope, and (2) an age-specific departure hazard
#' fitted from observed ABOG events. Both processes are propagated through Monte
#' Carlo parameter draws rather than held at a point estimate.
#'
#' The purpose is FORECAST calibration, not base-year calibration. Base-year
#' adequacy should be calibrated separately from capacity data
#' (see [baseline_gap()]); this function calibrates the trajectory.
#'
#' @param entrant_tbl A data frame with integer `year` and numeric `entrants`,
#'   one row per year.
#' @param departure_tbl A data frame with one row per provider-year and columns
#'   `provider_id`, `year`, `age`, and `departed` (0/1).
#' @param forecast_years Integer vector of years to project. Must span every
#'   year a downstream back-test will advance through.
#' @param n_draws Number of Monte Carlo parameter draws.
#' @param recent_years Number of recent entrant years defining the window of
#'   candidate post-break regimes.
#' @param min_retirement_age Minimum age included in the departure model.
#' @param seed Random seed.
#'
#' @return A named list with the fitted entrant model, the selected break year,
#'   per-year `entrant_draws` and `entrant_summary`, the fitted departure model,
#'   per-age `departure_hazard_draws` and `departure_summary`, the empirical
#'   `retirement_hazard_cv` (median CV over ages 60-80), and a `diagnostics`
#'   tibble.
#'
#' @seealso [advance_urps_supply_one_year()], [backtest_urps_supply_calibration()],
#'   [decompose_urps_forecast_miss()]
#' @family supply dynamics
#' @concept supply
#' @importFrom MASS mvrnorm
#' @export
calibrate_urps_supply_dynamics <- function(entrant_tbl, departure_tbl, forecast_years,
                                           n_draws = 5000L, recent_years = 5L,
                                           min_retirement_age = 50, seed = 42L) {
  base::message("Starting URPS supply-dynamics calibration.")
  base::message("Entrant rows: ", base::format(base::nrow(entrant_tbl), big.mark = ","))
  base::message("Departure provider-years: ",
                base::format(base::nrow(departure_tbl), big.mark = ","))
  base::message("Monte Carlo draws: ", base::format(n_draws, big.mark = ","))

  if (!base::all(c("year", "entrants") %in% base::names(entrant_tbl))) {
    base::stop("entrant_tbl must contain year and entrants.")
  }
  if (!base::all(c("provider_id", "year", "age", "departed") %in% base::names(departure_tbl))) {
    base::stop("departure_tbl must contain provider_id, year, age, and departed.")
  }
  base::set.seed(seed)

  # --- Empirical entrant series with a candidate regime break ----------------
  base::message("Preparing empirical entrant series.")
  entrant_clean <- entrant_tbl |>
    dplyr::transmute(year = base::as.integer(.data$year),
                     entrants = base::as.numeric(.data$entrants)) |>
    dplyr::filter(!base::is.na(.data$year), !base::is.na(.data$entrants),
                  .data$entrants >= 0) |>
    dplyr::arrange(.data$year)
  if (base::nrow(entrant_clean) < 6L) {
    base::stop("At least six entrant years are recommended.")
  }
  entrant_min_year <- base::min(entrant_clean$year)
  entrant_max_year <- base::max(entrant_clean$year)
  base::message("Entrant observation window: ", entrant_min_year, "-",
                entrant_max_year, ".")

  candidate_breaks <- entrant_clean |>
    dplyr::filter(.data$year >= entrant_max_year - recent_years - 2L,
                  .data$year <= entrant_max_year - 2L) |>
    dplyr::pull(.data$year)
  if (base::length(candidate_breaks) == 0L) {
    candidate_breaks <- entrant_clean |>
      dplyr::slice_tail(n = recent_years) |>
      dplyr::slice_head(n = 1L) |>
      dplyr::pull(.data$year)
  }
  base::message("Testing ", base::length(candidate_breaks),
                " candidate entrant regime breaks.")

  # Add the break-relative design columns for a given break year.
  add_break_terms <- function(tbl, break_year) {
    tbl |>
      dplyr::mutate(
        time = .data$year - entrant_min_year,
        post_break = base::as.integer(.data$year >= break_year),
        time_after_break = base::pmax(0, .data$year - break_year))
  }
  entrant_break_models <- purrr::map(candidate_breaks, function(break_year) {
    fitted_model <- stats::glm(
      entrants ~ time + post_break + time_after_break,
      family = stats::quasipoisson(link = "log"),
      data = add_break_terms(entrant_clean, break_year))
    tibble::tibble(break_year = break_year, model = list(fitted_model),
                   aic = stats::AIC(fitted_model))
  }) |>
    dplyr::bind_rows()

  if (base::all(!base::is.finite(entrant_break_models$aic))) {
    base::message("Quasi-Poisson AIC unavailable; selecting break by squared prediction error.")
    entrant_break_models <- entrant_break_models |>
      dplyr::mutate(prediction_error = purrr::map_dbl(.data$model, function(m) {
        base::sum((entrant_clean$entrants - stats::fitted(m))^2)
      }))
    selected_break_row <- entrant_break_models |>
      dplyr::slice_min(order_by = .data$prediction_error, n = 1L, with_ties = FALSE)
  } else {
    selected_break_row <- entrant_break_models |>
      dplyr::slice_min(order_by = .data$aic, n = 1L, with_ties = FALSE)
  }
  selected_break <- selected_break_row |> dplyr::pull(.data$break_year)
  selected_entrant_model <- selected_break_row |> dplyr::pull(.data$model) |> purrr::pluck(1L)
  base::message("Selected entrant regime break: ", selected_break, ".")

  entrant_coef <- stats::coef(selected_entrant_model)
  entrant_vcov <- stats::vcov(selected_entrant_model)
  entrant_dispersion <- stats::summary.glm(selected_entrant_model)$dispersion
  base::message("Entrant overdispersion estimate: ",
                base::format(entrant_dispersion, digits = 3), ".")

  # Sample coefficients; name the columns to match the design matrix later.
  entrant_beta_draws <- MASS::mvrnorm(n = n_draws, mu = entrant_coef, Sigma = entrant_vcov) |>
    tibble::as_tibble(.name_repair = "unique")
  base::names(entrant_beta_draws) <- base::names(entrant_coef)
  entrant_beta_draws <- entrant_beta_draws |> dplyr::mutate(draw = dplyr::row_number())

  # --- Age-specific departure hazard -----------------------------------------
  base::message("Estimating ABOG departure hazard.")
  departure_clean <- departure_tbl |>
    dplyr::transmute(provider_id = .data$provider_id,
                     year = base::as.integer(.data$year),
                     age = base::as.numeric(.data$age),
                     departed = base::as.integer(.data$departed)) |>
    dplyr::filter(!base::is.na(.data$provider_id), !base::is.na(.data$year),
                  !base::is.na(.data$age), !base::is.na(.data$departed),
                  .data$age >= min_retirement_age, .data$departed %in% c(0L, 1L))
  base::message("Eligible departure provider-years: ",
                base::format(base::nrow(departure_clean), big.mark = ","), ".")
  base::message("Observed departures: ",
                base::format(base::sum(departure_clean$departed), big.mark = ","), ".")
  if (base::sum(departure_clean$departed) < 20L) {
    base::warning("Fewer than 20 observed departures. Departure-hazard uncertainty may be large.")
  }

  departure_model <- stats::glm(
    departed ~ splines::ns(age, df = 4) + factor(year),
    family = stats::binomial(link = "cloglog"), data = departure_clean)
  departure_coef <- stats::coef(departure_model)
  departure_vcov <- stats::vcov(departure_model)
  departure_beta_draws <- MASS::mvrnorm(n = n_draws, mu = departure_coef, Sigma = departure_vcov) |>
    tibble::as_tibble(.name_repair = "unique")
  base::names(departure_beta_draws) <- base::names(departure_coef)
  departure_beta_draws <- departure_beta_draws |> dplyr::mutate(draw = dplyr::row_number())

  # --- Entrant forecast draws ------------------------------------------------
  base::message("Generating entrant forecast draws.")
  entrant_future_tbl <- add_break_terms(
    tibble::tibble(year = base::as.integer(forecast_years)), selected_break)
  entrant_design <- stats::model.matrix(~ time + post_break + time_after_break,
                                        data = entrant_future_tbl)
  entrant_beta_matrix <- base::as.matrix(entrant_beta_draws |> dplyr::select(-dplyr::all_of("draw")))
  entrant_mean_matrix <- base::exp(entrant_design %*% base::t(entrant_beta_matrix))
  entrant_draw_matrix <- base::matrix(
    stats::rpois(n = base::length(entrant_mean_matrix),
                 lambda = base::as.vector(entrant_mean_matrix)),
    nrow = base::nrow(entrant_mean_matrix), ncol = base::ncol(entrant_mean_matrix))
  entrant_draws <- entrant_draw_matrix |>
    tibble::as_tibble(.name_repair = "unique") |>
    dplyr::mutate(year = entrant_future_tbl$year) |>
    tidyr::pivot_longer(cols = -dplyr::all_of("year"),
                        names_to = "draw_name", values_to = "entrants") |>
    dplyr::group_by(.data$year) |>
    dplyr::mutate(draw = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::select("draw", "year", "entrants")
  entrant_summary <- entrant_draws |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      mean_entrants = base::mean(.data$entrants),
      sd_entrants = stats::sd(.data$entrants),
      median_entrants = stats::median(.data$entrants),
      p25_entrants = .sd_quantiles(.data$entrants)$p25,
      p75_entrants = .sd_quantiles(.data$entrants)$p75,
      p025_entrants = .sd_quantiles(.data$entrants)$p025,
      p975_entrants = .sd_quantiles(.data$entrants)$p975,
      .groups = "drop")

  # --- Age-specific departure-hazard draws -----------------------------------
  base::message("Generating age-specific departure-hazard draws.")
  age_grid <- base::seq(from = min_retirement_age, to = 90, by = 1)
  reference_year <- base::max(departure_clean$year)
  # The prediction frame carries a single year, so factor(year) would collapse
  # to one level and model.matrix() would throw "contrasts can be applied only
  # to factors with 2 or more levels"; passing the fitted model's xlevels
  # restores the full factor and the reference-year dummies line up with the
  # coefficient names.
  departure_design <- stats::model.matrix(
    stats::delete.response(stats::terms(departure_model)),
    data = tibble::tibble(age = age_grid, year = reference_year),
    xlev = departure_model$xlevels)
  departure_beta_matrix <- base::as.matrix(departure_beta_draws |> dplyr::select(-dplyr::all_of("draw")))
  common_terms <- base::intersect(base::colnames(departure_design),
                                  base::colnames(departure_beta_matrix))
  departure_linear_predictor <- departure_design[, common_terms, drop = FALSE] %*%
    base::t(departure_beta_matrix[, common_terms, drop = FALSE])
  departure_probability_matrix <- 1 - base::exp(-base::exp(departure_linear_predictor))
  departure_hazard_draws <- departure_probability_matrix |>
    tibble::as_tibble(.name_repair = "unique") |>
    dplyr::mutate(age = age_grid) |>
    tidyr::pivot_longer(cols = -dplyr::all_of("age"),
                        names_to = "draw_name", values_to = "departure_probability") |>
    dplyr::group_by(.data$age) |>
    dplyr::mutate(draw = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::select("draw", "age", "departure_probability")
  departure_summary <- departure_hazard_draws |>
    dplyr::group_by(.data$age) |>
    dplyr::summarise(
      mean_probability = base::mean(.data$departure_probability),
      sd_probability = stats::sd(.data$departure_probability),
      median_probability = stats::median(.data$departure_probability),
      p25_probability = .sd_quantiles(.data$departure_probability)$p25,
      p75_probability = .sd_quantiles(.data$departure_probability)$p75,
      p025_probability = .sd_quantiles(.data$departure_probability)$p025,
      p975_probability = .sd_quantiles(.data$departure_probability)$p975,
      .groups = "drop") |>
    dplyr::mutate(hazard_cv = dplyr::if_else(.data$mean_probability > 0,
                                             .data$sd_probability / .data$mean_probability,
                                             NA_real_))
  overall_hazard_cv <- departure_summary |>
    dplyr::filter(.data$age >= 60, .data$age <= 80, base::is.finite(.data$hazard_cv)) |>
    dplyr::summarise(hazard_cv = stats::median(.data$hazard_cv)) |>
    dplyr::pull(.data$hazard_cv)
  base::message("Estimated empirical retirement hazard CV: ",
                base::format(overall_hazard_cv, digits = 3), ".")

  calibration_diagnostics <- tibble::tibble(
    metric = c("entrant_break_year", "entrant_last_observed_year",
               "departure_reference_year", "observed_departures", "retirement_hazard_cv"),
    value = c(selected_break, entrant_max_year, reference_year,
              base::sum(departure_clean$departed), overall_hazard_cv))
  base::message("Supply-dynamics calibration complete.")

  base::list(
    entrant_model = selected_entrant_model, entrant_break_year = selected_break,
    entrant_draws = entrant_draws, entrant_summary = entrant_summary,
    departure_model = departure_model, departure_hazard_draws = departure_hazard_draws,
    departure_summary = departure_summary, retirement_hazard_cv = overall_hazard_cv,
    diagnostics = calibration_diagnostics)
}

#' Apply empirical supply uncertainty to one microsimulation draw
#'
#' @description
#' Evolves a provider population forward one year using an empirical ABOG-derived
#' departure-hazard draw and an empirical entrant draw. Ages every provider by one
#' year, applies the draw's age-specific departure probability, removes departed
#' providers, and appends the draw's entrant count.
#'
#' @param provider_tbl Current provider population with `provider_id` and `age`.
#' @param simulation_year Year being simulated.
#' @param draw Integer Monte Carlo draw number.
#' @param calibration Calibration object returned by
#'   [calibrate_urps_supply_dynamics()].
#' @param entrant_age Age assigned to newly independent URPS physicians.
#' @param entrant_prefix Prefix used to generate synthetic entrant IDs.
#'
#' @return Updated provider tibble after departures and entrants.
#'
#' @seealso [calibrate_urps_supply_dynamics()], [backtest_urps_supply_calibration()]
#' @family supply dynamics
#' @concept supply
#' @export
advance_urps_supply_one_year <- function(provider_tbl, simulation_year, draw, calibration,
                                        entrant_age = 35, entrant_prefix = "entrant") {
  base::message("Advancing supply to year ", simulation_year, " for draw ", draw, ".")
  # `draw` names both this argument and a column in the calibration draw tables.
  # Inside dplyr::filter() a bare `draw` resolves against the DATA first, so
  # `.data$draw == draw` would compare the column to itself (all TRUE, no
  # filtering). Capture the argument under a name that is not a column.
  target_draw <- draw

  # Keep only age + probability from the hazard draw. Carrying the draw table's
  # own `draw` column into the roster would collide on the next year's join.
  hazard_tbl <- calibration$departure_hazard_draws |>
    dplyr::filter(.data$draw == target_draw) |>
    dplyr::select("age", "departure_probability")
  current_providers <- provider_tbl |>
    dplyr::mutate(age = .data$age + 1) |>
    dplyr::mutate(age_integer = base::pmin(90L, base::pmax(50L,
                                    base::as.integer(base::round(.data$age))))) |>
    dplyr::left_join(hazard_tbl, by = c("age_integer" = "age")) |>
    dplyr::mutate(
      departure_probability = dplyr::coalesce(.data$departure_probability, 0),
      departure_draw = stats::runif(dplyr::n()),
      departed = .data$departure_draw < .data$departure_probability)
  base::message("Departures this year: ",
                base::format(base::sum(current_providers$departed), big.mark = ","), ".")
  surviving_providers <- current_providers |>
    dplyr::filter(!.data$departed) |>
    dplyr::select(-dplyr::any_of(c("age_integer", "departure_probability",
                                   "departure_draw", "departed")))

  entrant_count <- calibration$entrant_draws |>
    dplyr::filter(.data$draw == target_draw, .data$year == simulation_year) |>
    dplyr::pull(.data$entrants)
  if (base::length(entrant_count) != 1L) {
    base::stop("Expected exactly one entrant count for the requested year and draw.")
  }
  base::message("New independent URPS entrants: ",
                base::format(entrant_count, big.mark = ","), ".")
  if (entrant_count > 0L) {
    new_providers <- tibble::tibble(
      provider_id = base::paste(entrant_prefix, simulation_year, draw,
                                base::seq_len(entrant_count), sep = "_"),
      age = entrant_age)
    updated_providers <- dplyr::bind_rows(surviving_providers, new_providers)
  } else {
    updated_providers <- surviving_providers
  }
  base::message("End-of-year active supply: ",
                base::format(base::nrow(updated_providers), big.mark = ","), ".")
  updated_providers
}

#' Back-test calibration of the URPS supply model
#'
#' @description
#' Runs a hindcast from a historical base-year population and evaluates whether
#' the empirical entrant and departure processes recover observed future supply.
#'
#' The calibration targets are reported TOGETHER -- signed bias, empirical 95\%
#' interval coverage, and interval width -- so that a coverage failure cannot be
#' repaired simply by inflating the interval.
#'
#' @param base_provider_tbl Provider population at back-test start, with
#'   `provider_id` and `age`.
#' @param observed_supply_tbl Data frame with `year` and `observed_supply`.
#' @param calibration Calibration object from [calibrate_urps_supply_dynamics()].
#' @param start_year First year of the back-test.
#' @param end_year Last year of the back-test.
#' @param n_draws Number of Monte Carlo draws.
#' @param seed Random seed.
#'
#' @return List with per-draw `draws`, a `forecast_summary` (with `percent_error`,
#'   `covered_95`, `interval_width`), a one-row `metrics` summary, and a
#'   `summary_sentence`.
#'
#' @seealso [calibrate_urps_supply_dynamics()], [advance_urps_supply_one_year()],
#'   [decompose_urps_forecast_miss()]
#' @family supply dynamics
#' @concept supply
#' @export
backtest_urps_supply_calibration <- function(base_provider_tbl, observed_supply_tbl,
                                            calibration, start_year = 2020L, end_year = 2023L,
                                            n_draws = 5000L, seed = 42L) {
  base::message("Starting URPS supply back-test: ", start_year, " -> ", end_year, ".")
  base::set.seed(seed)
  backtest_draws <- purrr::map_dfr(base::seq_len(n_draws), function(draw_id) {
    provider_state <- base_provider_tbl
    yearly_counts <- tibble::tibble(draw = draw_id, year = start_year,
                                    predicted_supply = base::nrow(provider_state))
    for (simulation_year in base::seq.int(start_year + 1L, end_year)) {
      provider_state <- advance_urps_supply_one_year(
        provider_tbl = provider_state, simulation_year = simulation_year,
        draw = draw_id, calibration = calibration)
      yearly_counts <- yearly_counts |>
        dplyr::bind_rows(tibble::tibble(draw = draw_id, year = simulation_year,
                                        predicted_supply = base::nrow(provider_state)))
    }
    yearly_counts
  })

  base::message("Summarizing back-test distributions.")
  forecast_summary <- backtest_draws |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      mean_supply = base::mean(.data$predicted_supply),
      sd_supply = stats::sd(.data$predicted_supply),
      median_supply = stats::median(.data$predicted_supply),
      p25_supply = .sd_quantiles(.data$predicted_supply)$p25,
      p75_supply = .sd_quantiles(.data$predicted_supply)$p75,
      lower_95 = .sd_quantiles(.data$predicted_supply)$p025,
      upper_95 = .sd_quantiles(.data$predicted_supply)$p975,
      .groups = "drop") |>
    dplyr::left_join(
      observed_supply_tbl |>
        dplyr::transmute(year = base::as.integer(.data$year),
                         observed_supply = base::as.numeric(.data$observed_supply)),
      by = "year") |>
    dplyr::mutate(
      percent_error = 100 * (.data$mean_supply - .data$observed_supply) / .data$observed_supply,
      absolute_percent_error = base::abs(.data$percent_error),
      covered_95 = .data$observed_supply >= .data$lower_95 &
        .data$observed_supply <= .data$upper_95,
      interval_width = .data$upper_95 - .data$lower_95)

  calibration_metrics <- forecast_summary |>
    dplyr::filter(.data$year > start_year) |>
    dplyr::summarise(
      mean_percent_error = base::mean(.data$percent_error, na.rm = TRUE),
      sd_percent_error = stats::sd(.data$percent_error, na.rm = TRUE),
      median_absolute_percent_error = stats::median(.data$absolute_percent_error, na.rm = TRUE),
      p25_absolute_percent_error = stats::quantile(.data$absolute_percent_error,
                                                   probs = 0.25, na.rm = TRUE, names = FALSE),
      p75_absolute_percent_error = stats::quantile(.data$absolute_percent_error,
                                                   probs = 0.75, na.rm = TRUE, names = FALSE),
      coverage_95 = base::mean(.data$covered_95, na.rm = TRUE),
      mean_interval_width = base::mean(.data$interval_width, na.rm = TRUE))

  pull_at <- function(col, yr) forecast_summary[[col]][forecast_summary$year == yr]
  observed_change <- pull_at("observed_supply", end_year) - pull_at("observed_supply", start_year)
  predicted_change <- pull_at("mean_supply", end_year) - pull_at("mean_supply", start_year)
  slope_error <- predicted_change - observed_change
  direction_text <- dplyr::case_when(
    slope_error < 0 ~ "under-predicted supply growth",
    slope_error > 0 ~ "over-predicted supply growth",
    TRUE ~ "matched observed supply growth")
  summary_sentence <- base::sprintf(
    paste0("From %d to %d, observed URPS supply changed by %s clinicians, while the ",
           "calibrated model predicted a mean change of %s; the model %s by %s clinicians. ",
           "The empirical 95%% interval coverage was %.1f%%."),
    start_year, end_year, base::format(observed_change, big.mark = ","),
    base::format(base::round(predicted_change, 1), big.mark = ","), direction_text,
    base::format(base::round(base::abs(slope_error), 1), big.mark = ","),
    100 * calibration_metrics$coverage_95)
  base::message(summary_sentence)

  base::list(draws = backtest_draws, forecast_summary = forecast_summary,
             metrics = calibration_metrics, summary_sentence = summary_sentence)
}

#' Decompose a supply-forecast miss into break, model, and interval error
#'
#' @description
#' Implements the train/oracle experiment the naive back-test cannot: selecting
#' the entrant break on the complete series and then claiming a prospective
#' `start_year -> end_year` back-test leaks the future into the fit. This runs
#' two calibrations from the SAME base population:
#' \describe{
#'   \item{train}{Leakage-free. Both the entrant series and the departure events
#'     are truncated at `start_year` -- "what could the model have known then?"}
#'   \item{oracle}{Post-hoc. The complete series is used, so the break is chosen
#'     with hindsight.}
#' }
#' The gap between them attributes the miss:
#' \itemize{
#'   \item what the oracle STILL gets wrong is a deficient entrant model
#'     (structure hindsight cannot rescue);
#'   \item what the oracle recovers that the train fit missed is the
#'     unforeseeable-at-`start_year` regime break.
#' }
#' Each regime is scored on the same multi-metric target -- signed percent bias,
#' change/slope bias, 95\% coverage, and interval width, reported together.
#'
#' @param entrant_tbl Complete entrant series (`year`, `entrants`).
#' @param departure_tbl Complete provider-year departures (`provider_id`, `year`,
#'   `age`, `departed`).
#' @param base_provider_tbl Provider population at `start_year`.
#' @param observed_supply_tbl Observed supply (`year`, `observed_supply`) over
#'   the back-test window.
#' @param start_year First back-test year and the leakage cutoff for the train
#'   fit.
#' @param end_year Last back-test year.
#' @param n_draws Monte Carlo draws for each calibration and back-test.
#' @param min_retirement_age Minimum age in the departure model.
#' @param recent_years Candidate-break window passed to the calibration.
#' @param seed Random seed (shared so the two regimes differ only in their data).
#'
#' @return A list with per-regime `metrics` (a two-row tibble), a
#'   `decomposition` tibble splitting the slope error into
#'   `unforeseeable_regime_break` and `deficient_entrant_model` components, a
#'   `summary_sentence`, and the underlying `train` and `oracle` objects (each a
#'   list of `calibration` and `backtest`).
#'
#' @seealso [calibrate_urps_supply_dynamics()], [backtest_urps_supply_calibration()]
#' @family supply dynamics
#' @concept supply
#' @export
decompose_urps_forecast_miss <- function(entrant_tbl, departure_tbl, base_provider_tbl,
                                         observed_supply_tbl, start_year = 2020L,
                                         end_year = 2023L, n_draws = 2000L,
                                         min_retirement_age = 50, recent_years = 5L,
                                         seed = 42L) {
  base::message("Decomposing the ", start_year, " -> ", end_year,
                " supply-forecast miss (train vs oracle).")
  forecast_years <- base::seq.int(start_year, end_year)

  fit_regime <- function(regime_entrant, regime_departure, label) {
    base::message("Calibrating the ", label, " regime.")
    cal <- calibrate_urps_supply_dynamics(
      entrant_tbl = regime_entrant, departure_tbl = regime_departure,
      forecast_years = forecast_years, n_draws = n_draws,
      recent_years = recent_years, min_retirement_age = min_retirement_age, seed = seed)
    bt <- backtest_urps_supply_calibration(
      base_provider_tbl = base_provider_tbl, observed_supply_tbl = observed_supply_tbl,
      calibration = cal, start_year = start_year, end_year = end_year,
      n_draws = n_draws, seed = seed)
    base::list(calibration = cal, backtest = bt)
  }

  # Leakage-free: the model may only see data recorded through start_year.
  train_entrant <- entrant_tbl |> dplyr::filter(base::as.integer(.data$year) <= start_year)
  train_departure <- departure_tbl |> dplyr::filter(base::as.integer(.data$year) <= start_year)
  train <- fit_regime(train_entrant, train_departure, "train (leakage-free)")
  oracle <- fit_regime(entrant_tbl, departure_tbl, "oracle (post-hoc)")

  slope_of <- function(bt) {
    fs <- bt$forecast_summary
    (fs$mean_supply[fs$year == end_year] - fs$mean_supply[fs$year == start_year]) -
      (fs$observed_supply[fs$year == end_year] - fs$observed_supply[fs$year == start_year])
  }
  train_slope_error <- slope_of(train$backtest)
  oracle_slope_error <- slope_of(oracle$backtest)

  metric_row <- function(regime_label, bt, slope_error) {
    tibble::tibble(regime = regime_label,
                   signed_percent_bias = bt$metrics$mean_percent_error,
                   slope_bias = slope_error,
                   coverage_95 = bt$metrics$coverage_95,
                   mean_interval_width = bt$metrics$mean_interval_width)
  }
  metrics <- dplyr::bind_rows(
    metric_row("train", train$backtest, train_slope_error),
    metric_row("oracle", oracle$backtest, oracle_slope_error))

  # The oracle keeps its residual slope error even with hindsight: that portion
  # is a deficient entrant model. The part the oracle recovers relative to the
  # train fit was unknowable at start_year -- an unforeseeable regime break.
  decomposition <- tibble::tibble(
    component = c("unforeseeable_regime_break", "deficient_entrant_model"),
    slope_error_clinicians = c(train_slope_error - oracle_slope_error, oracle_slope_error))

  summary_sentence <- base::sprintf(
    paste0("Over %d-%d the leakage-free model missed the supply slope by %.1f clinicians; ",
           "hindsight (oracle) narrows that to %.1f. Of the miss, %.1f is attributable to an ",
           "unforeseeable regime break and %.1f to a deficient entrant model. ",
           "Train 95%% coverage %.1f%%, oracle %.1f%%."),
    start_year, end_year, train_slope_error, oracle_slope_error,
    train_slope_error - oracle_slope_error, oracle_slope_error,
    100 * train$backtest$metrics$coverage_95, 100 * oracle$backtest$metrics$coverage_95)
  base::message(summary_sentence)

  base::list(metrics = metrics, decomposition = decomposition,
             summary_sentence = summary_sentence, train = train, oracle = oracle)
}
