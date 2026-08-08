# Calibration and back-testing for the life-course demand pathway ----
#
# Anchors the life-course model's base-year service volumes to independent
# national totals (improvement plan Part C / IP sec 4), reusing the package's
# calibration machinery: fit_calibration_scalars() (scalar = observed/predicted,
# HDMM Exhibit 11) and apply_calibration_scalars(). The anchors and the back-test
# window are declared in config/calibration_targets.yml.
#
# A model with no anchor is not calibrated to anything; a back-test that
# reproduces a held-out year is what earns trust in a projection.

# Map life-course service lines to the national anchor categories in
# config/calibration_targets.yml. Services without an anchor are left uncalibrated
# (scalar 1). Top-level constant (assignment only).
LIFECOURSE_ANCHOR_MAP <- tibble::tribble(
  ~service,             ~category,
  "new_consultation",   "urps_office_visits",
  "return_visit",       "urps_office_visits",
  "sling_procedure",    "sling_procedure_volume",
  "prolapse_procedure", "prolapse_procedure_volume"
)

#' Aggregate life-course service volumes into national anchor categories
#'
#' @param service_volumes Tibble `year`, `service`, `volume` (from
#'   [lifecourse_demand_trajectory()] / [simulate_lifecourse_demand()]).
#' @param base_year Year whose totals are compared to the anchors. Default 2025.
#' @param map Service-to-anchor-category map. Default `LIFECOURSE_ANCHOR_MAP`.
#' @return Tibble `category`, `predicted` (base-year national totals).
#' @family demand lifecourse
#' @concept calibration
#' @export
lifecourse_anchor_predictions <- function(service_volumes, base_year = 2025L,
                                          map = LIFECOURSE_ANCHOR_MAP) {
  assertthat::assert_that(all(c("year", "service", "volume") %in% names(service_volumes)))
  service_volumes %>%
    dplyr::filter(.data$year == base_year) %>%
    dplyr::inner_join(map, by = "service") %>%
    dplyr::group_by(.data$category) %>%
    dplyr::summarise(predicted = sum(.data$volume), .groups = "drop")
}

#' Calibrate life-course service volumes to independent national anchors
#'
#' Fits multiplicative scalars (`observed / predicted`) against the anchor
#' categories via [fit_calibration_scalars()], so downstream FTE conversion runs
#' on anchored volumes rather than raw model output.
#'
#' @param service_volumes Tibble `year`, `service`, `volume`.
#' @param observed Tibble `category`, `observed` (independent national totals).
#' @param base_year Base year for the comparison. Default 2025.
#' @param max_scalar Scalars beyond this magnitude are flagged (structural
#'   mismatch, not an offset). Default 3.
#' @param map Service-to-anchor-category map.
#' @return List: `scalars` (from [fit_calibration_scalars()]) and
#'   `service_volumes` (calibrated; each service scaled by its category scalar,
#'   unanchored services unchanged).
#' @family demand lifecourse
#' @concept calibration
#' @export
calibrate_lifecourse_demand <- function(service_volumes, observed, base_year = 2025L,
                                        max_scalar = 3, map = LIFECOURSE_ANCHOR_MAP) {
  predicted <- lifecourse_anchor_predictions(service_volumes, base_year, map)
  scalars <- fit_calibration_scalars(predicted, observed, max_scalar = max_scalar)

  svc_scalar <- map %>%
    dplyr::inner_join(dplyr::select(scalars, "category", "scalar"), by = "category")
  calibrated <- service_volumes %>%
    dplyr::left_join(dplyr::select(svc_scalar, "service", "scalar"), by = "service") %>%
    dplyr::mutate(volume = .data$volume * dplyr::coalesce(.data$scalar, 1)) %>%
    dplyr::select("year", "service", "volume")

  list(scalars = scalars, service_volumes = calibrated)
}

#' Back-test the life-course demand model against a held-out year
#'
#' Fits calibration scalars on `fit_through_year`, carries them forward, and
#' compares the model's projected anchor totals at `target_year` with observed
#' totals — the credibility check the Dall-family models stop short of. Defaults
#' read `config/calibration_targets.yml$backtest`.
#'
#' @param service_volumes Tibble `year`, `service`, `volume` spanning both years.
#' @param observed_fit Tibble `category`, `observed` at `fit_through_year`.
#' @param observed_target Tibble `category`, `observed` at `target_year`.
#' @param fit_through_year,target_year Back-test window.
#' @param map Service-to-anchor-category map.
#' @return List: `by_category` (`category`, `predicted`, `observed`, `ratio`,
#'   `abs_pct_error`) and `summary` (`mape`, `n`, `target_year`).
#' @family demand lifecourse
#' @concept calibration
#' @export
backtest_lifecourse <- function(service_volumes, observed_fit, observed_target,
                                fit_through_year = 2017L, target_year = 2023L,
                                map = LIFECOURSE_ANCHOR_MAP) {
  fit_pred <- lifecourse_anchor_predictions(service_volumes, fit_through_year, map)
  scalars  <- fit_calibration_scalars(fit_pred, observed_fit)

  tgt_pred <- lifecourse_anchor_predictions(service_volumes, target_year, map) %>%
    dplyr::rename(raw_predicted = "predicted") %>%
    dplyr::left_join(dplyr::select(scalars, "category", "scalar"), by = "category") %>%
    dplyr::mutate(predicted = .data$raw_predicted * dplyr::coalesce(.data$scalar, 1))

  by_cat <- tgt_pred %>%
    dplyr::inner_join(observed_target, by = "category") %>%
    dplyr::mutate(
      ratio = dplyr::if_else(.data$observed > 0, .data$predicted / .data$observed, NA_real_),
      abs_pct_error = dplyr::if_else(.data$observed > 0,
                                     100 * abs(.data$predicted - .data$observed) / .data$observed,
                                     NA_real_)
    ) %>%
    dplyr::select("category", "predicted", "observed", "ratio", "abs_pct_error")

  list(
    by_category = by_cat,
    summary = tibble::tibble(
      target_year = target_year,
      n = nrow(by_cat),
      # NA (not a NaN "score") when no category is comparable, so an empty
      # back-test does not read as a normal result.
      mape = if (any(is.finite(by_cat$abs_pct_error)))
        mean(by_cat$abs_pct_error, na.rm = TRUE) else NA_real_
    )
  )
}
