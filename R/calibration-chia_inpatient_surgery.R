# Poisson Population-Offset Inpatient Surgery Rate Model ----
#
# Calibration tier: observed_regional (Massachusetts, FY2004-FY2018)
#
# Fits log(E[cases]) = X * beta + log(female_population) using a Poisson or
# quasi-Poisson rate model with female population as the offset.

#' Fit Poisson population-offset inpatient surgery rate model
#'
#' Fits an age- and year-stratified rate model for inpatient pelvic reconstructive
#' surgery using CHIA D6 series data with population at risk as offset.
#'
#' @param chia_d6_tbl Table from [build_chia_inpatient_urps_series()].
#' @param family Choice of `"poisson"` or `"quasipoisson"`. Default is `"quasipoisson"`.
#' @param include_interaction Logical. Include `year:age_band` interaction term? Default `TRUE`.
#'
#' @return A list with `model` (glm object), `coefficients` (summary table),
#'   `fitted_rates` (predicted rates table), and `dispersion` parameter.
#'
#' @family chia inpatient surgery
#' @concept calibration
#' @export
fit_inpatient_surgery_rate_model <- function(
    chia_d6_tbl,
    family = "quasipoisson",
    include_interaction = TRUE) {

  base::message("fit_inpatient_surgery_rate_model(): starting.")

  assertthat::assert_that(all(c("year", "age_band", "procedure_family", "inpatient_cases", "female_population") %in% names(chia_d6_tbl)))

  d6_data <- chia_d6_tbl |>
    dplyr::filter(
      is.finite(inpatient_cases),
      is.finite(female_population),
      female_population > 0
    )

  if (base::isTRUE(include_interaction)) {
    formula_str <- "inpatient_cases ~ procedure_family + age_band + year + year:age_band + offset(log(female_population))"
  } else {
    formula_str <- "inpatient_cases ~ procedure_family + age_band + year + offset(log(female_population))"
  }

  fam_obj <- if (family == "quasipoisson") stats::quasipoisson() else stats::poisson()

  fit <- stats::glm(
    formula = stats::as.formula(formula_str),
    family = fam_obj,
    data = d6_data
  )

  summary_fit <- stats::summary.glm(fit)

  fitted_df <- d6_data |>
    dplyr::mutate(
      predicted_cases = stats::predict(fit, type = "response"),
      predicted_rate_per_100k = (predicted_cases / female_population) * 100000,
      residual = inpatient_cases - predicted_cases
    )

  dispersion <- summary_fit$dispersion

  base::message("Model fit complete. Dispersion: ", round(dispersion, 3))

  list(
    model = fit,
    coefficients = stats::coef(summary_fit),
    fitted_rates = fitted_df,
    dispersion = dispersion
  )
}
