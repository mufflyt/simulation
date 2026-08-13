# CHIA all-payer <-> Medicare FFS bridge: fit and national projection --------
#
# The second half of the CHIA/Medicare workload bridge (see
# R/calibration-allpayer_medicare_bridge.R for claims ingestion and the
# provider-year panel builders). This file forms the MA overlap, fits the
# all-payer/Medicare relationship, projects it onto national Medicare-observed
# URPS, and reads off the empirical workload-by-age gradient. The `.bridge_comma`
# helper lives in the ingestion file and is visible here (one package namespace).
# Same guardrail: delivered-workload calibration, never a "calibrated" adequacy.

#' Join CHIA and Medicare provider-year workloads in Massachusetts
#'
#' @param chia_provider_year CHIA provider-year table.
#' @param medicare_provider_year Medicare provider-year table.
#'
#' @return Overlapping provider-year calibration sample.
#' @family allpayer bridge
#' @concept calibration
#' @export
join_chia_medicare_overlap <- function(
    chia_provider_year,
    medicare_provider_year) {

  base::message(
    "join_chia_medicare_overlap(): restricting Medicare to Massachusetts."
  )

  medicare_ma_tbl <- medicare_provider_year |>
    dplyr::filter(
      .data$state == "MA" |
        base::is.na(.data$state)
    ) |>
    dplyr::select(
      "npi",
      "year",
      medicare_claim_lines = "claim_lines",
      medicare_units = "service_units",
      medicare_patients = "unique_patients",
      medicare_wrvu = "total_wrvu",
      medicare_provider_age = "provider_age",
      medicare_provider_sex = "provider_sex"
    )

  base::message(
    "join_chia_medicare_overlap(): joining by NPI + year."
  )

  overlap_tbl <- chia_provider_year |>
    dplyr::select(
      "npi",
      "year",
      chia_claim_lines = "claim_lines",
      chia_units = "service_units",
      chia_patients = "unique_patients",
      chia_wrvu = "total_wrvu",
      chia_provider_age = "provider_age",
      chia_provider_sex = "provider_sex"
    ) |>
    dplyr::inner_join(
      medicare_ma_tbl,
      by = c("npi", "year")
    )

  base::message(
    "join_chia_medicare_overlap(): overlap = ",
    .bridge_comma(base::nrow(overlap_tbl)),
    " provider-years."
  )

  if (base::nrow(overlap_tbl) < 20L) {
    base::warning(
      "Only ",
      base::nrow(overlap_tbl),
      " overlapping provider-years were found. ",
      "Do not use a national bridge until NPI/year linkage is checked.",
      call. = FALSE
    )
  }

  overlap_tbl
}


#' Choose the strongest workload metric shared by CHIA and Medicare
#'
#' Preference: wRVU -> service units -> patients -> claim lines.
#'
#' @param overlap_tbl Joined CHIA/Medicare provider-year table.
#'
#' @return List describing the selected workload pair.
#' @family allpayer bridge
#' @concept calibration
#' @export
select_bridge_workload <- function(overlap_tbl) {

  base::message(
    "select_bridge_workload(): selecting common workload metric."
  )

  candidate_pairs <- list(
    wrvu = c("chia_wrvu", "medicare_wrvu"),
    units = c("chia_units", "medicare_units"),
    patients = c("chia_patients", "medicare_patients"),
    claim_lines = c(
      "chia_claim_lines",
      "medicare_claim_lines"
    )
  )

  for (metric_name in base::names(candidate_pairs)) {
    pair <- candidate_pairs[[metric_name]]

    usable <- overlap_tbl |>
      dplyr::filter(
        base::is.finite(.data[[pair[[1]]]]),
        base::is.finite(.data[[pair[[2]]]]),
        .data[[pair[[1]]]] > 0,
        .data[[pair[[2]]]] > 0
      )

    if (base::nrow(usable) >= 20L) {
      base::message(
        "select_bridge_workload(): selected ",
        metric_name,
        " using ",
        .bridge_comma(base::nrow(usable)),
        " provider-years."
      )

      return(
        list(
          metric = metric_name,
          chia_column = pair[[1]],
          medicare_column = pair[[2]],
          usable = usable
        )
      )
    }
  }

  base::stop(
    paste(
      "No workload metric has at least 20 provider-years with positive",
      "values in both CHIA and Medicare."
    ),
    call. = FALSE
  )
}


#' Fit the CHIA all-payer to Medicare workload bridge
#'
#' Fits a log-log provider-year model:
#'
#' \preformatted{log(all-payer workload) = intercept + beta * log(Medicare
#'   workload) + year effects}
#'
#' Provider age and sex enter when available.
#'
#' @param overlap_tbl Joined overlap sample.
#'
#' @return Bridge-fit object.
#' @family allpayer bridge
#' @concept calibration
#' @export
fit_chia_medicare_bridge <- function(overlap_tbl) {

  base::message(
    "fit_chia_medicare_bridge(): selecting workload."
  )

  workload_spec <- select_bridge_workload(
    overlap_tbl
  )

  bridge_tbl <- workload_spec$usable |>
    dplyr::mutate(
      allpayer_workload =
        .data[[workload_spec$chia_column]],
      medicare_workload =
        .data[[workload_spec$medicare_column]],
      provider_age = dplyr::coalesce(
        .data$chia_provider_age,
        .data$medicare_provider_age
      ),
      provider_sex = dplyr::coalesce(
        .data$chia_provider_sex,
        .data$medicare_provider_sex
      ),
      log_allpayer = base::log(
        .data$allpayer_workload
      ),
      log_medicare = base::log(
        .data$medicare_workload
      ),
      year_factor = base::factor(.data$year)
    )

  has_age <- base::sum(
    !base::is.na(bridge_tbl$provider_age)
  ) >= 20L

  has_sex <- base::length(
    base::unique(
      stats::na.omit(bridge_tbl$provider_sex)
    )
  ) >= 2L

  formula_terms <- c(
    "log_medicare",
    "year_factor"
  )

  if (has_age) {
    formula_terms <- c(
      formula_terms,
      "splines::ns(provider_age, df = 3)"
    )
  }

  if (has_sex) {
    formula_terms <- c(
      formula_terms,
      "provider_sex"
    )
  }

  bridge_formula <- stats::as.formula(
    base::paste(
      "log_allpayer ~",
      base::paste(
        formula_terms,
        collapse = " + "
      )
    )
  )

  base::message(
    "fit_chia_medicare_bridge(): fitting ",
    base::deparse(bridge_formula),
    "."
  )

  bridge_model <- stats::lm(
    formula = bridge_formula,
    data = bridge_tbl
  )

  residual_sd <- stats::sigma(
    bridge_model
  )

  smearing_factor <- base::mean(
    base::exp(
      stats::residuals(bridge_model)
    )
  )

  raw_ratio <- bridge_tbl |>
    dplyr::mutate(
      allpayer_to_medicare =
        .data$allpayer_workload /
        .data$medicare_workload
    )

  ratio_summary <- raw_ratio |>
    dplyr::summarise(
      n_provider_years = dplyr::n(),
      mean_ratio = base::mean(
        .data$allpayer_to_medicare
      ),
      sd_ratio = stats::sd(
        .data$allpayer_to_medicare
      ),
      p25_ratio = stats::quantile(
        .data$allpayer_to_medicare,
        probs = 0.25,
        names = FALSE
      ),
      median_ratio = stats::median(
        .data$allpayer_to_medicare
      ),
      p75_ratio = stats::quantile(
        .data$allpayer_to_medicare,
        probs = 0.75,
        names = FALSE
      )
    )

  base::message(
    "fit_chia_medicare_bridge(): median all-payer/Medicare ratio = ",
    base::sprintf(
      "%.2f",
      ratio_summary$median_ratio
    ),
    "."
  )

  list(
    model = bridge_model,
    workload_metric = workload_spec$metric,
    calibration_sample = bridge_tbl,
    ratio_summary = ratio_summary,
    smearing_factor = smearing_factor,
    residual_sd = residual_sd,
    calibration_status =
      "measured_input_unvalidated_response"
  )
}


#' Apply CHIA/Medicare bridge to national Medicare provider-years
#'
#' @param bridge_fit Output from fit_chia_medicare_bridge().
#' @param medicare_provider_year National Medicare provider-year panel.
#'
#' @return Provider-year table with estimated all-payer workload.
#' @family allpayer bridge
#' @concept calibration
#' @export
predict_allpayer_from_medicare <- function(
    bridge_fit,
    medicare_provider_year) {

  base::message(
    "predict_allpayer_from_medicare(): projecting national workload."
  )

  metric_column <- switch(
    bridge_fit$workload_metric,
    wrvu = "total_wrvu",
    units = "service_units",
    patients = "unique_patients",
    claim_lines = "claim_lines"
  )

  prediction_tbl <- medicare_provider_year |>
    dplyr::filter(
      base::is.finite(.data[[metric_column]]),
      .data[[metric_column]] > 0
    ) |>
    dplyr::mutate(
      medicare_workload =
        .data[[metric_column]],
      log_medicare =
        base::log(.data$medicare_workload),
      year_factor =
        base::factor(.data$year)
    )

  prediction_log <- stats::predict(
    bridge_fit$model,
    newdata = prediction_tbl,
    se.fit = TRUE
  )

  prediction_tbl <- prediction_tbl |>
    dplyr::mutate(
      estimated_allpayer_workload =
        base::exp(
          prediction_log$fit
        ) *
        bridge_fit$smearing_factor,
      estimated_allpayer_low =
        base::exp(
          prediction_log$fit -
            1.96 * prediction_log$se.fit
        ) *
        bridge_fit$smearing_factor,
      estimated_allpayer_high =
        base::exp(
          prediction_log$fit +
            1.96 * prediction_log$se.fit
        ) *
        bridge_fit$smearing_factor,
      allpayer_medicare_multiplier =
        .data$estimated_allpayer_workload /
        .data$medicare_workload,
      calibration_status =
        "measured_input_unvalidated_response"
    )

  base::message(
    "predict_allpayer_from_medicare(): generated ",
    .bridge_comma(base::nrow(prediction_tbl)),
    " national provider-year estimates."
  )

  prediction_tbl
}


#' Estimate empirical provider workload by age
#'
#' This is the claims-derived replacement candidate for the SHAPE of the
#' borrowed HWSM hours/FTE curve. It does not assert that workload equals hours;
#' it estimates the empirical age gradient in delivered all-payer workload and
#' normalizes that gradient to age 45-54.
#'
#' @param provider_year_tbl Projected all-payer provider-year table.
#' @param minimum_provider_years Minimum observations per age group.
#'
#' @return Age-specific workload factors.
#' @family allpayer bridge
#' @concept calibration
#' @export
estimate_workload_age_curve <- function(
    provider_year_tbl,
    minimum_provider_years = 20L) {

  base::message(
    "estimate_workload_age_curve(): estimating age gradient."
  )

  age_curve_tbl <- provider_year_tbl |>
    dplyr::filter(
      base::is.finite(.data$provider_age),
      base::is.finite(
        .data$estimated_allpayer_workload
      ),
      .data$estimated_allpayer_workload > 0
    ) |>
    dplyr::mutate(
      age_group = base::cut(
        .data$provider_age,
        breaks = c(
          0,
          44,
          54,
          64,
          74,
          Inf
        ),
        labels = c(
          "<45",
          "45-54",
          "55-64",
          "65-74",
          "75+"
        ),
        right = TRUE
      )
    ) |>
    dplyr::group_by(
      .data$age_group
    ) |>
    dplyr::summarise(
      n_provider_years = dplyr::n(),
      mean_workload = base::mean(
        .data$estimated_allpayer_workload
      ),
      sd_workload = stats::sd(
        .data$estimated_allpayer_workload
      ),
      p25_workload = stats::quantile(
        .data$estimated_allpayer_workload,
        probs = 0.25,
        names = FALSE
      ),
      median_workload = stats::median(
        .data$estimated_allpayer_workload
      ),
      p75_workload = stats::quantile(
        .data$estimated_allpayer_workload,
        probs = 0.75,
        names = FALSE
      ),
      .groups = "drop"
    ) |>
    dplyr::filter(
      .data$n_provider_years >=
        minimum_provider_years
    )

  reference_workload <- age_curve_tbl |>
    dplyr::filter(
      .data$age_group == "45-54"
    ) |>
    dplyr::pull(
      .data$median_workload
    )

  if (base::length(reference_workload) != 1L) {
    base::stop(
      paste(
        "Could not identify a usable 45-54 reference group.",
        "Do not normalize the age curve."
      ),
      call. = FALSE
    )
  }

  age_curve_tbl <- age_curve_tbl |>
    dplyr::mutate(
      relative_workload =
        .data$median_workload /
        reference_workload
    )

  base::message(
    "estimate_workload_age_curve(): complete."
  )

  age_curve_tbl
}


