# Per-year provider-capacity computation for run_end_to_end_simulation ------

#' Compute one simulation year's provider capacity
#'
#' Extracted from [run_end_to_end_simulation()]'s per-year loop so that
#' function stays under the repository's module code-line ceiling; this is a
#' pure code move (identical logic, now callable on its own) not a behavior
#' change. Dispatches on `productivity_engine`: `"benchmark"` uses a flat
#' 1,600 patients/FTE assumption, `"lmer_fitted"` predicts from a fitted
#' mixed-effects productivity model (via a caller-supplied
#' `productivity_predictor`, or the built-in
#' `.lmer_fitted_predictor_bridge()` + [predict_provider_capacity()] path,
#' which is a SYNTHETIC-FIT demonstration unless the model was trained on a
#' real, non-mock productivity panel).
#'
#' @param active_providers Active-provider tibble for `simulation_year`.
#' @param simulation_year The calendar year being simulated.
#' @param productivity_engine Either `"benchmark"` or `"lmer_fitted"`.
#' @param productivity_predictor Optional custom predictor function
#'   `(model, providers, year) -> capacity`. When `NULL` and
#'   `productivity_engine == "lmer_fitted"`, falls back to
#'   `.lmer_fitted_predictor_bridge()` + [predict_provider_capacity()].
#' @param fitted_productivity_model A `model_bundle` from
#'   [fit_provider_productivity_model()], required when
#'   `productivity_engine == "lmer_fitted"`.
#'
#' @return A tibble with `provider_id` and `annual_patient_capacity`.
#' @keywords internal
.run_provider_capacity_year <- function(active_providers,
                                         simulation_year,
                                         productivity_engine,
                                         productivity_predictor,
                                         fitted_productivity_model) {
  if (productivity_engine == "benchmark") {
    provider_capacity <- active_providers |>
      dplyr::transmute(
        provider_id = .data$provider_id,
        annual_patient_capacity = .data$fte * 1600
      )
  } else {
    base::message("Predicting fitted provider capacity for ",
      simulation_year, ".")
    if (!base::is.function(productivity_predictor)) {
      base::message(
        "NOTE: productivity_engine = \"lmer_fitted\" with no custom ",
        "productivity_predictor uses .lmer_fitted_predictor_bridge(), ",
        "which fills sex/rural/app_support_rate/case-mix-share with ",
        "fixed placeholder constants (provider_cohort has no real ",
        "per-provider data for them). This is a SYNTHETIC-FIT capacity ",
        "path -- fit_provider_productivity_model()'s trained data is not ",
        "empirical unless a real, non-mock productivity_panel was ",
        "supplied. Treat these results as a mechanism demonstration, ",
        "not an empirical estimate."
      )
    }
    if (base::is.function(productivity_predictor)) {
      predicted_capacity <- productivity_predictor(
        fitted_productivity_model,
        active_providers,
        simulation_year
      )
    } else {
      # fitted_productivity_model is a model_bundle (list: model,
      # analysis_panel, diagnostics, outcome, formula), not a bare lme4
      # fit -- stats::predict() on the bundle itself does not dispatch to
      # predict.merMod() and would error before reaching a prediction.
      # predict_provider_capacity() (R/demand-measure_provider_productivity.R)
      # is the correct, pre-existing wrapper: it extracts $model,
      # predicts on the model's log(outcome) scale, and exponentiates --
      # calling stats::predict() directly here previously returned raw
      # log-scale values as if they were already annual_patient_capacity.
      bridged_providers <- .lmer_fitted_predictor_bridge(active_providers)
      predicted_panel <- predict_provider_capacity(
        fitted_productivity_model, bridged_providers
      )
      # UNIT CHECK, not a formality: the model_bundle's outcome can be
      # wrvu_per_clinical_fte or wrvu_per_clinical_hour -- workload
      # measures, not a patient headcount -- and treating either as
      # annual_patient_capacity would silently corrupt the patient-flow
      # conservation identity downstream by roughly an order of
      # magnitude. Only encounters_per_clinical_fte means "patient
      # capacity" in this runner's units.
      if (!base::identical(
        predicted_panel$capacity_outcome[[1]], "encounters_per_clinical_fte"
      )) {
        base::stop(
          "run_end_to_end_simulation(): fitted_productivity_model was ",
          "fit on outcome = '", predicted_panel$capacity_outcome[[1]],
          "', not 'encounters_per_clinical_fte'. Re-fit with ",
          "fit_provider_productivity_model(outcome = ",
          "\"encounters_per_clinical_fte\") -- a wRVU-scale outcome is ",
          "not a patient headcount and must not be used as ",
          "annual_patient_capacity.",
          call. = FALSE
        )
      }
      predicted_capacity <- predicted_panel |>
        dplyr::transmute(
          provider_id = .data$provider_id,
          predicted_capacity = .data$predicted_capacity
        )
    }
    if (base::is.data.frame(predicted_capacity)) {
      capacity_columns <- base::intersect(
        base::c(
          "annual_patient_capacity",
          "predicted_capacity",
          "capacity"
        ),
        base::names(predicted_capacity)
      )
      if (base::length(capacity_columns) == 0L ||
          !"provider_id" %in% base::names(predicted_capacity)) {
        base::stop(
          paste0(
            "A tabular productivity prediction must contain provider_id ",
            "and a recognized capacity column."
          ),
          call. = FALSE
        )
      }
      capacity_column <- capacity_columns[[1L]]
      provider_capacity <- predicted_capacity |>
        dplyr::transmute(
          provider_id = .data$provider_id,
          annual_patient_capacity = .data[[capacity_column]]
        )
    } else {
      provider_capacity <- active_providers |>
        dplyr::transmute(
          provider_id = .data$provider_id,
          annual_patient_capacity = base::as.numeric(
            predicted_capacity
          )
        )
    }
    if (base::any(!base::is.finite(
      provider_capacity$annual_patient_capacity
    )) || base::any(provider_capacity$annual_patient_capacity < 0)) {
      base::stop(
        "Fitted productivity predictions must be finite and nonnegative.",
        call. = FALSE
      )
    }
  }

  provider_capacity
}
