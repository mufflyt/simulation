#' Build a provider-year urogynecology productivity panel
#'
#' Creates measured annual productivity outcomes without using an adequacy,
#' demand, or required-FTE anchor. Actual operative minutes are preferred;
#' published estimates are second; CMS physician-time estimates are last.
#'
#' @param provider_year A provider-year tibble with provider characteristics.
#' @param services A service-level tibble with provider, date, service, and
#'   work-RVU fields.
#' @param time_supplements Optional CPT-level tibble containing published and
#'   CMS time estimates.
#' @param app_support Optional provider-year tibble of APP-supported volume.
#' @param provider_id Provider identifier column name.
#' @param service_date Date-of-service column name.
#' @param cpt CPT/HCPCS column name.
#' @param service_type Service category column name.
#' @param work_rvu Work-RVU column name.
#' @param actual_minutes Actual operative-minute column name.
#' @return A provider-year tibble ready for productivity modeling.
#' @export
build_provider_year_productivity_panel <- function(
    provider_year,
    services,
    time_supplements = NULL,
    app_support = NULL,
    provider_id = "provider_id",
    service_date = "service_date",
    cpt = "cpt",
    service_type = "service_type",
    work_rvu = "work_rvu",
    actual_minutes = "actual_operative_minutes") {
  base::message("Starting provider-year productivity panel construction.")
  base::message(
    "Inputs: ", nrow(provider_year), " provider-year rows; ",
    nrow(services), " service rows."
  )

  required_provider <- c(
    provider_id, "year", "clinical_fte", "clinical_hours_week",
    "age", "sex", "academic", "rural", "years_since_fellowship"
  )
  required_services <- c(
    provider_id, service_date, cpt, service_type, work_rvu
  )
  missing_provider <- base::setdiff(required_provider, names(provider_year))
  missing_services <- base::setdiff(required_services, names(services))

  if (length(missing_provider) > 0L) {
    base::stop(
      "provider_year is missing: ",
      base::paste(missing_provider, collapse = ", ")
    )
  }
  if (length(missing_services) > 0L) {
    base::stop(
      "services is missing: ",
      base::paste(missing_services, collapse = ", ")
    )
  }
  if (anyDuplicated(provider_year[c(provider_id, "year")]) > 0L) {
    base::stop("provider_year must contain one row per provider and year.")
  }

  if (!actual_minutes %in% names(services)) {
    services[[actual_minutes]] <- NA_real_
    base::message(
      "Actual operative minutes were absent; created an explicit missing field."
    )
  }

  service_panel <- services |>
    dplyr::mutate(
      year = base::as.integer(base::format(base::as.Date(.data[[service_date]]), "%Y")),
      cpt_join = base::as.character(.data[[cpt]]),
      work_rvu_value = base::as.numeric(.data[[work_rvu]]),
      actual_minutes_value = base::as.numeric(.data[[actual_minutes]])
    )
  base::message("Derived calendar year and normalized service fields.")

  if (!is.null(time_supplements)) {
    required_time <- c("cpt", "published_minutes", "cms_minutes")
    missing_time <- base::setdiff(required_time, names(time_supplements))
    if (length(missing_time) > 0L) {
      base::stop(
        "time_supplements is missing: ",
        base::paste(missing_time, collapse = ", ")
      )
    }
    time_lookup <- time_supplements |>
      dplyr::transmute(
        cpt_join = base::as.character(.data$cpt),
        published_minutes = base::as.numeric(.data$published_minutes),
        cms_minutes = base::as.numeric(.data$cms_minutes)
      ) |>
      dplyr::distinct(.data$cpt_join, .keep_all = TRUE)
    service_panel <- service_panel |>
      dplyr::left_join(time_lookup, by = "cpt_join")
    base::message("Joined published and CMS time supplements by CPT.")
  } else {
    service_panel <- service_panel |>
      dplyr::mutate(
        published_minutes = NA_real_,
        cms_minutes = NA_real_
      )
    base::message("No time supplements supplied; retaining observed time only.")
  }

  service_panel <- service_panel |>
    dplyr::mutate(
      operative_minutes = dplyr::coalesce(
        .data$actual_minutes_value,
        .data$published_minutes,
        .data$cms_minutes
      ),
      time_source = dplyr::case_when(
        !is.na(.data$actual_minutes_value) ~ "observed",
        !is.na(.data$published_minutes) ~ "published",
        !is.na(.data$cms_minutes) ~ "cms",
        TRUE ~ "missing"
      ),
      service_group = base::tolower(base::as.character(
        .data[[service_type]]
      )),
      is_new_visit = .data$service_group == "new_visit",
      is_return_visit = .data$service_group == "return_visit",
      is_office_procedure = .data$service_group == "office_procedure",
      is_surgical_procedure = .data$service_group ==
        "surgical_procedure"
    )

  annual_services <- service_panel |>
    dplyr::group_by(.data[[provider_id]], .data$year) |>
    dplyr::summarise(
      work_rvus = sum(.data$work_rvu_value, na.rm = TRUE),
      new_visits = sum(.data$is_new_visit, na.rm = TRUE),
      return_visits = sum(.data$is_return_visit, na.rm = TRUE),
      office_procedures = sum(.data$is_office_procedure, na.rm = TRUE),
      surgical_procedures = sum(
        .data$is_surgical_procedure,
        na.rm = TRUE
      ),
      operative_minutes = sum(.data$operative_minutes, na.rm = TRUE),
      observed_operative_minutes = sum(
        dplyr::if_else(
          .data$time_source == "observed",
          .data$operative_minutes,
          0
        ),
        na.rm = TRUE
      ),
      supplemented_operative_minutes = sum(
        dplyr::if_else(
          .data$time_source %in% c("published", "cms"),
          .data$operative_minutes,
          0
        ),
        na.rm = TRUE
      ),
      procedures_with_observed_time = sum(
        .data$time_source == "observed",
        na.rm = TRUE
      ),
      procedures_with_published_time = sum(
        .data$time_source == "published",
        na.rm = TRUE
      ),
      procedures_with_cms_time = sum(
        .data$time_source == "cms",
        na.rm = TRUE
      ),
      procedures_missing_time = sum(
        .data$is_surgical_procedure & .data$time_source == "missing",
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  base::message("Aggregated services to provider-year observations.")

  panel <- provider_year |>
    dplyr::left_join(
      annual_services,
      by = stats::setNames(c(provider_id, "year"),
                           c(provider_id, "year"))
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(c(
          "work_rvus", "new_visits", "return_visits",
          "office_procedures", "surgical_procedures",
          "operative_minutes", "observed_operative_minutes",
          "supplemented_operative_minutes",
          "procedures_with_observed_time",
          "procedures_with_published_time",
          "procedures_with_cms_time", "procedures_missing_time"
        )),
        ~ tidyr::replace_na(.x, 0)
      )
    )

  if (!is.null(app_support)) {
    required_app <- c(provider_id, "year", "app_supported_volume")
    missing_app <- base::setdiff(required_app, names(app_support))
    if (length(missing_app) > 0L) {
      base::stop(
        "app_support is missing: ",
        base::paste(missing_app, collapse = ", ")
      )
    }
    app_panel <- app_support |>
      dplyr::group_by(.data[[provider_id]], .data$year) |>
      dplyr::summarise(
        app_supported_volume = sum(
          .data$app_supported_volume,
          na.rm = TRUE
        ),
        .groups = "drop"
      )
    panel <- panel |>
      dplyr::left_join(
        app_panel,
        by = stats::setNames(c(provider_id, "year"),
                             c(provider_id, "year"))
      ) |>
      dplyr::mutate(
        app_supported_volume = tidyr::replace_na(
          .data$app_supported_volume,
          0
        )
      )
    base::message("Added measured APP-supported volume.")
  } else if (!"app_supported_volume" %in% names(panel)) {
    panel <- panel |>
      dplyr::mutate(app_supported_volume = 0)
    base::message("APP support unavailable; set to zero and flag accordingly.")
  }

  panel <- panel |>
    dplyr::mutate(
      total_encounters = .data$new_visits + .data$return_visits +
        .data$office_procedures + .data$surgical_procedures,
      annual_clinical_hours = .data$clinical_hours_week * 46,
      wrvu_per_clinical_fte = dplyr::if_else(
        .data$clinical_fte > 0,
        .data$work_rvus / .data$clinical_fte,
        NA_real_
      ),
      encounters_per_clinical_fte = dplyr::if_else(
        .data$clinical_fte > 0,
        .data$total_encounters / .data$clinical_fte,
        NA_real_
      ),
      wrvu_per_clinical_hour = dplyr::if_else(
        .data$annual_clinical_hours > 0,
        .data$work_rvus / .data$annual_clinical_hours,
        NA_real_
      ),
      surgical_wrvu_share = dplyr::if_else(
        .data$total_encounters > 0,
        .data$surgical_procedures / .data$total_encounters,
        0
      ),
      office_procedure_share = dplyr::if_else(
        .data$total_encounters > 0,
        .data$office_procedures / .data$total_encounters,
        0
      ),
      new_visit_share = dplyr::if_else(
        .data$total_encounters > 0,
        .data$new_visits / .data$total_encounters,
        0
      ),
      app_support_rate = dplyr::if_else(
        .data$total_encounters > 0,
        .data$app_supported_volume / .data$total_encounters,
        0
      ),
      supplemented_time_share = dplyr::if_else(
        .data$operative_minutes > 0,
        .data$supplemented_operative_minutes /
          .data$operative_minutes,
        NA_real_
      ),
      app_support_observed = !is.null(app_support)
    ) |>
    dplyr::filter(
      .data$clinical_fte > 0,
      .data$work_rvus > 0
    )
  base::message("Calculated capacity, case-mix, and provenance measures.")
  base::message("Output: ", nrow(panel), " analyzable provider-year rows.")
  panel
}

#' Fit a provider-year productivity model
#'
#' @param panel Output from build_provider_year_productivity_panel().
#' @param outcome One of wrvu_per_clinical_fte,
#'   encounters_per_clinical_fte, or wrvu_per_clinical_hour.
#' @param provider_id Provider identifier column name.
#' @param include_year_effect Include calendar-year fixed effects.
#' @return A list containing the mixed model, analysis panel, and diagnostics.
#' @export
fit_provider_productivity_model <- function(
    panel,
    outcome = "wrvu_per_clinical_fte",
    provider_id = "provider_id",
    include_year_effect = TRUE) {
  allowed_outcomes <- c(
    "wrvu_per_clinical_fte",
    "encounters_per_clinical_fte",
    "wrvu_per_clinical_hour"
  )
  if (!outcome %in% allowed_outcomes) {
    base::stop(
      "outcome must be one of: ",
      base::paste(allowed_outcomes, collapse = ", ")
    )
  }
  if (!requireNamespace("lme4", quietly = TRUE)) {
    base::stop("Install the lme4 package to fit the mixed model.")
  }
  base::message("Starting mixed-effects productivity model.")
  base::message("Outcome: ", outcome, ".")

  model_panel <- panel |>
    dplyr::mutate(
      log_capacity = base::log(.data[[outcome]]),
      sex = base::factor(.data$sex),
      academic = base::factor(.data$academic),
      rural = base::factor(.data$rural),
      year_factor = base::factor(.data$year),
      provider_factor = base::factor(.data[[provider_id]])
    ) |>
    dplyr::filter(
      is.finite(.data$log_capacity),
      !is.na(.data$age),
      !is.na(.data$years_since_fellowship),
      !is.na(.data$sex),
      !is.na(.data$academic),
      !is.na(.data$rural),
      !is.na(.data$app_support_rate),
      !is.na(.data$surgical_wrvu_share),
      !is.na(.data$office_procedure_share),
      !is.na(.data$new_visit_share)
    )
  if (nrow(model_panel) < 30L) {
    base::stop("At least 30 complete provider-year rows are required.")
  }
  repeated_providers <- model_panel |>
    dplyr::count(.data$provider_factor) |>
    dplyr::filter(.data$n > 1L) |>
    nrow()
  if (repeated_providers < 2L) {
    base::stop("At least two providers need repeated annual observations.")
  }
  base::message(
    "Complete-case model panel: ", nrow(model_panel), " rows and ",
    dplyr::n_distinct(model_panel$provider_factor), " providers."
  )

  fixed_terms <- c(
    "splines::ns(age, df = 4)",
    "sex", "academic", "rural", "app_support_rate",
    "surgical_wrvu_share", "office_procedure_share",
    "new_visit_share", "splines::ns(years_since_fellowship, df = 3)"
  )
  if (isTRUE(include_year_effect)) {
    fixed_terms <- c(fixed_terms, "year_factor")
  }
  formula_text <- base::paste(
    "log_capacity ~",
    base::paste(fixed_terms, collapse = " + "),
    "+ (1 | provider_factor)"
  )
  productivity_formula <- stats::as.formula(formula_text)
  base::message("Model formula: ", formula_text)

  fitted_model <- lme4::lmer(
    formula = productivity_formula,
    data = model_panel,
    REML = TRUE,
    na.action = stats::na.fail,
    control = lme4::lmerControl(
      optimizer = "bobyqa",
      optCtrl = list(maxfun = 200000)
    )
  )
  base::message("Mixed-effects productivity model fitted.")

  residual_values <- stats::residuals(fitted_model)
  diagnostics <- tibble::tibble(
    metric = c(
      "provider_years", "providers", "repeated_providers",
      "singular_fit", "residual_mean", "residual_sd",
      "residual_median", "residual_p25", "residual_p75"
    ),
    value = c(
      nrow(model_panel),
      dplyr::n_distinct(model_panel$provider_factor),
      repeated_providers,
      base::as.numeric(lme4::isSingular(fitted_model)),
      mean(residual_values),
      stats::sd(residual_values),
      stats::median(residual_values),
      stats::quantile(residual_values, 0.25, names = FALSE),
      stats::quantile(residual_values, 0.75, names = FALSE)
    )
  )
  base::message("Computed model diagnostics.")

  model_bundle <- list(
    model = fitted_model,
    analysis_panel = model_panel,
    diagnostics = diagnostics,
    outcome = outcome,
    formula = productivity_formula
  )
  class(model_bundle) <- c("provider_productivity_model", class(model_bundle))
  base::message("Productivity model bundle is ready.")
  model_bundle
}

#' Predict measured provider capacity
#'
#' @param model_bundle Output from fit_provider_productivity_model().
#' @param new_provider_year Provider-year covariates to predict.
#' @param include_provider_effect Include known provider random effects.
#' @return Input rows with median expected capacity and a residual interval.
#' @export
predict_provider_capacity <- function(
    model_bundle,
    new_provider_year,
    include_provider_effect = FALSE) {
  base::message("Starting provider capacity prediction.")
  fitted_model <- model_bundle$model
  random_level <- if (isTRUE(include_provider_effect)) NULL else NA
  log_prediction <- stats::predict(
    fitted_model,
    newdata = new_provider_year,
    re.form = random_level,
    allow.new.levels = TRUE
  )
  residual_sd <- stats::sigma(fitted_model)
  predicted_panel <- new_provider_year |>
    dplyr::mutate(
      predicted_capacity = base::exp(log_prediction),
      predicted_capacity_low = base::exp(
        log_prediction - 1.96 * residual_sd
      ),
      predicted_capacity_high = base::exp(
        log_prediction + 1.96 * residual_sd
      ),
      capacity_outcome = model_bundle$outcome
    )
  base::message("Predicted ", nrow(predicted_panel), " provider-year rows.")
  base::message(
    "Intervals are residual prediction intervals, not confidence intervals."
  )
  predicted_panel
}

#' Save provider productivity artifacts
#'
#' @param model_bundle Fitted productivity model bundle.
#' @param directory Destination directory.
#' @param prefix Filename prefix.
#' @return Named character vector of exact saved paths.
#' @export
save_provider_productivity_artifacts <- function(
    model_bundle,
    directory = ".",
    prefix = "provider_productivity") {
  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  model_path <- base::file.path(
    directory,
    base::paste0(prefix, "_model_", timestamp, ".rds")
  )
  panel_path <- base::file.path(
    directory,
    base::paste0(prefix, "_panel_", timestamp, ".csv")
  )
  diagnostic_path <- base::file.path(
    directory,
    base::paste0(prefix, "_diagnostics_", timestamp, ".csv")
  )
  base::message("Saving fitted model to: ", model_path)
  base::saveRDS(model_bundle, model_path)
  base::message("Saving analysis panel to: ", panel_path)
  readr::write_csv(model_bundle$analysis_panel, panel_path)
  base::message("Saving diagnostics to: ", diagnostic_path)
  readr::write_csv(model_bundle$diagnostics, diagnostic_path)
  saved_paths <- c(
    model = base::normalizePath(model_path),
    panel = base::normalizePath(panel_path),
    diagnostics = base::normalizePath(diagnostic_path)
  )
  base::message(
    "Saved exact paths: ", base::paste(saved_paths, collapse = "; ")
  )
  saved_paths
}
