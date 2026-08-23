# Empirical adequacy feature engineering and model fitting -----------------

#' Build standardized geographic adequacy features in DuckDB
#'
#' @param db_path Adequacy DuckDB.
#' @param feature_spec Tibble with `table_name`, `geography_col`, `value_col`,
#'   `feature_name`, and `aggregation`. Aggregation is `mean`, `sum`, or
#'   `weighted_mean`; the latter also requires `weight_col`.
#' @param overwrite Whether to replace `adequacy_geographic_features`.
#' @return Geographic feature tibble.
#' @export
build_adequacy_geographic_features <- function(
    db_path,
    feature_spec,
    overwrite = TRUE) {
  required_cols <- c(
    "table_name", "geography_col", "value_col", "feature_name",
    "aggregation"
  )
  if (!base::all(required_cols %in% base::names(feature_spec))) {
    base::stop(
      "`feature_spec` is missing: ",
      base::paste(
        base::setdiff(required_cols, base::names(feature_spec)),
        collapse = ", "
      ),
      call. = FALSE
    )
  }
  if (base::anyDuplicated(feature_spec$feature_name) > 0L) {
    base::stop("Feature names must be unique.", call. = FALSE)
  }
  allowed_aggregations <- c("mean", "sum", "weighted_mean")
  invalid_aggregations <- base::setdiff(
    base::unique(feature_spec$aggregation),
    allowed_aggregations
  )
  if (base::length(invalid_aggregations) > 0L) {
    base::stop("Unsupported aggregation: ",
               base::paste(invalid_aggregations, collapse = ", "),
               call. = FALSE)
  }
  if (!base::file.exists(db_path)) {
    base::stop("Adequacy DuckDB does not exist: ", db_path, call. = FALSE)
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path,
                        read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  available_tables <- DBI::dbListTables(con)
  feature_rows <- base::list()

  for (row_index in base::seq_len(base::nrow(feature_spec))) {
    specification <- feature_spec[row_index, , drop = FALSE]
    table_name <- specification$table_name[[1L]]
    feature_name <- specification$feature_name[[1L]]
    if (!table_name %in% available_tables) {
      base::message("Feature source table missing; skipping: ", table_name)
      next
    }
    table_fields <- DBI::dbListFields(con, table_name)
    needed_fields <- c(
      specification$geography_col[[1L]],
      specification$value_col[[1L]]
    )
    if (specification$aggregation[[1L]] == "weighted_mean") {
      if (!"weight_col" %in% base::names(specification) ||
          base::is.na(specification$weight_col[[1L]])) {
        base::stop("Weighted mean requires `weight_col` for ", feature_name,
                   call. = FALSE)
      }
      needed_fields <- c(needed_fields, specification$weight_col[[1L]])
    }
    missing_fields <- base::setdiff(needed_fields, table_fields)
    if (base::length(missing_fields) > 0L) {
      base::stop("Source fields absent in ", table_name, ": ",
                 base::paste(missing_fields, collapse = ", "),
                 call. = FALSE)
    }
    quote_id <- function(identifier) {
      base::as.character(DBI::dbQuoteIdentifier(con, identifier))
    }
    geography_sql <- quote_id(specification$geography_col[[1L]])
    value_sql <- quote_id(specification$value_col[[1L]])
    aggregation_sql <- switch(
      specification$aggregation[[1L]],
      mean = base::paste0("AVG(TRY_CAST(", value_sql, " AS DOUBLE))"),
      sum = base::paste0("SUM(TRY_CAST(", value_sql, " AS DOUBLE))"),
      weighted_mean = {
        weight_sql <- quote_id(specification$weight_col[[1L]])
        base::paste0(
          "SUM(TRY_CAST(", value_sql, " AS DOUBLE) * TRY_CAST(",
          weight_sql, " AS DOUBLE)) / NULLIF(SUM(TRY_CAST(", weight_sql,
          " AS DOUBLE)), 0)"
        )
      }
    )
    query <- base::paste0(
      "SELECT CAST(", geography_sql, " AS VARCHAR) AS geography, ",
      aggregation_sql, " AS feature_value FROM ", quote_id(table_name),
      " WHERE ", geography_sql, " IS NOT NULL GROUP BY 1"
    )
    base::message("Building feature ", feature_name, " from ", table_name,
                  ".")
    feature_rows[[feature_name]] <- DBI::dbGetQuery(con, query) |>
      tibble::as_tibble() |>
      dplyr::rename(!!feature_name := "feature_value")
  }

  if (base::length(feature_rows) == 0L) {
    base::stop("No geographic features could be built.", call. = FALSE)
  }
  geographic_features <- purrr::reduce(
    feature_rows,
    dplyr::full_join,
    by = "geography"
  ) |>
    dplyr::arrange(.data$geography)
  DBI::dbWriteTable(
    con,
    "adequacy_geographic_features",
    geographic_features,
    overwrite = overwrite
  )
  base::message(
    "Built adequacy_geographic_features: ",
    base::format(base::nrow(geographic_features), big.mark = ","),
    " geographies and ", base::ncol(geographic_features) - 1L,
    " external features."
  )
  geographic_features
}

#' Join standardized DuckDB evidence to an adequacy calibration table
#'
#' @param calibration_tbl Core geographic calibration table.
#' @param db_path Adequacy DuckDB created by [load_adequacy_sources_duckdb()].
#' @param feature_table Standardized table containing `geography` and optional
#'   state or county adequacy features.
#' @return Calibration table with external evidence and source-coverage fields.
#' @export
augment_adequacy_from_duckdb <- function(
    calibration_tbl,
    db_path,
    feature_table = "adequacy_geographic_features") {
  if (!base::file.exists(db_path)) {
    base::stop("Adequacy DuckDB does not exist: ", db_path, call. = FALSE)
  }
  if (!"geography" %in% base::names(calibration_tbl)) {
    base::stop("`calibration_tbl` requires `geography`.", call. = FALSE)
  }
  handle <- open_research_db(
    path = db_path,
    required_tables = c("adequacy_source_ingest_audit", feature_table),
    what = "adequacy evidence DuckDB"
  )
  on.exit(DBI::dbDisconnect(handle$con, shutdown = TRUE), add = TRUE)
  feature_tbl <- DBI::dbReadTable(handle$con, feature_table) |>
    tibble::as_tibble()
  if (!"geography" %in% base::names(feature_tbl)) {
    base::stop("Feature table requires `geography`.", call. = FALSE)
  }
  if (base::anyDuplicated(feature_tbl$geography) > 0L) {
    base::stop("Feature table must have one row per geography.",
               call. = FALSE)
  }
  augmented_tbl <- calibration_tbl |>
    dplyr::left_join(feature_tbl, by = "geography")
  evidence_cols <- base::setdiff(
    base::names(feature_tbl),
    "geography"
  )
  augmented_tbl |>
    dplyr::mutate(
      external_evidence_n = base::rowSums(
        !base::is.na(dplyr::pick(dplyr::all_of(evidence_cols)))
      ),
      external_evidence_complete = .data$external_evidence_n ==
        base::length(evidence_cols)
    )
}

#' Fit an evidence-informed geographic appointment-access model
#'
#' This is an empirical binomial model, not a latent Bayesian model. It uses
#' mystery-caller appointment counts as the outcome and external indicators as
#' predictors. Missing predictors receive an explicit missingness indicator;
#' the filled value has no standalone interpretation.
#'
#' @param calibration_tbl Geographic table with appointment counts and optional
#'   external evidence from [augment_adequacy_from_duckdb()].
#' @param predictor_names Candidate numeric predictors. Unavailable or constant
#'   predictors are logged and omitted.
#' @param population_col Population weighting column.
#' @param bootstrap_reps Number of geographic bootstrap replicates.
#' @param seed Random seed.
#' @return Model, coefficients, geographic estimates, national draws, and an
#'   evidence-coverage audit.
#' @export
fit_empirical_adequacy_glm <- function(
    calibration_tbl,
    predictor_names = c(
      "wait_days",
      "medicaid_fee_ratio",
      "medicaid_enrollment_share",
      "managed_care_access_score",
      "network_adequacy_score",
      "facility_per_100k",
      "hospital_concentration",
      "hcris_operating_margin",
      "nhis_delayed_care_pct",
      "brfss_cost_barrier_pct",
      "pums_uninsured_pct",
      "pums_no_vehicle_pct",
      "pulse_delayed_care_pct",
      "active_license_per_100k"
    ),
    population_col = "female_population",
    bootstrap_reps = 500L,
    seed = 20260821L) {
  required_cols <- c(
    "geography", "appointments_offered", "appointment_attempts",
    population_col
  )
  missing_cols <- base::setdiff(required_cols, base::names(calibration_tbl))
  if (base::length(missing_cols) > 0L) {
    base::stop("Calibration table is missing: ",
               base::paste(missing_cols, collapse = ", "),
               call. = FALSE)
  }
  offered <- base::as.integer(calibration_tbl$appointments_offered)
  attempts <- base::as.integer(calibration_tbl$appointment_attempts)
  if (base::any(base::is.na(offered)) ||
      base::any(base::is.na(attempts)) ||
      base::any(attempts <= 0L) ||
      base::any(offered < 0L | offered > attempts)) {
    base::stop("Appointment counts require 0 <= offered <= attempts and ",
               "attempts > 0.", call. = FALSE)
  }
  population <- base::as.numeric(calibration_tbl[[population_col]])
  if (base::any(!base::is.finite(population)) ||
      base::any(population <= 0)) {
    base::stop("Population weights must be positive and finite.",
               call. = FALSE)
  }

  available_names <- base::intersect(
    predictor_names,
    base::names(calibration_tbl)
  )
  model_frame <- tibble::tibble(
    geography = base::as.character(calibration_tbl$geography),
    offered = offered,
    not_offered = attempts - offered,
    population = population
  )
  retained_names <- base::character()
  coverage_rows <- base::list()

  for (predictor_name in available_names) {
    predictor_value <- base::as.numeric(calibration_tbl[[predictor_name]])
    observed <- base::is.finite(predictor_value)
    observed_n <- base::sum(observed)
    unique_n <- base::length(base::unique(predictor_value[observed]))
    coverage_rows[[predictor_name]] <- tibble::tibble(
      predictor = predictor_name,
      observed_n = observed_n,
      missing_n = base::length(predictor_value) - observed_n,
      observed_pct = observed_n / base::length(predictor_value),
      unique_n = unique_n,
      used = observed_n >= 5L && unique_n >= 2L
    )
    if (observed_n < 5L || unique_n < 2L) {
      base::message("Omitting unavailable or constant predictor: ",
                    predictor_name)
      next
    }
    center <- stats::median(predictor_value[observed])
    spread <- stats::mad(predictor_value[observed], center = center)
    if (!base::is.finite(spread) || spread <= 0) {
      spread <- stats::sd(predictor_value[observed])
    }
    if (!base::is.finite(spread) || spread <= 0) spread <- 1
    filled_value <- predictor_value
    filled_value[!observed] <- center
    safe_name <- base::make.names(predictor_name)
    model_frame[[safe_name]] <- (filled_value - center) / spread
    missing_name <- base::paste0(safe_name, "_missing")
    model_frame[[missing_name]] <- base::as.integer(!observed)
    retained_names <- c(retained_names, safe_name)
    if (base::any(!observed)) {
      retained_names <- c(retained_names, missing_name)
    }
  }
  if (base::length(retained_names) == 0L) {
    base::stop("No external predictor had at least five observations and ",
               "two unique values.", call. = FALSE)
  }
  model_formula <- stats::as.formula(
    base::paste(
      "cbind(offered, not_offered) ~",
      base::paste(retained_names, collapse = " + ")
    )
  )
  base::message("Fitting empirical adequacy model with predictors: ",
                base::paste(retained_names, collapse = ", "))
  access_fit <- stats::glm(
    model_formula,
    family = stats::binomial(),
    weights = base::rep(1, base::nrow(model_frame)),
    data = model_frame
  )
  predicted_adequacy <- stats::predict(
    access_fit,
    newdata = model_frame,
    type = "response"
  )

  base::set.seed(seed)
  bootstrap_national <- base::rep(NA_real_, bootstrap_reps)
  geography_n <- base::nrow(model_frame)
  for (replicate_index in base::seq_len(bootstrap_reps)) {
    sampled_index <- base::sample.int(
      geography_n,
      size = geography_n,
      replace = TRUE
    )
    bootstrap_frame <- model_frame[sampled_index, , drop = FALSE]
    bootstrap_fit <- base::tryCatch(
      stats::glm(
        model_formula,
        family = stats::binomial(),
        data = bootstrap_frame
      ),
      error = function(condition) NULL
    )
    if (base::is.null(bootstrap_fit) ||
        !base::isTRUE(bootstrap_fit$converged)) {
      next
    }
    replicate_prediction <- base::tryCatch(
      stats::predict(
        bootstrap_fit,
        newdata = model_frame,
        type = "response"
      ),
      error = function(condition) base::rep(NA_real_, geography_n)
    )
    if (base::all(base::is.finite(replicate_prediction))) {
      bootstrap_national[[replicate_index]] <- stats::weighted.mean(
        replicate_prediction,
        population
      )
    }
  }
  bootstrap_national <- bootstrap_national[
    base::is.finite(bootstrap_national)
  ]
  if (base::length(bootstrap_national) < 0.80 * bootstrap_reps) {
    base::stop("Fewer than 80% of geographic bootstrap fits converged.",
               call. = FALSE)
  }

  national_adequacy <- stats::weighted.mean(
    predicted_adequacy,
    population
  )
  national_summary <- tibble::tibble(
    adequacy_mean = national_adequacy,
    adequacy_sd = stats::sd(bootstrap_national),
    adequacy_median = stats::median(bootstrap_national),
    adequacy_p25 = stats::quantile(bootstrap_national, 0.25),
    adequacy_p75 = stats::quantile(bootstrap_national, 0.75),
    adequacy_p025 = stats::quantile(bootstrap_national, 0.025),
    adequacy_p975 = stats::quantile(bootstrap_national, 0.975)
  )
  summary_sentence <- base::sprintf(
    paste0(
      "Population-weighted appointment adequacy was %.1f%% ",
      "(SD %.1f%%; median %.1f%%, p25 %.1f%%, p75 %.1f%%)."
    ),
    100 * national_summary$adequacy_mean,
    100 * national_summary$adequacy_sd,
    100 * national_summary$adequacy_median,
    100 * national_summary$adequacy_p25,
    100 * national_summary$adequacy_p75
  )
  base::message(summary_sentence)
  coefficient_matrix <- base::summary(access_fit)$coefficients
  confidence_matrix <- stats::confint.default(access_fit)
  coefficient_tbl <- tibble::tibble(
    term = base::rownames(coefficient_matrix),
    estimate = coefficient_matrix[, "Estimate"],
    std_error = coefficient_matrix[, "Std. Error"],
    statistic = coefficient_matrix[, "z value"],
    p_value = coefficient_matrix[, "Pr(>|z|)"],
    conf_low = confidence_matrix[, 1L],
    conf_high = confidence_matrix[, 2L]
  )
  geographic_summary <- model_frame |>
    dplyr::transmute(
      geography = .data$geography,
      adequacy_mean = predicted_adequacy,
      female_population = .data$population
    )
  coverage_tbl <- dplyr::bind_rows(coverage_rows)

  base::list(
    fit = access_fit,
    coefficients = coefficient_tbl,
    geographic_summary = geographic_summary,
    national_summary = national_summary,
    national_draws = tibble::tibble(adequacy = bootstrap_national),
    evidence_coverage = coverage_tbl,
    summary_sentence = summary_sentence,
    method = "binomial_glm_with_geographic_bootstrap"
  )
}
