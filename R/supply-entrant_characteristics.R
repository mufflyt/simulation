# Model characteristics of new FPMRS workforce entrants ------------------
#
# Generates entrant-level characteristics for simulated fellowship cohorts.
# Historical entrant profiles are sampled jointly with greater weight given
# to cohorts near the simulated cohort year. Fellowship completion, regional
# retention, and academic practice are modeled separately with logistic
# regression when sufficient historical information is available.

#' Model characteristics of new FPMRS workforce entrants
#'
#' Generates entrant-level characteristics for simulated fellowship cohorts.
#' Historical entrant profiles are sampled jointly with greater weight given
#' to cohorts near the simulated cohort year. Fellowship completion, regional
#' retention, and academic practice are modeled separately with logistic
#' regression when sufficient historical information is available.
#'
#' @param historical_entrants Historical entrant-level tibble.
#' @param entrant_counts Tibble containing `cohort_year` and `n_entrants`.
#' @param cohort_year_col Cohort year column in `historical_entrants`.
#' @param age_col Age at fellowship entry column.
#' @param sex_col Sex column.
#' @param parent_specialty_col Parent specialty column.
#' @param training_region_col Training region column.
#' @param fellowship_duration_col Fellowship duration in years.
#' @param completion_col Fellowship completion indicator.
#' @param practice_region_col Initial practice region column.
#' @param practice_setting_col Initial practice setting column.
#' @param clinical_fte_col Initial clinical FTE column.
#' @param academic_col Initial academic-practice indicator.
#' @param case_mix_cols Character vector containing expected case-mix columns.
#' @param temporal_bandwidth Controls weighting by historical cohort distance.
#' @param min_model_events Minimum events and nonevents for logistic models.
#' @param seed Random-number seed.
#' @param save_dir Optional directory in which to save simulated entrants.
#'
#' @return A list containing `entrants` (tibble), `cohort_summary` (tibble),
#'   and optional `saved_path`.
#' @family supply
#' @concept supply
#' @export
simulate_joint_entrant_characteristics <- function(
    historical_entrants,
    entrant_counts,
    cohort_year_col = "cohort_year",
    age_col = "age_at_entry",
    sex_col = "sex",
    parent_specialty_col = "parent_specialty",
    training_region_col = "training_region",
    fellowship_duration_col = "fellowship_duration_years",
    completion_col = "completed_fellowship",
    practice_region_col = "practice_region",
    practice_setting_col = "initial_practice_setting",
    clinical_fte_col = "initial_clinical_fte",
    academic_col = "academic",
    case_mix_cols = NULL,
    temporal_bandwidth = 4,
    min_model_events = 10,
    seed = 20260820,
    save_dir = NULL) {

  base::message(
    "simulate_joint_entrant_characteristics(): starting entrant simulation."
  )
  base::message(
    "Historical entrant rows: ",
    scales::comma(base::nrow(historical_entrants))
  )
  base::message(
    "Projected cohort rows: ",
    scales::comma(base::nrow(entrant_counts))
  )
  base::message("Random seed: ", seed)
  base::message("Temporal bandwidth: ", temporal_bandwidth)

  required_historical <- base::c(
    cohort_year_col,
    age_col,
    sex_col,
    parent_specialty_col,
    training_region_col,
    fellowship_duration_col,
    completion_col,
    practice_region_col,
    practice_setting_col,
    clinical_fte_col,
    academic_col
  )

  required_counts <- base::c(
    "cohort_year",
    "n_entrants"
  )

  missing_historical <- base::setdiff(
    required_historical,
    base::names(historical_entrants)
  )

  missing_counts <- base::setdiff(
    required_counts,
    base::names(entrant_counts)
  )

  if (base::length(missing_historical) > 0) {
    base::stop(
      "Missing historical entrant columns: ",
      base::paste(missing_historical, collapse = ", ")
    )
  }

  if (base::length(missing_counts) > 0) {
    base::stop(
      "Missing entrant-count columns: ",
      base::paste(missing_counts, collapse = ", ")
    )
  }

  if (!base::is.null(case_mix_cols)) {
    missing_case_mix <- base::setdiff(
      case_mix_cols,
      base::names(historical_entrants)
    )

    if (base::length(missing_case_mix) > 0) {
      base::stop(
        "Missing case-mix columns: ",
        base::paste(missing_case_mix, collapse = ", ")
      )
    }
  }

  if (!base::is.numeric(temporal_bandwidth) ||
      base::length(temporal_bandwidth) != 1 ||
      temporal_bandwidth <= 0) {
    base::stop("`temporal_bandwidth` must be a positive number.")
  }

  historical_tbl <- historical_entrants |>
    tibble::as_tibble()

  counts_tbl <- entrant_counts |>
    tibble::as_tibble() |>
    dplyr::mutate(
      cohort_year = base::as.integer(.data$cohort_year),
      n_entrants = base::as.integer(.data$n_entrants)
    )

  if (base::any(counts_tbl$n_entrants < 0, na.rm = TRUE)) {
    base::stop("`n_entrants` cannot contain negative values.")
  }

  historical_tbl <- historical_tbl |>
    dplyr::filter(
      !base::is.na(.data[[cohort_year_col]])
    ) |>
    dplyr::mutate(
      .historical_row_id = dplyr::row_number()
    )

  base::message(
    "Historical records with valid cohort year: ",
    scales::comma(base::nrow(historical_tbl))
  )

  if (base::nrow(historical_tbl) == 0) {
    base::stop("No historical entrant records have a valid cohort year.")
  }

  model_binary_probability <- function(
      training_tbl,
      response_col,
      predictor_cols,
      prediction_tbl,
      minimum_events) {

    analysis_tbl <- training_tbl |>
      dplyr::select(
        dplyr::all_of(
          base::c(response_col, predictor_cols)
        )
      ) |>
      tidyr::drop_na()

    response_values <- analysis_tbl[[response_col]]

    n_event <- base::sum(response_values == 1)
    n_nonevent <- base::sum(response_values == 0)

    marginal_probability <- base::mean(
      response_values,
      na.rm = TRUE
    )

    if (!base::is.finite(marginal_probability)) {
      marginal_probability <- 0.5
    }

    if (n_event < minimum_events ||
        n_nonevent < minimum_events) {

      base::message(
        "Insufficient events for ", response_col,
        "; using empirical probability ",
        scales::percent(
          marginal_probability,
          accuracy = 0.1
        ),
        "."
      )

      return(
        base::rep(
          marginal_probability,
          base::nrow(prediction_tbl)
        )
      )
    }

    model_formula <- stats::reformulate(
      termlabels = predictor_cols,
      response = response_col
    )

    fitted_model <- tryCatch(
      stats::glm(
        formula = model_formula,
        family = stats::binomial(),
        data = analysis_tbl
      ),
      error = function(error_condition) {
        base::message(
          "Logistic model failed for ", response_col,
          ": ", base::conditionMessage(error_condition)
        )
        NULL
      }
    )

    if (base::is.null(fitted_model)) {
      return(
        base::rep(
          marginal_probability,
          base::nrow(prediction_tbl)
        )
      )
    }

    predicted_probability <- tryCatch(
      stats::predict(
        fitted_model,
        newdata = prediction_tbl,
        type = "response"
      ),
      error = function(error_condition) {
        base::message(
          "Prediction failed for ", response_col,
          ": ", base::conditionMessage(error_condition)
        )
        base::rep(
          marginal_probability,
          base::nrow(prediction_tbl)
        )
      }
    )

    predicted_probability |>
      base::pmax(0.01) |>
      base::pmin(0.99)
  }

  sample_donor_indices <- function(
      historical_years,
      target_year,
      number_needed,
      bandwidth) {

    year_distance <- base::abs(
      historical_years - target_year
    )

    sampling_weight <- base::exp(
      -year_distance / bandwidth
    )

    sampling_weight[
      !base::is.finite(sampling_weight)
    ] <- 0

    if (base::sum(sampling_weight) <= 0) {
      sampling_weight <- base::rep(
        1,
        base::length(historical_years)
      )
    }

    base::sample.int(
      n = base::length(historical_years),
      size = number_needed,
      replace = TRUE,
      prob = sampling_weight
    )
  }

  base::set.seed(seed)

  base::message(
    "Expanding projected entrant counts into entrant-level records."
  )

  entrant_scaffold <- counts_tbl |>
    dplyr::filter(.data$n_entrants > 0) |>
    tidyr::uncount(
      weights = .data$n_entrants,
      .id = "entrant_number"
    ) |>
    dplyr::mutate(
      entrant_id = base::sprintf(
        "ENT-%d-%04d",
        .data$cohort_year,
        .data$entrant_number
      )
    ) |>
    dplyr::select(
      .data$entrant_id,
      .data$cohort_year,
      .data$entrant_number
    )

  base::message(
    "Entrants to characterize: ",
    scales::comma(base::nrow(entrant_scaffold))
  )

  simulated_groups <- base::lapply(
    base::split(
      entrant_scaffold,
      entrant_scaffold$cohort_year
    ),
    function(cohort_tbl) {

      simulated_year <- base::unique(
        cohort_tbl$cohort_year
      )

      number_needed <- base::nrow(cohort_tbl)

      donor_indices <- sample_donor_indices(
        historical_years =
          historical_tbl[[cohort_year_col]],
        target_year = simulated_year,
        number_needed = number_needed,
        bandwidth = temporal_bandwidth
      )

      donor_tbl <- historical_tbl |>
        dplyr::slice(donor_indices)

      selected_cols <- base::unique(
        base::c(
          age_col,
          sex_col,
          parent_specialty_col,
          training_region_col,
          fellowship_duration_col,
          practice_setting_col,
          clinical_fte_col,
          case_mix_cols,
          ".historical_row_id"
        )
      )

      cohort_tbl |>
        dplyr::bind_cols(
          donor_tbl |>
            dplyr::select(
              dplyr::all_of(selected_cols)
            )
        )
    }
  )

  simulated_tbl <- dplyr::bind_rows(
    simulated_groups
  )

  base::message(
    "Joint historical profiles sampled for all entrants."
  )

  completion_predictors <- base::c(
    cohort_year_col,
    age_col,
    sex_col,
    parent_specialty_col,
    training_region_col,
    fellowship_duration_col
  )

  completion_training <- historical_tbl |>
    dplyr::mutate(
      simulation_cohort_year =
        .data[[cohort_year_col]]
    )

  completion_prediction <- simulated_tbl |>
    dplyr::mutate(
      !!cohort_year_col := .data$cohort_year
    )

  base::message(
    "Estimating fellowship completion probabilities."
  )

  completion_probability <- model_binary_probability(
    training_tbl = completion_training,
    response_col = completion_col,
    predictor_cols = completion_predictors,
    prediction_tbl = completion_prediction,
    minimum_events = min_model_events
  )

  simulated_tbl <- simulated_tbl |>
    dplyr::mutate(
      completion_probability =
        completion_probability,
      completed_fellowship =
        stats::rbinom(
          n = dplyr::n(),
          size = 1,
          prob = .data$completion_probability
        ) == 1
    )

  base::message(
    "Simulated fellowship completions: ",
    scales::comma(
      base::sum(simulated_tbl$completed_fellowship)
    ),
    " of ",
    scales::comma(base::nrow(simulated_tbl)),
    " (",
    scales::percent(
      base::mean(simulated_tbl$completed_fellowship),
      accuracy = 0.1
    ),
    ")."
  )

  historical_retention <- historical_tbl |>
    dplyr::mutate(
      stayed_training_region =
        .data[[practice_region_col]] ==
        .data[[training_region_col]]
    ) |>
    dplyr::mutate(
      stayed_training_region =
        base::as.integer(.data$stayed_training_region)
    ) |>
    dplyr::filter(
      .data[[completion_col]] == 1
    )

  retention_predictors <- base::c(
    cohort_year_col,
    age_col,
    sex_col,
    parent_specialty_col,
    training_region_col
  )

  base::message(
    "Estimating probability of remaining in the training region."
  )

  retention_probability <- model_binary_probability(
    training_tbl = historical_retention,
    response_col = "stayed_training_region",
    predictor_cols = retention_predictors,
    prediction_tbl = completion_prediction,
    minimum_events = min_model_events
  )

  simulated_tbl <- simulated_tbl |>
    dplyr::mutate(
      regional_retention_probability =
        retention_probability,
      stayed_training_region =
        dplyr::if_else(
          .data$completed_fellowship,
          stats::rbinom(
            n = dplyr::n(),
            size = 1,
            prob =
              .data$regional_retention_probability
          ) == 1,
          FALSE
        )
    )

  base::message(
    "Assigning geographic destinations for entrants who move."
  )

  destination_pool <- historical_tbl |>
    dplyr::filter(
      !base::is.na(.data[[practice_region_col]]),
      !base::is.na(.data[[training_region_col]]),
      .data[[practice_region_col]] !=
        .data[[training_region_col]]
    )

  sample_destination_region <- function(
      training_region,
      number_needed) {

    eligible_destinations <- destination_pool |>
      dplyr::filter(
        .data[[practice_region_col]] !=
          training_region
      )

    if (base::nrow(eligible_destinations) == 0) {
      eligible_destinations <- historical_tbl |>
        dplyr::filter(
          !base::is.na(
            .data[[practice_region_col]]
          )
        )
    }

    if (base::nrow(eligible_destinations) == 0) {
      return(
        base::rep(
          NA_character_,
          number_needed
        )
      )
    }

    base::sample(
      eligible_destinations[[practice_region_col]],
      size = number_needed,
      replace = TRUE
    )
  }

  simulated_tbl <- simulated_tbl |>
    dplyr::group_by(
      .data[[training_region_col]]
    ) |>
    dplyr::mutate(
      sampled_destination_region =
        sample_destination_region(
          training_region =
            dplyr::first(
              .data[[training_region_col]]
            ),
          number_needed = dplyr::n()
        )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      initial_practice_region =
        dplyr::case_when(
          !.data$completed_fellowship ~
            NA_character_,
          .data$stayed_training_region ~
            base::as.character(
              .data[[training_region_col]]
            ),
          TRUE ~ .data$sampled_destination_region
        )
    ) |>
    dplyr::select(
      -dplyr::all_of(
        "sampled_destination_region"
      )
    )

  academic_training <- historical_tbl |>
    dplyr::filter(
      .data[[completion_col]] == 1
    )

  academic_predictors <- base::c(
    cohort_year_col,
    age_col,
    sex_col,
    parent_specialty_col,
    training_region_col,
    practice_setting_col,
    clinical_fte_col
  )

  base::message(
    "Estimating initial academic-practice probabilities."
  )

  academic_probability <- model_binary_probability(
    training_tbl = academic_training,
    response_col = academic_col,
    predictor_cols = academic_predictors,
    prediction_tbl = completion_prediction,
    minimum_events = min_model_events
  )

  simulated_tbl <- simulated_tbl |>
    dplyr::mutate(
      academic_probability =
        academic_probability,
      academic =
        dplyr::if_else(
          .data$completed_fellowship,
          stats::rbinom(
            n = dplyr::n(),
            size = 1,
            prob = .data$academic_probability
          ) == 1,
          FALSE
        ),
      workforce_entry_year =
        dplyr::if_else(
          .data$completed_fellowship,
          .data$cohort_year +
            base::as.integer(
              base::round(
                .data[[fellowship_duration_col]]
              )
            ),
          NA_integer_
        ),
      enters_workforce =
        .data$completed_fellowship
    )

  simulated_tbl <- simulated_tbl |>
    dplyr::rename(
      age_at_entry =
        dplyr::all_of(age_col),
      sex =
        dplyr::all_of(sex_col),
      parent_specialty =
        dplyr::all_of(parent_specialty_col),
      training_region =
        dplyr::all_of(training_region_col),
      fellowship_duration_years =
        dplyr::all_of(fellowship_duration_col),
      initial_practice_setting =
        dplyr::all_of(practice_setting_col),
      initial_clinical_fte =
        dplyr::all_of(clinical_fte_col)
    ) |>
    dplyr::arrange(
      .data$cohort_year,
      .data$entrant_number
    ) |>
    dplyr::select(
      .data$entrant_id,
      .data$cohort_year,
      .data$entrant_number,
      .data$age_at_entry,
      .data$sex,
      .data$parent_specialty,
      .data$fellowship_duration_years,
      .data$completion_probability,
      .data$completed_fellowship,
      .data$workforce_entry_year,
      .data$training_region,
      .data$regional_retention_probability,
      .data$stayed_training_region,
      .data$initial_practice_region,
      .data$initial_practice_setting,
      .data$initial_clinical_fte,
      .data$academic_probability,
      .data$academic,
      dplyr::all_of(case_mix_cols),
      .data$enters_workforce,
      .data$.historical_row_id
    )

  base::message(
    "Entrant simulation complete: ",
    scales::comma(base::nrow(simulated_tbl)),
    " fellowship entrants and ",
    scales::comma(
      base::sum(simulated_tbl$enters_workforce)
    ),
    " projected workforce entrants."
  )

  cohort_summary <- simulated_tbl |>
    dplyr::group_by(.data$cohort_year) |>
    dplyr::summarise(
      n_fellowship_entrants = dplyr::n(),
      n_workforce_entrants =
        base::sum(.data$enters_workforce),
      completion_rate =
        base::mean(.data$completed_fellowship),
      mean_age =
        base::mean(
          .data$age_at_entry,
          na.rm = TRUE
        ),
      sd_age =
        stats::sd(
          .data$age_at_entry,
          na.rm = TRUE
        ),
      mean_initial_clinical_fte =
        base::mean(
          .data$initial_clinical_fte,
          na.rm = TRUE
        ),
      sd_initial_clinical_fte =
        stats::sd(
          .data$initial_clinical_fte,
          na.rm = TRUE
        ),
      academic_rate =
        base::mean(
          .data$academic[
            .data$enters_workforce
          ],
          na.rm = TRUE
        ),
      training_region_retention =
        base::mean(
          .data$stayed_training_region[
            .data$enters_workforce
          ],
          na.rm = TRUE
        ),
      .groups = "drop"
    )

  base::message(
    "Created cohort-level validation summary."
  )

  saved_path <- NULL

  if (!base::is.null(save_dir)) {
    if (!base::dir.exists(save_dir)) {
      base::dir.create(
        save_dir,
        recursive = TRUE
      )
      base::message(
        "Created save directory: ",
        base::normalizePath(
          save_dir,
          mustWork = FALSE
        )
      )
    }

    timestamp <- base::format(
      base::Sys.time(),
      "%Y%m%d_%H%M%S"
    )

    saved_path <- base::file.path(
      save_dir,
      base::paste0(
        "simulated_entrant_characteristics_",
        timestamp,
        ".csv"
      )
    )

    readr::write_csv(
      simulated_tbl,
      saved_path
    )

    base::message(
      "Saved entrant simulation to: ",
      base::normalizePath(
        saved_path,
        mustWork = FALSE
      )
    )
  }

  base::message(
    "Returning entrant-level simulation and cohort summary."
  )

  base::list(
    entrants = simulated_tbl,
    cohort_summary = cohort_summary,
    saved_path = saved_path
  )
}

#' Build evidence-based URPS entrant parameters
#'
#' Constructs cohort-specific entrant parameters from the strongest available
#' empirical evidence. ACGME year-1 counts determine parent-specialty mix.
#' AAMC URPS-specific counts inform sex, geographic retention, and full-time
#' academic faculty probabilities. Individual provider profiles, when supplied,
#' determine age, clinical FTE, employment, urbanicity, and practice setting.
#'
#' The function deliberately fails closed for important quantities that cannot
#' be estimated from empirical provider profiles unless `strict = FALSE`.
#'
#' @param cohort_counts Tibble with `cohort_year` and `n_entrants`.
#' @param provider_profiles Optional historical entrant-level provider tibble.
#' @param available_by Latest publication year permitted in calibration.
#' @param recent_years Number of recent years used for empirical profiles.
#' @param strict If TRUE, stop when age or clinical FTE lacks empirical data.
#' @param seed Random seed.
#' @param save_dir Optional directory for timestamped parameter output.
#'
#' @return List containing cohort parameters, evidence registry, historical
#'   ACGME series, and a dynamic summary sentence.
#'
#' @family supply
#' @concept supply
#' @export
build_empirical_entrant_parameters <- function(
    cohort_counts,
    provider_profiles = NULL,
    available_by = 2026L,
    recent_years = 10L,
    strict = TRUE,
    seed = 20260821L,
    save_dir = NULL) {

  base::message(
    "[entrant-evidence] Starting empirical entrant calibration."
  )
  base::message(
    "[entrant-evidence] Publication cutoff: ",
    available_by,
    "."
  )
  base::message(
    "[entrant-evidence] Random seed: ",
    seed,
    "."
  )

  base::set.seed(seed)

  required_counts <- base::c(
    "cohort_year",
    "n_entrants"
  )

  missing_counts <- base::setdiff(
    required_counts,
    base::names(cohort_counts)
  )

  if (base::length(missing_counts) > 0L) {
    base::stop(
      "Missing cohort-count columns: ",
      base::paste(
        missing_counts,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  cohort_tbl <- cohort_counts |>
    tibble::as_tibble() |>
    dplyr::transmute(
      cohort_year = base::as.integer(.data$cohort_year),
      n_entrants = base::as.integer(.data$n_entrants)
    ) |>
    dplyr::arrange(.data$cohort_year)

  if (base::anyNA(cohort_tbl)) {
    base::stop(
      "`cohort_counts` cannot contain missing values.",
      call. = FALSE
    )
  }

  if (base::any(cohort_tbl$n_entrants < 0L)) {
    base::stop(
      "`n_entrants` cannot be negative.",
      call. = FALSE
    )
  }

  base::message(
    "[entrant-evidence] Requested ",
    scales::comma(base::sum(cohort_tbl$n_entrants)),
    " entrants across ",
    scales::comma(base::nrow(cohort_tbl)),
    " cohorts."
  )

  base::message(
    "[entrant-evidence] Reading ACGME year-1 fellow counts."
  )

  acgme_tbl <- acgme_urps_fellows(
    available_by = available_by
  ) |>
    tibble::as_tibble() |>
    dplyr::filter(
      base::is.finite(.data$year_1),
      .data$year_1 >= 0
    )

  if (base::nrow(acgme_tbl) == 0L) {
    base::stop(
      "No usable ACGME entrant observations.",
      call. = FALSE
    )
  }

  pathway_tbl <- acgme_tbl |>
    dplyr::group_by(.data$entry_year) |>
    dplyr::summarise(
      n_obgyn = base::sum(
        .data$year_1[
          .data$parent == "obgyn"
        ],
        na.rm = TRUE
      ),
      n_urology = base::sum(
        .data$year_1[
          .data$parent == "urology"
        ],
        na.rm = TRUE
      ),
      n_total = .data$n_obgyn + .data$n_urology,
      prop_obgyn = .data$n_obgyn / .data$n_total,
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_total > 0L) |>
    dplyr::arrange(.data$entry_year)

  base::message(
    "[entrant-evidence] ACGME years: ",
    base::min(pathway_tbl$entry_year),
    "-",
    base::max(pathway_tbl$entry_year),
    "."
  )

  base::message(
    "[entrant-evidence] Latest ACGME cohort: ",
    scales::comma(
      pathway_tbl$n_total[
        base::nrow(pathway_tbl)
      ]
    ),
    " entrants."
  )

  pathway_model <- stats::glm(
    cbind(n_obgyn, n_urology) ~ entry_year,
    data = pathway_tbl,
    family = stats::binomial()
  )

  pathway_prediction_tbl <- cohort_tbl |>
    dplyr::transmute(
      entry_year = .data$cohort_year
    )

  predicted_obgyn <- stats::predict(
    pathway_model,
    newdata = pathway_prediction_tbl,
    type = "response"
  )

  predicted_obgyn <- base::pmin(
    0.995,
    base::pmax(
      0.005,
      predicted_obgyn
    )
  )

  base::message(
    "[entrant-evidence] Adding AAMC URPS-specific sex evidence."
  )

  aamc_sex_tbl <- tibble::tribble(
    ~parent, ~female, ~total,
    "obgyn", 108L, 138L,
    "urology", 28L, 33L
  ) |>
    dplyr::mutate(
      male = .data$total - .data$female,
      beta_alpha = .data$female + 0.5,
      beta_beta = .data$male + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  female_obgyn <- aamc_sex_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  female_urology <- aamc_sex_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  predicted_female <- (
    predicted_obgyn * female_obgyn
  ) + (
    (1 - predicted_obgyn) * female_urology
  )

  fellowship_years_obgyn <- 3
  fellowship_years_urology <- 2

  base::message(
    "[entrant-evidence] Estimating ACGME-to-certification realization."
  )

  conversion_fit <- entrant_to_cert_ratio(
    source = "acgme",
    through_year = available_by,
    pooled = TRUE,
    exclude_disrupted = TRUE
  )

  pipeline_realization <- conversion_fit$ratio

  if (!base::is.finite(pipeline_realization) ||
      pipeline_realization <= 0 ||
      pipeline_realization > 1) {
    base::stop(
      "Empirical pipeline realization is outside (0, 1].",
      call. = FALSE
    )
  }

  base::message(
    "[entrant-evidence] Pooled entry-to-certification realization: ",
    scales::percent(
      pipeline_realization,
      accuracy = 0.1
    ),
    "."
  )

  base::message(
    "[entrant-evidence] Adding AAMC same-state retention evidence."
  )

  retention_tbl <- tibble::tribble(
    ~parent, ~retained, ~total,
    "obgyn", 81L, 147L,
    "urology", 23L, 41L
  ) |>
    dplyr::mutate(
      moved = .data$total - .data$retained,
      beta_alpha = .data$retained + 0.5,
      beta_beta = .data$moved + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  retention_obgyn <- retention_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  retention_urology <- retention_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  predicted_retention <- (
    predicted_obgyn * retention_obgyn
  ) + (
    (1 - predicted_obgyn) * retention_urology
  )

  base::message(
    "[entrant-evidence] Adding AAMC full-time faculty lower bound."
  )

  faculty_tbl <- tibble::tribble(
    ~parent, ~faculty, ~total,
    "obgyn", 55L, 150L,
    "urology", 14L, 42L
  ) |>
    dplyr::mutate(
      nonfaculty = .data$total - .data$faculty,
      beta_alpha = .data$faculty + 0.5,
      beta_beta = .data$nonfaculty + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  faculty_obgyn <- faculty_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  faculty_urology <- faculty_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  faculty_lower_bound <- (
    predicted_obgyn * faculty_obgyn
  ) + (
    (1 - predicted_obgyn) * faculty_urology
  )

  empirical_age_mean <- NA_real_
  empirical_age_sd <- NA_real_
  empirical_age_median <- NA_real_
  empirical_age_p25 <- NA_real_
  empirical_age_p75 <- NA_real_

  empirical_fte_mean <- NA_real_
  empirical_fte_sd <- NA_real_
  empirical_fte_median <- NA_real_
  empirical_fte_p25 <- NA_real_
  empirical_fte_p75 <- NA_real_

  empirical_academic <- NA_real_
  empirical_employed <- NA_real_
  empirical_urban <- NA_real_

  profile_n <- 0L

  if (!base::is.null(provider_profiles)) {

    base::message(
      "[entrant-evidence] Processing provider-level entrant profiles."
    )

    profile_tbl <- provider_profiles |>
      tibble::as_tibble()

    if ("entry_year" %in% base::names(profile_tbl)) {

      max_profile_year <- base::max(
        profile_tbl$entry_year,
        na.rm = TRUE
      )

      profile_tbl <- profile_tbl |>
        dplyr::filter(
          .data$entry_year >=
            max_profile_year - recent_years + 1L
        )

      base::message(
        "[entrant-evidence] Restricted provider profiles to ",
        max_profile_year - recent_years + 1L,
        "-",
        max_profile_year,
        "."
      )
    }

    profile_n <- base::nrow(profile_tbl)

    base::message(
      "[entrant-evidence] Empirical donor profiles: ",
      scales::comma(profile_n),
      "."
    )

    if ("age_at_entry" %in% base::names(profile_tbl)) {

      age_values <- profile_tbl$age_at_entry

      age_values <- age_values[
        base::is.finite(age_values) &
          age_values >= 25 &
          age_values <= 60
      ]

      if (base::length(age_values) >= 10L) {

        empirical_age_mean <- base::mean(age_values)
        empirical_age_sd <- stats::sd(age_values)
        empirical_age_median <- stats::median(age_values)
        empirical_age_p25 <- base::unname(
          stats::quantile(age_values, 0.25)
        )
        empirical_age_p75 <- base::unname(
          stats::quantile(age_values, 0.75)
        )

        base::message(
          "[entrant-evidence] Age at entry: mean ",
          base::sprintf("%.1f", empirical_age_mean),
          " (SD ",
          base::sprintf("%.1f", empirical_age_sd),
          "), median ",
          base::sprintf("%.1f", empirical_age_median),
          " (p25 ",
          base::sprintf("%.1f", empirical_age_p25),
          ", p75 ",
          base::sprintf("%.1f", empirical_age_p75),
          ")."
        )
      }
    }

    if ("initial_clinical_fte" %in%
        base::names(profile_tbl)) {

      fte_values <- profile_tbl$initial_clinical_fte

      fte_values <- fte_values[
        base::is.finite(fte_values) &
          fte_values > 0 &
          fte_values <= 2
      ]

      if (base::length(fte_values) >= 10L) {

        empirical_fte_mean <- base::mean(fte_values)
        empirical_fte_sd <- stats::sd(fte_values)
        empirical_fte_median <- stats::median(fte_values)
        empirical_fte_p25 <- base::unname(
          stats::quantile(fte_values, 0.25)
        )
        empirical_fte_p75 <- base::unname(
          stats::quantile(fte_values, 0.75)
        )

        base::message(
          "[entrant-evidence] Clinical FTE: mean ",
          base::sprintf("%.2f", empirical_fte_mean),
          " (SD ",
          base::sprintf("%.2f", empirical_fte_sd),
          "), median ",
          base::sprintf("%.2f", empirical_fte_median),
          " (p25 ",
          base::sprintf("%.2f", empirical_fte_p25),
          ", p75 ",
          base::sprintf("%.2f", empirical_fte_p75),
          ")."
        )
      }
    }

    if ("academic" %in% base::names(profile_tbl)) {
      empirical_academic <- base::mean(
        profile_tbl$academic,
        na.rm = TRUE
      )
    }

    if ("employed" %in% base::names(profile_tbl)) {
      empirical_employed <- base::mean(
        profile_tbl$employed,
        na.rm = TRUE
      )
    }

    if ("urban" %in% base::names(profile_tbl)) {
      empirical_urban <- base::mean(
        profile_tbl$urban,
        na.rm = TRUE
      )
    }
  }

  if (base::isTRUE(strict) &&
      !base::is.finite(empirical_age_mean)) {
    base::stop(
      paste0(
        "No empirical `age_at_entry` distribution is available. ",
        "Do not silently restore age_mean = 34.5. Build age at entry ",
        "from the provider-level entrant cohort or set `strict = FALSE` ",
        "for exploratory analysis."
      ),
      call. = FALSE
    )
  }

  if (base::isTRUE(strict) &&
      !base::is.finite(empirical_fte_mean)) {
    base::stop(
      paste0(
        "No empirical `initial_clinical_fte` distribution is available. ",
        "HRSA professional hours are not clinical FTE. Supply an ",
        "entrant-level clinical-FTE source before production simulation."
      ),
      call. = FALSE
    )
  }

  age_mean_used <- if (base::is.finite(empirical_age_mean)) {
    empirical_age_mean
  } else {
    34.5
  }

  age_sd_used <- if (base::is.finite(empirical_age_sd)) {
    empirical_age_sd
  } else {
    2.8
  }

  fte_mean_used <- if (base::is.finite(empirical_fte_mean)) {
    empirical_fte_mean
  } else {
    0.82
  }

  fte_sd_used <- if (base::is.finite(empirical_fte_sd)) {
    empirical_fte_sd
  } else {
    0.12
  }

  academic_used <- if (base::is.finite(empirical_academic)) {
    empirical_academic
  } else {
    faculty_lower_bound
  }

  employed_used <- if (base::is.finite(empirical_employed)) {
    empirical_employed
  } else {
    0.84
  }

  urban_used <- if (base::is.finite(empirical_urban)) {
    empirical_urban
  } else {
    0.90
  }

  parameter_tbl <- cohort_tbl |>
    dplyr::mutate(
      age_mean = age_mean_used,
      age_sd = age_sd_used,
      age_min = base::pmax(
        25,
        .data$age_mean - 3 * .data$age_sd
      ),
      age_max = base::pmin(
        60,
        .data$age_mean + 4 * .data$age_sd
      ),
      prob_female = predicted_female,
      prob_obgyn = predicted_obgyn,
      fellowship_years_obgyn =
        fellowship_years_obgyn,
      fellowship_years_urology =
        fellowship_years_urology,
      completion_prob_obgyn =
        pipeline_realization,
      completion_prob_urology =
        pipeline_realization,
      prob_academic = academic_used,
      prob_employed = employed_used,
      prob_urban = urban_used,
      fte_mean = fte_mean_used,
      fte_sd = fte_sd_used,
      same_state_retention =
        predicted_retention,
      academic_faculty_lower_bound =
        faculty_lower_bound
    )

  evidence_tbl <- tibble::tribble(
    ~parameter,
    ~source,
    ~evidence_type,
    ~status,
    ~interpretation,

    "entrant_count",
    "ACGME Data Resource Book",
    "URPS-specific direct count",
    "empirical",
    "Year-1 fellows on duty; preferred entrant flow.",

    "parent_specialty",
    "ACGME Data Resource Book",
    "URPS-specific direct count",
    "empirical",
    "OB/GYN and urology year-1 fellows modeled separately.",

    "fellowship_duration",
    "NRMP URPS Match",
    "URPS-specific program rule",
    "empirical",
    "Three years after OB/GYN; two after urology.",

    "sex",
    "AAMC Report on Residents Table B3",
    "URPS-specific active-fellow counts",
    "empirical_prior",
    paste0(
      "2024-25 stock distribution; used as a Beta prior, ",
      "not an exact entrant-flow rate."
    ),

    "pipeline_realization",
    "ACGME + ABOG/ABU certification series",
    "URPS-specific longitudinal aggregate",
    "empirical",
    paste0(
      "Entry-to-certification realization; must not be ",
      "described as pure fellowship completion."
    ),

    "training_state_retention",
    "AAMC Report on Residents Table C4",
    "URPS-specific post-GME location",
    "empirical_prior",
    "Same-state retention after training.",

    "academic_probability",
    "AAMC faculty appointment tables",
    "URPS-specific post-GME faculty status",
    "lower_bound",
    paste0(
      "Full-time U.S. medical-school faculty undercounts ",
      "all academic practice."
    ),

    "age_at_entry",
    "Provider-level entrant panel",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_age_mean),
      "empirical",
      "legacy_exploratory"
    ),
    dplyr::if_else(
      base::is.finite(empirical_age_mean),
      "Estimated from observed recent entrants.",
      "No direct empirical distribution supplied."
    ),

    "initial_clinical_fte",
    "Provider productivity/practice survey",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_fte_mean),
      "empirical",
      "legacy_exploratory"
    ),
    dplyr::if_else(
      base::is.finite(empirical_fte_mean),
      "Estimated from observed recent entrants.",
      paste0(
        "HRSA professional hours deliberately not treated ",
        "as clinical FTE."
      )
    ),

    "employment",
    "Provider-level affiliation/practice data",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_employed),
      "empirical",
      "legacy_exploratory"
    ),
    "Should come from NPPES/PECOS/practice affiliations.",

    "urbanicity",
    "Provider geography + RUCC",
    "Individual-level geography",
    dplyr::if_else(
      base::is.finite(empirical_urban),
      "empirical",
      "legacy_exploratory"
    ),
    "Should be derived from provider location, not assumed."
  )

  entrant_mean <- base::mean(
    pathway_tbl$n_total
  )

  entrant_sd <- stats::sd(
    pathway_tbl$n_total
  )

  entrant_median <- stats::median(
    pathway_tbl$n_total
  )

  entrant_p25 <- base::unname(
    stats::quantile(
      pathway_tbl$n_total,
      0.25
    )
  )

  entrant_p75 <- base::unname(
    stats::quantile(
      pathway_tbl$n_total,
      0.75
    )
  )

  entrant_trend <- stats::lm(
    n_total ~ entry_year,
    data = pathway_tbl
  )

  trend_coef <- summary(
    entrant_trend
  )$coefficients

  trend_per_year <- trend_coef[
    "entry_year",
    "Estimate"
  ]

  trend_p <- trend_coef[
    "entry_year",
    "Pr(>|t|)"
  ]

  trend_direction <- dplyr::if_else(
    trend_per_year >= 0,
    "increased",
    "decreased"
  )

  summary_sentence <- base::sprintf(
    paste0(
      "Across %d-%d, ACGME recorded a mean of %s ",
      "(SD %.1f) first-year URPS fellows per year and a median ",
      "of %.1f (p25 %.1f, p75 %.1f); entrant volume %s by ",
      "%.2f fellows per year (p = %.4g)."
    ),
    base::min(pathway_tbl$entry_year),
    base::max(pathway_tbl$entry_year),
    scales::comma(
      base::round(entrant_mean, 1)
    ),
    entrant_sd,
    entrant_median,
    entrant_p25,
    entrant_p75,
    trend_direction,
    base::abs(trend_per_year),
    trend_p
  )

  base::message(
    "[entrant-evidence] ",
    summary_sentence
  )

  saved_path <- NULL

  if (!base::is.null(save_dir)) {

    if (!base::dir.exists(save_dir)) {
      base::dir.create(
        save_dir,
        recursive = TRUE
      )
    }

    timestamp <- base::format(
      base::Sys.time(),
      "%Y%m%d_%H%M%S"
    )

    saved_path <- base::file.path(
      save_dir,
      base::paste0(
        "entrant_parameters_empirical_",
        timestamp,
        ".csv"
      )
    )

    readr::write_csv(
      parameter_tbl,
      saved_path
    )

    base::message(
      "[entrant-evidence] Saved parameter table: ",
      base::normalizePath(
        saved_path,
        mustWork = FALSE
      )
    )
  }

  base::list(
    parameters = parameter_tbl,
    evidence_registry = evidence_tbl,
    acgme_series = pathway_tbl,
    summary_sentence = summary_sentence,
    saved_path = saved_path
  )
}
