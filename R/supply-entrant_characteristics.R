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
