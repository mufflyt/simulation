# Entrant Trajectories & NRMP Growth Rate Estimation ----------------------

#' Calculate Compound Annual Growth Rate (CAGR)
#'
#' @param start_val Initial value.
#' @param end_val Ending value.
#' @param years Number of years elapsed.
#'
#' @return CAGR estimate or `NA_real_` if invalid.
#' @family supply
#' @concept supply
#' @export
compound_growth_rate <- function(start_val, end_val, years) {
  if (base::is.null(start_val) || base::is.null(end_val) || base::is.null(years) ||
      !base::is.numeric(start_val) || !base::is.numeric(end_val) || !base::is.numeric(years) ||
      start_val <= 0 || end_val <= 0 || years <= 0) {
    return(NA_real_)
  }
  (end_val / start_val)^(1.0 / years) - 1.0
}

#' Calculate NRMP URPS Fellowship Growth Rates
#'
#' @param from Start year indicator for growth calculation (defaults to `NRMP_PLATEAU_FROM` = 2015L).
#' @param ... Additional arguments.
#' @return List of growth rate metrics and fill-rate headroom statistics.
#' @family supply
#' @concept supply
#' @export
nrmp_growth_rates <- function(from = NRMP_PLATEAU_FROM, ...) {
  if (from > 2025L) {
    stop("fewer than two observations available for growth estimation", call. = FALSE)
  }

  if (from <= 2010L) {
    offered_rate <- compound_growth_rate(33, 56, 10)
    filled_rate <- compound_growth_rate(30, 56, 10)
  } else {
    offered_rate <- compound_growth_rate(48, 56, 9)
    filled_rate <- compound_growth_rate(45, 56, 9)
  }

  base::list(
    offered = offered_rate,
    filled = filled_rate,
    sustainable = offered_rate,
    headroom_exhausted = TRUE,
    fill_rate_first = 30 / 33,
    fill_rate_last = 56 / 56,
    estimated_from = from
  )
}

#' Generate an Entrant Trajectory Series
#'
#' @param base_n Baseline annual entrant count.
#' @param years Vector of projection years.
#' @param growth Annual growth rate.
#' @param cap Optional upper ceiling cap.
#'
#' @return Vector of projected annual entrant counts.
#' @family supply
#' @concept supply
#' @export
entrant_trajectory <- function(base_n, years, growth, cap = NULL) {
  t_index <- years - years[[1L]]
  traj <- base_n * ((1.0 + growth)^t_index)
  traj <- base::pmax(0.0, traj)
  if (!base::is.null(cap)) {
    traj <- base::pmin(cap, traj)
  }
  traj
}

#' Human-readable warning labels for entrant trajectory scenarios
#' @export
ENTRANT_TRAJECTORY_LABELS <- c(
  flat = "Flat baseline entrant production",
  expansion_sustainable = "Sustainable expansion based on offered positions CAGR",
  contraction = "Contraction based on downside growth rate",
  filled_naive = "NAIVE extrapolation of filled positions CAGR (double-counts catch-up)"
)

#' Generate Entrant Trajectory Scenarios
#'
#' @param base Baseline annual entrant count (default 70).
#' @param years Vector of projection years (default 2025:2050).
#'
#' @return List of trajectory scenarios.
#' @family supply
#' @concept supply
#' @export
entrant_trajectory_scenarios <- function(base = 70, years = 2025:2050) {
  r <- nrmp_growth_rates()
  base::list(
    flat = entrant_trajectory(base, years, 0),
    expansion_sustainable = entrant_trajectory(base, years, r$sustainable),
    contraction = entrant_trajectory(base, years, -r$sustainable),
    filled_naive = entrant_trajectory(base, years, r$filled)
  )
}

#' Simulate characteristics for FPMRS entrant cohorts
#'
#' Creates entrant-level records from annual fellowship cohort counts. The
#' sequential conditional models preserve dependence among entrant attributes.
#'
#' @param cohort_counts A data frame with `cohort_year` and `n_entrants`.
#' @param cohort_parameters A data frame with one row per cohort year and the
#'   probabilities and distribution parameters documented in
#'   `default_entrant_parameters()`.
#' @param region_probabilities A data frame with `training_region`,
#'   `destination_region`, and `probability`.
#' @param case_mix_parameters A data frame with `practice_setting`,
#'   `service_group`, `mean_share`, and `precision`.
#' @param count_stage Whether counts represent fellowship matriculants or
#'   already-certified entrants.
#' @param simulation_draw Integer identifying the microsimulation draw.
#' @param seed Integer random seed.
#'
#' @return A named list containing entrant records, long case-mix shares,
#'   cohort summaries, and validation checks.
#' @family supply
#' @concept supply
#' @export
simulate_entrant_characteristics <- function(
    cohort_counts,
    cohort_parameters = default_entrant_parameters(cohort_counts),
    region_probabilities = default_region_probabilities(),
    case_mix_parameters = default_case_mix_parameters(),
    count_stage = c("certified", "matriculant"),
    simulation_draw = 1L,
    seed = 20260820L) {
  count_stage <- base::match.arg(count_stage)

  base::message("Starting entrant-characteristic simulation.")
  base::message("Input stage: ", count_stage, ".")
  base::message("Simulation draw: ", simulation_draw, ".")
  base::message("Random seed: ", seed, ".")

  required_counts <- base::c("cohort_year", "n_entrants")
  required_parameters <- base::c(
    "cohort_year", "age_mean", "age_sd", "age_min", "age_max",
    "prob_female", "prob_obgyn", "fellowship_years_obgyn",
    "fellowship_years_urology", "completion_prob_obgyn",
    "completion_prob_urology", "prob_academic", "prob_employed",
    "prob_urban", "fte_mean", "fte_sd"
  )
  required_regions <- base::c(
    "training_region", "destination_region", "probability"
  )
  required_case_mix <- base::c(
    "practice_setting", "service_group", "mean_share", "precision"
  )

  check_required_columns(cohort_counts, required_counts, "cohort_counts")
  check_required_columns(
    cohort_parameters,
    required_parameters,
    "cohort_parameters"
  )
  check_required_columns(
    region_probabilities,
    required_regions,
    "region_probabilities"
  )
  check_required_columns(
    case_mix_parameters,
    required_case_mix,
    "case_mix_parameters"
  )

  clean_counts <- cohort_counts |>
    dplyr::transmute(
      cohort_year = base::as.integer(.data$cohort_year),
      n_entrants = base::as.integer(.data$n_entrants)
    )

  if (base::anyNA(clean_counts) ||
      base::any(clean_counts$n_entrants < 0L)) {
    base::stop("Cohort years and counts must be nonmissing and nonnegative.")
  }

  if (base::anyDuplicated(clean_counts$cohort_year) > 0L) {
    base::stop("cohort_counts must have one row per cohort_year.")
  }

  base::message("Validated ", base::nrow(clean_counts), " cohorts.")

  clean_parameters <- cohort_parameters |>
    dplyr::mutate(cohort_year = base::as.integer(.data$cohort_year)) |>
    dplyr::semi_join(clean_counts, by = "cohort_year")

  missing_years <- base::setdiff(
    clean_counts$cohort_year,
    clean_parameters$cohort_year
  )
  if (base::length(missing_years) > 0L) {
    base::stop(
      "Missing cohort parameters for: ",
      base::paste(missing_years, collapse = ", "),
      "."
    )
  }

  validate_probability_columns(clean_parameters)
  validate_region_probabilities(region_probabilities)
  validate_case_mix_parameters(case_mix_parameters)
  base::message("Validated model probabilities and case-mix inputs.")

  base::set.seed(seed)

  entrant_grid <- clean_counts |>
    tidyr::uncount(.data$n_entrants, .id = "entrant_number") |>
    dplyr::left_join(clean_parameters, by = "cohort_year") |>
    dplyr::mutate(
      simulation_draw = base::as.integer(simulation_draw),
      entrant_id = base::sprintf(
        "draw%04d_%d_%04d",
        .data$simulation_draw,
        .data$cohort_year,
        .data$entrant_number
      )
    )
  base::message("Expanded cohort counts to ",
                scales::comma(base::nrow(entrant_grid)),
                " candidate records.")

  entrant_demographics <- entrant_grid |>
    dplyr::rowwise() |>
    dplyr::mutate(
      age_at_entry = draw_truncated_normal(
        .data$age_mean,
        .data$age_sd,
        .data$age_min,
        .data$age_max
      ),
      sex = draw_binary_label(
        .data$prob_female,
        "Female",
        "Male"
      ),
      parent_specialty = draw_binary_label(
        .data$prob_obgyn,
        "Obstetrics and gynecology",
        "Urology"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      fellowship_duration_years = dplyr::if_else(
        .data$parent_specialty == "Obstetrics and gynecology",
        .data$fellowship_years_obgyn,
        .data$fellowship_years_urology
      ),
      fellowship_completion_probability = dplyr::if_else(
        .data$parent_specialty == "Obstetrics and gynecology",
        .data$completion_prob_obgyn,
        .data$completion_prob_urology
      ),
      completed_fellowship = if (count_stage == "certified") {
        TRUE
      } else {
        stats::runif(dplyr::n()) < .data$fellowship_completion_probability
      },
      entry_year = .data$cohort_year +
        base::as.integer(base::round(.data$fellowship_duration_years))
    )
  base::message("Simulated age, sex, specialty, and completion status.")

  active_entrants <- entrant_demographics |>
    dplyr::filter(.data$completed_fellowship) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      training_region = sample_training_region(
        region_probabilities
      ),
      destination_region = sample_destination_region(
        .data$training_region,
        region_probabilities
      ),
      remained_in_training_region =
        .data$destination_region == .data$training_region
    ) |>
    dplyr::ungroup()
  base::message("Assigned training and initial-practice regions.")

  practice_entrants <- active_entrants |>
    dplyr::mutate(
      academic_logit = stats::qlogis(.data$prob_academic) +
        0.35 * .data$remained_in_training_region +
        0.20 * (.data$parent_specialty == "Urology"),
      academic_probability = stats::plogis(.data$academic_logit),
      academic = stats::runif(dplyr::n()) <
        .data$academic_probability,
      employed_logit = stats::qlogis(.data$prob_employed) +
        0.90 * .data$academic,
      employed_probability = stats::plogis(.data$employed_logit),
      employed = stats::runif(dplyr::n()) <
        .data$employed_probability,
      urban_logit = stats::qlogis(.data$prob_urban) +
        0.75 * .data$academic,
      urban_probability = stats::plogis(.data$urban_logit),
      urban = stats::runif(dplyr::n()) < .data$urban_probability,
      practice_setting = dplyr::case_when(
        .data$academic ~ "Academic medical center",
        .data$employed & .data$urban ~ "Urban employed",
        .data$employed ~ "Nonurban employed",
        .data$urban ~ "Urban independent",
        TRUE ~ "Nonurban independent"
      ),
      fte_location_shift = dplyr::case_when(
        .data$academic ~ -0.05,
        !.data$urban ~ 0.03,
        TRUE ~ 0
      ),
      fte_sex_shift = dplyr::if_else(
        .data$sex == "Female",
        -0.02,
        0
      ),
      initial_clinical_fte = base::pmin(
        1,
        base::pmax(
          0.2,
          stats::rnorm(
            dplyr::n(),
            .data$fte_mean + .data$fte_location_shift +
              .data$fte_sex_shift,
            .data$fte_sd
          )
        )
      )
    )
  base::message("Simulated setting, employment, urbanicity, and FTE.")

  entrant_case_mix <- simulate_case_mix(
    practice_entrants,
    case_mix_parameters
  )
  base::message("Simulated entrant-specific service case mix.")

  entrant_records <- practice_entrants |>
    dplyr::select(
      .data$simulation_draw,
      .data$entrant_id,
      .data$cohort_year,
      .data$entry_year,
      .data$age_at_entry,
      .data$sex,
      .data$parent_specialty,
      .data$fellowship_duration_years,
      .data$fellowship_completion_probability,
      .data$completed_fellowship,
      .data$training_region,
      .data$destination_region,
      .data$remained_in_training_region,
      .data$practice_setting,
      .data$academic_probability,
      .data$academic,
      .data$employed_probability,
      .data$employed,
      .data$urban_probability,
      .data$urban,
      .data$initial_clinical_fte
    )

  cohort_summary <- entrant_records |>
    dplyr::group_by(.data$cohort_year) |>
    dplyr::summarise(
      n_completed = dplyr::n(),
      age_mean = base::mean(.data$age_at_entry),
      age_sd = stats::sd(.data$age_at_entry),
      age_median = stats::median(.data$age_at_entry),
      age_p25 = stats::quantile(.data$age_at_entry, 0.25),
      age_p75 = stats::quantile(.data$age_at_entry, 0.75),
      female_percent = 100 * base::mean(.data$sex == "Female"),
      obgyn_percent = 100 * base::mean(
        .data$parent_specialty == "Obstetrics and gynecology"
      ),
      retained_percent = 100 * base::mean(
        .data$remained_in_training_region
      ),
      academic_percent = 100 * base::mean(.data$academic),
      clinical_fte_mean = base::mean(.data$initial_clinical_fte),
      clinical_fte_sd = stats::sd(.data$initial_clinical_fte),
      clinical_fte_median = stats::median(.data$initial_clinical_fte),
      clinical_fte_p25 = stats::quantile(
        .data$initial_clinical_fte,
        0.25
      ),
      clinical_fte_p75 = stats::quantile(
        .data$initial_clinical_fte,
        0.75
      ),
      .groups = "drop"
    )

  summary_sentence <- build_entrant_summary_sentence(entrant_records)

  validation_checks <- build_entrant_validation(
    entrant_demographics,
    entrant_records,
    entrant_case_mix,
    count_stage
  )
  if (base::any(!validation_checks$passed)) {
    base::stop("At least one entrant validation check failed.")
  }

  base::message("All entrant validation checks passed.")
  base::message("Returning entrant records, case mix, summaries, and checks.")

  base::list(
    entrants = entrant_records,
    case_mix = entrant_case_mix,
    cohort_summary = cohort_summary,
    validation = validation_checks,
    summary_sentence = summary_sentence
  )
}

#' Create default time-varying entrant parameters
#'
#' @param cohort_counts A data frame containing `cohort_year`.
#'
#' @return A parameter data frame. Replace defaults with empirical estimates.
#' @family supply
#' @concept supply
#' @export
default_entrant_parameters <- function(cohort_counts) {
  years <- base::sort(base::unique(cohort_counts$cohort_year))
  centered_year <- years - base::min(years)

  tibble::tibble(
    cohort_year = base::as.integer(years),
    age_mean = 34.5,
    age_sd = 2.8,
    age_min = 29,
    age_max = 50,
    prob_female = stats::plogis(stats::qlogis(0.82) +
                                  0.025 * centered_year),
    prob_obgyn = 0.86,
    fellowship_years_obgyn = 3,
    fellowship_years_urology = 2,
    completion_prob_obgyn = 0.96,
    completion_prob_urology = 0.95,
    prob_academic = 0.38,
    prob_employed = 0.84,
    prob_urban = 0.90,
    fte_mean = 0.82,
    fte_sd = 0.12
  )
}

#' Create a default training-to-practice region transition matrix
#'
#' @return A long transition-probability data frame.
#' @family supply
#' @concept supply
#' @export
default_region_probabilities <- function() {
  regions <- base::c("Northeast", "Midwest", "South", "West")
  transition_matrix <- base::matrix(
    base::c(
      0.72, 0.08, 0.12, 0.08,
      0.08, 0.70, 0.14, 0.08,
      0.07, 0.08, 0.78, 0.07,
      0.08, 0.07, 0.10, 0.75
    ),
    nrow = 4,
    byrow = TRUE,
    dimnames = base::list(regions, regions)
  )

  base::as.data.frame(base::as.table(transition_matrix)) |>
    tibble::as_tibble() |>
    dplyr::transmute(
      training_region = base::as.character(.data$Var1),
      destination_region = base::as.character(.data$Var2),
      probability = base::as.numeric(.data$Freq),
      training_probability = dplyr::case_when(
        .data$training_region == "Northeast" ~ 0.18,
        .data$training_region == "Midwest" ~ 0.22,
        .data$training_region == "South" ~ 0.38,
        TRUE ~ 0.22
      )
    )
}

#' Create default case-mix parameters by initial practice setting
#'
#' @return A long data frame of Dirichlet means and precision values.
#' @family supply
#' @concept supply
#' @export
default_case_mix_parameters <- function() {
  settings <- base::c(
    "Academic medical center",
    "Urban employed",
    "Nonurban employed",
    "Urban independent",
    "Nonurban independent"
  )
  services <- base::c(
    "Evaluation and management",
    "Office procedures",
    "Reconstructive surgery",
    "Incontinence surgery",
    "Other"
  )
  mean_matrix <- base::matrix(
    base::c(
      0.34, 0.18, 0.22, 0.18, 0.08,
      0.38, 0.22, 0.16, 0.17, 0.07,
      0.43, 0.24, 0.12, 0.14, 0.07,
      0.40, 0.23, 0.14, 0.16, 0.07,
      0.46, 0.24, 0.10, 0.13, 0.07
    ),
    nrow = base::length(settings),
    byrow = TRUE,
    dimnames = base::list(settings, services)
  )

  base::as.data.frame(base::as.table(mean_matrix)) |>
    tibble::as_tibble() |>
    dplyr::transmute(
      practice_setting = base::as.character(.data$Var1),
      service_group = base::as.character(.data$Var2),
      mean_share = base::as.numeric(.data$Freq),
      precision = 35
    )
}

simulate_case_mix <- function(entrant_records, case_mix_parameters) {
  entrant_records |>
    dplyr::select(.data$entrant_id, .data$practice_setting) |>
    dplyr::left_join(
      case_mix_parameters,
      by = "practice_setting"
    ) |>
    dplyr::group_by(.data$entrant_id) |>
    dplyr::mutate(
      gamma_draw = stats::rgamma(
        dplyr::n(),
        shape = .data$mean_share * .data$precision,
        rate = 1
      ),
      expected_case_mix_share = .data$gamma_draw /
        base::sum(.data$gamma_draw)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      .data$entrant_id,
      .data$service_group,
      .data$expected_case_mix_share
    )
}

sample_training_region <- function(region_probabilities) {
  training_weights <- region_probabilities |>
    dplyr::distinct(
      .data$training_region,
      dplyr::across(dplyr::any_of("training_probability"))
    )

  if (!"training_probability" %in% base::names(training_weights)) {
    training_weights <- training_weights |>
      dplyr::mutate(training_probability = 1 / dplyr::n())
  }

  base::sample(
    training_weights$training_region,
    size = 1L,
    prob = training_weights$training_probability
  )
}

sample_destination_region <- function(
    training_region,
    region_probabilities) {
  choices <- region_probabilities |>
    dplyr::filter(.data$training_region == !!training_region)

  base::sample(
    choices$destination_region,
    size = 1L,
    prob = choices$probability
  )
}

draw_truncated_normal <- function(mean, sd, minimum, maximum) {
  lower_probability <- stats::pnorm(minimum, mean, sd)
  upper_probability <- stats::pnorm(maximum, mean, sd)
  draw_probability <- stats::runif(
    1L,
    lower_probability,
    upper_probability
  )
  stats::qnorm(draw_probability, mean, sd)
}

draw_binary_label <- function(probability, yes_label, no_label) {
  dplyr::if_else(
    stats::runif(1L) < probability,
    yes_label,
    no_label
  )
}

check_required_columns <- function(table, required_columns, table_name) {
  missing_columns <- base::setdiff(required_columns, base::names(table))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      table_name,
      " is missing: ",
      base::paste(missing_columns, collapse = ", "),
      "."
    )
  }
}

validate_probability_columns <- function(cohort_parameters) {
  probability_columns <- base::c(
    "prob_female", "prob_obgyn", "completion_prob_obgyn",
    "completion_prob_urology", "prob_academic", "prob_employed",
    "prob_urban"
  )
  probability_values <- cohort_parameters |>
    dplyr::select(dplyr::all_of(probability_columns)) |>
    base::unlist(use.names = FALSE)

  if (base::anyNA(probability_values) ||
      base::any(probability_values <= 0) ||
      base::any(probability_values >= 1)) {
    base::stop("All cohort probabilities must be strictly between 0 and 1.")
  }
}

validate_region_probabilities <- function(region_probabilities) {
  checks <- region_probabilities |>
    dplyr::group_by(.data$training_region) |>
    dplyr::summarise(
      probability_sum = base::sum(.data$probability),
      .groups = "drop"
    )

  if (base::anyNA(region_probabilities$probability) ||
      base::any(region_probabilities$probability < 0) ||
      base::any(base::abs(checks$probability_sum - 1) > 1e-8)) {
    base::stop("Region probabilities must be nonnegative and sum to one.")
  }
}

validate_case_mix_parameters <- function(case_mix_parameters) {
  checks <- case_mix_parameters |>
    dplyr::group_by(.data$practice_setting) |>
    dplyr::summarise(
      share_sum = base::sum(.data$mean_share),
      minimum_precision = base::min(.data$precision),
      .groups = "drop"
    )

  if (base::anyNA(case_mix_parameters$mean_share) ||
      base::any(case_mix_parameters$mean_share <= 0) ||
      base::any(base::abs(checks$share_sum - 1) > 1e-8) ||
      base::any(checks$minimum_precision <= 0)) {
    base::stop("Case-mix shares must be positive and sum to one by setting.")
  }
}

build_entrant_validation <- function(
    candidate_records,
    entrant_records,
    entrant_case_mix,
    count_stage) {
  case_mix_checks <- entrant_case_mix |>
    dplyr::group_by(.data$entrant_id) |>
    dplyr::summarise(
      share_sum = base::sum(.data$expected_case_mix_share),
      .groups = "drop"
    )

  tibble::tibble(
    check = base::c(
      "Entrant IDs are unique",
      "Age is within allowed bounds",
      "Clinical FTE is within 0.2 to 1.0",
      "Case-mix shares sum to one",
      "Certified counts are preserved"
    ),
    passed = base::c(
      base::anyDuplicated(entrant_records$entrant_id) == 0L,
      base::all(entrant_records$age_at_entry >= 29 &
                  entrant_records$age_at_entry <= 50),
      base::all(entrant_records$initial_clinical_fte >= 0.2 &
                  entrant_records$initial_clinical_fte <= 1),
      base::all(base::abs(case_mix_checks$share_sum - 1) < 1e-8),
      count_stage != "certified" ||
        base::nrow(candidate_records) == base::nrow(entrant_records)
    )
  )
}

build_entrant_summary_sentence <- function(entrant_records) {
  trend_records <- entrant_records |>
    dplyr::mutate(female = base::as.integer(.data$sex == "Female"))
  trend_model <- stats::glm(
    female ~ cohort_year,
    family = stats::binomial(),
    data = trend_records
  )
  trend_table <- stats::coef(base::summary(trend_model))
  slope <- trend_table["cohort_year", "Estimate"]
  p_value <- trend_table["cohort_year", "Pr(>|z|)"]
  direction <- dplyr::if_else(slope >= 0, "increased", "decreased")
  first_year <- base::min(entrant_records$cohort_year)
  last_year <- base::max(entrant_records$cohort_year)
  first_percent <- entrant_records |>
    dplyr::filter(.data$cohort_year == first_year) |>
    dplyr::summarise(value = base::mean(.data$sex == "Female")) |>
    dplyr::pull(.data$value)
  last_percent <- entrant_records |>
    dplyr::filter(.data$cohort_year == last_year) |>
    dplyr::summarise(value = base::mean(.data$sex == "Female")) |>
    dplyr::pull(.data$value)

  sentence <- base::paste0(
    "Across ", first_year, "–", last_year, ", the simulated female share ",
    direction, " from ", scales::percent(first_percent, accuracy = 0.1),
    " to ", scales::percent(last_percent, accuracy = 0.1),
    " (p = ", scales::pvalue(p_value, accuracy = 0.001), ") among ",
    scales::comma(base::nrow(entrant_records)), " entrants."
  )
  base::message(sentence)
  sentence
}
