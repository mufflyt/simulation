# County Synthetic Female Population & Micro-Trajectory Engine -------------

#' Build county synthetic female populations with IPF calibration
#'
#' Reweights complete survey donor records to county marginal targets, samples
#' integer synthetic persons, and optionally simulates annual pelvic-floor
#' disorder trajectories. Joint donor records preserve observed dependence.
#'
#' @param donors Female survey donor records with `donor_id`, `survey_weight`,
#'   calibration variables, and baseline health variables.
#' @param county_targets Long table with `county_fips`, `year`, `variable`,
#'   `level`, and `target_n`.
#' @param transition_models Named list of annual transition specifications.
#' @param start_year First trajectory year.
#' @param end_year Last trajectory year.
#' @param persons_per_county Integer synthetic sample size per county, or
#'   `NULL` to use the target female population.
#' @param max_iterations Maximum IPF iterations.
#' @param tolerance Maximum relative marginal error allowed.
#' @param trim_quantiles Two weight-ratio trimming quantiles.
#' @param seed Random seed.
#' @param save_directory Optional directory for timestamped RDS files.
#'
#' @return A list containing synthetic persons, trajectories, diagnostics,
#'   targets, and metadata.
#' @family demand
#' @concept synthetic population
#' @export
build_county_synthetic_female_population <- function(
    donors,
    county_targets,
    transition_models = default_pfd_transition_models(),
    start_year,
    end_year,
    persons_per_county = 1000L,
    max_iterations = 500L,
    tolerance = 1e-6,
    trim_quantiles = c(0.005, 0.995),
    seed = 20260821L,
    save_directory = NULL) {
  base::message("Starting county synthetic female population build.")
  base::message("Input donors: ",
                scales::comma(base::nrow(donors)), ".")
  base::message("Input target rows: ",
                scales::comma(base::nrow(county_targets)), ".")

  validate_synthetic_population_inputs(
    donors = donors,
    county_targets = county_targets,
    start_year = start_year,
    end_year = end_year,
    persons_per_county = persons_per_county,
    trim_quantiles = trim_quantiles
  )

  target_years <- county_targets |>
    dplyr::distinct(.data$county_fips, .data$year) |>
    dplyr::arrange(.data$county_fips, .data$year)

  base::message("Calibrating ", scales::comma(base::nrow(target_years)),
                " county-year populations.")
  set.seed(seed)

  calibrated <- purrr::pmap(
    target_years,
    function(county_fips, year) {
      one_target <- county_targets |>
        dplyr::filter(
          .data$county_fips == county_fips,
          .data$year == year
        )

      calibrate_one_county_ipf(
        donors = donors,
        one_target = one_target,
        persons_per_county = persons_per_county,
        max_iterations = max_iterations,
        tolerance = tolerance,
        trim_quantiles = trim_quantiles
      )
    }
  )

  synthetic_people <- calibrated |>
    purrr::map("people") |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      synthetic_person_id = base::sprintf(
        "%s-%d-%07d",
        .data$county_fips,
        .data$year,
        dplyr::row_number()
      )
    ) |>
    dplyr::relocate(.data$synthetic_person_id)

  calibration_diagnostics <- calibrated |>
    purrr::map("diagnostics") |>
    dplyr::bind_rows()

  base::message("Simulating individual multi-morbidity trajectories.")
  trajectories <- simulate_pfd_trajectories(
    baseline_people = synthetic_people |>
      dplyr::filter(.data$year == start_year),
    transition_models = transition_models,
    start_year = start_year,
    end_year = end_year,
    seed = seed + 1L
  )

  metadata <- tibble::tibble(
    created_utc = base::format(
      base::Sys.time(),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    ),
    n_counties = dplyr::n_distinct(county_targets$county_fips),
    n_synthetic_people = base::nrow(synthetic_people),
    start_year = start_year,
    end_year = end_year,
    seed = seed,
    tolerance = tolerance
  )

  population_bundle <- list(
    synthetic_people = synthetic_people,
    trajectories = trajectories,
    calibration_diagnostics = calibration_diagnostics,
    county_targets = county_targets,
    metadata = metadata
  )

  validate_synthetic_population_bundle(population_bundle, tolerance)

  if (!base::is.null(save_directory)) {
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    saved_path <- base::file.path(
      save_directory,
      base::paste0("county_synthetic_population_", timestamp, ".rds")
    )
    base::dir.create(save_directory, recursive = TRUE, showWarnings = FALSE)
    base::saveRDS(population_bundle, saved_path)
    base::message("Saved population bundle to: ",
                  base::normalizePath(saved_path, mustWork = TRUE))
  }

  base::message("Completed synthetic population build with ",
                scales::comma(base::nrow(synthetic_people)),
                " sampled county-year persons.")
  population_bundle
}


#' Calibrate one county-year with iterative proportional fitting
#'
#' @param donors Donor records.
#' @param one_target One county-year target table.
#' @param persons_per_county Requested integer population size.
#' @param max_iterations Maximum IPF iterations.
#' @param tolerance Convergence tolerance.
#' @param trim_quantiles Weight-ratio trimming quantiles.
#'
#' @return A list with sampled people and diagnostics.
#' @keywords internal
calibrate_one_county_ipf <- function(
    donors,
    one_target,
    persons_per_county,
    max_iterations,
    tolerance,
    trim_quantiles) {
  county_fips <- one_target$county_fips[[1L]]
  target_year <- one_target$year[[1L]]
  target_total <- one_target |>
    dplyr::group_by(.data$variable) |>
    dplyr::summarise(
      variable_total = base::sum(.data$target_n),
      .groups = "drop"
    ) |>
    dplyr::summarise(target_total = stats::median(.data$variable_total)) |>
    dplyr::pull(.data$target_total)

  target_checks <- one_target |>
    dplyr::group_by(.data$variable) |>
    dplyr::summarise(
      variable_total = base::sum(.data$target_n),
      .groups = "drop"
    )
  if (base::any(base::abs(
      target_checks$variable_total - target_total
    ) > base::max(1, target_total * tolerance))) {
    base::stop("County ", county_fips, " has inconsistent marginal totals.")
  }

  variables <- base::unique(one_target$variable)
  missing_variables <- base::setdiff(variables, base::names(donors))
  if (base::length(missing_variables) > 0L) {
    base::stop("Missing donor calibration variables: ",
               base::paste(missing_variables, collapse = ", "), ".")
  }

  calibrated_weight <- donors$survey_weight
  converged <- FALSE
  maximum_error <- Inf

  for (iteration in base::seq_len(max_iterations)) {
    for (variable_name in variables) {
      variable_targets <- one_target |>
        dplyr::filter(.data$variable == variable_name)
      donor_level <- base::as.character(donors[[variable_name]])

      unsupported <- base::setdiff(
        base::as.character(
          variable_targets$level[variable_targets$target_n > 0]
        ),
        base::unique(donor_level)
      )
      if (base::length(unsupported) > 0L) {
        base::stop("No donor support for ", variable_name, " level(s): ",
                   base::paste(unsupported, collapse = ", "), ".")
      }

      current_n <- base::tapply(
        calibrated_weight,
        donor_level,
        base::sum,
        default = 0
      )
      target_lookup <- stats::setNames(
        variable_targets$target_n,
        base::as.character(variable_targets$level)
      )
      adjustment <- target_lookup / current_n[base::names(target_lookup)]
      calibrated_weight <- calibrated_weight * adjustment[donor_level]
    }

    maximum_error <- calculate_maximum_marginal_error(
      donors = donors,
      calibrated_weight = calibrated_weight,
      one_target = one_target
    )
    if (maximum_error <= tolerance) {
      converged <- TRUE
      break
    }
  }

  if (!converged) {
    base::stop("IPF failed to converge for county ", county_fips,
               " in ", max_iterations, " iterations; maximum error = ",
               scales::number(maximum_error, accuracy = 1e-8), ".")
  }

  weight_ratio <- calibrated_weight / donors$survey_weight
  trim_limits <- stats::quantile(
    weight_ratio,
    probs = trim_quantiles,
    na.rm = TRUE,
    names = FALSE
  )
  weight_ratio <- base::pmin(
    base::pmax(weight_ratio, trim_limits[[1L]]),
    trim_limits[[2L]]
  )
  calibrated_weight <- donors$survey_weight * weight_ratio
  calibrated_weight <- calibrated_weight /
    base::sum(calibrated_weight) * target_total

  post_trim_converged <- FALSE
  for (post_trim_iteration in base::seq_len(max_iterations)) {
    for (variable_name in variables) {
      variable_targets <- one_target |>
        dplyr::filter(.data$variable == variable_name)
      donor_level <- base::as.character(donors[[variable_name]])
      current_n <- base::tapply(
        calibrated_weight,
        donor_level,
        base::sum,
        default = 0
      )
      target_lookup <- stats::setNames(
        variable_targets$target_n,
        base::as.character(variable_targets$level)
      )
      adjustment <- target_lookup / current_n[base::names(target_lookup)]
      calibrated_weight <- calibrated_weight * adjustment[donor_level]
    }
    post_trim_error <- calculate_maximum_marginal_error(
      donors = donors,
      calibrated_weight = calibrated_weight,
      one_target = one_target
    )
    if (post_trim_error <= tolerance) {
      post_trim_converged <- TRUE
      break
    }
  }
  if (!post_trim_converged) {
    base::stop("Post-trimming IPF failed for county ", county_fips,
               "; maximum error = ",
               scales::number(post_trim_error, accuracy = 1e-8), ".")
  }

  sample_size <- if (base::is.null(persons_per_county)) {
    base::as.integer(base::round(target_total))
  } else {
    base::as.integer(persons_per_county)
  }
  selected_rows <- base::sample.int(
    n = base::nrow(donors),
    size = sample_size,
    replace = TRUE,
    prob = calibrated_weight
  )

  people <- donors[selected_rows, , drop = FALSE] |>
    tibble::as_tibble() |>
    dplyr::mutate(
      county_fips = county_fips,
      year = target_year,
      person_weight = target_total / sample_size,
      source_donor_id = .data$donor_id
    ) |>
    dplyr::relocate(
      .data$county_fips,
      .data$year,
      .data$person_weight,
      .data$source_donor_id
    )

  effective_sample_size <- base::sum(calibrated_weight)^2 /
    base::sum(calibrated_weight^2)

  diagnostics <- tibble::tibble(
    county_fips = county_fips,
    year = target_year,
    iterations = iteration,
    post_trim_iterations = post_trim_iteration,
    converged = converged,
    maximum_error_before_trimming = maximum_error,
    maximum_error_after_trimming = post_trim_error,
    effective_sample_size = effective_sample_size,
    design_effect = base::nrow(donors) / effective_sample_size,
    minimum_weight = base::min(calibrated_weight),
    maximum_weight = base::max(calibrated_weight)
  )

  list(people = people, diagnostics = diagnostics)
}


#' Calculate maximum relative marginal error
#'
#' @keywords internal
calculate_maximum_marginal_error <- function(
    donors,
    calibrated_weight,
    one_target) {
  purrr::map_dbl(
    base::unique(one_target$variable),
    function(variable_name) {
      variable_targets <- one_target |>
        dplyr::filter(.data$variable == variable_name)
      donor_level <- base::as.character(donors[[variable_name]])
      current_n <- base::tapply(
        calibrated_weight,
        donor_level,
        base::sum,
        default = 0
      )
      estimated_n <- current_n[
        base::as.character(variable_targets$level)
      ]
      denominator <- base::pmax(variable_targets$target_n, 1)
      base::max(base::abs(estimated_n - variable_targets$target_n) /
                  denominator)
    }
  ) |>
    base::max()
}


#' Simulate annual pelvic-floor disorder trajectories
#'
#' @param baseline_people Baseline synthetic people.
#' @param transition_models Named list from `default_pfd_transition_models()`.
#' @param start_year First year.
#' @param end_year Last year.
#' @param seed Random seed.
#'
#' @return Person-year trajectory table.
#' @family demand
#' @concept synthetic population
#' @export
simulate_pfd_trajectories <- function(
    baseline_people,
    transition_models = default_pfd_transition_models(),
    start_year,
    end_year,
    seed = 20260822L) {
  required_states <- c(
    "sui", "uui", "pop_stage", "fecal_incontinence", "oab",
    "prior_hysterectomy"
  )
  missing_states <- base::setdiff(required_states,
                                  base::names(baseline_people))
  if (base::length(missing_states) > 0L) {
    base::stop("Missing baseline state variables: ",
               base::paste(missing_states, collapse = ", "), ".")
  }

  set.seed(seed)
  current_people <- baseline_people |>
    dplyr::mutate(year = start_year)
  trajectory_tables <- list(current_people)

  if (end_year > start_year) {
    for (next_year in base::seq.int(start_year + 1L, end_year)) {
      current_people <- advance_pfd_states_one_year(
        current_people = current_people,
        transition_models = transition_models,
        next_year = next_year
      )
      trajectory_tables[[base::length(trajectory_tables) + 1L]] <-
        current_people
    }
  }

  dplyr::bind_rows(trajectory_tables) |>
    dplyr::arrange(.data$synthetic_person_id, .data$year)
}


#' Advance all health states by one year
#'
#' @keywords internal
advance_pfd_states_one_year <- function(
    current_people,
    transition_models,
    next_year) {
  updated_people <- current_people |>
    dplyr::mutate(
      age = .data$age + 1,
      year = next_year,
      prior_hysterectomy = simulate_binary_state(
        current_state = .data$prior_hysterectomy,
        linear_predictor = calculate_transition_predictor(
          current_people = dplyr::pick(dplyr::everything()),
          specification = transition_models$prior_hysterectomy
        ),
        absorbing = TRUE
      )
    )

  binary_states <- c("sui", "uui", "fecal_incontinence", "oab")
  for (state_name in binary_states) {
    updated_people[[state_name]] <- simulate_binary_state(
      current_state = updated_people[[state_name]],
      linear_predictor = calculate_transition_predictor(
        current_people = updated_people,
        specification = transition_models[[state_name]]
      ),
      absorbing = FALSE,
      remission_intercept = transition_models[[state_name]]$remission_intercept
    )
  }

  updated_people$pop_stage <- simulate_pop_stage(
    current_stage = updated_people$pop_stage,
    progression_predictor = calculate_transition_predictor(
      current_people = updated_people,
      specification = transition_models$pop_stage
    ),
    regression_probability = transition_models$pop_stage$regression_probability
  )
  updated_people
}


#' Calculate a logistic transition predictor
#'
#' @keywords internal
calculate_transition_predictor <- function(
    current_people,
    specification) {
  predictor <- base::rep(specification$intercept,
                         base::nrow(current_people))
  for (term_name in base::names(specification$coefficients)) {
    if (!term_name %in% base::names(current_people)) {
      base::stop("Transition predictor is missing variable: ", term_name, ".")
    }
    predictor <- predictor + specification$coefficients[[term_name]] *
      current_people[[term_name]]
  }
  predictor
}


#' Simulate a binary annual state transition
#'
#' @keywords internal
simulate_binary_state <- function(
    current_state,
    linear_predictor,
    absorbing = FALSE,
    remission_intercept = -Inf) {
  incidence_probability <- stats::plogis(linear_predictor)
  remission_probability <- stats::plogis(remission_intercept)
  next_state <- current_state
  unaffected <- current_state == 0L
  next_state[unaffected] <- stats::rbinom(
    base::sum(unaffected),
    size = 1L,
    prob = incidence_probability[unaffected]
  )
  if (!absorbing) {
    affected <- current_state == 1L
    next_state[affected] <- 1L - stats::rbinom(
      base::sum(affected),
      size = 1L,
      prob = remission_probability
    )
  }
  base::as.integer(next_state)
}


#' Simulate annual POP stage transitions
#'
#' @keywords internal
simulate_pop_stage <- function(
    current_stage,
    progression_predictor,
    regression_probability) {
  progression <- stats::rbinom(
    base::length(current_stage),
    size = 1L,
    prob = stats::plogis(progression_predictor)
  )
  regression <- stats::rbinom(
    base::length(current_stage),
    size = 1L,
    prob = regression_probability
  )
  next_stage <- current_stage + progression - regression
  base::as.integer(base::pmin(base::pmax(next_stage, 0L), 4L))
}


#' Default placeholder transition models
#'
#' These values are deliberately labeled placeholders. Replace them with
#' estimates fitted to longitudinal claims, cohort, or panel data before policy
#' inference. Coefficients act on numeric covariates.
#'
#' @return Named transition specifications.
#' @family demand
#' @concept synthetic population
#' @export
default_pfd_transition_models <- function() {
  common_coefficients <- c(
    age = 0.025,
    bmi = 0.035,
    parity = 0.080,
    diabetes = 0.250,
    prior_hysterectomy = 0.180
  )
  list(
    sui = list(
      intercept = -5.1,
      coefficients = common_coefficients,
      remission_intercept = -3.2
    ),
    uui = list(
      intercept = -5.4,
      coefficients = common_coefficients,
      remission_intercept = -3.4
    ),
    fecal_incontinence = list(
      intercept = -6.2,
      coefficients = common_coefficients,
      remission_intercept = -3.7
    ),
    oab = list(
      intercept = -5.0,
      coefficients = common_coefficients,
      remission_intercept = -3.0
    ),
    prior_hysterectomy = list(
      intercept = -7.2,
      coefficients = c(age = 0.035, bmi = 0.015)
    ),
    pop_stage = list(
      intercept = -6.0,
      coefficients = common_coefficients,
      regression_probability = 0.015
    )
  )
}


#' Validate synthetic-population inputs
#'
#' @keywords internal
validate_synthetic_population_inputs <- function(
    donors,
    county_targets,
    start_year,
    end_year,
    persons_per_county,
    trim_quantiles) {
  donor_columns <- c("donor_id", "survey_weight")
  target_columns <- c(
    "county_fips", "year", "variable", "level", "target_n"
  )
  missing_donor_columns <- base::setdiff(donor_columns, base::names(donors))
  missing_target_columns <- base::setdiff(
    target_columns,
    base::names(county_targets)
  )
  if (base::length(missing_donor_columns) > 0L) {
    base::stop("Missing donor columns: ",
               base::paste(missing_donor_columns, collapse = ", "), ".")
  }
  if (base::length(missing_target_columns) > 0L) {
    base::stop("Missing target columns: ",
               base::paste(missing_target_columns, collapse = ", "), ".")
  }
  if (base::any(!base::is.finite(donors$survey_weight)) ||
      base::any(donors$survey_weight <= 0)) {
    base::stop("All survey weights must be finite and positive.")
  }
  if (base::any(!base::is.finite(county_targets$target_n)) ||
      base::any(county_targets$target_n < 0)) {
    base::stop("All targets must be finite and nonnegative.")
  }
  if (base::any(base::nchar(county_targets$county_fips) != 5L)) {
    base::stop("county_fips must be a zero-padded five-character value.")
  }
  if (end_year < start_year) {
    base::stop("end_year must be at least start_year.")
  }
  if (!start_year %in% county_targets$year) {
    base::stop("county_targets must include start_year.")
  }
  if (!base::is.null(persons_per_county) && persons_per_county < 1L) {
    base::stop("persons_per_county must be positive or NULL.")
  }
  if (base::length(trim_quantiles) != 2L ||
      base::any(trim_quantiles < 0 | trim_quantiles > 1) ||
      trim_quantiles[[1L]] >= trim_quantiles[[2L]]) {
    base::stop("trim_quantiles must contain two increasing probabilities.")
  }
  base::invisible(TRUE)
}


#' Validate a completed population bundle
#'
#' @keywords internal
validate_synthetic_population_bundle <- function(
    population_bundle,
    tolerance) {
  diagnostics <- population_bundle$calibration_diagnostics
  if (base::any(!diagnostics$converged)) {
    base::stop("At least one county-year did not converge.")
  }
  if (base::any(!base::is.finite(diagnostics$effective_sample_size)) ||
      base::any(diagnostics$effective_sample_size <= 0)) {
    base::stop("Invalid effective sample size in diagnostics.")
  }
  trajectories <- population_bundle$trajectories
  if (base::any(!trajectories$pop_stage %in% 0:4)) {
    base::stop("POP stage left its valid 0 to 4 range.")
  }
  binary_states <- c(
    "sui", "uui", "fecal_incontinence", "oab",
    "prior_hysterectomy"
  )
  invalid_binary <- purrr::map_lgl(
    binary_states,
    function(state_name) {
      base::any(!trajectories[[state_name]] %in% 0:1)
    }
  )
  if (base::any(invalid_binary)) {
    base::stop("At least one binary health state left the 0/1 range.")
  }
  if (base::any(
      diagnostics$maximum_error_before_trimming > tolerance
    )) {
    base::stop("A county exceeded the requested pre-trimming tolerance.")
  }
  if (base::any(
      diagnostics$maximum_error_after_trimming > tolerance
    )) {
    base::stop("A county exceeded the requested post-trimming tolerance.")
  }
  base::message("Validation passed for convergence, weights, and states.")
  base::invisible(TRUE)
}


#' Summarize synthetic pelvic-floor disease burden
#'
#' @param trajectories Person-year trajectories.
#'
#' @return County-year weighted prevalence summaries.
#' @family demand
#' @concept synthetic population
#' @export
summarize_county_pfd_burden <- function(trajectories) {
  trajectories |>
    dplyr::group_by(.data$county_fips, .data$year) |>
    dplyr::summarise(
      female_population = base::sum(.data$person_weight),
      sui_prevalence = stats::weighted.mean(
        .data$sui,
        .data$person_weight
      ),
      uui_prevalence = stats::weighted.mean(
        .data$uui,
        .data$person_weight
      ),
      pop_stage_2_plus_prevalence = stats::weighted.mean(
        .data$pop_stage >= 2L,
        .data$person_weight
      ),
      fecal_incontinence_prevalence = stats::weighted.mean(
        .data$fecal_incontinence,
        .data$person_weight
      ),
      oab_prevalence = stats::weighted.mean(
        .data$oab,
        .data$person_weight
      ),
      hysterectomy_prevalence = stats::weighted.mean(
        .data$prior_hysterectomy,
        .data$person_weight
      ),
      multimorbidity_prevalence = stats::weighted.mean(
        .data$sui + .data$uui +
          (.data$pop_stage >= 2L) +
          .data$fecal_incontinence + .data$oab >= 2L,
        .data$person_weight
      ),
      .groups = "drop"
    )
}
