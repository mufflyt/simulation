# SWAN longitudinal incontinence Markov model ----------------------------
#
# Complements the existing binary DMDM onset/remission fit
# (R/demand-dmdm_fit_transitions.R) rather than replacing it: that model
# tracks has_ui/has_pop/has_ai as two-state (binary) transitions, with
# optional graded progression where a stage_cols mapping exists. This module
# adds a genuinely multi-state model SPECIFIC to urinary incontinence,
# modeling the destination Sandvik severity category (none/slight/moderate/
# severe/very_severe), not merely onset vs remission -- built on the
# already-existing build_swan_incontinence_panel() and
# score_sandvik_severity() (R/demand-severity_sandvik.R) rather than
# duplicating either.
#
# Sandvik scoring recap (score_sandvik_severity()): frequency (1-4) x amount
# (1-3) = an 8-value index in {1,2,3,4,6,8,9,12} (the real 1-12 instrument
# range), cut into slight/moderate/severe/very_severe. Continent women
# (leakage_ever == FALSE) are OUTSIDE the instrument's domain and score
# sandvik_category = NA there by design, not a zero category -- state 0
# ("none") below is derived from leakage_ever directly, not from a
# would-be-zero Sandvik score.

#' SWAN incontinence Markov state labels
#'
#' State zero represents no current incontinence. States 1-4 correspond
#' to the four ordered Sandvik severity categories.
#'
#' @return Named integer vector.
#' @family swan markov
#' @concept demand
#' @export
swan_ui_markov_states <- function() {
  c(
    none = 0L,
    slight = 1L,
    moderate = 2L,
    severe = 3L,
    very_severe = 4L
  )
}

#' Convert scored SWAN severity to a Markov state
#'
#' @param panel_tbl SWAN panel returned by score_sandvik_severity().
#'
#' @return Input tibble with integer ui_state appended.
#' @family swan markov
#' @concept demand
#' @export
add_swan_ui_markov_state <- function(panel_tbl) {
  required <- c(
    "swan_id",
    "visit",
    "leakage_ever",
    "sandvik_category"
  )
  missing <- base::setdiff(
    required,
    base::names(panel_tbl)
  )
  if (base::length(missing) > 0L) {
    base::stop(
      "add_swan_ui_markov_state(): missing column(s): ",
      base::paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[swan-markov] Converting Sandvik categories to Markov states."
  )

  state_tbl <- panel_tbl |>
    dplyr::mutate(
      ui_state = dplyr::case_when(
        .data$leakage_ever == FALSE ~ 0L,
        .data$sandvik_category == "slight" ~ 1L,
        .data$sandvik_category == "moderate" ~ 2L,
        .data$sandvik_category == "severe" ~ 3L,
        .data$sandvik_category == "very_severe" ~ 4L,
        TRUE ~ NA_integer_
      )
    )

  base::message(
    "[swan-markov] State known for ",
    base::format(
      base::sum(!base::is.na(state_tbl$ui_state)),
      big.mark = ","
    ),
    " of ",
    base::format(
      base::nrow(state_tbl),
      big.mark = ","
    ),
    " participant-visits."
  )

  state_tbl
}

#' Prepare longitudinal SWAN Markov calibration rows
#'
#' @param panel_tbl Longitudinal SWAN participant-visit panel.
#' @param visits Visits to use. Defaults to visits 5 through 10.
#'
#' @return List containing participant and transition tibbles.
#' @family swan markov
#' @concept demand
#' @export
prepare_swan_ui_markov_panel <- function(
    panel_tbl,
    visits = 5:10) {
  required <- c(
    "swan_id",
    "visit",
    "ui_state",
    "age",
    "bmi",
    "parity",
    "hysterectomy",
    "hormone_therapy"
  )
  missing <- base::setdiff(
    required,
    base::names(panel_tbl)
  )
  if (base::length(missing) > 0L) {
    base::stop(
      "prepare_swan_ui_markov_panel(): missing column(s): ",
      base::paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[swan-markov] Restricting calibration to visits ",
    base::paste(visits, collapse = ", "),
    "."
  )

  participant_tbl <- panel_tbl |>
    dplyr::filter(
      .data$visit %in% visits
    ) |>
    dplyr::mutate(
      age_c = (.data$age - 50) / 10,
      bmi_c = (.data$bmi - 27) / 5,
      parity = base::as.numeric(.data$parity),
      hysterectomy = base::as.integer(.data$hysterectomy),
      hormone_therapy = base::as.integer(.data$hormone_therapy)
    ) |>
    dplyr::arrange(
      .data$swan_id,
      .data$visit
    )

  duplicate_tbl <- participant_tbl |>
    dplyr::count(
      .data$swan_id,
      .data$visit,
      name = "row_n"
    ) |>
    dplyr::filter(
      .data$row_n > 1L
    )
  if (base::nrow(duplicate_tbl) > 0L) {
    base::stop(
      "Duplicate SWAN participant-visit rows detected.",
      call. = FALSE
    )
  }

  base::message(
    "[swan-markov] Creating observed one-visit transitions."
  )

  transition_tbl <- participant_tbl |>
    dplyr::group_by(
      .data$swan_id
    ) |>
    dplyr::mutate(
      next_visit = dplyr::lead(.data$visit),
      to_state = dplyr::lead(.data$ui_state)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(
      !base::is.na(.data$ui_state),
      !base::is.na(.data$to_state),
      .data$next_visit == .data$visit + 1L
    ) |>
    dplyr::mutate(
      from_state = .data$ui_state
    )

  base::message(
    "[swan-markov] Calibration transitions: ",
    base::format(
      base::nrow(transition_tbl),
      big.mark = ","
    ),
    "."
  )

  base::list(
    participant_panel = participant_tbl,
    transitions = transition_tbl
  )
}

#' Fit SWAN multi-state incontinence transitions
#'
#' Fits two ordinal logistic models. The initial-state model estimates
#' severity conditional on patient characteristics. The transition model
#' estimates next-year severity conditional on current state, age, BMI,
#' parity, hysterectomy, and hormone therapy.
#'
#' This is a first-order, time-inhomogeneous Markov model:
#'
#' P(S\[t + 1\] | S\[t\], X\[t\])
#'
#' where S has five states: none, slight, moderate, severe, and very
#' severe.
#'
#' @param panel_tbl SWAN panel containing ui_state and covariates.
#' @param visits SWAN visits used for calibration.
#'
#' @return A swan_ui_markov_fit object.
#' @family swan markov
#' @concept demand
#' @export
fit_swan_ui_markov <- function(
    panel_tbl,
    visits = 5:10) {
  base::message(
    "[swan-markov] Starting multi-state transition calibration."
  )

  prepared <- prepare_swan_ui_markov_panel(
    panel_tbl = panel_tbl,
    visits = visits
  )
  participant_tbl <- prepared$participant_panel
  transition_tbl <- prepared$transitions

  state_levels <- 0:4

  initial_tbl <- participant_tbl |>
    dplyr::filter(
      !base::is.na(.data$ui_state),
      !base::is.na(.data$age_c),
      !base::is.na(.data$bmi_c),
      !base::is.na(.data$parity),
      !base::is.na(.data$hysterectomy),
      !base::is.na(.data$hormone_therapy)
    ) |>
    dplyr::mutate(
      state_factor = base::ordered(
        .data$ui_state,
        levels = state_levels
      )
    )

  transition_tbl <- transition_tbl |>
    dplyr::filter(
      !base::is.na(.data$age_c),
      !base::is.na(.data$bmi_c),
      !base::is.na(.data$parity),
      !base::is.na(.data$hysterectomy),
      !base::is.na(.data$hormone_therapy)
    ) |>
    dplyr::mutate(
      from_state_factor = base::factor(
        .data$from_state,
        levels = state_levels
      ),
      to_state_factor = base::ordered(
        .data$to_state,
        levels = state_levels
      )
    )

  if (base::nrow(initial_tbl) < 100L) {
    base::stop(
      "Too few observed SWAN states to fit initial-state model.",
      call. = FALSE
    )
  }
  if (base::nrow(transition_tbl) < 100L) {
    base::stop(
      "Too few observed SWAN transitions to fit Markov model.",
      call. = FALSE
    )
  }

  base::message(
    "[swan-markov] Fitting initial severity model on ",
    base::format(base::nrow(initial_tbl), big.mark = ","),
    " rows."
  )
  initial_fit <- MASS::polr(
    state_factor ~
      age_c +
      bmi_c +
      parity +
      hysterectomy +
      hormone_therapy,
    data = initial_tbl,
    method = "logistic",
    Hess = TRUE
  )

  base::message(
    "[swan-markov] Fitting transition model on ",
    base::format(base::nrow(transition_tbl), big.mark = ","),
    " observed annual transitions."
  )
  transition_fit <- MASS::polr(
    to_state_factor ~
      from_state_factor +
      age_c +
      bmi_c +
      parity +
      hysterectomy +
      hormone_therapy,
    data = transition_tbl,
    method = "logistic",
    Hess = TRUE
  )

  empirical_tbl <- transition_tbl |>
    dplyr::count(
      .data$from_state,
      .data$to_state,
      name = "transition_n"
    ) |>
    tidyr::complete(
      from_state = state_levels,
      to_state = state_levels,
      fill = base::list(transition_n = 0L)
    ) |>
    dplyr::group_by(
      .data$from_state
    ) |>
    dplyr::mutate(
      transition_probability =
        (.data$transition_n + 0.5) /
        (base::sum(.data$transition_n) + 2.5)
    ) |>
    dplyr::ungroup()

  base::message(
    "[swan-markov] Markov calibration complete."
  )

  fit_bundle <- base::list(
    initial_model = initial_fit,
    transition_model = transition_fit,
    empirical_transitions = empirical_tbl,
    visits = visits,
    states = swan_ui_markov_states(),
    calibration_n = base::nrow(transition_tbl),
    fitted_at = base::Sys.time()
  )
  base::class(fit_bundle) <- c(
    "swan_ui_markov_fit",
    "list"
  )

  fit_bundle
}

#' Predict SWAN Markov transition probabilities
#'
#' @param markov_fit Object returned by fit_swan_ui_markov().
#' @param predictor_tbl Rows containing current covariates.
#' @param current_state Integer current state, 0-4.
#' @param initial Logical; predict initial state rather than transition.
#'
#' @return Tibble containing probabilities for all five states.
#' @family swan markov
#' @concept demand
#' @export
predict_swan_ui_markov_probabilities <- function(
    markov_fit,
    predictor_tbl,
    current_state = NULL,
    initial = FALSE) {
  required <- c(
    "age",
    "bmi",
    "parity",
    "hysterectomy",
    "hormone_therapy"
  )
  missing <- base::setdiff(
    required,
    base::names(predictor_tbl)
  )
  if (base::length(missing) > 0L) {
    base::stop(
      "predict_swan_ui_markov_probabilities(): missing: ",
      base::paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  predictor_ready <- predictor_tbl |>
    dplyr::mutate(
      age_c = (.data$age - 50) / 10,
      bmi_c = (.data$bmi - 27) / 5,
      parity = base::as.numeric(.data$parity),
      hysterectomy = base::as.integer(.data$hysterectomy),
      hormone_therapy = base::as.integer(.data$hormone_therapy)
    )

  if (base::isTRUE(initial)) {
    probability_matrix <- stats::predict(
      markov_fit$initial_model,
      newdata = predictor_ready,
      type = "probs"
    )
  } else {
    if (base::is.null(current_state)) {
      base::stop(
        "current_state is required for transition prediction.",
        call. = FALSE
      )
    }
    if (base::length(current_state) == 1L) {
      current_state <- base::rep(
        current_state,
        base::nrow(predictor_ready)
      )
    }
    predictor_ready$from_state_factor <- base::factor(
      current_state,
      levels = 0:4
    )
    probability_matrix <- stats::predict(
      markov_fit$transition_model,
      newdata = predictor_ready,
      type = "probs"
    )
  }

  if (base::is.null(base::dim(probability_matrix))) {
    # predict.polr() returns a plain NAMED numeric vector (not a matrix) when
    # newdata has exactly one row. matrix(x, nrow = 1L) silently drops
    # names(x) -- caught by testing this path directly: the resulting
    # data.frame got default column names ("V1".."V5"), none of the
    # required_states below matched, every state fell through the "absent"
    # branch to 0, and the whole row summed to 0/NaN. dimnames must be
    # carried through explicitly.
    probability_matrix <- base::matrix(
      probability_matrix,
      nrow = 1L,
      dimnames = base::list(NULL, base::names(probability_matrix))
    )
  }

  probability_frame <- base::as.data.frame(
    probability_matrix,
    check.names = FALSE
  )
  required_states <- base::as.character(0:4)
  for (state_name in required_states) {
    if (!state_name %in% base::names(probability_frame)) {
      probability_frame[[state_name]] <- 0
    }
  }
  probability_frame <- probability_frame[
    ,
    required_states,
    drop = FALSE
  ]
  row_total <- base::rowSums(probability_frame)
  probability_frame <- probability_frame / row_total

  tibble::tibble(
    p_none = probability_frame[["0"]],
    p_slight = probability_frame[["1"]],
    p_moderate = probability_frame[["2"]],
    p_severe = probability_frame[["3"]],
    p_very_severe = probability_frame[["4"]]
  )
}

#' Propagate longitudinal SWAN incontinence states
#'
#' Missing severity states are generated from the fitted multi-state model.
#' Observed states may be retained, allowing the function to replace old
#' static baseline fallbacks only where the survey instrument is unavailable.
#'
#' @param markov_fit Object returned by fit_swan_ui_markov().
#' @param trajectory_tbl Participant-visit panel with covariates.
#' @param seed Random seed.
#' @param preserve_observed Keep directly observed severity states.
#'
#' @return Participant-visit panel with propagated_ui_state.
#' @family swan markov
#' @concept demand
#' @export
propagate_swan_ui_markov <- function(
    markov_fit,
    trajectory_tbl,
    seed = 20260819L,
    preserve_observed = TRUE) {
  required <- c(
    "swan_id",
    "visit",
    "ui_state",
    "age",
    "bmi",
    "parity",
    "hysterectomy",
    "hormone_therapy"
  )
  missing <- base::setdiff(
    required,
    base::names(trajectory_tbl)
  )
  if (base::length(missing) > 0L) {
    base::stop(
      "propagate_swan_ui_markov(): missing column(s): ",
      base::paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[swan-markov] Propagating individual longitudinal states."
  )
  base::set.seed(seed)

  simulated_tbl <- trajectory_tbl |>
    dplyr::arrange(
      .data$swan_id,
      .data$visit
    ) |>
    dplyr::mutate(
      propagated_ui_state = NA_integer_,
      state_source = NA_character_
    )

  participant_ids <- base::unique(
    simulated_tbl$swan_id
  )

  for (participant_id in participant_ids) {
    row_index <- base::which(
      simulated_tbl$swan_id == participant_id
    )

    for (position in base::seq_along(row_index)) {
      current_index <- row_index[[position]]
      observed_state <- simulated_tbl$ui_state[[current_index]]

      if (
        base::isTRUE(preserve_observed) &&
          !base::is.na(observed_state)
      ) {
        simulated_tbl$propagated_ui_state[[current_index]] <-
          observed_state
        simulated_tbl$state_source[[current_index]] <-
          "observed"
        next
      }

      predictor_row <- simulated_tbl[
        current_index,
        ,
        drop = FALSE
      ]

      if (position == 1L) {
        probability_tbl <- predict_swan_ui_markov_probabilities(
          markov_fit = markov_fit,
          predictor_tbl = predictor_row,
          initial = TRUE
        )
      } else {
        previous_index <- row_index[[position - 1L]]
        previous_state <-
          simulated_tbl$propagated_ui_state[[previous_index]]
        previous_row <- simulated_tbl[
          previous_index,
          ,
          drop = FALSE
        ]
        probability_tbl <- predict_swan_ui_markov_probabilities(
          markov_fit = markov_fit,
          predictor_tbl = previous_row,
          current_state = previous_state,
          initial = FALSE
        )
      }

      probability_vector <- base::as.numeric(
        probability_tbl[1, ]
      )

      simulated_tbl$propagated_ui_state[[current_index]] <-
        base::sample.int(
          n = 5L,
          size = 1L,
          prob = probability_vector
        ) - 1L
      simulated_tbl$state_source[[current_index]] <-
        "markov"
    }
  }

  state_labels <- base::names(
    swan_ui_markov_states()
  )
  simulated_tbl <- simulated_tbl |>
    dplyr::mutate(
      propagated_ui_category = base::factor(
        state_labels[.data$propagated_ui_state + 1L],
        levels = state_labels,
        ordered = TRUE
      ),
      has_ui = .data$propagated_ui_state > 0L,
      severe_ui = .data$propagated_ui_state >= 3L
    )

  base::message(
    "[swan-markov] Markov-generated participant-visits: ",
    base::format(
      base::sum(simulated_tbl$state_source == "markov"),
      big.mark = ","
    ),
    "."
  )

  simulated_tbl
}
