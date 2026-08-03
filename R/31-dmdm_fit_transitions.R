# Fitting DMDM onset/remission hazards from longitudinal data (e.g. SWAN) ----
#
# The DMDM engines (R/29, R/30) run on a `transitions` object of onset log-odds
# coefficients, per-condition remission probabilities and a mortality curve. This
# module estimates that object from an observed person-year panel (the Study of
# Women's Health Across the Nation, SWAN, is the intended source), replacing the
# placeholder coefficients in dmdm_default_transitions().
#
#   dmdm_transition_data()  reshapes a state panel into at-risk transition rows
#   fit_dmdm_transitions()  fits onset (logistic) + remission per condition and
#                           returns a transitions object usable as-is by the engines
#
# Base R (stats::glm), so it is unit-testable by recovering known coefficients
# from simulated data. SWAN microdata is not distributed with the package; supply
# it at fit time.

#' Reshape a disease-state panel into at-risk transition rows
#'
#' From a longitudinal panel of women (one row per person-year) with binary
#' condition states, build the year-to-year transitions: for each consecutive
#' observed year, a row per condition carrying the covariates at time t, the
#' at-risk state (`from`), and whether a transition occurred (`event`).
#'
#' Pelvic organ prolapse is graded (POP-Q stage 0-4), not merely present/absent,
#' and it can regress. When a graded stage column is available for a condition,
#' pass it via `stage_cols` to additionally carry the stage at t (`from_stage`)
#' and t+1 (`to_stage`); the binary `from`/`event` are then derived from the stage
#' (present := stage > 0) so existing two-state consumers are unaffected. The
#' staged columns feed [fit_dmdm_transitions()]'s per-stage progression/regression
#' fit. `from_stage`/`to_stage` are `NA` for conditions without a stage column.
#'
#' @param panel Data frame with `person_id`, `year`, the covariates `age`,
#'   `cumulative_vaginal_deliveries`, `years_since_last_vaginal_birth`, `bmi`,
#'   `hysterectomy`, `menopause_status`, `comorbidity`, and states `has_ui`,
#'   `has_pop`, `has_ai`.
#' @param conditions Conditions to build. Default all three.
#' @param stage_cols Optional named character vector mapping a condition to a
#'   graded-stage column in `panel`, e.g. `c(pop = "pop_stage")`. The column must
#'   hold integer stages (0 = none). Conditions not named here are treated as
#'   two-state.
#' @return Tibble: `person_id`, `condition`, `year`, the covariates at t, `from`
#'   (0/1 at-risk state), `event` (1 if the binary state changed by t+1), and
#'   `from_stage`/`to_stage` (graded stage at t and t+1, or `NA`).
#' @export
dmdm_transition_data <- function(panel, conditions = c("ui", "pop", "ai"),
                                 stage_cols = NULL) {
  cov <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
           "bmi", "hysterectomy", "menopause_status", "comorbidity")
  need <- c("person_id", "year", cov, paste0("has_", conditions))
  assertthat::assert_that(is.data.frame(panel), all(need %in% names(panel)))
  if (!is.null(stage_cols)) {
    assertthat::assert_that(!is.null(names(stage_cols)),
                            all(stage_cols %in% names(panel)))
  }

  panel <- panel[order(panel$person_id, panel$year), , drop = FALSE]
  out <- lapply(conditions, function(cc) {
    scol <- paste0("has_", cc)
    stg  <- if (!is.null(stage_cols) && cc %in% names(stage_cols)) stage_cols[[cc]] else NA_character_
    df <- dplyr::group_by(panel, .data$person_id) %>%
      dplyr::mutate(.next = dplyr::lead(.data[[scol]]),
                    .next_year = dplyr::lead(.data$year),
                    .stage = if (!is.na(stg)) as.integer(.data[[stg]]) else NA_integer_,
                    .next_stage = if (!is.na(stg)) dplyr::lead(as.integer(.data[[stg]]))
                                  else NA_integer_) %>%
      dplyr::ungroup() %>%
      # keep consecutive, observed year pairs among the living (state not NA)
      dplyr::filter(!is.na(.data$.next), .data$.next_year == .data$year + 1L,
                    !is.na(.data[[scol]]))
    # For a staged condition, define the binary state from the stage so `from`/
    # `event` stay consistent with `from_stage`/`to_stage`.
    binary_from <- if (!is.na(stg)) as.integer(df$.stage > 0L) else as.integer(df[[scol]])
    binary_next <- if (!is.na(stg)) as.integer(df$.next_stage > 0L) else as.integer(df$.next)
    dplyr::transmute(df,
                     person_id = .data$person_id, condition = cc, year = .data$year,
                     age = .data$age,
                     cumulative_vaginal_deliveries = .data$cumulative_vaginal_deliveries,
                     years_since_last_vaginal_birth = .data$years_since_last_vaginal_birth,
                     bmi = .data$bmi, hysterectomy = .data$hysterectomy,
                     menopause_status = .data$menopause_status,
                     comorbidity = .data$comorbidity,
                     from = binary_from,
                     event = as.integer(binary_next != binary_from),
                     from_stage = .data$.stage,
                     to_stage = .data$.next_stage)
  })
  dplyr::bind_rows(out)
}

# Fit one onset logistic and return the coefficient vector in the engine's order.
.fit_onset_coefs <- function(df) {
  zero <- c(a0 = -6, avag = 0, aage = 0, absl = 0, abmi = 0, ahyst = 0, ameno = 0, acom = 0)
  d <- df[df$from == 0L, , drop = FALSE]
  # Fall back to an intercept-only estimate when the model cannot be identified
  # (no at-risk person-years, no variation in the outcome, or -- the case a
  # staged-transition table hits -- no covariate columns at all). Reaching for an
  # absent column on a tibble yields NULL, so `d$age_c <- (d$age - 50)/10`
  # assigned numeric(0) and errored instead of taking this documented path.
  covars <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
              "bmi", "hysterectomy", "menopause_status", "comorbidity")
  missing_covars <- setdiff(covars, names(d))
  if (nrow(d) < 2L || length(unique(d$event)) < 2L || length(missing_covars)) {
    if (length(missing_covars) && nrow(d)) {
      .msg_warn(sprintf(paste(
        "Onset fit falls back to intercept-only: %s absent. Every covariate slope",
        "is returned as 0, so onset depends on nothing but the baseline rate."),
        paste(missing_covars, collapse = ", ")))
    }
    rate <- if (nrow(d)) mean(d$event) else 0
    zero["a0"] <- if (rate > 0 && rate < 1) stats::qlogis(rate) else -6
    return(zero)
  }
  d$age_c <- (d$age - 50) / 10
  d$ysl_c <- d$years_since_last_vaginal_birth / 10
  d$bmi_c <- (d$bmi - 27) / 5
  fit <- stats::glm(event ~ age_c + cumulative_vaginal_deliveries + ysl_c + bmi_c +
                      hysterectomy + menopause_status + comorbidity,
                    family = stats::binomial(), data = d)
  b <- stats::coef(fit)
  g <- function(nm) unname(if (nm %in% names(b) && !is.na(b[[nm]])) b[[nm]] else 0)
  c(a0 = g("(Intercept)"), avag = g("cumulative_vaginal_deliveries"),
    aage = g("age_c"), absl = g("ysl_c"), abmi = g("bmi_c"),
    ahyst = g("hysterectomy"), ameno = g("menopause_status"), acom = g("comorbidity"))
}

# Fit per-stage annual progression (stage k -> k+1) and regression (k -> k-1)
# probabilities from staged transition rows (`from_stage`/`to_stage`). Returns
# named numeric vectors indexed by the originating stage (character key), so a
# graded consumer can look up the hazard by stage. Base R; unit-testable by
# recovering known per-stage rates from simulated stage paths.
.fit_stage_transitions <- function(df) {
  empty <- stats::setNames(numeric(0), character(0))
  d <- df[!is.na(df$from_stage) & !is.na(df$to_stage), , drop = FALSE]
  if (!nrow(d)) return(list(progression = empty, regression = empty, max_stage = NA_integer_))
  by_stage <- function(cmp) {
    stages <- sort(unique(d$from_stage))
    vals <- vapply(stages, function(s) {
      rows <- d[d$from_stage == s, , drop = FALSE]
      if (!nrow(rows)) return(NA_real_)
      mean(cmp(rows$to_stage, s))
    }, numeric(1))
    stats::setNames(vals, as.character(stages))
  }
  list(progression = by_stage(function(to, s) to > s),
       regression  = by_stage(function(to, s) to < s),
       max_stage   = max(d$from_stage, d$to_stage, na.rm = TRUE))
}

#' Fit DMDM transition parameters from a transition dataset
#'
#' Fits, per condition, an onset logistic (among at-risk, `from == 0`) whose
#' coefficients map directly onto the engine's onset parameterisation, and a
#' remission probability (the observed annual `from == 1 -> 0` rate). Returns a
#' `transitions` object with `status = "fitted"`, usable directly by
#' [simulate_dmdm()] / [simulate_dmdm_open()].
#'
#' For a graded condition (e.g. POP, whose transition rows carry
#' `from_stage`/`to_stage` from [dmdm_transition_data()]), naming it in
#' `stage_conditions` additionally fits per-stage annual progression
#' (stage k -> k+1) and regression (stage k -> k-1) probabilities, attached as
#' `<cc>_progression` and `<cc>_regression` named vectors. This captures the two
#' features that distinguish POP from incontinence: it is graded, and mild stages
#' regress. The extra elements are inert for the two-state engine and available to
#' a staged consumer.
#'
#' @param transition_data Tibble from [dmdm_transition_data()].
#' @param mortality Mortality curve `c(m0, mage)`; defaults to the package
#'   placeholder. Fit separately from a life table.
#' @param conditions Conditions to fit. Default all three.
#' @param stage_conditions Conditions for which to also fit per-stage
#'   progression/regression (requires `from_stage`/`to_stage`). Default none.
#' @return A transitions list: `status`, `onset` (per-condition coef vectors),
#'   `remission` (per-condition annual probabilities), `mortality`, and for each
#'   staged condition `<cc>_progression`/`<cc>_regression` per-stage vectors.
#' @export
fit_dmdm_transitions <- function(transition_data,
                                 mortality = dmdm_default_transitions()$mortality,
                                 conditions = c("ui", "pop", "ai"),
                                 stage_conditions = character(0)) {
  assertthat::assert_that(all(c("condition", "from", "event") %in% names(transition_data)))
  if (length(stage_conditions))
    assertthat::assert_that(all(c("from_stage", "to_stage") %in% names(transition_data)))
  onset <- list(); remission <- stats::setNames(numeric(length(conditions)), conditions)
  staged <- list()
  for (cc in conditions) {
    df <- transition_data[transition_data$condition == cc, , drop = FALSE]
    onset[[cc]] <- .fit_onset_coefs(df)
    at_risk1 <- df[df$from == 1L, , drop = FALSE]
    remission[[cc]] <- if (nrow(at_risk1)) mean(at_risk1$event) else 0
    if (cc %in% stage_conditions) {
      st <- .fit_stage_transitions(df)
      staged[[paste0(cc, "_progression")]] <- st$progression
      staged[[paste0(cc, "_regression")]]  <- st$regression
    }
  }
  c(list(status = "fitted", onset = onset, remission = remission, mortality = mortality),
    staged)
}
