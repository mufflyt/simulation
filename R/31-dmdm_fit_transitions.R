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
#' @param panel Data frame with `person_id`, `year`, the covariates `age`,
#'   `cumulative_vaginal_deliveries`, `years_since_last_vaginal_birth`, `bmi`,
#'   `hysterectomy`, `menopause_status`, `comorbidity`, and states `has_ui`,
#'   `has_pop`, `has_ai`.
#' @param conditions Conditions to build. Default all three.
#' @return Tibble: `person_id`, `condition`, `year`, the covariates at t, `from`
#'   (0/1 at-risk state), and `event` (1 if the state changed by t+1).
#' @export
dmdm_transition_data <- function(panel, conditions = c("ui", "pop", "ai")) {
  cov <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
           "bmi", "hysterectomy", "menopause_status", "comorbidity")
  need <- c("person_id", "year", cov, paste0("has_", conditions))
  assertthat::assert_that(is.data.frame(panel), all(need %in% names(panel)))

  panel <- panel[order(panel$person_id, panel$year), , drop = FALSE]
  out <- lapply(conditions, function(cc) {
    scol <- paste0("has_", cc)
    dplyr::group_by(panel, .data$person_id) %>%
      dplyr::mutate(.next = dplyr::lead(.data[[scol]]),
                    .next_year = dplyr::lead(.data$year)) %>%
      dplyr::ungroup() %>%
      # keep consecutive, observed year pairs among the living (state not NA)
      dplyr::filter(!is.na(.data$.next), .data$.next_year == .data$year + 1L,
                    !is.na(.data[[scol]])) %>%
      dplyr::transmute(person_id = .data$person_id, condition = cc, year = .data$year,
                       age = .data$age,
                       cumulative_vaginal_deliveries = .data$cumulative_vaginal_deliveries,
                       years_since_last_vaginal_birth = .data$years_since_last_vaginal_birth,
                       bmi = .data$bmi, hysterectomy = .data$hysterectomy,
                       menopause_status = .data$menopause_status,
                       comorbidity = .data$comorbidity,
                       from = as.integer(.data[[scol]]),
                       event = as.integer(.data$.next != .data[[scol]]))
  })
  dplyr::bind_rows(out)
}

# Fit one onset logistic and return the coefficient vector in the engine's order.
.fit_onset_coefs <- function(df) {
  zero <- c(a0 = -6, avag = 0, aage = 0, absl = 0, abmi = 0, ahyst = 0, ameno = 0, acom = 0)
  d <- df[df$from == 0L, , drop = FALSE]
  # Fall back to an intercept-only estimate when the model cannot be identified
  # (no at-risk person-years, or no variation in the outcome).
  if (nrow(d) < 2L || length(unique(d$event)) < 2L) {
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

#' Fit DMDM transition parameters from a transition dataset
#'
#' Fits, per condition, an onset logistic (among at-risk, `from == 0`) whose
#' coefficients map directly onto the engine's onset parameterisation, and a
#' remission probability (the observed annual `from == 1 -> 0` rate). Returns a
#' `transitions` object with `status = "fitted"`, usable directly by
#' [simulate_dmdm()] / [simulate_dmdm_open()].
#'
#' @param transition_data Tibble from [dmdm_transition_data()].
#' @param mortality Mortality curve `c(m0, mage)`; defaults to the package
#'   placeholder. Fit separately from a life table.
#' @param conditions Conditions to fit. Default all three.
#' @return A transitions list: `status`, `onset` (per-condition coef vectors),
#'   `remission` (per-condition annual probabilities), `mortality`.
#' @export
fit_dmdm_transitions <- function(transition_data,
                                 mortality = dmdm_default_transitions()$mortality,
                                 conditions = c("ui", "pop", "ai")) {
  assertthat::assert_that(all(c("condition", "from", "event") %in% names(transition_data)))
  onset <- list(); remission <- stats::setNames(numeric(length(conditions)), conditions)
  for (cc in conditions) {
    df <- transition_data[transition_data$condition == cc, , drop = FALSE]
    onset[[cc]] <- .fit_onset_coefs(df)
    at_risk1 <- df[df$from == 1L, , drop = FALSE]
    remission[[cc]] <- if (nrow(at_risk1)) mean(at_risk1$event) else 0
  }
  list(status = "fitted", onset = onset, remission = remission, mortality = mortality)
}
