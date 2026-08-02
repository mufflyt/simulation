# Open-population extension of the DMDM ----
#
# The closed-cohort engine (R/29) follows a fixed set of women who only age and
# die, so its population shrinks and cannot yield population prevalence for a
# demand projection. This module makes the multistate model OPEN: each year new
# women enter at the youngest modelled age (childbearing largely complete, so
# their cumulative vaginal-delivery exposure is set at entry) and existing women
# age, develop/resolve disease and die. Population prevalence then reflects the
# whole female population each year, which is what demand needs.
#
# The engine is DETERMINISTIC: each agent-track carries a survival weight and the
# MARGINAL probability of each condition, updated by a Markov step
#   p(t+1) = p(t)(1 - remission) + (1 - p(t)) * onset(t)
# and weight(t+1) = weight(t) * (1 - mortality). This is base R, needs no RNG,
# and is unit-testable in isolation. The tidyverse builder reuses R/25.
#
# Placeholders throughout (status = "placeholder_uncalibrated"); this is a v1
# open-population structure, not calibrated demography. Reweighting the annual
# age distribution to Census projections is a documented refinement.

# Shared hazard helpers (unname so results never carry stray coefficient names).
.dmdm_onset_p <- function(a, age, vag, ysl, bmi, hyst, meno, com) {
  unname(stats::plogis(
    a["a0"] + a["avag"] * vag + a["aage"] * ((age - 50) / 10) +
      a["absl"] * (ysl / 10) + a["abmi"] * ((bmi - 27) / 5) +
      a["ahyst"] * hyst + a["ameno"] * meno + a["acom"] * com))
}
.dmdm_mort_p <- function(age, m) unname(stats::plogis(m["m0"] + m["mage"] * ((age - 70) / 10)))

#' Simulate an OPEN population through the pelvic-floor multistate model
#'
#' Deterministic cohort-component engine. Each agent-track carries a survival
#' `weight` (national women represented) and the marginal probabilities
#' `p_ui`/`p_pop`/`p_ai`. Every year: entrants for that year are added, population
#' prevalence is recorded, then the marginal probabilities take a Markov step,
#' weights decay by mortality, and everyone ages.
#'
#' @param init Data frame of agent-tracks present at `start_year` (all ages), with
#'   `age`, `cumulative_vaginal_deliveries`, `years_since_last_vaginal_birth`,
#'   `bmi`, `hysterectomy`, `menopause_status`, `comorbidity`, `weight`, and the
#'   marginal `p_ui`/`p_pop`/`p_ai`.
#' @param entrants Data frame of new entrants with the same columns plus
#'   `entry_year` (added at the start of that year); or `NULL`.
#' @param start_year,end_year Simulation window (inclusive).
#' @param transitions Transition parameters; see [dmdm_default_transitions()].
#' @return A data frame, one row per year: `year`, `population` (sum of weights),
#'   `prev_ui`/`prev_pop`/`prev_ai` (weighted population prevalence) and
#'   `inc_ui`/`inc_pop`/`inc_ai` (expected national new cases in the year).
#' @export
simulate_dmdm_open <- function(init, entrants = NULL, start_year, end_year,
                               transitions = dmdm_default_transitions()) {
  req <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
           "bmi", "hysterectomy", "menopause_status", "comorbidity",
           "weight", "p_ui", "p_pop", "p_ai")
  stopifnot(is.data.frame(init), all(req %in% names(init)), end_year >= start_year)
  if (!is.null(entrants)) stopifnot(all(c(req, "entry_year") %in% names(entrants)))

  a <- init[, req, drop = FALSE]
  years <- start_year:end_year
  rows <- vector("list", length(years))
  for (i in seq_along(years)) {
    y <- years[i]
    if (!is.null(entrants) && y > start_year) {
      add <- entrants[entrants$entry_year == y, req, drop = FALSE]
      if (nrow(add)) a <- rbind(a, add)
    }
    W <- sum(a$weight)
    wm <- function(p) if (W > 0) sum(a$weight * p) / W else NA_real_
    rec <- data.frame(year = y, population = W,
                      prev_ui = wm(a$p_ui), prev_pop = wm(a$p_pop), prev_ai = wm(a$p_ai),
                      inc_ui = NA_real_, inc_pop = NA_real_, inc_ai = NA_real_)

    for (cc in c("ui", "pop", "ai")) {
      on <- .dmdm_onset_p(transitions$onset[[cc]], a$age, a$cumulative_vaginal_deliveries,
                          a$years_since_last_vaginal_birth, a$bmi, a$hysterectomy,
                          a$menopause_status, a$comorbidity)
      pcol <- paste0("p_", cc)
      rem <- transitions$remission[[cc]]
      rec[[paste0("inc_", cc)]] <- sum(a$weight * (1 - a[[pcol]]) * on)
      a[[pcol]] <- a[[pcol]] * (1 - rem) + (1 - a[[pcol]]) * on
    }
    a$weight <- a$weight * (1 - .dmdm_mort_p(a$age, transitions$mortality))
    a$age <- a$age + 1
    a$years_since_last_vaginal_birth <- ifelse(
      a$cumulative_vaginal_deliveries > 0,
      a$years_since_last_vaginal_birth + 1, a$years_since_last_vaginal_birth)
    a$menopause_status <- pmax(a$menopause_status, as.integer(a$age >= 51))
    rows[[i]] <- rec
  }
  do.call(rbind, rows)
}

# ---- builder + wrapper (reuse R/25) ---------------------------------------

# Build one agent frame (attributes + weight + seeded marginal p_c) at a given
# year, restricted to ages >= entry_age, with weights summing to the national
# population represented. Internal.
.dmdm_open_agents <- function(pop_by_age, year, entry_age, n, seed = NULL,
                              risk_params = lifecourse_risk_params()) {
  pa <- pop_by_age[pop_by_age$age >= entry_age, , drop = FALSE]
  pop <- .lifecourse_population(pa, year, n, cesarean_rate = 0.32, seed = seed)
  pop <- .lifecourse_risk(pop, risk_params)
  total <- sum(pa$population)
  dplyr::mutate(pop,
                weight = total / dplyr::n(),
                p_ui = .data$p_ui, p_pop = .data$p_pop, p_ai = .data$p_ai)
}

#' Open-population prevalence + incidence trajectory
#'
#' Builds the base-year population (all ages >= `entry_age`) and one entrant
#' cohort at `entry_age` for each subsequent year, then runs [simulate_dmdm_open()].
#' Entrants are seeded from the cross-sectional risk at `entry_age` (low, since
#' most disease develops later) and then evolve dynamically.
#'
#' @param pop_by_age_year Tibble with `year`, `age`, `population`.
#' @param start_year,end_year Simulation window.
#' @param entry_age Age at which new women enter. Default 40.
#' @param n_init,n_entrants Agent-track counts for the base year and each entrant
#'   cohort.
#' @param seed,risk_params,transitions Passed through.
#' @return The per-year data frame from [simulate_dmdm_open()].
#' @export
dmdm_open_prevalence_trajectory <- function(pop_by_age_year, start_year, end_year,
                                            entry_age = 40L, n_init = 5e4, n_entrants = 2e3,
                                            seed = NULL, risk_params = lifecourse_risk_params(),
                                            transitions = dmdm_default_transitions()) {
  stopifnot(all(c("year", "age", "population") %in% names(pop_by_age_year)))
  pa0 <- pop_by_age_year[pop_by_age_year$year == start_year, c("age", "population")]
  init <- .dmdm_open_agents(pa0, start_year, entry_age, n_init, seed = seed,
                            risk_params = risk_params)

  entry_years <- (start_year + 1L):end_year
  ent_list <- lapply(entry_years, function(y) {
    pae <- pop_by_age_year[pop_by_age_year$year == y & pop_by_age_year$age == entry_age,
                           c("age", "population"), drop = FALSE]
    if (!nrow(pae)) return(NULL)
    ag <- .dmdm_open_agents(pae, y, entry_age, n_entrants, seed = seed,
                            risk_params = risk_params)
    dplyr::mutate(ag, entry_year = y)
  })
  entrants <- dplyr::bind_rows(ent_list)

  simulate_dmdm_open(init, entrants, start_year, end_year, transitions = transitions)
}
