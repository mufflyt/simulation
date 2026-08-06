# Open-population extension of the DMDM ----
#
# The closed-cohort engine (R/demand-dynamic_multistate) follows a fixed set of women who only age and
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
# and is unit-testable in isolation. The tidyverse builder reuses R/demand-lifecourse.
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
#'   Must be `"fitted"`/`"calibrated"` unless `allow_uncalibrated = TRUE`; see
#'   [assert_calibrated_transitions()].
#' @param allow_uncalibrated Declare an exploratory run on placeholder
#'   transitions. Defaults to FALSE. Reweighting to a Census projection anchors
#'   the population counts but does nothing for the disease rates, so an
#'   uncalibrated open run is exactly as illustrative as a closed one.
#' @param pop_by_age_year Optional tibble of `year`, `age`, `population`. When
#'   supplied, each year's age-specific weights are rescaled to it, so population
#'   counts follow the Census projection while the model supplies the disease
#'   rates. `NULL` (default) runs the internal cohort-component demography.
#' @param conservation_tolerance Largest share of a year's simulated population
#'   that may sit at ages the projection does not carry, and so escape
#'   reweighting. Exceeding it warns (strict: errors), because beyond that point
#'   `population` and the weighted prevalences mix Census-anchored with
#'   free-floating mass. Only consulted when `pop_by_age_year` is supplied.
#' @return A data frame, one row per year: `year`, `population` (sum of weights),
#'   `prev_ui`/`prev_pop`/`prev_ai` (weighted population prevalence) and
#'   `inc_ui`/`inc_pop`/`inc_ai` (expected national new cases in the year). When
#'   reweighting, also `share_unanchored`, with the full per-year audit on the
#'   `population_audit` attribute (see [dmdm_population_audit()]).
#' @export
simulate_dmdm_open <- function(init, entrants = NULL, start_year, end_year,
                               transitions = dmdm_default_transitions(),
                               pop_by_age_year = NULL,
                               conservation_tolerance = 0.01,
                               allow_uncalibrated = FALSE) {
  assert_calibrated_transitions(transitions, allow_uncalibrated,
                                what = "DMDM open-population transitions")
  req <- c("age", "cumulative_vaginal_deliveries", "years_since_last_vaginal_birth",
           "bmi", "hysterectomy", "menopause_status", "comorbidity",
           "weight", "p_ui", "p_pop", "p_ai")
  stopifnot(is.data.frame(init), all(req %in% names(init)), end_year >= start_year)
  if (!is.null(entrants)) stopifnot(all(c(req, "entry_year") %in% names(entrants)))
  if (!is.null(pop_by_age_year)) {
    stopifnot(all(c("year", "age", "population") %in% names(pop_by_age_year)))
  }

  a <- init[, req, drop = FALSE]
  years <- start_year:end_year
  rows <- vector("list", length(years))
  audit <- vector("list", length(years))
  for (i in seq_along(years)) {
    y <- years[i]
    if (!is.null(entrants) && y > start_year) {
      add <- entrants[entrants$entry_year == y, req, drop = FALSE]
      if (nrow(add)) a <- rbind(a, add)
    }
    # Optional: rescale weights within each integer age so counts match the
    # Census projection for year y, while the disease RATES (p_ui/p_pop/p_ai)
    # carried by each track are left untouched. Dynamics supply the rates;
    # official demography supplies the counts. Ages absent from the projection
    # keep their current weight.
    if (!is.null(pop_by_age_year)) {
      proj <- pop_by_age_year[pop_by_age_year$year == y, , drop = FALSE]
      tgt <- stats::setNames(proj$population, as.character(proj$age))
      cur <- tapply(a$weight, a$age, sum)
      sf <- stats::setNames(rep(1, length(cur)), names(cur))
      common <- intersect(names(cur), names(tgt))
      for (ag in common) if (cur[[ag]] > 0) sf[[ag]] <- tgt[[ag]] / cur[[ag]]
      a$weight <- a$weight * sf[as.character(a$age)]

      # CONSERVATION AUDIT. Ages present in the simulation but absent from the
      # projection keep their pre-rescaling weight, so the reported `population`
      # is a mix of Census-anchored and free-floating mass with nothing marking
      # the boundary. The leak is systematic rather than incidental: a closed
      # cohort ages past the projection's top age every year it runs, so the
      # unanchored share grows monotonically and concentrates in exactly the
      # oldest ages, which carry the highest disease prevalence.
      unanchored_ages <- setdiff(names(cur), names(tgt))
      unanchored <- if (length(unanchored_ages)) {
        sum(a$weight[as.character(a$age) %in% unanchored_ages])
      } else 0
      total_now <- sum(a$weight)
      # Projection mass at ages the simulation spans but has no agents for: the
      # converse leak, where the target exists and the model cannot carry it.
      span <- as.character(seq(min(a$age), max(a$age)))
      uncovered <- sum(tgt[setdiff(intersect(span, names(tgt)), names(cur))])
      audit[[i]] <- data.frame(
        year = y,
        population_simulated = total_now,
        population_anchored = total_now - unanchored,
        population_unanchored = unanchored,
        share_unanchored = if (total_now > 0) unanchored / total_now else NA_real_,
        n_ages_unanchored = length(unanchored_ages),
        age_min_unanchored = if (length(unanchored_ages)) min(as.numeric(unanchored_ages)) else NA_real_,
        age_max_unanchored = if (length(unanchored_ages)) max(as.numeric(unanchored_ages)) else NA_real_,
        population_target_uncovered = uncovered
      )
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
  out <- do.call(rbind, rows)

  if (!is.null(pop_by_age_year)) {
    aud <- do.call(rbind, audit)
    out$share_unanchored <- aud$share_unanchored
    attr(out, "population_audit") <- aud
    worst <- max(aud$share_unanchored, na.rm = TRUE)
    if (is.finite(worst) && worst > conservation_tolerance) {
      hit <- aud[which.max(aud$share_unanchored), ]
      msg <- sprintf(paste(
        "Population conservation: %.1f%% of the simulated population in %d is NOT",
        "anchored to the supplied projection (ages %g-%g are absent from it), above",
        "the %.1f%% tolerance. Reported `population` and every weighted prevalence",
        "mix Census-matched and free-floating mass. Extend pop_by_age_year to cover",
        "the ages the cohort reaches, or raise conservation_tolerance knowingly."),
        100 * hit$share_unanchored, hit$year,
        hit$age_min_unanchored, hit$age_max_unanchored,
        100 * conservation_tolerance)
      if (identical(resolve_reproducibility_mode(), "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }
  out
}

#' Per-year population-conservation audit from an open-population run
#'
#' Reweighting to a Census projection only rescales ages the projection carries;
#' ages it omits keep their simulated weight. This reports how much of each
#' year's population is anchored, how much is not, and which ages leak -- the
#' quantity that decides whether a reweighted trajectory may be read as a
#' Census-consistent population.
#'
#' @param x Result of [simulate_dmdm_open()] run with `pop_by_age_year`.
#' @return Tibble of the per-year audit, or NULL when the run did not reweight.
#' @export
dmdm_population_audit <- function(x) {
  a <- attr(x, "population_audit", exact = TRUE)
  if (is.null(a)) return(NULL)
  tibble::as_tibble(a)
}

# ---- builder + wrapper (reuse R/demand-lifecourse) ---------------------------------------

# Build one agent frame (attributes + weight + seeded marginal p_c) at a given
# year, restricted to ages >= entry_age, with weights summing to the national
# population represented. Internal.
.dmdm_open_agents <- function(pop_by_age, year, entry_age, n, seed = NULL,
                              risk_params = lifecourse_risk_params()) {
  pa <- pop_by_age[pop_by_age$age >= entry_age, , drop = FALSE]
  pop <- .lifecourse_population(pa, year, n, cesarean_rate = 0.32, seed = seed)
  pop <- .lifecourse_risk(pop, risk_params)
  total <- sum(pa$population)
  # p_ui/p_pop/p_ai already come from .lifecourse_risk() above; re-assigning them
  # to themselves was a no-op that read as if the marginals were being derived here.
  dplyr::mutate(pop, weight = total / dplyr::n())
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
#' @param reweight_to_projection If TRUE, rescale each year's age-specific weights
#'   to `pop_by_age_year` so population counts match the Census projection while
#'   the model supplies the disease rates. Default FALSE (internal cohort-component
#'   demography).
#' @param allow_uncalibrated Declare an exploratory run; passed through to
#'   [simulate_dmdm_open()].
#' @return The per-year data frame from [simulate_dmdm_open()].
#' @export
dmdm_open_prevalence_trajectory <- function(pop_by_age_year, start_year, end_year,
                                            entry_age = 40L, n_init = 5e4, n_entrants = 2e3,
                                            seed = NULL, risk_params = lifecourse_risk_params(),
                                            transitions = dmdm_default_transitions(),
                                            reweight_to_projection = FALSE,
                                            allow_uncalibrated = FALSE) {
  # Gate before building agent tracks: n_init defaults to 5e4, so a refused run
  # should be refused before that work, and before the message names the engine
  # rather than the wrapper the caller actually invoked.
  assert_calibrated_transitions(transitions, allow_uncalibrated,
                                what = "DMDM open-trajectory transitions")
  stopifnot(all(c("year", "age", "population") %in% names(pop_by_age_year)))
  pa0 <- pop_by_age_year[pop_by_age_year$year == start_year, c("age", "population")]
  init <- .dmdm_open_agents(pa0, start_year, entry_age, n_init, seed = seed,
                            risk_params = risk_params)

  # seq_len, not `(start+1):end`: the colon operator counts DOWN when the window
  # is a single year, so start_year == end_year built entrant cohorts for
  # start_year + 1 and start_year -- the second duplicating the base population's
  # entry-age group. Neither is ever added (the engine only admits entrants for
  # y > start_year), but the frame was wrong and one bad indexing change away
  # from double-counting.
  entry_years <- start_year + seq_len(max(end_year - start_year, 0L))
  ent_list <- lapply(entry_years, function(y) {
    pae <- pop_by_age_year[pop_by_age_year$year == y & pop_by_age_year$age == entry_age,
                           c("age", "population"), drop = FALSE]
    if (!nrow(pae)) return(NULL)
    ag <- .dmdm_open_agents(pae, y, entry_age, n_entrants, seed = seed,
                            risk_params = risk_params)
    dplyr::mutate(ag, entry_year = y)
  })
  # bind_rows() of an empty (or all-NULL) list is a 0x0 frame with no columns,
  # which trips the engine's column contract. "No entrants" is NULL, not an empty
  # frame -- reachable both for a single-year window and when no entrant year is
  # present in `pop_by_age_year`.
  entrants <- dplyr::bind_rows(ent_list)
  if (!nrow(entrants)) entrants <- NULL

  simulate_dmdm_open(init, entrants, start_year, end_year, transitions = transitions,
                     pop_by_age_year = if (reweight_to_projection) pop_by_age_year else NULL,
                     allow_uncalibrated = allow_uncalibrated)
}
