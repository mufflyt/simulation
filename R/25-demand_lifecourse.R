# Reproductive life-course demand generator ----
#
# A demand pathway organized around the OBSTETRIC LIFE COURSE (Zarek et al. 2025,
# an application of Dall's workforce demand architecture), complementing the
# aging-population denominators in R/13-demand_urps.R. The primary exposure is
# cumulative vaginal-delivery history; BMI and the other covariates are risk
# MODIFIERS, not the demographic engine:
#
#   female population
#     -> childbirth exposures (cumulative vaginal deliveries)
#     -> pelvic-floor conditions (UI / POP / AI)
#     -> recognition -> care-seeking -> referral -> treatment
#     -> annual SERVICE VOLUMES (by service line)
#
# This module stops at service volumes and hands them to R/17-workload_to_fte.R
# (convert_workload_to_fte / apportion_service_volume): the provider-type
# delegation matrix and the work-RVU FTE conversion already live there, so demand
# is NOT converted to FTE here and provider substitution is NOT duplicated here.
#
# The DEMOGRAPHIC EXPOSURE marginals (completed parity and cesarean share) are
# CALIBRATED from cited birth-cohort series -- .lifecourse_population() reuses
# R/13b cohort_vaginal_exposure() rather than the old crude parity formula and
# scalar cesarean rate. The risk/pathway COEFFICIENT tables remain placeholders
# by default; lifecourse_risk_params_cited() supplies literature-anchored
# vaginal-delivery log-odds (with ranges) as a documented option.

# ---- Parameter tables (placeholder) ---------------------------------------

# Log-odds coefficients for each pelvic-floor condition. `bvag` (cumulative
# vaginal deliveries) is the primary term; `bbmi` is a modifier. POP (prolapse)
# carries the strongest vaginal-delivery and hysterectomy effects, reflecting
# obstetric etiology rather than an obesity-first model. Internal (override by
# passing your own list to simulate_lifecourse_demand()).
lifecourse_risk_params <- function() {
  mk <- function(b0, bvag, bage, bysl, bbmi, bhyst, bmeno, bcomorb)
    list(b0 = b0, bvag = bvag, bage = bage, bysl = bysl,
         bbmi = bbmi, bhyst = bhyst, bmeno = bmeno, bcomorb = bcomorb)
  list(
    status = "placeholder_uncalibrated",
    ui  = mk(-1.60, 0.18, 0.35, 0.05, 0.12, 0.10, 0.25, 0.15),
    pop = mk(-2.40, 0.42, 0.30, 0.08, 0.08, 0.45, 0.20, 0.05),
    ai  = mk(-2.90, 0.22, 0.25, 0.04, 0.06, 0.05, 0.10, 0.20)
  )
}

#' Literature-anchored life-course risk coefficients (cited option)
#'
#' An explicit, documented alternative to [lifecourse_risk_params()]: the
#' vaginal-delivery log-odds (`bvag`, the primary term) and the BMI modifier
#' (`bbmi`) are anchored to cited urogynecologic epidemiology; all other terms
#' inherit the internal placeholder set. Pass via `risk_params` to
#' [simulate_lifecourse_demand()]. `bvag` per cumulative vaginal delivery
#' (log-odds), with the literature RANGE noted:
#'   POP OR ~1.35/delivery (log 0.30); range 1.10-1.21 (Hendrix WHI, per birth)
#'          up to steeper (Mant Oxford-FPA RR 8.4 at 2 / 10.9 at 4 vaginal births).
#'   UI  OR ~1.16/delivery (log 0.15) (Rortveit 2001/2003, EPINCONT).
#'   AI  OR ~1.10/delivery (log 0.10), weak/uncertain; OASI-specific OR 2.66
#'          (LaCross 2015) is a distinct, stronger contrast.
#' `bbmi` per +5 kg/m^2 from Giri 2017 AJOG (obese vs normal RR ~1.47). All
#' PROVISIONAL: full-text verification recommended before publication.
#' @return A risk-params list in the shape of [lifecourse_risk_params()].
#' @export
lifecourse_risk_params_cited <- function() {
  base <- lifecourse_risk_params()
  base$status   <- "obstetric_literature_anchored"
  base$ui$bvag  <- 0.15; base$ui$bbmi  <- 0.26
  base$pop$bvag <- 0.30; base$pop$bbmi <- 0.26
  base$ai$bvag  <- 0.10
  base
}

# Care-pathway probabilities per condition (recognition -> seek -> referral ->
# treated). Internal.
lifecourse_pathway_params <- function() {
  list(
    status      = "placeholder_uncalibrated",
    recognition = c(ui = 0.55, pop = 0.60, ai = 0.35),
    p_seek      = c(ui = 0.45, pop = 0.50, ai = 0.30),
    p_referral  = c(ui = 0.40, pop = 0.55, ai = 0.45),
    p_treated   = c(ui = 0.70, pop = 0.65, ai = 0.60)
  )
}

# Expected annual service units per TREATED patient, by condition and service.
# Service names must match the workload basket in R/23-cms_rvu.R so the volumes
# feed convert_workload_to_fte() (min_match_rate = 1.0). Internal.
lifecourse_service_map <- function() {
  tibble::tribble(
    ~condition, ~service,              ~per_treated,
    "ui",       "new_consultation",    1.0,
    "ui",       "return_visit",        2.0,
    "ui",       "urodynamics",         0.6,
    "ui",       "cystoscopy",          0.3,
    "ui",       "ptns",                0.8,
    "ui",       "botox_bladder",       0.15,
    "ui",       "sling_procedure",     0.12,
    "pop",      "new_consultation",    1.0,
    "pop",      "return_visit",        2.2,
    "pop",      "pessary_care",        1.5,
    "pop",      "prolapse_procedure",  0.25,
    "ai",       "new_consultation",    1.0,
    "ai",       "return_visit",        1.8,
    "ai",       "ptns",                0.5,
    "ai",       "urodynamics",         0.3
  )
}

# ---- Internal pipeline steps ----------------------------------------------

# Build a synthetic person-year panel centered on vaginal-delivery exposure.
# Base R vectors assembled into a tibble (no survey/tidyverse NSE), so the
# marginals are transparent and swappable.
.lifecourse_population <- function(pop_by_age, year, n, cesarean_rate = NULL, seed = NULL) {
  if (!is.null(seed)) set.seed(seed)
  stopifnot(all(c("age", "population") %in% names(pop_by_age)))
  idx <- sample(seq_len(nrow(pop_by_age)), size = n, replace = TRUE,
                prob = pop_by_age$population)
  age <- pop_by_age$age[idx]
  # CITED cohort marginals (reuse R/13b's cohort_vaginal_exposure; do not redefine):
  # completed parity and cesarean share BY BIRTH COHORT replace the crude
  # (age-20)/12 parity formula and the single scalar cesarean rate. An 85-year-old
  # in 2040 was born 1955 and delivered when cesarean was ~10-19%, not 32%.
  cohort <- year - age
  ex <- cohort_vaginal_exposure(sort(unique(cohort)))
  m <- match(cohort, ex$birth_cohort)
  parity <- stats::rpois(n, pmax(0, ex$mean_total_parity[m]))
  # Explicit scalar cesarean_rate overrides the cited baseline (delivery_mode lever).
  ces_p <- if (is.null(cesarean_rate)) ex$cohort_cesarean_fraction[m] else cesarean_rate
  cesarean_births <- stats::rbinom(n, parity, ces_p)
  vaginal_births <- parity - cesarean_births
  age_at_last_vaginal <- pmin(age, 26L + stats::rpois(n, 4))
  ysl <- ifelse(vaginal_births > 0, pmax(0, age - age_at_last_vaginal), 0)
  tibble::tibble(
    person_id = seq_len(n), year = year, age = age, alive = 1L,
    parity = parity, cesarean_births = cesarean_births, vaginal_births = vaginal_births,
    cumulative_vaginal_deliveries = vaginal_births,
    years_since_last_vaginal_birth = ysl,
    bmi = pmax(16, stats::rnorm(n, 27 + 0.03 * (age - 40), 5)),
    menopause_status = as.integer(age >= 51 | stats::runif(n) < pmax(0, (age - 45) / 12)),
    hysterectomy = stats::rbinom(n, 1, pmin(0.45, pmax(0, 0.008 * (age - 30)))),
    comorbidity = stats::rbinom(n, 1, pmin(0.5, 0.1 + 0.006 * age)),
    high_barrier = stats::rbinom(n, 1, 0.35)
  )
}

# Attach per-person condition probabilities (vaginal deliveries primary).
.lifecourse_risk <- function(pop, risk_params) {
  lp <- function(p) {
    p$b0 +
      p$bvag    * pop$cumulative_vaginal_deliveries +
      p$bage    * ((pop$age - 50) / 10) +
      p$bysl    * (pop$years_since_last_vaginal_birth / 10) +
      p$bbmi    * ((pop$bmi - 27) / 5) +
      p$bhyst   * pop$hysterectomy +
      p$bmeno   * pop$menopause_status +
      p$bcomorb * pop$comorbidity
  }
  pop$p_ui  <- stats::plogis(lp(risk_params$ui))
  pop$p_pop <- stats::plogis(lp(risk_params$pop))
  pop$p_ai  <- stats::plogis(lp(risk_params$ai))
  pop
}

# Expected treated fraction per condition + care-pathway state fields.
.lifecourse_treated <- function(pop, pathway, access_gain) {
  seek <- function(cond) {
    base <- pathway$p_seek[[cond]]
    ifelse(pop$high_barrier == 1L, pmin(1, base * access_gain), base)
  }
  treated <- function(cond, prev) {
    prev * pathway$recognition[[cond]] * seek(cond) *
      pathway$p_referral[[cond]] * pathway$p_treated[[cond]]
  }
  pop$treated_ui  <- treated("ui",  pop$p_ui)
  pop$treated_pop <- treated("pop", pop$p_pop)
  pop$treated_ai  <- treated("ai",  pop$p_ai)
  pop$care_seeking_state <-
    1 - (1 - pop$p_ui  * pathway$recognition[["ui"]]  * seek("ui")) *
        (1 - pop$p_pop * pathway$recognition[["pop"]] * seek("pop")) *
        (1 - pop$p_ai  * pathway$recognition[["ai"]]  * seek("ai"))
  pop$treatment_state <-
    1 - (1 - pop$treated_ui) * (1 - pop$treated_pop) * (1 - pop$treated_ai)
  pop
}

# ---- Public entry points ---------------------------------------------------

#' Simulate one year of life-course pelvic-floor demand as service volumes
#'
#' Runs the reproductive life-course pipeline (childbirth exposure -> pelvic-floor
#' conditions -> care pathway -> service use) for a single calendar year and
#' scenario, and returns national, population-scaled annual SERVICE VOLUMES ready
#' for [convert_workload_to_fte()] / [apportion_service_volume()] in
#' R/17-workload_to_fte.R. Demand is not converted to FTE here and provider
#' substitution is handled by that module's delegation matrix.
#'
#' Scenarios: `"baseline"`; `"delivery_mode"` (shifts the primary exposure via
#' `cesarean_rate`); `"reduced_barriers"` (raises care-seeking for high-barrier
#' women via `access_gain`); `"prevention"` (cuts one transition — BMI reduction
#' or a condition-specific risk reduction — the ONLY place BMI interventions
#' live). Provider-mix / treatment-substitution scenarios belong to
#' R/17-workload_to_fte.R, not here.
#'
#' @param pop_by_age Tibble with `age` and `population` (female population for the
#'   target year).
#' @param year Calendar year (stamped on the output).
#' @param scenario One of "baseline", "delivery_mode", "reduced_barriers",
#'   "prevention".
#' @param n Number of synthetic persons.
#' @param seed Optional RNG seed.
#' @param cesarean_rate Share of live births delivered by cesarean (delivery-mode
#'   lever). Default `NULL` uses the CITED cohort-varying cesarean baseline (via
#'   R/13b `cohort_vaginal_exposure()`); pass a scalar to override it.
#' @param access_gain Multiplier on care-seeking for high-barrier women
#'   (reduced-barriers lever). Default 1; the "reduced_barriers" scenario sets 1.6
#'   when left at 1.
#' @param prevention_target One of "pop", "ui", "ai", "bmi" (prevention lever).
#' @param prevention_effect Fractional reduction applied by the prevention lever.
#' @param risk_params,pathway_params,service_map Parameter tables; defaults are
#'   placeholders — override to calibrate.
#' @return A list with `person_years`, `service_volumes` (tibble `year`,
#'   `service`, `volume`), `care_seeking_national`, `treated_national`, and `meta`.
#' @export
simulate_lifecourse_demand <- function(pop_by_age, year, scenario = "baseline",
                                       n = 1e5, seed = NULL,
                                       cesarean_rate = NULL, access_gain = 1,
                                       prevention_target = c("pop", "ui", "ai", "bmi"),
                                       prevention_effect = 0.20,
                                       risk_params = lifecourse_risk_params(),
                                       pathway_params = lifecourse_pathway_params(),
                                       service_map = lifecourse_service_map()) {
  scenario <- match.arg(scenario,
                        c("baseline", "delivery_mode", "reduced_barriers", "prevention"))
  prevention_target <- match.arg(prevention_target)
  if (scenario == "reduced_barriers" && access_gain == 1) access_gain <- 1.6

  pop <- .lifecourse_population(pop_by_age, year, n, cesarean_rate, seed)
  if (scenario == "prevention" && prevention_target == "bmi") {
    pop$bmi <- pop$bmi - prevention_effect * pmax(0, pop$bmi - 25)
  }
  pop <- .lifecourse_risk(pop, risk_params)
  if (scenario == "prevention" && prevention_target != "bmi") {
    col <- paste0("p_", prevention_target)
    pop[[col]] <- pop[[col]] * (1 - prevention_effect)
  }
  pop <- .lifecourse_treated(pop, pathway_params, access_gain)

  scale_factor <- sum(pop_by_age$population) / n
  treated_national <- c(ui  = sum(pop$treated_ui),
                        pop = sum(pop$treated_pop),
                        ai  = sum(pop$treated_ai)) * scale_factor

  treated_tbl <- tibble::tibble(condition = names(treated_national),
                                treated = as.numeric(treated_national))
  vols <- dplyr::inner_join(service_map, treated_tbl, by = "condition")
  vols <- dplyr::mutate(vols, volume = .data$per_treated * .data$treated, year = year)
  service_volumes <- dplyr::summarise(
    dplyr::group_by(vols, .data$year, .data$service),
    volume = sum(.data$volume), .groups = "drop")

  list(
    person_years          = pop,
    service_volumes       = service_volumes,
    care_seeking_national = sum(pop$care_seeking_state) * scale_factor,
    treated_national      = treated_national,
    meta = list(scenario = scenario, year = year, n = n,
                cesarean_rate = if (is.null(cesarean_rate)) "cited_cohort_varying"
                                else cesarean_rate,
                access_gain = access_gain,
                status = if (identical(risk_params$status, "obstetric_literature_anchored"))
                           "cohort_marginals_cited; risk_coeffs_literature_anchored"
                         else "cohort_marginals_cited; risk_coeffs_placeholder")
  )
}

#' Life-course demand trajectory across years
#'
#' Runs [simulate_lifecourse_demand()] for each year and collects the service
#' volumes plus a national demand summary (care-seeking and total service units),
#' the latter feeding the contract tiers 5-6 in [export_hdmm_demand_contract()].
#'
#' @param pop_by_age_year Tibble with `year`, `age`, `population`.
#' @param years Years to run; default all years present in `pop_by_age_year`.
#' @param scenario,n,seed,... Passed to [simulate_lifecourse_demand()].
#' @return A list with `service_volumes` (tibble `year`, `service`, `volume`) and
#'   `demand_summary` (tibble `year`, `care_seeking_national`,
#'   `service_units_national`).
#' @export
lifecourse_demand_trajectory <- function(pop_by_age_year,
                                         years = sort(unique(pop_by_age_year$year)),
                                         scenario = "baseline", n = 1e5, seed = NULL, ...) {
  stopifnot(all(c("year", "age", "population") %in% names(pop_by_age_year)))
  runs <- purrr::map(years, function(y) {
    pa <- dplyr::filter(pop_by_age_year, .data$year == y)
    pa <- dplyr::select(pa, "age", "population")
    simulate_lifecourse_demand(pa, year = y, scenario = scenario, n = n, seed = seed, ...)
  })
  service_volumes <- dplyr::bind_rows(purrr::map(runs, "service_volumes"))
  demand_summary <- tibble::tibble(
    year                   = years,
    care_seeking_national  = purrr::map_dbl(runs, "care_seeking_national"),
    service_units_national = purrr::map_dbl(runs, function(r) sum(r$service_volumes$volume))
  )
  list(service_volumes = service_volumes, demand_summary = demand_summary)
}

# ---- Integration with the workforce FTE conversion (R/17) -------------------

#' Required provider FTE from the life-course demand pathway
#'
#' Runs [lifecourse_demand_trajectory()] and converts its annual service volumes
#' to required FTE with [convert_workload_to_fte()] (R/17). The provider-type
#' delegation matrix and the work-RVU conversion are therefore REUSED, not
#' reimplemented here.
#'
#' @param pop_by_age_year Tibble with `year`, `age`, `population`.
#' @param wrvu_per_fte Annual work RVUs per clinical FTE. No default on purpose:
#'   derive it from a base-year anchor with [calibrate_wrvu_per_fte()] rather than
#'   assuming a productivity level. For an illustrative run pass
#'   `WRVU_PER_FTE_BENCHMARK[["median"]]`.
#' @param scenario,n,seed,... Passed to [lifecourse_demand_trajectory()].
#' @return A list with `service_volumes`, `demand_summary`, and `required_fte`
#'   (the tibble returned by [convert_workload_to_fte()]).
#' @export
lifecourse_required_fte <- function(pop_by_age_year, wrvu_per_fte,
                                    scenario = "baseline", n = 1e5, seed = NULL, ...) {
  traj <- lifecourse_demand_trajectory(pop_by_age_year, scenario = scenario,
                                       n = n, seed = seed, ...)
  fte <- convert_workload_to_fte(traj$service_volumes, wrvu_per_fte = wrvu_per_fte,
                                 method = "wrvu")
  list(service_volumes = traj$service_volumes,
       demand_summary   = traj$demand_summary,
       required_fte     = fte)
}

#' Life-course demand as a concordance estimand
#'
#' Reshapes a life-course demand summary into the long demand-estimand format used
#' by [compute_demand_denominators()], [compute_growth_adequacy()] and
#' [assess_demand_concordance()], so the childbirth-driven pathway can be checked
#' for concordance against the other estimands. Its generator differs, so it is
#' not a proportional rescaling — exactly the real-concordance check the framework
#' wants.
#'
#' Estimand id note: this is `D5`, the life-course *service* demand (downstream of
#' the care pathway). It is distinct from `D4` in
#' [compute_demand_denominators_lifecourse()] (R/13b), which is an obstetric-
#' exposure-weighted prevalent-case *denominator*. D1–D3 are the published
#' denominators (R/13); D4 is the obstetric-weighted denominator; D5 is the
#' dynamic service-demand series.
#'
#' @param demand_summary The `demand_summary` from [lifecourse_demand_trajectory()].
#' @param measure Which series to treat as demand: "service_units" (procedural
#'   load) or "care_seeking".
#' @param estimand,label Estimand id and label.
#' @return Tibble with `year`, `estimand`, `label`, `demand_cases`.
#' @export
lifecourse_demand_estimand <- function(demand_summary,
                                       measure = c("service_units", "care_seeking"),
                                       estimand = "D5_lifecourse_service",
                                       label = "Life-course service demand (care-pathway, vaginal-delivery driven)") {
  measure <- match.arg(measure)
  col <- if (measure == "service_units") "service_units_national" else "care_seeking_national"
  assertthat::assert_that(is.data.frame(demand_summary),
                          all(c("year", col) %in% names(demand_summary)))
  tibble::tibble(year = demand_summary$year, estimand = estimand, label = label,
                 demand_cases = demand_summary[[col]])
}
