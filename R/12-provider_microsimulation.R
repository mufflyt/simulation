# Provider-Career Microsimulation (Supply) ----
#
# Turns the DETERMINISTIC supply loop in R/workforce.R
# (supply[t] = supply[t-1] + entrants - retirements) into a STOCHASTIC,
# individual-level microsimulation, matching the per-agent Monte-Carlo style
# already used by the DPMM disease model (R/01-dppm_setup.R::run_microsimulation_analysis).
#
# Approaches ported:
#   * cliff  (R/workforce_cliff_engine.R::wc_project): age-band departure
#     hazards, fixed-age entrant injection, WC_BANDS / entry-age lifecycle.
#   * cliff  (scripts/urps_module_a_effective_supply): age-productivity weighting
#     so we report BOTH headcount and "effective FTE", and the replacement-ratio
#     outlook classifier (classify_workforce_outlook).
#   * isochrones (R/retirement_filter_utilities.R + create_year_specific_temporal_maps.R):
#     the load-bearing "active in year Y <=> retirement_year > Y" STRICT predicate,
#     with retirement_year stored as first-inactive (last_active + 1). The active
#     cohort is RECOMPUTED per year from one immutable agent table (temporal cohort).
#
# Deterministic mean-field (wc_project) vs. this stochastic microsimulation are
# kept side-by-side so the microsim can be validated against the analytic
# expectation (cliff code/05_validate_with_monte_carlo.R does exactly this).

# ---- Lifecycle constants (SSOT, guarded) ----------------------------------

# Age bands, matching cliff WC_BANDS. Left-closed, right-open: [0,45), [45,50)...
MICROSIM_AGE_BANDS <- c(0, 45, 50, 55, 60, 65, 70, Inf)
MICROSIM_AGE_BAND_LABELS <- c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+")

MICROSIM_ENTRY_AGE   <- 34L   # cliff WC_ENTRY_AGE: age at entry to practice
MICROSIM_AGE_AT_CERT <- 30L   # cliff WC_AGE_AT_CERT
MICROSIM_BASELINE_YEAR <- 2025L  # cliff PROJECTION_BASELINE_YEAR / WC_YEAR0

# Reference age-band ABSOLUTE annual departure hazards (illustrative FPMRS
# gradient; increasing with age, steep 65+). Override with EMPIRICAL hazards
# from cliff::wc_band_counts() (person-years/events) when available.
MICROSIM_REFERENCE_HAZARD <- c(
  "<45"   = 0.010,
  "45-49" = 0.015,
  "50-54" = 0.025,
  "55-59" = 0.045,
  "60-64" = 0.090,
  "65-69" = 0.180,
  "70+"   = 0.300
)
MICROSIM_REFERENCE_BASELINE_RATE <- 0.044  # FPMRS ~4.4%/yr (cliff SENSITIVITY_ANALYSES.md)

# Per-subspecialty overall annual retirement rates (cliff empirical baselines,
# validated vs state boards at 94.4% agreement).
MICROSIM_BASELINE_RATES <- c(FPMRS = 0.044, GO = 0.052, MIGS = 0.034)

#' Safe per-subspecialty baseline retirement rate
#'
#' Falls back to the reference rate for an unknown subspecialty (a named-vector
#' `[[` on a missing name would otherwise error, defeating a `%||%` guard).
#'
#' @param subspecialty Subspecialty label.
#' @return Numeric annual baseline retirement rate.
#' @export
microsim_baseline_rate <- function(subspecialty) {
  if (subspecialty %in% names(MICROSIM_BASELINE_RATES)) {
    MICROSIM_BASELINE_RATES[[subspecialty]]
  } else {
    .msg_warn(sprintf("No baseline rate for subspecialty '%s'; using reference %.3f",
                      subspecialty, MICROSIM_REFERENCE_BASELINE_RATE))
    MICROSIM_REFERENCE_BASELINE_RATE
  }
}

# Replacement-ratio outlook cutpoints (cliff workforce_constants.R).
WORKFORCE_OUTLOOK_ADEQUATE_MIN <- 1.2
WORKFORCE_OUTLOOK_MARGINAL_MIN <- 0.8

stopifnot(
  length(MICROSIM_AGE_BANDS) == length(MICROSIM_AGE_BAND_LABELS) + 1L,
  MICROSIM_ENTRY_AGE > MICROSIM_AGE_AT_CERT,
  all(MICROSIM_REFERENCE_HAZARD >= 0 & MICROSIM_REFERENCE_HAZARD <= 1)
)

# ---- Lifecycle helpers ----------------------------------------------------

#' Bucket an age (or vector of ages) into a cliff-style age band label
#' @param age Numeric age(s).
#' @return Character age-band label(s).
#' @export
microsim_age_band_of <- function(age) {
  idx <- cut(age, breaks = MICROSIM_AGE_BANDS, labels = MICROSIM_AGE_BAND_LABELS,
             right = FALSE, include.lowest = TRUE)
  as.character(idx)
}

#' Build a per-age-band absolute-hazard table for a subspecialty
#'
#' Scales the reference age gradient so the level matches the subspecialty's
#' published overall annual retirement rate.
#'
#' @param baseline_rate Overall annual retirement probability for the subspecialty.
#' @param reference_shape Reference age-band hazards (the age gradient).
#' @param reference_rate The overall rate implied by `reference_shape`.
#' @return Named numeric vector of absolute annual hazards by age band.
#' @export
build_hazard_table <- function(baseline_rate,
                               reference_shape = MICROSIM_REFERENCE_HAZARD,
                               reference_rate = MICROSIM_REFERENCE_BASELINE_RATE) {
  scaled <- reference_shape * (baseline_rate / reference_rate)
  pmin(scaled, 1)  # a hazard is a probability
}

#' Look up the annual departure hazard for an age given a hazard table
#' @param age Numeric age(s).
#' @param hazard_table Named vector of band hazards (see [build_hazard_table()]).
#' @return Numeric hazard(s) in the unit interval.
#' @export
retirement_hazard <- function(age, hazard_table) {
  bands <- microsim_age_band_of(age)
  h <- hazard_table[bands]
  h[is.na(h)] <- max(hazard_table)  # ages beyond the table take the max hazard
  unname(pmin(h, 1))
}

#' Age-productivity weight (DEPRECATED step function)
#'
#' Superseded by the hours-based FTE definition in R/16-provider_lifecycle.R.
#' Every Dall-family model defines FTE as an HOURS THRESHOLD and estimates weekly
#' hours by regression on age and sex; a hand-chosen step function normalised so
#' the base-year cohort averages 1.0 cannot reproduce the two properties those
#' regressions show (hours flat until the late 50s, and a sex gap that varies
#' with age). Retained only so old results can be reproduced for comparison.
#'
#' @param age Numeric age(s).
#' @return Numeric relative-productivity weight(s).
#' @export
productivity_weight_raw <- function(age) {
  dplyr::case_when(
    age < 50  ~ 1.00,
    age < 60  ~ 0.95,
    age < 65  ~ 0.85,
    age < 70  ~ 0.70,
    TRUE      ~ 0.55
  )
}

#' Clinical FTE contributed by each active provider
#'
#' Dispatches on the FTE method:
#'   "hours"          weekly clinical hours / the FTE hours threshold (default;
#'                    Dall 2013, Dall 2021, Zarek 2025)
#'   "participation"  categorical full-time / part-time / no-patient-care
#'                    expectation (Fraher & Knapton FutureDocs)
#'   "legacy_weight"  the deprecated normalised step function
#'
#' @param age Numeric age(s).
#' @param sex Character sex.
#' @param method FTE method.
#' @param hours_model Optional fitted hours model.
#' @param fte_hours Weekly clinical hours defining 1.0 FTE.
#' @param legacy_norm Normalisation constant for the legacy method.
#' @param hours_intercept Intercept for the reference hours schedule; supply the
#'   value from [calibrate_hours_intercept()] so the hours schedule and the FTE
#'   threshold are internally consistent.
#' @return Numeric clinical FTE per provider.
#' @export
provider_clinical_fte <- function(age, sex = "female",
                                  method = c("hours", "participation", "legacy_weight"),
                                  hours_model = NULL,
                                  fte_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK,
                                  legacy_norm = 1,
                                  hours_intercept = HWSM_HOURS_INTERCEPT) {
  method <- match.arg(method)
  switch(
    method,
    hours = if (is.null(hours_model)) {
      hwsm_reference_hours(age, sex, intercept = hours_intercept) / fte_hours
    } else {
      predict_clinical_fte(age, sex, hours_model, fte_hours)
    },
    participation = participation_fte(age, sex),
    legacy_weight = productivity_weight_raw(age) * legacy_norm
  )
}

#' STRICT "active in year Y" predicate (isochrones retirement contract)
#'
#' A provider is active in year Y iff they have entered practice AND their
#' retirement_year (first-inactive = last_active + 1) is strictly greater than Y.
#' The strict `>` and first-inactive convention are load-bearing: retirement_year
#' == Y means last active Y-1, i.e. NOT active in Y.
#'
#' @param agents Agent tibble with `entry_year` and `retirement_year` (NA = never).
#' @param year Integer year.
#' @return Logical vector, TRUE where the agent is active in `year`.
#' @export
provider_active_in_year <- function(agents, year) {
  entered <- agents$entry_year <= year
  not_retired <- is.na(agents$retirement_year) | agents$retirement_year > year
  entered & not_retired
}

# ---- Cohort initialisation ------------------------------------------------

#' Initialise a starting cohort of provider agents
#'
#' @param n Number of providers in the base-year workforce.
#' @param subspecialty Subspecialty label (used to pick the hazard baseline).
#' @param baseline_year Calendar year of the starting workforce.
#' @param age_distribution Function(n) returning n starting ages, or a numeric
#'   vector of length `n`. Defaults to a plausible subspecialist age spread.
#' @return A tibble of agents (one row per provider).
#' @export
initialize_provider_agents <- function(n,
                                       subspecialty = "FPMRS",
                                       baseline_year = MICROSIM_BASELINE_YEAR,
                                       age_distribution = NULL) {
  assertthat::assert_that(is.numeric(n), n > 0)

  if (is.null(age_distribution)) {
    # Right-skewed spread centred in mid-career, clamped to a plausible range.
    ages <- round(stats::rnorm(n, mean = 52, sd = 9))
    ages <- pmin(pmax(ages, MICROSIM_ENTRY_AGE), 78)
  } else if (is.function(age_distribution)) {
    ages <- age_distribution(n)
  } else {
    assertthat::assert_that(length(age_distribution) == n)
    ages <- age_distribution
  }

  tibble::tibble(
    provider_id = sprintf("P%06d", seq_len(n)),
    subspecialty = subspecialty,
    age = as.numeric(ages),
    entry_year = baseline_year - pmax(as.numeric(ages) - MICROSIM_ENTRY_AGE, 0),
    retirement_year = NA_real_,
    origin_cohort = "baseline"
  )
}

# ---- Single stochastic trajectory -----------------------------------------

#' Simulate one stochastic provider-career trajectory
#'
#' Each year: draw Bernoulli retirement per active agent using the age-band
#' hazard, age everyone by one year, then inject `entrants_per_year` new agents
#' at the entry age. Records active headcount, effective FTE, and mean age.
#'
#' @param agents Starting agent tibble ([initialize_provider_agents()]).
#' @param years Integer vector of calendar years to simulate (ascending).
#' @param entrants_per_year Numeric annual entrant count (may be fractional; the
#'   fractional part is realised stochastically).
#' @param hazard_table Age-band hazard table ([build_hazard_table()]).
#' @param conversion_floor Fraction of entrants that actually enter practice
#'   (cliff WORKFORCE_CONVERSION_FLOOR = 0.70). Applied as a haircut on entrants.
#' @param subspecialty Subspecialty label for injected entrants.
#' @param retirement_schedule Single-year-of-age retirement hazard schedule.
#'   Scenarios pass a SHIFTED schedule (retire n years earlier/later).
#' @param career_change_hazard Annual departure hazard below the retirement age.
#'   Set to 0, with a zero retirement schedule, for a no-attrition run.
#' @param fte_method FTE method: "hours", "participation", or "legacy_weight".
#' @param hours_model Optional fitted hours model from
#'   [fit_clinical_hours_model()]; when NULL the reference schedule is used.
#' @param hours_multiplier Scenario knob scaling hours worked.
#' @param hours_intercept Intercept for the reference hours schedule; use
#'   [calibrate_hours_intercept()] so hours and the FTE threshold agree.
#' @param late_career_fte_factor Multiplier on clinical FTE applied only from
#'   `late_career_fte_onset_age` (a mufflyaccess scenario field).
#' @param late_career_fte_onset_age Age from which the late-career factor
#'   applies; NA disables it.
#' @param entrant_female_share Share of new entrants drawn as female.
#' @param placement_shares Optional tibble of `geo` and `share` enabling entrant
#'   placement and mid-career migration.
#' @return List with `panel` (per-year summary tibble) and `agents` (final agent
#'   table incl. drawn retirement years) so the temporal cohort is reconstructible.
#' @export
simulate_provider_career_once <- function(agents,
                                          years,
                                          entrants_per_year,
                                          hazard_table = NULL,
                                          conversion_floor = 1.0,
                                          subspecialty = "FPMRS",
                                          retirement_schedule = RETIREMENT_HAZARD_BY_AGE,
                                          career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50,
                                          fte_method = "hours",
                                          hours_model = NULL,
                                          hours_multiplier = 1.0,
                                          hours_intercept = HWSM_HOURS_INTERCEPT,
                                          late_career_fte_factor = 1.0,
                                          late_career_fte_onset_age = NA_real_,
                                          entrant_female_share = 0.82,
                                          placement_shares = NULL) {
  years <- sort(unique(as.integer(years)))
  base_year <- min(years)

  if (!"sex" %in% names(agents)) agents$sex <- "female"

  # Legacy step-function weights are normalised so the base-year cohort averages
  # 1.0; the hours and participation methods are absolute and need no scaling.
  prod_norm <- 1
  if (identical(fte_method, "legacy_weight")) {
    base_active <- provider_active_in_year(agents, base_year)
    raw_w <- productivity_weight_raw(agents$age[base_active])
    prod_norm <- if (length(raw_w) > 0 && mean(raw_w) > 0) 1 / mean(raw_w) else 1
  }

  # `late_career_fte_factor` is applied only from `late_career_fte_onset_age`,
  # which is what the mufflyaccess scenario registry specifies. A uniform hours
  # multiplier cannot represent it: the registry's lower_late_career_fte
  # scenario reduces clinical FTE by 25% from age 60 ONLY, leaving younger
  # providers untouched.
  apply_late_career <- is.finite(late_career_fte_onset_age) &&
    !isTRUE(all.equal(late_career_fte_factor, 1))
  fte_of <- function(age, sex) {
    base <- provider_clinical_fte(age, sex, method = fte_method, hours_model = hours_model,
                                  legacy_norm = prod_norm,
                                  hours_intercept = hours_intercept) * hours_multiplier
    if (apply_late_career) {
      base <- base * ifelse(age >= late_career_fte_onset_age, late_career_fte_factor, 1)
    }
    base
  }

  effective_entrants <- entrants_per_year * conversion_floor

  # ---- Preallocated plain-vector state --------------------------------------
  # The inner loop previously grew a tibble with dplyr::bind_rows() once per
  # simulated year and built a one-row tibble per year for the panel. Both are
  # O(n) copies inside an O(years x iterations) loop and dominated the runtime.
  # Here the agent state lives in preallocated atomic vectors and the tibbles are
  # constructed once, at the end.
  n0 <- nrow(agents)
  n_years <- length(years)
  capacity <- n0 + n_years * (as.integer(ceiling(effective_entrants)) + 1L)

  v_age <- c(as.numeric(agents$age), rep(NA_real_, capacity - n0))
  v_sex <- c(as.character(agents$sex), rep(NA_character_, capacity - n0))
  v_entry <- c(as.numeric(agents$entry_year), rep(NA_real_, capacity - n0))
  v_retire <- c(as.numeric(agents$retirement_year), rep(NA_real_, capacity - n0))
  v_state <- if ("state" %in% names(agents)) {
    c(as.character(agents$state), rep(NA_character_, capacity - n0))
  } else NULL
  v_id <- c(as.character(agents$provider_id), rep(NA_character_, capacity - n0))
  v_origin <- c(as.character(agents$origin_cohort %||% "baseline"),
                rep(NA_character_, capacity - n0))
  n_used <- n0

  p_year <- integer(n_years)
  p_head <- integer(n_years)
  p_fte <- numeric(n_years)
  p_age <- numeric(n_years)
  next_entrant_seq <- 1L

  for (i in seq_along(years)) {
    year <- years[i]
    live <- seq_len(n_used)

    # --- Departures: Bernoulli draw per active agent ---
    # retirement_year is first-inactive, so an agent retiring THIS year (value
    # year + 1) is still active this year. The active set therefore does not
    # change when the draws are applied, and one recomputation is enough.
    active <- live[(v_entry[live] <= year) &
                     (is.na(v_retire[live]) | v_retire[live] > year)]

    if (length(active)) {
      hz <- if (is.null(hazard_table)) {
        departure_hazard(v_age[active], v_sex[active],
                         retirement_schedule = retirement_schedule,
                         career_change_hazard = career_change_hazard)
      } else {
        retirement_hazard(v_age[active], hazard_table)
      }
      retiring <- active[stats::runif(length(active)) < hz]
      v_retire[retiring] <- year + 1L
    }

    # --- Record end-of-year state BEFORE injecting next year's entrants ---
    p_year[i] <- year
    p_head[i] <- length(active)
    p_fte[i] <- if (length(active)) sum(fte_of(v_age[active], v_sex[active])) else 0
    p_age[i] <- if (length(active)) mean(v_age[active]) else NA_real_

    # --- Optional mid-career geographic migration ---
    if (!is.null(placement_shares) && !is.null(v_state) && length(active)) {
      yrs <- year - v_entry[active]
      h <- migration_hazard(yrs, v_age[active])
      movers <- active[stats::runif(length(active)) < h]
      if (length(movers)) {
        v_state[movers] <- assign_entrant_geography(length(movers), placement_shares)
      }
    }

    # --- Age everyone by one year (survivors and retirees alike) ---
    v_age[live] <- v_age[live] + 1

    # --- Inject entrants for the next year (stochastic fractional part) ---
    n_new <- floor(effective_entrants) +
      as.integer(stats::runif(1) < (effective_entrants - floor(effective_entrants)))
    if (n_new > 0) {
      slot <- seq.int(n_used + 1L, length.out = n_new)
      if (max(slot) > capacity) {
        grow <- max(slot) - capacity + n_new
        v_age <- c(v_age, rep(NA_real_, grow));    v_sex <- c(v_sex, rep(NA_character_, grow))
        v_entry <- c(v_entry, rep(NA_real_, grow)); v_retire <- c(v_retire, rep(NA_real_, grow))
        v_id <- c(v_id, rep(NA_character_, grow)); v_origin <- c(v_origin, rep(NA_character_, grow))
        if (!is.null(v_state)) v_state <- c(v_state, rep(NA_character_, grow))
        capacity <- capacity + grow
      }
      # HWSM: entrant sex is a uniform draw against the recent-entrant share.
      v_sex[slot] <- ifelse(stats::runif(n_new) < entrant_female_share, "female", "male")
      v_age[slot] <- MICROSIM_ENTRY_AGE
      v_entry[slot] <- year + 1L
      v_retire[slot] <- NA_real_
      v_id[slot] <- sprintf("E%d_%06d", year, seq.int(next_entrant_seq, length.out = n_new))
      v_origin[slot] <- "entrant"
      if (!is.null(v_state)) {
        v_state[slot] <- assign_entrant_geography(n_new, placement_shares)
      }
      n_used <- n_used + n_new
      next_entrant_seq <- next_entrant_seq + n_new
    }
  }

  keep <- seq_len(n_used)
  final_agents <- tibble::tibble(
    provider_id = v_id[keep],
    subspecialty = subspecialty,
    sex = v_sex[keep],
    age = v_age[keep],
    entry_year = v_entry[keep],
    retirement_year = v_retire[keep],
    origin_cohort = v_origin[keep]
  )
  if (!is.null(v_state)) final_agents$state <- v_state[keep]

  list(
    panel = tibble::tibble(
      year = p_year,
      subspecialty = subspecialty,
      headcount = p_head,
      effective_fte = p_fte,
      mean_age = p_age
    ),
    agents = final_agents
  )
}

# ---- Monte-Carlo supply microsimulation -----------------------------------

#' Run the supply microsimulation with Monte-Carlo replicates
#'
#' Repeats [simulate_provider_career_once()] `n_iterations` times and summarises
#' the distribution of headcount / effective-FTE per year (median + CI band),
#' giving DISTRIBUTIONAL supply projections rather than a single deterministic
#' line. Seeded via [seed_microsimulation()] for reproducibility.
#'
#' @param initial_workforce Base-year workforce size, OR a prebuilt agent tibble.
#' @param years Integer vector of years to project.
#' @param entrants_per_year Annual entrants.
#' @param subspecialty Subspecialty label (selects the baseline hazard).
#' @param n_iterations Number of Monte-Carlo replicates.
#' @param conversion_floor cliff graduate-to-practice conversion (0.70-1.0).
#' @param retirement_schedule Single-year retirement hazard schedule; scenarios
#'   supply a SHIFTED schedule (retire +/- n years) rather than a multiplier.
#' @param hazard_table Optional legacy age-band hazard table. Supplying it
#'   switches back to the coarse seven-band gradient.
#' @param fte_method FTE method (see [provider_clinical_fte()]).
#' @param hours_model Optional fitted hours model.
#' @param hours_multiplier Scenario knob on hours worked.
#' @param hours_intercept Hours-schedule intercept; use
#'   [calibrate_hours_intercept()] so the schedule and the FTE threshold agree.
#' @param late_career_fte_factor Multiplier on clinical FTE from
#'   `late_career_fte_onset_age` (mufflyaccess scenario field).
#' @param late_career_fte_onset_age Age from which the factor applies.
#' @param placement_shares Optional geographic share table enabling entrant
#'   placement and mid-career migration.
#' @param param_spec Optional [supply_parameter_spec()]. When supplied, the
#'   entrant rate (and any other quantified parameter) is REDRAWN each
#'   iteration, so the reported intervals carry forecast uncertainty rather than
#'   Monte Carlo sampling noise alone. Without it the intervals are far too
#'   narrow -- see docs/BACKTEST_2020_TO_2023.md.
#' @param ci Width of the reported credible band (default 0.95).
#' @param seed Integer RNG seed.
#' @param verbose Logical.
#' @return List: `summary` (per-year quantiles), `iterations` (all replicate
#'   panels), and `scenario` metadata.
#' @export
run_supply_microsimulation <- function(initial_workforce = 1306,
                                        years = 2025:2050,
                                        entrants_per_year = 55,
                                        subspecialty = "FPMRS",
                                        n_iterations = 500,
                                        conversion_floor = 1.0,
                                        retirement_schedule = RETIREMENT_HAZARD_BY_AGE,
                                        hazard_table = NULL,
                                        fte_method = "hours",
                                        hours_model = NULL,
                                        hours_multiplier = 1.0,
                                        hours_intercept = HWSM_HOURS_INTERCEPT,
                                        late_career_fte_factor = 1.0,
                                        late_career_fte_onset_age = NA_real_,
                                        placement_shares = NULL,
                                        param_spec = NULL,
                                        ci = 0.95,
                                        seed = 20260801L,
                                        verbose = TRUE) {
  seed_microsimulation(seed)

  baseline_rate <- microsim_baseline_rate(subspecialty)

  if (verbose) {
    .msg_info(sprintf("Supply microsimulation: %s, %d iterations, %d-%d",
                      subspecialty, n_iterations, min(years), max(years)))
    .msg_info(sprintf("Entrants %s/yr; conversion %.2f; FTE method '%s'",
                      format(entrants_per_year), conversion_floor, fte_method))
  }

  # Build the starting cohort ONCE when a size was supplied, so replicate-to-
  # replicate variation comes from the simulated career decisions rather than
  # from redrawing the base-year age distribution every iteration.
  base_agents <- if (is.data.frame(initial_workforce)) {
    initial_workforce
  } else {
    initialize_provider_agents(initial_workforce, subspecialty, min(years))
  }

  if (!is.null(param_spec)) assert_parameter_uncertainty(param_spec)

  iteration_panels <- vector("list", n_iterations)
  for (it in seq_len(n_iterations)) {
    if (verbose && it %% 100 == 0) .msg_info(sprintf("  iteration %d/%d", it, n_iterations))

    # Draw the parameters for THIS iteration before simulating individuals, so
    # the replicate spread reflects both sources of variation.
    it_entrants <- entrants_per_year
    it_schedule <- retirement_schedule
    if (!is.null(param_spec)) {
      d <- draw_supply_parameters(param_spec, retirement_schedule)
      it_entrants <- d$entrants
      it_schedule <- d$retirement_schedule
    }

    sim <- simulate_provider_career_once(
      base_agents, years, it_entrants,
      hazard_table = hazard_table,
      conversion_floor = conversion_floor,
      subspecialty = subspecialty,
      retirement_schedule = it_schedule,
      fte_method = fte_method,
      hours_model = hours_model,
      hours_multiplier = hours_multiplier,
      hours_intercept = hours_intercept,
      late_career_fte_factor = late_career_fte_factor,
      late_career_fte_onset_age = late_career_fte_onset_age,
      placement_shares = placement_shares
    )
    iteration_panels[[it]] <- dplyr::mutate(sim$panel, iteration = it)
  }

  all_panels <- dplyr::bind_rows(iteration_panels)
  lo <- (1 - ci) / 2
  hi <- 1 - lo

  summary <- all_panels %>%
    dplyr::group_by(.data$year, .data$subspecialty) %>%
    dplyr::summarise(
      headcount_median = stats::median(.data$headcount),
      headcount_lo = stats::quantile(.data$headcount, lo),
      headcount_hi = stats::quantile(.data$headcount, hi),
      effective_fte_median = stats::median(.data$effective_fte),
      effective_fte_lo = stats::quantile(.data$effective_fte, lo),
      effective_fte_hi = stats::quantile(.data$effective_fte, hi),
      mean_age_median = stats::median(.data$mean_age, na.rm = TRUE),
      .groups = "drop"
    )

  list(
    summary = summary,
    iterations = all_panels,
    scenario = list(
      subspecialty = subspecialty,
      initial_workforce = if (is.data.frame(initial_workforce)) nrow(initial_workforce) else initial_workforce,
      entrants_per_year = entrants_per_year,
      conversion_floor = conversion_floor,
      n_iterations = n_iterations,
      baseline_rate = baseline_rate,
      parameter_uncertainty = if (is.null(param_spec)) "none (intervals are sampling noise only)" else
        paste(names(param_spec$quantified)[param_spec$quantified], collapse = ", "),
      implied_departure_rate = implied_annual_departure_rate(
        base_agents$age,
        if ("sex" %in% names(base_agents)) base_agents$sex else "female",
        retirement_schedule = retirement_schedule
      ),
      fte_method = fte_method,
      hours_multiplier = hours_multiplier,
      ci = ci,
      seed = seed
    )
  )
}

# ---- Deterministic mean-field backbone (cliff wc_project analog) ----------

#' Deterministic age-structured projection (validation backbone)
#'
#' A faithful port of cliff::wc_project — expected departures (count * hazard)
#' rather than Bernoulli draws — used to validate the stochastic microsimulation
#' against its analytic expectation (they should agree to within a few percent;
#' cliff code/05 reports 4.51% mean agreement).
#'
#' @param agents Starting agent tibble.
#' @param years Integer years to project.
#' @param entrants_per_year Annual entrants (BEFORE the conversion haircut).
#' @param hazard_table Optional age-band hazard table; when NULL the single-year
#'   [departure_hazard()] schedule is used, matching the stochastic engine.
#' @param conversion_floor Graduate-to-practice conversion. Previously ABSENT
#'   from this function while the stochastic engine applied it, so the two
#'   disagreed by 37% at 2050 whenever conversion was not 1.0 -- read as model
#'   disagreement when it was a missing argument.
#' @param retirement_schedule Single-year retirement hazard schedule.
#' @param fte_method FTE method (see [provider_clinical_fte()]).
#' @param hours_model Optional fitted hours model.
#' @param hours_intercept Intercept for the reference hours schedule.
#' @param sex Sex mix to evaluate the hazard and hours schedules against.
#' @return Per-year tibble with expected headcount and effective FTE.
#' @export
project_supply_deterministic <- function(agents, years, entrants_per_year,
                                         hazard_table = NULL,
                                         conversion_floor = 1.0,
                                         retirement_schedule = RETIREMENT_HAZARD_BY_AGE,
                                         fte_method = "hours",
                                         hours_model = NULL,
                                         hours_intercept = HWSM_HOURS_INTERCEPT,
                                         sex = "female") {
  years <- sort(unique(as.integer(years)))
  base_year <- min(years)

  # Represent the cohort as (age -> expected count) so departures are fractional.
  active0 <- provider_active_in_year(agents, base_year)
  ages <- agents$age[active0]
  sexes <- if ("sex" %in% names(agents)) agents$sex[active0] else rep(sex, length(ages))
  count <- rep(1, length(ages))

  prod_norm <- 1
  if (identical(fte_method, "legacy_weight")) {
    base_w <- productivity_weight_raw(ages)
    prod_norm <- if (length(base_w) > 0 && mean(base_w) > 0) 1 / mean(base_w) else 1
  }
  fte_of <- function(a, s) {
    provider_clinical_fte(a, s, method = fte_method, hours_model = hours_model,
                          legacy_norm = prod_norm, hours_intercept = hours_intercept)
  }

  effective_entrants <- entrants_per_year * conversion_floor

  out <- vector("list", length(years))
  for (i in seq_along(years)) {
    hz <- if (is.null(hazard_table)) {
      departure_hazard(ages, sexes, retirement_schedule = retirement_schedule)
    } else {
      retirement_hazard(ages, hazard_table)
    }
    survivors <- count * (1 - hz)

    out[[i]] <- tibble::tibble(
      year = years[i],
      headcount = sum(count),
      effective_fte = sum(count * fte_of(ages, sexes))
    )

    # Age and inject entrants for next year.
    ages <- ages + 1
    count <- survivors
    ages <- c(ages, MICROSIM_ENTRY_AGE)
    sexes <- c(sexes, sex)
    count <- c(count, effective_entrants)
  }
  dplyr::bind_rows(out)
}

# ---- Workforce-outlook classification (cliff) -----------------------------

#' Classify workforce outlook from a replacement ratio
#'
#' Replacement ratio = annual entrants / annual departures. cliff cutpoints:
#' Adequate (>= 1.2), Marginal (0.8-1.2), Insufficient (< 0.8).
#'
#' @param ratio Numeric replacement ratio(s).
#' @return Character classification(s).
#' @export
classify_workforce_outlook <- function(ratio) {
  dplyr::case_when(
    is.na(ratio) ~ NA_character_,
    ratio >= WORKFORCE_OUTLOOK_ADEQUATE_MIN ~ "Adequate",
    ratio >= WORKFORCE_OUTLOOK_MARGINAL_MIN ~ "Marginal",
    TRUE ~ "Insufficient"
  )
}
