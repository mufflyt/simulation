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
    logger::log_warn("No baseline rate for subspecialty '{subspecialty}'; using reference {MICROSIM_REFERENCE_BASELINE_RATE}")
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
#' @return Numeric hazard(s) in [0, 1].
#' @export
retirement_hazard <- function(age, hazard_table) {
  bands <- microsim_age_band_of(age)
  h <- hazard_table[bands]
  h[is.na(h)] <- max(hazard_table)  # ages beyond the table take the max hazard
  unname(pmin(h, 1))
}

#' Age-productivity weight (cliff effective-FTE concept)
#'
#' Older surgeons operate less; weight each provider-year by relative
#' productivity. Raw weights are normalised elsewhere so the base-year active
#' cohort averages 1.0 (effective(base) == headcount(base)).
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
#' @return List with `panel` (per-year summary tibble) and `agents` (final agent
#'   table incl. drawn retirement years) so the temporal cohort is reconstructible.
#' @export
simulate_provider_career_once <- function(agents,
                                          years,
                                          entrants_per_year,
                                          hazard_table,
                                          conversion_floor = 1.0,
                                          subspecialty = "FPMRS") {
  years <- sort(unique(as.integer(years)))
  base_year <- min(years)

  # Normalise productivity so the base-year active cohort averages 1.0.
  base_active <- provider_active_in_year(agents, base_year)
  raw_w <- productivity_weight_raw(agents$age[base_active])
  prod_norm <- if (length(raw_w) > 0 && mean(raw_w) > 0) 1 / mean(raw_w) else 1

  effective_entrants <- entrants_per_year * conversion_floor
  panel_rows <- vector("list", length(years))
  next_entrant_seq <- 1L

  for (i in seq_along(years)) {
    year <- years[i]

    # --- Departures: Bernoulli draw per active agent ---
    active_mask <- provider_active_in_year(agents, year)
    if (any(active_mask)) {
      hz <- retirement_hazard(agents$age[active_mask], hazard_table)
      draws <- stats::runif(sum(active_mask)) < hz
      # retirement_year = first-inactive = this year + 1 (last active = year).
      idx_active <- which(active_mask)
      agents$retirement_year[idx_active[draws]] <- year + 1L
    }

    # --- Record end-of-year state BEFORE injecting next year's entrants ---
    active_now <- provider_active_in_year(agents, year)
    active_ages <- agents$age[active_now]
    eff_fte <- sum(productivity_weight_raw(active_ages) * prod_norm)
    panel_rows[[i]] <- tibble::tibble(
      year = year,
      subspecialty = subspecialty,
      headcount = sum(active_now),
      effective_fte = eff_fte,
      mean_age = if (length(active_ages) > 0) mean(active_ages) else NA_real_
    )

    # --- Age everyone by one year (survivors and retirees alike) ---
    agents$age <- agents$age + 1

    # --- Inject entrants for the next year (stochastic fractional part) ---
    n_new <- floor(effective_entrants) +
      as.integer(stats::runif(1) < (effective_entrants - floor(effective_entrants)))
    if (n_new > 0) {
      new_agents <- tibble::tibble(
        provider_id = sprintf("E%d_%06d", year, seq.int(next_entrant_seq, length.out = n_new)),
        subspecialty = subspecialty,
        age = MICROSIM_ENTRY_AGE,
        entry_year = year + 1L,
        retirement_year = NA_real_,
        origin_cohort = "entrant"
      )
      agents <- dplyr::bind_rows(agents, new_agents)
      next_entrant_seq <- next_entrant_seq + n_new
    }
  }

  list(
    panel = dplyr::bind_rows(panel_rows),
    agents = agents
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
#' @param ci Width of the reported credible band (default 0.95).
#' @param seed Integer RNG seed.
#' @param verbose Logical.
#' @return List: `summary` (per-year quantiles), `iterations` (all replicate
#'   panels), and `scenario` metadata.
#' @export
run_supply_microsimulation <- function(initial_workforce = 1169,
                                        years = 2025:2050,
                                        entrants_per_year = 55,
                                        subspecialty = "FPMRS",
                                        n_iterations = 500,
                                        conversion_floor = 1.0,
                                        ci = 0.95,
                                        seed = 20260801L,
                                        verbose = TRUE) {
  seed_microsimulation(seed)

  baseline_rate <- microsim_baseline_rate(subspecialty)
  hazard_table <- build_hazard_table(baseline_rate)

  if (verbose) {
    logger::log_info("Supply microsimulation: {subspecialty}, {n_iterations} iterations, {min(years)}-{max(years)}")
    logger::log_info("Baseline hazard {round(100*baseline_rate,1)}%/yr; entrants {entrants_per_year}/yr; conversion {conversion_floor}")
  }

  iteration_panels <- vector("list", n_iterations)
  for (it in seq_len(n_iterations)) {
    if (verbose && it %% 100 == 0) logger::log_info("  iteration {it}/{n_iterations}")
    agents <- if (is.data.frame(initial_workforce)) {
      initial_workforce
    } else {
      initialize_provider_agents(initial_workforce, subspecialty, min(years))
    }
    sim <- simulate_provider_career_once(
      agents, years, entrants_per_year, hazard_table, conversion_floor, subspecialty
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
#' @param entrants_per_year Annual entrants.
#' @param hazard_table Age-band hazard table.
#' @return Per-year tibble with expected headcount and effective FTE.
#' @export
project_supply_deterministic <- function(agents, years, entrants_per_year, hazard_table) {
  years <- sort(unique(as.integer(years)))
  base_year <- min(years)

  # Represent the cohort as (age -> expected count) so departures are fractional.
  active0 <- provider_active_in_year(agents, base_year)
  ages <- agents$age[active0]
  count <- rep(1, length(ages))

  base_w <- productivity_weight_raw(ages)
  prod_norm <- if (length(base_w) > 0 && mean(base_w) > 0) 1 / mean(base_w) else 1

  out <- vector("list", length(years))
  for (i in seq_along(years)) {
    hz <- retirement_hazard(ages, hazard_table)
    survivors <- count * (1 - hz)

    out[[i]] <- tibble::tibble(
      year = years[i],
      headcount = sum(count),
      effective_fte = sum(count * productivity_weight_raw(ages) * prod_norm)
    )

    # Age and inject entrants for next year.
    ages <- ages + 1
    count <- survivors
    ages <- c(ages, MICROSIM_ENTRY_AGE)
    count <- c(count, entrants_per_year)
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
