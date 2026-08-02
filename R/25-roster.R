# Base-Year Provider Cohort ----
#
# The starting cohort was a normal draw: `round(rnorm(n, mean = 52, sd = 9))`.
# That is a placeholder, and it conflates two populations with completely
# different age structures.
#
# WHAT THE CONTRACT ACTUALLY PROVIDES
#
# `mufflyaccess::urps_counts_long()` gives aggregate active counts by year,
# geography and board pathway for 2013-2025. It does NOT provide age, sex,
# state, or individual records; `n_retired` is 0 in every row and `n_active`
# equals `n_ever_certified`, so the bundled artifact is a CUMULATIVE
# CERTIFICATION SERIES rather than an active roster net of attrition. An
# external artifact hook exists (`use_urps_artifact(dir)`) but the shipped
# source is `bundled_bootstrap`, with `canonical_release = FALSE`.
#
# A genuine individual-level roster therefore still has to come from outside the
# contract. `agents_from_roster()` (R/16) remains the production path.
#
# WHAT CAN BE DERIVED ANYWAY
#
# URPS subspecialty certification began in 2013, and the contract's
# active-in-year definition keys on the subspecialty certification year. The
# year-over-year differences are therefore certification cohort sizes:
#
#   2014 +175   2015 +102   2016  +36   2017  +33   2018  +40
#   2019  +48   2020  +10   2021  +81   2022  +54   2023  +72
#
# Two populations sit inside the 2023 total of 1,306:
#
#   * 651 (49.8%) certified 2014-2023. These are FELLOWSHIP GRADUATES entering
#     at a narrow age around the entry age, and their age in any later year is
#     known from the cohort year. OBSERVED.
#
#   * 655 (50.2%) certified in or before 2013. This is the initial
#     backlog-clearance cohort: established practitioners of many ages who sat
#     the first examinations. Certification year says nothing about career stage
#     for this group, so their age distribution is the one genuine assumption
#     in the base cohort. ASSUMED.
#
# Modelling those two groups with one normal draw is wrong in a specific way: it
# gives the fellowship-graduate half an implausibly wide age spread and the
# backlog half an implausibly narrow one. This module separates them and reports
# what share of the cohort is observed.

URPS_FIRST_CERTIFICATION_YEAR <- 2013L

#' Certification cohort sizes from the mufflyaccess contract
#'
#' @param geography "national" or "conus".
#' @param board_pathway One of ABOG, ABU_NET_NEW, ABOG_PLUS_ABU.
#' @return Tibble of `cert_year`, `n_certified`, `basis`.
#' @export
urps_certification_cohorts <- function(geography = "national",
                                       board_pathway = "ABOG_PLUS_ABU") {
  .require_mufflyaccess("The URPS certification series")
  x <- mufflyaccess::urps_counts_long()
  a <- x[x$measure == "board_certified_active" &
           x$geography == geography &
           x$board_pathway == board_pathway, c("year", "n_active")]
  a <- a[order(a$year), ]
  if (nrow(a) < 2) stop("urps_certification_cohorts: series too short", call. = FALSE)

  tibble::tibble(
    cert_year = c(a$year[1], a$year[-1]),
    n_certified = c(a$n_active[1], diff(a$n_active)),
    basis = c("initial backlog (cert year uninformative about age)",
              rep("fellowship graduate cohort", nrow(a) - 1L))
  )
}

#' Entrant rate implied by the observed certification series
#'
#' The model previously carried a hardcoded `baseline_entrants = 55`. The
#' observed series implies a materially different number, and it is derivable.
#'
#' The early years are excluded by default: net growth averaged 86.5/yr over
#' 2014-2017 while the initial certification backlog was cleared, against 53/yr
#' from 2018. Fitting on the ramp-up would badly over-project.
#'
#' @param from_year First year of the steady-state window.
#' @param geography,board_pathway Contract dimensions.
#' @return List with `mean_net_growth`, `window`, and the cohort table.
#' @export
observed_entrant_rate <- function(from_year = 2018L,
                                  geography = "national",
                                  board_pathway = "ABOG_PLUS_ABU") {
  coh <- urps_certification_cohorts(geography, board_pathway)
  steady <- coh[coh$cert_year >= from_year, ]
  if (!nrow(steady)) stop("observed_entrant_rate: no years in the window", call. = FALSE)

  list(
    mean_net_growth = mean(steady$n_certified),
    window = range(steady$cert_year),
    n_years = nrow(steady),
    cohorts = coh,
    note = paste("Net growth in new certifications. Gross entrants are this plus",
                 "departures; use implied_annual_departure_rate() on the base",
                 "cohort to complete the balance.")
  )
}

#' Gross annual entrants implied by observed growth and modelled attrition
#'
#' entrants = observed net growth + expected departures. Reported alongside the
#' assumed value so a divergence is visible rather than buried.
#'
#' @param agents Base-year agent cohort (supplies the age structure).
#' @param from_year First year of the steady-state window.
#' @param assumed Value the model would otherwise use, for comparison.
#' @param ... Passed to [observed_entrant_rate()].
#' @return List with `gross_entrants`, `net_growth`, `departures`, `assumed`.
#' @export
implied_gross_entrants <- function(agents, from_year = 2018L, assumed = NA_real_, ...) {
  obs <- observed_entrant_rate(from_year = from_year, ...)
  rate <- implied_annual_departure_rate(
    agents$age, if ("sex" %in% names(agents)) agents$sex else "female"
  )
  departures <- nrow(agents) * rate
  gross <- obs$mean_net_growth + departures

  if (is.finite(assumed) && abs(gross - assumed) / gross > 0.15) {
    .msg_warn(sprintf(
      "Assumed entrants (%s/yr) differ from the observed-series implication (%s/yr) by %.0f%%. The observed value is derivable from mufflyaccess::urps_counts_long(); prefer it.",
      format(round(assumed)), format(round(gross)),
      100 * abs(gross - assumed) / gross))
  }

  list(
    gross_entrants = gross,
    net_growth = obs$mean_net_growth,
    departures = departures,
    departure_rate = rate,
    assumed = assumed,
    window = obs$window
  )
}

# ---- Cohort construction ---------------------------------------------------

# Age distribution assumed for the 2013 backlog-clearance cohort: established
# practitioners who sat the first examinations, so broad and centred later than
# a fellowship-graduate cohort. THIS IS THE ASSUMPTION in the base cohort and is
# reported as such by `cohort_composition()`.
BACKLOG_COHORT_AGE_MEAN_AT_CERT <- 45
BACKLOG_COHORT_AGE_SD_AT_CERT <- 8

#' Build a base-year cohort from the observed certification series
#'
#' Fellowship-graduate cohorts (2014 onward) get an age derived from their
#' certification year, with only the small spread of fellowship completion age.
#' The 2013 backlog cohort gets the assumed distribution above. The result
#' carries `cohort_source` per agent so the observed and assumed halves stay
#' distinguishable downstream.
#'
#' @param baseline_year Calendar year of the base cohort.
#' @param geography,board_pathway Contract dimensions.
#' @param entry_age Age at entry to practice for fellowship graduates.
#' @param entry_age_sd Spread of fellowship completion age.
#' @param female_share Share of the cohort drawn female.
#' @param subspecialty Subspecialty label.
#' @return Agent tibble compatible with [simulate_provider_career_once()].
#' @export
agents_from_certification_cohorts <- function(baseline_year = 2023L,
                                              geography = "national",
                                              board_pathway = "ABOG_PLUS_ABU",
                                              entry_age = MICROSIM_ENTRY_AGE,
                                              entry_age_sd = 2.5,
                                              female_share = 0.55,
                                              subspecialty = "URPS") {
  coh <- urps_certification_cohorts(geography, board_pathway)
  coh <- coh[coh$cert_year <= baseline_year & coh$n_certified > 0, ]

  parts <- lapply(seq_len(nrow(coh)), function(i) {
    yr <- coh$cert_year[i]
    n <- coh$n_certified[i]
    backlog <- yr <= URPS_FIRST_CERTIFICATION_YEAR

    age_at_cert <- if (backlog) {
      stats::rnorm(n, BACKLOG_COHORT_AGE_MEAN_AT_CERT, BACKLOG_COHORT_AGE_SD_AT_CERT)
    } else {
      stats::rnorm(n, entry_age, entry_age_sd)
    }
    age_now <- age_at_cert + (baseline_year - yr)

    tibble::tibble(
      provider_id = sprintf("C%d_%05d", yr, seq_len(n)),
      subspecialty = subspecialty,
      sex = ifelse(stats::runif(n) < female_share, "female", "male"),
      age = pmin(pmax(round(age_now), entry_age), MICROSIM_TERMINAL_AGE - 1L),
      entry_year = yr,
      retirement_year = NA_real_,
      origin_cohort = if (backlog) "backlog_2013" else "fellowship_cohort",
      cert_year = yr,
      cohort_source = if (backlog) "assumed" else "observed"
    )
  })

  out <- dplyr::bind_rows(parts)
  .msg_info(sprintf(
    "Base cohort from certification series: %d providers, %.1f%% with an observed certification year.",
    nrow(out), 100 * mean(out$cohort_source == "observed")))
  out
}

#' Composition of a base-year cohort: what is observed, what is assumed
#'
#' @param agents Agent tibble.
#' @return Tibble summarising each cohort source.
#' @export
cohort_composition <- function(agents) {
  if (!"cohort_source" %in% names(agents)) {
    return(tibble::tibble(cohort_source = "synthetic", n = nrow(agents),
                          share = 1, mean_age = mean(agents$age),
                          note = "Fully synthetic cohort; no observed component."))
  }
  agents %>%
    dplyr::group_by(.data$cohort_source) %>%
    dplyr::summarise(
      n = dplyr::n(),
      share = dplyr::n() / nrow(agents),
      mean_age = mean(.data$age),
      min_age = min(.data$age),
      max_age = max(.data$age),
      .groups = "drop"
    )
}

#' Provenance of the base-year cohort
#'
#' Records which construction path produced the cohort, so a published number
#' can never be mistaken for one built on a real roster.
#'
#' @param agents Agent tibble.
#' @return List with `source`, `is_production`, and a note.
#' @export
cohort_provenance <- function(agents) {
  # Check the roster marker FIRST: agents_from_roster() stamps origin_cohort but
  # carries no cohort_source column, so gating on cohort_source would mislabel a
  # production roster as unknown.
  src <- if (any(agents$origin_cohort == "roster", na.rm = TRUE)) {
    "roster"
  } else if ("cohort_source" %in% names(agents)) {
    "certification_cohorts"
  } else if (all(agents$origin_cohort %in% c("baseline", "entrant"), na.rm = TRUE)) {
    "synthetic"
  } else "unknown"

  list(
    source = src,
    is_production = identical(src, "roster"),
    observed_share = if ("cohort_source" %in% names(agents)) {
      mean(agents$cohort_source == "observed")
    } else 0,
    note = switch(
      src,
      roster = "Individual-level provider roster; production path.",
      certification_cohorts = paste(
        "Derived from mufflyaccess::urps_counts_long() certification cohorts.",
        "Age is observed for fellowship cohorts (2014+) and ASSUMED for the",
        "2013 backlog cohort. Better than a synthetic draw, but not a roster:",
        "the contract ships aggregate counts only, with no age, sex or state."),
      synthetic = "Fully synthetic normal draw. Examples and tests only.",
      "Unrecognised cohort construction."
    )
  )
}
