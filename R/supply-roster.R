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
# contract. `agents_from_roster()` (R/supply-provider_lifecycle) remains the production path.
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
#' WHAT `n_certified` IS. An annual FLOW of newly certified subspecialists, not
#' a stock and not a net change. The stock series is its cumulative sum: summing
#' 2013-2020 reproduces the contract's 2020 count of 1,099 exactly, and the
#' 2020->2023 stock change (1,306 - 1,099 = 207) equals the 2021-2023 flows
#' (81 + 54 + 72 = 207) to the person. Nothing is ever removed. This matches
#' `validate_backtest_target()`, which reports
#' `observed_series_applies_attrition = FALSE` for the same contract.
#'
#' The consequence matters for anything downstream: because no departures are
#' netted out, the mean of this flow IS the gross entrant rate. Adding modelled
#' departures on top double-counts them -- see [implied_gross_entrants()].
#'
#' @param from_year First year of the steady-state window.
#' @param geography,board_pathway Contract dimensions.
#' @return List with `mean_gross_entrants`, `window`, and the cohort table.
#'   `mean_net_growth` is retained as a deprecated alias of the same value.
#' @export
observed_entrant_rate <- function(from_year = 2018L,
                                  geography = "national",
                                  board_pathway = "ABOG_PLUS_ABU") {
  coh <- urps_certification_cohorts(geography, board_pathway)
  steady <- coh[coh$cert_year >= from_year, ]
  if (!nrow(steady)) stop("observed_entrant_rate: no years in the window", call. = FALSE)

  mean_flow <- mean(steady$n_certified)

  list(
    mean_gross_entrants = mean_flow,
    # Deprecated alias. The name was wrong: this was never a net quantity, and
    # callers that read it as one added departures to it a second time.
    mean_net_growth = mean_flow,
    sd_gross_entrants = if (nrow(steady) > 1) stats::sd(steady$n_certified) else NA_real_,
    series_applies_attrition = FALSE,
    window = range(steady$cert_year),
    n_years = nrow(steady),
    cohorts = coh,
    note = paste("Mean annual GROSS entrants: new certifications per year. The",
                 "source series never removes departures, so this is already a",
                 "gross flow -- do not add modelled departures to it.")
  )
}

#' Gross annual entrants implied by the observed certification series
#'
#' Reported alongside the assumed value so a divergence is visible rather than
#' buried.
#'
#' THE DEPARTURE BALANCE DEPENDS ON THE SOURCE SERIES. The identity
#' `gross = net growth + departures` holds only when the source series actually
#' nets departures out. The URPS certification series does not: it is a flow of
#' new certifications that nothing is ever subtracted from (see
#' [observed_entrant_rate()], and `observed_series_applies_attrition = FALSE` in
#' [validate_backtest_target()]). For that series the observed flow IS the gross
#' entrant rate, and adding modelled departures inflates it -- previously from
#' 51/yr to 87/yr, a spurious 37% divergence from the shipped assumption that
#' was reported to users on every run.
#'
#' `series_applies_attrition` therefore selects the correct balance rather than
#' hardcoding one, so a future stock-based series can be passed without
#' reintroducing the error.
#'
#' @param agents Base-year agent cohort (supplies the age structure).
#' @param from_year First year of the steady-state window.
#' @param assumed Value the model would otherwise use, for comparison.
#' @param series_applies_attrition Whether the source series removes departures.
#'   `NULL` (default) reads the flag from [observed_entrant_rate()].
#' @param ... Passed to [observed_entrant_rate()].
#' @return List with `gross_entrants`, `observed_flow`, `departures`, `assumed`.
#' @export
implied_gross_entrants <- function(agents, from_year = 2018L, assumed = NA_real_,
                                   series_applies_attrition = NULL, ...) {
  obs <- observed_entrant_rate(from_year = from_year, ...)
  rate <- implied_annual_departure_rate(
    agents$age, if ("sex" %in% names(agents)) agents$sex else "female"
  )
  departures <- nrow(agents) * rate

  nets_out <- series_applies_attrition %||% obs$series_applies_attrition
  gross <- if (isTRUE(nets_out)) obs$mean_gross_entrants + departures else obs$mean_gross_entrants

  if (is.finite(assumed) && abs(gross - assumed) / gross > 0.15) {
    # Name the window. The certification flow spans a structural break -- URPS
    # fellowship expansion, and a COVID examination disruption that pushed the
    # 2020 cohort (n = 10) into 2021 (n = 81) -- so a 2018-start window and a
    # recent window give materially different answers for reasons that have
    # nothing to do with the entrant rate. A bare percentage invites the reader
    # to treat a windowing artifact as a modelling error.
    .msg_warn(sprintf(
      paste("Entrants in use (%s/yr) differ by %.0f%% from the certification flow",
            "over %d-%d (%s/yr). These are different observed series, not a right",
            "and a wrong one: NRMP counts filled fellowship positions, the",
            "contract counts board certifications ~2 years later. Check the window",
            "before treating the gap as an error -- 2020-21 is an exam-scheduling",
            "artifact, and 2021-23 averages %s/yr."),
      format(round(assumed)), 100 * abs(gross - assumed) / gross,
      obs$window[1], obs$window[2], format(round(gross, 1)),
      format(round(mean(utils::tail(
        obs$cohorts$n_certified[obs$cohorts$cert_year >= obs$window[1]], 3)), 1))))
  }

  list(
    gross_entrants = gross,
    observed_flow = obs$mean_gross_entrants,
    # Deprecated alias retained for callers written against the old field name.
    net_growth = obs$mean_gross_entrants,
    departures = departures,
    departure_rate = rate,
    departures_added = isTRUE(nets_out),
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
