# Entrant Trajectories ----
#
# Supply grows 62% over 2025-2050 in the status-quo projection, against demand
# growth of 15%. The surplus is therefore a SUPPLY story, and the whole of the
# supply story rests on one number held constant for twenty-five years: NRMP
# filled fellowship positions in 2025.
#
# Fellowship output is a policy variable, and it has moved. The scenario
# registry could only shift its LEVEL (+/-10%), which asks "what if output had
# always been 10% higher" rather than "what if it grows". Those are different
# questions and only the second one is about policy.
#
# WHAT THE NRMP SERIES ACTUALLY SHOWS (appointment years 2017-2025):
#
#   positions offered   64, 60, 64, 65, ... 70      CAGR +1.13%/yr
#   positions filled    59, 59, 58, 56, ... 70      CAGR +2.16%/yr
#   fill rate           92%, 98%, 91%, 86%, ... 100%
#
# The filled series grew nearly twice as fast as the offered series, and the
# difference is entirely fill-rate catch-up: 86-92% to 100%. THAT SOURCE OF
# GROWTH IS NOW EXHAUSTED -- a fill rate cannot exceed 1 -- so extrapolating the
# filled CAGR forward double-counts a one-off. The sustainable ceiling on
# entrant growth is the rate at which PROGRAMS AND POSITIONS expand, +1.13%/yr,
# and even that assumes the recent expansion continues.
#
# This module exists so that reasoning is in code rather than in a footnote.

#' Compound annual growth rate between the endpoints of a series
#'
#' @param first,last Endpoint values.
#' @param years Number of years between them.
#' @return Numeric annual growth rate.
#' @export
compound_growth_rate <- function(first, last, years) {
  if (!is.finite(first) || !is.finite(last) || first <= 0 || years <= 0) return(NA_real_)
  (last / first)^(1 / years) - 1
}

#' Growth rates implied by the NRMP fellowship series
#'
#' @param series Tibble from [nrmp_entrant_series()].
#' @return List with `offered`, `filled`, `fill_rate_first`, `fill_rate_last`,
#'   `sustainable` and `headroom_exhausted`.
#' @export
nrmp_growth_rates <- function(series = nrmp_entrant_series(), from = NULL) {
  s <- series[order(series$appointment_year), , drop = FALSE]
  # ESTIMATE ON THE PLATEAU, NOT THE ESTABLISHMENT RAMP. Extending the series
  # back to 2010 exposed a structural break: filled positions grew 30 -> 50
  # across 2010-2014 while FPMRS was becoming a subspecialty and programs were
  # still being accredited (~13.6%/yr), then went flat at 57.0 (sd 2.3) across
  # 2015-2020. A first-to-last CAGR over the whole series returns ~4.9%/yr,
  # which is an artifact of averaging a one-off ramp with a plateau and would
  # project establishment-era growth forward for twenty-five years.
  from <- from %||% NRMP_PLATEAU_FROM
  s <- s[s$appointment_year >= from, , drop = FALSE]
  if (nrow(s) < 2L) {
    stop("nrmp_growth_rates(): fewer than two observations at or after ", from,
         call. = FALSE)
  }
  n <- nrow(s)
  span <- s$appointment_year[n] - s$appointment_year[1]
  fr_first <- s$positions_filled[1] / s$positions_offered[1]
  fr_last <- s$positions_filled[n] / s$positions_offered[n]

  list(
    offered = compound_growth_rate(s$positions_offered[1], s$positions_offered[n], span),
    filled = compound_growth_rate(s$positions_filled[1], s$positions_filled[n], span),
    fill_rate_first = fr_first,
    fill_rate_last = fr_last,
    span_years = span,
    # The sustainable rate is the POSITIONS rate. Filled cannot outgrow offered
    # once the fill rate is at its ceiling, and it effectively is.
    sustainable = compound_growth_rate(s$positions_offered[1], s$positions_offered[n], span),
    headroom_exhausted = fr_last >= 0.995,
    estimated_from = from
  )
}

#' Build an annual entrant trajectory
#'
#' @param base Entrants in the first projected year.
#' @param years Projection years.
#' @param growth Annual growth rate (0 = flat).
#' @param cap Optional upper bound on annual entrants.
#' @return Numeric vector, one entrant count per year in `years`.
#' @export
entrant_trajectory <- function(base, years, growth = 0, cap = NULL) {
  assertthat::assert_that(is.numeric(base), length(base) == 1L, is.finite(base), base >= 0)
  assertthat::assert_that(is.numeric(growth), length(growth) == 1L, is.finite(growth))
  n <- length(years)
  out <- base * (1 + growth)^(seq_len(n) - 1L)
  if (!is.null(cap)) out <- pmin(out, cap)
  pmax(out, 0)
}

#' Prespecified entrant-trajectory scenarios, grounded in the NRMP series
#'
#' Every rate is derived from the observed series rather than chosen: the
#' expansion and contraction arms use the POSITIONS-OFFERED growth rate and its
#' negation, because that is the rate at which training capacity has actually
#' moved. `filled_naive` is included precisely so its double-count is visible --
#' it extrapolates a filled-position growth rate that was partly fill-rate
#' catch-up, and the catch-up is spent.
#'
#' @param base Entrants in the first projected year; defaults to the observed
#'   NRMP value.
#' @param years Projection years.
#' @param rates Optional [nrmp_growth_rates()] output.
#' @return Named list of numeric trajectories.
#' @export
entrant_trajectory_scenarios <- function(base = NULL, years = 2025:2050,
                                         rates = nrmp_growth_rates()) {
  if (is.null(base)) {
    s <- nrmp_entrant_series()
    base <- s$positions_filled[which.max(s$appointment_year)]
  }
  list(
    flat = entrant_trajectory(base, years, 0),
    expansion_sustainable = entrant_trajectory(base, years, rates$sustainable),
    contraction = entrant_trajectory(base, years, -rates$sustainable),
    filled_naive = entrant_trajectory(base, years, rates$filled)
  )
}

# Labels, kept beside the trajectories so a plot or table cannot show the naive
# arm without its warning.
ENTRANT_TRAJECTORY_LABELS <- c(
  flat = "Flat at the observed NRMP level (status quo assumption)",
  expansion_sustainable = "Training capacity keeps expanding at its observed rate",
  contraction = "Training capacity contracts at the same rate",
  filled_naive = "NAIVE: extrapolates filled-position growth (double-counts spent fill-rate headroom)"
)
