# ACGME URPS fellows: the entry count NRMP cannot see ----
#
# WHY THIS SOURCE EXISTS ALONGSIDE NRMP, AND WHAT IT DID NOT FIX. The conversion
# calibrated against NRMP came out at 0.75, and successive windows showed it
# rising past 1.0 for 2021-2023 -- more certifying than matched three years
# earlier. The obvious reading was that NRMP undercounts entry and the
# conversion was absorbing the missing denominator. Fetching ACGME shows that
# reading is mostly WRONG, and the decomposition is worth keeping:
#
#   NRMP,  mean-of-annual, pre-2020 window   0.754    the old default
#   NRMP,  POOLED, full series               0.850    +0.096  window + pooling
#   ACGME, POOLED, full series               0.857    +0.007  changing source
#
# Nearly all of the correction came from pooling over a window that spans the
# COVID trough AND its release. A ratio above 1.0 is what a 3-year window looks
# like when it contains the release but not the entry cohort that was deferred
# into it; ACGME reduces that window's ratio from 1.197 to 1.125 without
# removing it. Undercounting was a small part of the story and deferral was
# nearly all of it.
#
# THE UNDERCOUNT IS STILL REAL, AND IT MATTERS FOR PROJECTION RATHER THAN FOR
# THE RATIO. ACGME counts fellows ON DUTY, so it sees entrants who never passed
# through the match, and the gap against NRMP filled positions is WIDENING:
#
#   entry year   2017  2018  2019  2020  2021  2022  2023  2024
#   ACGME year 1   62    60    61    63    68    73    76    74
#   NRMP filled    59    59    58    56    62    61    61    65
#   gap            +3    +1    +3    +7    +6   +12   +15    +9
#
# Historically the ratio hid this, because numerator and denominator were both
# wrong in compensating directions. Forward it does not compensate: feeding the
# pipeline NRMP filled positions as the entry FLOW understates entry by roughly
# fifteen fellows a year at the current gap, and the gap is growing. The entry
# count is where this source earns its place -- not the conversion.
#
# TWO PATHWAYS, COUNTED SEPARATELY. ACGME reports the subspecialty under BOTH
# parent specialties -- Obstetrics and Gynecology, and Urology -- which is the
# same split the ABOG/ABU certification counts carry and which a single NRMP line
# cannot express. Roughly a fifth of entering fellows are urology-based.
#
# THE NAME CHANGE IS WHY THIS WAS MISSED FOR SO LONG. ACGME renamed the
# subspecialty from "Female pelvic medicine and reconstructive surgery" to
# "Urogynecology and reconstructive pelvic surgery"; NRMP still printed the old
# name through its 2025 report. A search on either name alone finds one source
# and misses the other.
#
# YEAR 1 IS A FLOW, active_total IS A STOCK. `active_total` counts every fellow
# on duty across all program years and must never be used as an annual entry
# rate; it is carried only so the arithmetic gate (year columns summing to the
# printed total) remains checkable in-package.
#
# LEAKAGE. Each Data Resource Book is published in the autumn AFTER its academic
# year closes, so `available_by_year` is entry_year + 1. A back-test at cutoff
# 2020 may use books through academic year 2019-2020, not 2020-2021.
#
# Regenerate with scripts/data_acquisition/08_fetch_acgme_urps_series.R. The
# 2013-2014 and 2014-2015 editions predate the resident-year table and are
# reported as INCOMPATIBLE rather than imputed.

ACGME_URPS_FELLOWS <- tibble::tribble(
  ~academic_year, ~entry_year, ~parent, ~active_total, ~year_1, ~year_2, ~year_3, ~available_by_year,
  "2015-2016", 2015L, "obgyn", 132L, 48L, 43L, 41L, 2016L,
  "2015-2016", 2015L, "urology", 28L, 10L, 13L, 5L, 2016L,
  "2016-2017", 2016L, "obgyn", 139L, 46L, 49L, 44L, 2017L,
  "2016-2017", 2016L, "urology", 29L, 6L, 14L, 9L, 2017L,
  "2017-2018", 2017L, "obgyn", 142L, 49L, 45L, 48L, 2018L,
  "2017-2018", 2017L, "urology", 34L, 13L, 12L, 9L, 2018L,
  "2018-2019", 2018L, "obgyn", 145L, 50L, 50L, 45L, 2019L,
  "2018-2019", 2018L, "urology", 37L, 10L, 20L, 7L, 2019L,
  "2019-2020", 2019L, "obgyn", 149L, 51L, 45L, 53L, 2020L,
  "2019-2020", 2019L, "urology", 35L, 10L, 13L, 12L, 2020L,
  "2020-2021", 2020L, "obgyn", 146L, 49L, 51L, 46L, 2021L,
  "2020-2021", 2020L, "urology", 37L, 14L, 15L, 8L, 2021L,
  "2021-2022", 2021L, "obgyn", 151L, 53L, 47L, 51L, 2022L,
  "2021-2022", 2021L, "urology", 38L, 15L, 16L, 7L, 2022L,
  "2022-2023", 2022L, "obgyn", 155L, 57L, 51L, 47L, 2023L,
  "2022-2023", 2022L, "urology", 42L, 16L, 18L, 8L, 2023L,
  "2023-2024", 2023L, "obgyn", 167L, 60L, 56L, 51L, 2024L,
  "2023-2024", 2023L, "urology", 38L, 16L, 16L, 6L, 2024L,
  "2024-2025", 2024L, "obgyn", 175L, 57L, 61L, 57L, 2025L,
  "2024-2025", 2024L, "urology", 40L, 17L, 17L, 6L, 2025L,
)

#' ACGME URPS fellow counts by academic year and parent specialty
#'
#' @param available_by Keep only books published by this calendar year. Supply a
#'   back-test cutoff to guarantee no later book informs a parameter. Each book
#'   appears in the autumn after its academic year closes, so a book covering
#'   entry year Y is available in Y + 1.
#' @param parent Optional filter, `"obgyn"` or `"urology"`.
#' @return Tibble of `academic_year`, `entry_year`, `parent`, `active_total`,
#'   `year_1`, `year_2`, `year_3`, `available_by_year`.
#' @family acgme fellows
#' @concept supply
#' @export
acgme_urps_fellows <- function(available_by = NULL, parent = NULL) {
  x <- ACGME_URPS_FELLOWS
  if (!is.null(available_by)) {
    x <- x[x$available_by_year <= available_by, , drop = FALSE]
    if (!nrow(x)) {
      stop("acgme_urps_fellows(): no ACGME book published by ", available_by,
           call. = FALSE)
    }
  }
  if (!is.null(parent)) {
    parent <- match.arg(parent, c("obgyn", "urology"))
    x <- x[x$parent == parent, , drop = FALSE]
  }
  x
}

#' Entering URPS fellow cohort per year, both pathways combined
#'
#' The year-1 count, which is a FLOW and the quantity a forward projection needs.
#' Both parent specialties are summed, because the certification series this is
#' calibrated against (ABOG plus ABU) also spans both.
#'
#' @param available_by Passed to [acgme_urps_fellows()].
#' @return Tibble of `entry_year`, `entering_cohort`, `n_pathways`.
#' @family acgme fellows
#' @concept supply
#' @export
acgme_entering_cohort <- function(available_by = NULL) {
  x <- acgme_urps_fellows(available_by)
  out <- x %>%
    dplyr::group_by(.data$entry_year) %>%
    dplyr::summarise(entering_cohort = sum(.data$year_1),
                     n_pathways = dplyr::n(), .groups = "drop")
  # A year with only one pathway reported is not comparable with one carrying
  # both, and silently summing it would understate entry.
  if (any(out$n_pathways != 2L)) {
    .msg_warn(sprintf(paste(
      "acgme_entering_cohort(): entry year(s) %s report only one parent pathway;",
      "their totals are not comparable with the two-pathway years."),
      paste(out$entry_year[out$n_pathways != 2L], collapse = ", ")))
  }
  out
}

#' Entrant series from a named source, on a common shape
#'
#' Lets the conversion be estimated against either entry source without the
#' caller rewriting the alignment. `"acgme"` counts fellows on duty and sees
#' off-match entry; `"nrmp"` counts matched positions and does not.
#'
#' @param source `"acgme"` or `"nrmp"`.
#' @param available_by Publication-year cutoff, applied to whichever source.
#' @return Tibble of `entry_year`, `entrants`, `source`.
#' @family acgme fellows
#' @concept supply
#' @export
entrant_source_series <- function(source = c("acgme", "nrmp"),
                                  available_by = NULL) {
  source <- match.arg(source)
  if (identical(source, "acgme")) {
    s <- acgme_entering_cohort(available_by)
    tibble::tibble(entry_year = s$entry_year, entrants = as.numeric(s$entering_cohort),
                   source = "acgme")
  } else {
    s <- nrmp_entrant_series(available_by = available_by)
    tibble::tibble(entry_year = as.integer(s$appointment_year),
                   entrants = as.numeric(s$positions_filled), source = "nrmp")
  }
}

#' Entry-to-certification conversion, estimated against a chosen entry source
#'
#' Certifications in year `Y` are divided by entrants in `Y - cert_lag`.
#'
#' `pooled = TRUE` (the default) sums both sides over the usable window before
#' dividing, rather than averaging annual ratios. That matters here because a
#' disruption and its release are the SAME fellows counted in two different
#' years: a pooled window spanning both nets the deferral out, while a mean of
#' annual ratios is dominated by whichever side of the break the window lands on.
#' It is why this estimator is stable where the NRMP-based one drifted from 0.755
#' to 1.197 across successive windows.
#'
#' @param source Entry source, passed to [entrant_source_series()].
#' @param through_year Latest year either series may be read to.
#' @param cert_lag Years from entry to certification.
#' @param exclude_disrupted Drop backlog years, and -- when not pooling --
#'   disrupted and release years too. Under pooling the disruption is retained on
#'   purpose, because dropping half of a deferral pair biases the ratio.
#' @param pooled Sum both sides before dividing.
#' @return List with `ratio`, `source`, `years`, `n_years`, `excluded`,
#'   `cert_lag`, `pooled`, and `annual`.
#' @family acgme fellows
#' @concept supply
#' @export
entrant_to_cert_ratio <- function(source = c("acgme", "nrmp"),
                                  through_year = NULL,
                                  cert_lag = URPS_FELLOWSHIP_YEARS,
                                  exclude_disrupted = TRUE,
                                  pooled = TRUE) {
  source <- match.arg(source)
  entry <- entrant_source_series(source, available_by = through_year)
  certs <- urps_entrant_series(if (is.null(through_year)) 2100L else through_year)

  excluded <- integer(0)
  if (isTRUE(exclude_disrupted)) {
    reg <- classify_certification_regimes(
      as.data.frame(certs[, c("year", "count")]), verbose = FALSE)
    # Backlog years certified an already-practising pool that never entered a
    # fellowship, so their ratio is meaningless under any weighting.
    drop <- if (isTRUE(pooled)) "backlog" else c("backlog", "disrupted", "release")
    excluded <- reg$year[reg$regime %in% drop]
    certs <- certs[!certs$year %in% excluded, , drop = FALSE]
  }

  src <- certs$year - as.integer(cert_lag)
  ent <- entry$entrants[match(src, entry$entry_year)]
  keep <- is.finite(ent) & ent > 0
  if (!any(keep)) {
    stop("entrant_to_cert_ratio(): no certification year has a matching entry ",
         "year at lag ", cert_lag, " in the '", source, "' series within the ",
         "cutoff.", call. = FALSE)
  }
  annual <- stats::setNames(certs$count[keep] / ent[keep], certs$year[keep])
  ratio <- if (isTRUE(pooled)) sum(certs$count[keep]) / sum(ent[keep]) else mean(annual)

  list(ratio = unname(ratio), source = source, years = certs$year[keep],
       n_years = sum(keep), excluded = excluded, cert_lag = as.integer(cert_lag),
       pooled = isTRUE(pooled), annual = annual)
}
