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
#' @param allow_implausible Return a conversion above 1.0 instead of erroring.
#'   Only for deliberately demonstrating a misalignment -- a rate above one means
#'   more people reached the outcome than entered, so the default refuses it.
#' @return List with `ratio`, `source`, `years`, `n_years`, `excluded`,
#'   `cert_lag`, `pooled`, and `annual`.
#' @family acgme fellows
#' @concept supply
#' @export
entrant_to_cert_ratio <- function(source = c("acgme", "nrmp"),
                                  through_year = NULL,
                                  cert_lag = URPS_FELLOWSHIP_YEARS,
                                  exclude_disrupted = TRUE,
                                  pooled = TRUE,
                                  allow_implausible = FALSE) {
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

  .assert_possible_conversion(ratio, "entrant_to_cert_ratio()", cert_lag, source,
                              allow_implausible)
  list(ratio = unname(ratio), source = source, years = certs$year[keep],
       n_years = sum(keep), excluded = excluded, cert_lag = as.integer(cert_lag),
       pooled = isTRUE(pooled), annual = annual)
}

# ---- Fellowship length, completion, and the pipeline already in train -------
#
# Fellowship length by pathway now lives in R/calibration-sources.R, beside the
# derived URPS_FELLOWSHIP_YEARS scalar, so the two cannot be defined apart.

#' Follow each entering cohort through its program
#'
#' Reads the ACGME books on the diagonal: fellows entering in year `Y` are
#' `year_1` in the book for `Y`, `year_2` in the book for `Y + 1`, and so on. The
#' ratio of the final program year to `year_1` measures how much of an entering
#' class is still enrolled at the end -- within-fellowship attrition, which the
#' entrant pipeline otherwise has to assume.
#'
#' Only years within the pathway's own fellowship length are followed, so the
#' urology cohorts are tracked to year 2 rather than year 3.
#'
#' @param parent `"obgyn"` or `"urology"`.
#' @return Tibble of `entry_year`, `year_1`, `final_year`, `final_year_n`,
#'   `retention`.
#' @export
acgme_cohort_tracking <- function(parent = c("obgyn", "urology")) {
  parent <- match.arg(parent)
  d <- acgme_urps_fellows(parent = parent)
  len <- URPS_FELLOWSHIP_YEARS_BY_PATHWAY[[parent]]
  col <- paste0("year_", len)
  rows <- list()
  for (y in sort(d$entry_year)) {
    y1 <- d$year_1[d$entry_year == y]
    fin <- d[[col]][d$entry_year == y + (len - 1L)]
    if (!length(fin) || !length(y1) || !is.finite(y1) || y1 <= 0) next
    rows[[length(rows) + 1L]] <- tibble::tibble(
      entry_year = y, year_1 = y1, final_year = len,
      final_year_n = fin, retention = fin / y1)
  }
  if (!length(rows)) {
    stop("acgme_cohort_tracking(): no cohort can be followed to year ", len,
         " for '", parent, "'.", call. = FALSE)
  }
  # NO conversion guard here on purpose: retention above 1 is exactly the
  # finding for the urology diagonal, and fellowship_completion_rate() is where
  # it is refused. Guarding it here would hide the evidence.
  dplyr::bind_rows(rows)
}

#' Measured within-fellowship completion
#'
#' The mean retention from [acgme_cohort_tracking()]. For the OB/GYN pathway it is
#' ~0.99 across eight cohorts, and that is the point: fellowship attrition is
#' close to zero, so a conversion of 0.84 from entry to certification is NOT
#' mostly people leaving training. It is certification behaviour -- not sitting
#' the exam, not passing it, or certifying later than the lag allows. Those have
#' different causes and different levers from dropping out, and one parameter
#' spanning both invites the wrong reading.
#'
#' IT IS NOT IDENTIFIABLE FOR UROLOGY, and this fails rather than returning a
#' number. The urology year_2/year_1 diagonal runs from 1.00 to 2.00 -- more
#' fellows in the second year than entered the first. Retention cannot exceed 1,
#' so those columns are not following a cohort, and averaging them yields 1.349,
#' which would be reported as a completion rate and read as one. Whatever the
#' urology columns encode, it is not a cohort progression, and an unusable
#' quantity is refused rather than dressed up.
#'
#' @param parent `"obgyn"` or `"urology"`.
#' @return List with `rate`, `n_cohorts`, `range`, and `parent`.
#' @export
fellowship_completion_rate <- function(parent = c("obgyn", "urology")) {
  parent <- match.arg(parent)
  tr <- acgme_cohort_tracking(parent)
  rate <- mean(tr$retention)
  if (rate > 1 || mean(tr$retention > 1) > 0.5) {
    stop(sprintf(paste(
      "fellowship_completion_rate(): retention for '%s' averages %.3f, and %.0f%%",
      "of cohorts exceed 1.0. A completion rate above one is impossible, so the",
      "year-by-year columns are not tracking a cohort for this pathway and no",
      "completion rate is identifiable from them. Do not substitute the combined",
      "conversion here -- that would hide the problem rather than state it."),
      parent, rate, 100 * mean(tr$retention > 1)), call. = FALSE)
  }
  list(rate = rate, n_cohorts = nrow(tr), range = range(tr$retention),
       parent = parent)
}

#' Certifications already determined by fellows in training
#'
#' Fellows partway through a programme will certify on a known schedule, so the
#' next year or two of certifications is largely fixed already and needs no
#' entrant model, no regime-break term, and no view on which entry source
#' undercounts. This is the tightest forecast available at short horizon, and the
#' only one whose inputs are observed rather than projected.
#'
#' @param available_by Passed to [acgme_urps_fellows()].
#' @param certification_rate Named vector of per-pathway conversions. Defaults to
#'   the pathway-specific values from [entrant_to_cert_ratio_by_pathway()], each
#'   aligned on its own fellowship length; a single pooled number would apply the
#'   OB/GYN conversion to urology fellows at the wrong lag.
#' @return Tibble of `certifying_year`, `parent`, `fellows_in_training`,
#'   `program_year`, `certification_rate`, `expected_certifications`.
#' @export
locked_in_certifications <- function(available_by = NULL,
                                     certification_rate = NULL) {
  if (is.null(certification_rate)) {
    r <- entrant_to_cert_ratio_by_pathway(through_year = available_by)
    certification_rate <- stats::setNames(r$ratio, r$parent)
  }
  d <- acgme_urps_fellows(available_by)
  latest <- max(d$entry_year)
  d <- d[d$entry_year == latest, , drop = FALSE]
  rows <- list()
  for (i in seq_len(nrow(d))) {
    p <- d$parent[i]
    len <- URPS_FELLOWSHIP_YEARS_BY_PATHWAY[[p]]
    for (yr in seq_len(len)) {
      n <- d[[paste0("year_", yr)]][i]
      if (!length(n) || !is.finite(n)) next
      cr <- if (p %in% names(certification_rate)) certification_rate[[p]] else
        stop("locked_in_certifications(): no certification rate for pathway '",
             p, "'.", call. = FALSE)
      rows[[length(rows) + 1L]] <- tibble::tibble(
        # A fellow in program year `yr` has (len - yr) years left, then certifies.
        certifying_year = latest + 1L + (len - yr),
        parent = p, program_year = yr, fellows_in_training = n,
        certification_rate = cr, expected_certifications = n * cr)
    }
  }
  out <- dplyr::bind_rows(rows)
  out[order(out$certifying_year, out$parent), ]
}

#' Entry-to-certification conversion with a pathway-specific lag
#'
#' Splits [entrant_to_cert_ratio()] by pathway so each is aligned on its own
#' fellowship length, and matches each to the certification series it actually
#' produces: ABOG certifications come from the OB/GYN pathway, ABU from urology.
#'
#' @param through_year Latest year either series may be read to.
#' @param exclude_disrupted Drop backlog years from the certification side.
#' @return Tibble of `parent`, `cert_lag`, `certifications`, `entrants`, `ratio`,
#'   `years`.
#' @export
entrant_to_cert_ratio_by_pathway <- function(through_year = NULL,
                                             exclude_disrupted = TRUE) {
  .require_mufflyaccess("The certification series")
  ec <- mufflyaccess::urps_entry_counts()
  ec <- ec[ec$geography == "national", , drop = FALSE]
  cert_col <- c(obgyn = "abog_entrants", urology = "abu_entrants")

  drop_years <- integer(0)
  if (isTRUE(exclude_disrupted)) {
    reg <- classify_certification_regimes(
      data.frame(year = ec$year, count = ec$combined_entrants), verbose = FALSE)
    drop_years <- reg$year[reg$regime == "backlog"]
  }

  rows <- list()
  for (p in names(URPS_FELLOWSHIP_YEARS_BY_PATHWAY)) {
    lag <- URPS_FELLOWSHIP_YEARS_BY_PATHWAY[[p]]
    ent <- acgme_urps_fellows(available_by = through_year, parent = p)
    cy <- ec$year[!ec$year %in% drop_years]
    if (!is.null(through_year)) cy <- cy[cy <= through_year]
    e <- ent$year_1[match(cy - lag, ent$entry_year)]
    keep <- is.finite(e) & e > 0
    if (!any(keep)) next
    rows[[length(rows) + 1L]] <- tibble::tibble(
      parent = p, cert_lag = lag,
      certifications = sum(ec[[cert_col[[p]]]][match(cy[keep], ec$year)]),
      entrants = sum(e[keep]),
      ratio = sum(ec[[cert_col[[p]]]][match(cy[keep], ec$year)]) / sum(e[keep]),
      years = paste(range(cy[keep]), collapse = "-"))
  }
  out <- dplyr::bind_rows(rows)
  for (i in seq_len(nrow(out))) {
    .assert_possible_conversion(
      out$ratio[i],
      sprintf("entrant_to_cert_ratio_by_pathway() for '%s'", out$parent[i]),
      out$cert_lag[i])
  }
  out
}

# ---- The guard that would have caught the wrong fellowship length ----------
#
# A CONVERSION ABOVE 1.0 IS THE SIGNATURE OF A MISALIGNMENT, and it has appeared
# four times in this model's history, each time as a plausible-looking number
# that a reader would have taken at face value:
#
#   1.197  NRMP entry, 3-year lag, window holding a deferral release
#   1.050  ABU certifications against urology entrants at a 3-year lag
#   1.349  "fellowship completion" from a urology cohort diagonal
#   4.019  certifications against entrants including the backlog era
#
# None was caught by a guard. Each was caught by someone noticing that a rate
# exceeded one, which is not a control. More people certified than ever entered
# is arithmetically impossible, so the estimate is not "high" -- it is measuring
# something other than what its name says, and the usual cause is that the lag,
# the window, or the denominator does not match the numerator.
#
# Documenting the fellowship length did not prevent the 1.050: the length WAS
# documented, as a single scalar, and the scalar was correct for OB/GYN and
# wrong for urology. A constant cannot enforce that a caller applies it to the
# right pathway. This can.
.assert_possible_conversion <- function(ratio, what, cert_lag = NA_integer_,
                                        source = NA_character_,
                                        allow_implausible = FALSE,
                                        tolerance = 1e-8) {
  bad <- is.finite(ratio) & ratio > 1 + tolerance
  if (!any(bad) || isTRUE(allow_implausible)) return(invisible(ratio))
  stop(sprintf(paste(
    "%s returned a conversion of %s, which is impossible: more people reached",
    "the outcome than ever entered. The estimate is not high, it is misaligned.",
    "Check, in this order: (1) the lag -- fellowship length is PATHWAY-SPECIFIC",
    "(URPS_FELLOWSHIP_YEARS_BY_PATHWAY: obgyn %d, urology %d), and a uniform lag",
    "produced exactly this at 1.050; (2) the window -- one holding a deferral",
    "release but not the entry cohort deferred into it inflates the ratio; (3)",
    "the denominator -- backlog-era certifications never passed through a",
    "fellowship at all.%s Pass allow_implausible = TRUE only to demonstrate the",
    "defect deliberately."),
    what, paste(sprintf("%.3f", ratio[bad]), collapse = ", "),
    URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["obgyn"]],
    URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["urology"]],
    if (is.finite(cert_lag)) sprintf(" Lag used: %d.", cert_lag) else ""),
    call. = FALSE)
}
