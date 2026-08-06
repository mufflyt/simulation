# NAMCS Demand Calibration ----
#
# `assert_demand_calibrated()` has warned on every run since it was written,
# because nothing ever fitted a scalar. This module supplies the missing
# independent national anchor: NAMCS office-based visit volume for the URPS
# condition set, against which the model's own base-year ambulatory volume can
# be scaled. It is the HDMM Exhibit 11 procedure, whose published scalars run
# 0.243 (Orthopaedics) to 1.665 (Paediatrics) -- see
# `published_calibration_scalars()` for the reference magnitudes.
#
# WHAT THIS IS NOT. Calibration is not validation. Scaling model output to an
# independent national total removes a level offset; it says nothing about
# whether the projection's trajectory or its intervals are right. The
# 2020->2023 back-test is the validation, it is supply-side only, and it still
# fails -- see R/validation-backtest_status.R.
#
# NO BACK-TEST LEAKAGE. The back-test projects HEADCOUNT only: `run_backtest()`
# and `run_backtest_arm()` touch no demand object, no service volume and no
# calibration scalar (grep either file for "demand" -- there are no matches).
# Nothing here can move a back-test arm, so the NAMCS 2019 public-use release
# date (2021, after the 2020 cutoff) cannot contaminate the historical
# forecast. Were demand ever added to the back-test, this anchor would have to
# be re-derived from a file released before the cutoff.

#' Provenance of the NAMCS demand anchor
#'
#' Every field a reader needs to decide whether the anchor is fit for their
#' purpose, carried with the number rather than in a document beside it.
#'
#' @return Named list of provenance fields.
#' @export
namcs_anchor_provenance <- function() {
  list(
    source = "National Ambulatory Medical Care Survey (NAMCS) Public Use File",
    agency = "National Center for Health Statistics, CDC",
    data_year = 2019L,
    public_release = "2021 (after the 2020 back-test cutoff; see module note)",
    universe = paste("Visits to non-federal office-based physicians in the US.",
                     "EXCLUDES hospital outpatient and emergency departments,",
                     "which NHAMCS covers separately -- so this anchor is a",
                     "LOWER bound on total ambulatory URPS care."),
    population = "Female patients aged 18+",
    weight = "PATWT (patient visit weight, annualised national estimate)",
    design = "CSTRATM strata, CPSUM PSUs",
    case_definition = paste(
      "Any of DIAG1/DIAG2/DIAG3 beginning with an ICD-10-CM prefix in",
      "URPS_ICD10_PREFIXES:", paste(URPS_ICD10_PREFIXES, collapse = ", ")),
    sex_coding = paste("NAMCS codes SEX 1 = Female, 2 = Male -- the reverse of",
                       "Census/ACS/BRFSS/MEPS. Verified against sex-specific",
                       "diagnoses in this file."),
    reliability_standard = paste(
      "NCHS presentation standards treat an estimate from fewer than 30",
      "unweighted records as unreliable; the anchor reports its record count",
      "and refuses to be used below the floor.")
  )
}

# NCHS reliability floor for a publishable NAMCS estimate.
NAMCS_MIN_RECORDS <- 30L

#' National annual URPS ambulatory-visit anchor from NAMCS
#'
#' @param namcs Tibble from [load_namcs_2019()]; loaded when NULL.
#' @param min_records Unweighted-record floor below which the estimate is
#'   marked unreliable.
#' @return One-row tibble with `category`, `observed`, `n_records`, `reliable`,
#'   carrying `provenance` and `data_year` attributes.
#' @export
namcs_urps_visit_anchor <- function(namcs = NULL, min_records = NAMCS_MIN_RECORDS) {
  if (is.null(namcs)) namcs <- load_namcs_2019()
  flagged <- flag_urps_visits(namcs)

  # SEX == 1 is FEMALE in NAMCS. This was inverted in the stratum builder and
  # silently produced a male-visit estimate for a female-predominant
  # subspecialty; the anchor states the convention rather than inheriting it.
  keep <- flagged$is_urps & flagged$SEX == 1L & flagged$AGE >= 18 &
    !is.na(flagged$PATWT)
  n_rec <- sum(keep)
  total <- sum(flagged$PATWT[keep])

  if (n_rec < min_records) {
    .msg_warn(sprintf(paste(
      "NAMCS URPS anchor rests on %d unweighted records, below the NCHS",
      "reliability floor of %d. Treat the national total as indicative only."),
      n_rec, min_records))
  }

  structure(
    tibble::tibble(
      category = "ambulatory_visits",
      observed = total,
      n_records = n_rec,
      reliable = n_rec >= min_records
    ),
    provenance = namcs_anchor_provenance(),
    data_year = 2019L
  )
}

# Model services that are ambulatory VISITS, and so comparable with a NAMCS
# visit count. Procedures and pessary care are excluded: NAMCS counts the
# encounter, and a procedure performed at a counted visit is not a second
# visit. Including them would inflate the model side and shrink the scalar for
# a purely definitional reason.
NAMCS_COMPARABLE_SERVICES <- c("new_consultation", "return_visit")

#' Fit demand calibration scalars against the NAMCS national anchor
#'
#' @param service_volumes Tibble with `year`, `service`, `volume` (the
#'   `service_volumes` element of a [run_workforce_microsimulation()] result).
#' @param base_year Year of model output to compare; defaults to the earliest.
#' @param anchor Anchor tibble from [namcs_urps_visit_anchor()].
#' @param services Model services counted as ambulatory visits.
#' @return The [fit_calibration_scalars()] tibble, carrying `provenance`,
#'   `base_year` and `anchor_year` attributes.
#' @export
namcs_demand_calibration <- function(service_volumes,
                                     base_year = NULL,
                                     anchor = namcs_urps_visit_anchor(),
                                     services = NAMCS_COMPARABLE_SERVICES) {
  assertthat::assert_that(is.data.frame(service_volumes),
                          all(c("year", "service", "volume") %in% names(service_volumes)))
  if (is.null(base_year)) base_year <- min(service_volumes$year, na.rm = TRUE)

  rows <- service_volumes[service_volumes$year == base_year &
                            service_volumes$service %in% services, , drop = FALSE]
  if (nrow(rows) == 0) {
    stop(sprintf(paste("namcs_demand_calibration(): no rows for year %s and",
                       "services %s. The service names come from the workload",
                       "basket and must match."),
                 base_year, paste(services, collapse = ", ")), call. = FALSE)
  }

  predicted <- tibble::tibble(category = "ambulatory_visits",
                              predicted = sum(rows$volume, na.rm = TRUE))

  # A year gap between the anchor and the model base year is a real limitation,
  # not a rounding detail: population and utilisation both move. Stated rather
  # than silently absorbed into the scalar.
  gap <- base_year - attr(anchor, "data_year")
  if (!is.na(gap) && abs(gap) > 3) {
    .msg_warn(sprintf(paste(
      "NAMCS anchor is %d years from the model base year (%d vs %d). The",
      "scalar absorbs %d years of population and utilisation change along with",
      "the level offset."), gap, attr(anchor, "data_year"), base_year, gap))
  }

  out <- fit_calibration_scalars(predicted, anchor[, c("category", "observed")])
  attr(out, "provenance") <- attr(anchor, "provenance")
  attr(out, "base_year") <- base_year
  attr(out, "anchor_year") <- attr(anchor, "data_year")
  attr(out, "anchor_records") <- anchor$n_records
  attr(out, "services") <- services
  out
}

#' Apply demand calibration to a service-volume table
#'
#' Scales ONLY the services the anchor is comparable with. A NAMCS visit count
#' says nothing about how many slings were performed, so scaling the procedure
#' rows by a visit-derived scalar would propagate the anchor beyond its
#' evidence. Untouched rows keep `calibration_scalar = 1`, so the table records
#' what was and was not calibrated.
#'
#' @param volumes Tibble with `year`, `service`, `volume`.
#' @param calibration Tibble from [namcs_demand_calibration()].
#' @return `volumes` with `volume` scaled and a `calibration_scalar` column.
#' @export
apply_demand_calibration <- function(volumes, calibration) {
  assertthat::assert_that(is.data.frame(volumes),
                          all(c("service", "volume") %in% names(volumes)))
  if (!is.data.frame(calibration) || nrow(calibration) == 0 ||
      !"scalar" %in% names(calibration)) {
    return(volumes)
  }
  services <- attr(calibration, "services") %||% NAMCS_COMPARABLE_SERVICES
  s <- calibration$scalar[calibration$category == "ambulatory_visits"]
  if (length(s) != 1L || !is.finite(s)) return(volumes)

  volumes$calibration_scalar <- ifelse(volumes$service %in% services, s, 1)
  volumes$volume <- volumes$volume * volumes$calibration_scalar
  volumes
}
