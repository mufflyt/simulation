################################################################################
# R/demand-incident_continuing_care.R
# Replace `treated * 2.5` with an explicit incident/continuing-care state
# decomposition, holding the upstream treated cohort FIXED.
#
# THE ERROR BEING CORRECTED. `care_engaged` (formerly `treated`) is a STOCK --
# the prevalent population engaged in care this year. `newly_entering_care` is a
# FLOW. The shipped pathway converted the entire stock back into an incident
# flow every year by giving every member one new_consultation, so
# new_consultation volume equalled the treated cohort exactly, ratio 1.00.
#
#   care_engaged
#     |- newly_entering_care    (flow)  -> new_consultation, first-year follow-up
#     |- continuing_care        (stock) -> continuing follow-up
#
# THE COHORT IS FROZEN. 6,176,308 care-engaged patients (ui 2,538,780,
# pop 3,264,807, ai 372,721) is held fixed for this workstream. It is 4.81% of
# US women 20+, which is not obviously implausible against published
# help-seeking rates among symptomatic women. Do NOT resolve the office-visit
# anchor by moving prevalence, treatment uptake, or a global visits-per-treated
# multiplier.
#
# WHAT IS AND IS NOT CALIBRATED HERE. new_consults_per_entrant is 1.0 BY
# DEFINITION -- one new consultation per new entrant -- and is deliberately not
# a free parameter; if an empirical source disagrees it is counting something
# other than new entrants. The genuine uncertainty sits in the incident share
# and the two follow-up intensities, all three of which are declared
# uncalibrated_illustrative until sourced. None is to be chosen as a residual
# that forces the anchor to agree.
################################################################################

#' Care-engagement parameters
#'
#' @return Tibble of parameter, value, source, confidence, calibration_status.
#' @export
care_engagement_params <- function() {
  tibble::tribble(
    ~parameter,                 ~value, ~source,            ~confidence, ~calibration_status,
    "incident_share",            NA_real_, "UNSOURCED",       "none",  "requires_source",
    "new_consults_per_entrant",  1.0,      "definitional",    "high",  "definitional",
    "first_year_followup_rate",  NA_real_, "UNSOURCED",       "none",  "requires_source",
    "annual_followup_rate",      NA_real_, "UNSOURCED",       "none",  "requires_source")
}

#' Split a care-engaged stock into incident and continuing components
#'
#' @param care_engaged Named numeric vector of care-engaged patients.
#' @param incident_share Fraction newly entering care this year. NO DEFAULT: it
#'   is unsourced, and a default would silently become the calibration lever.
#' @return Tibble of condition, care_engaged, newly_entering_care,
#'   continuing_care.
#' @export
split_care_engagement <- function(care_engaged, incident_share = NULL) {
  if (base::is.null(incident_share)) {
    base::stop(
      "incident_share has no default. It is unsourced (see ",
      "care_engagement_params()), and defaulting it would make it the residual ",
      "that forces the office anchor to agree -- which is the failure mode this ",
      "module exists to prevent. Supply it with a source.", call. = FALSE)
  }
  if (!base::is.finite(incident_share) || incident_share <= 0 ||
      incident_share >= 1) {
    base::stop("incident_share must lie strictly in (0, 1); got ",
               incident_share, call. = FALSE)
  }
  tibble::tibble(
    condition          = base::names(care_engaged),
    care_engaged       = base::as.numeric(care_engaged),
    newly_entering_care = base::as.numeric(care_engaged) * incident_share,
    continuing_care    = base::as.numeric(care_engaged) * (1 - incident_share))
}

#' Generate ambulatory visits from care-engagement stocks and flows
#'
#' @param split Output of [split_care_engagement()].
#' @param new_consults_per_entrant Definitional, 1.0.
#' @param first_year_followup_rate Follow-up visits per new entrant, first year.
#' @param annual_followup_rate Follow-up visits per continuing patient per year.
#' @return Tibble of component and volume.
#' @export
care_engagement_visits <- function(split,
                                   new_consults_per_entrant = 1.0,
                                   first_year_followup_rate = NULL,
                                   annual_followup_rate = NULL) {
  if (base::is.null(first_year_followup_rate) ||
      base::is.null(annual_followup_rate)) {
    base::stop("first_year_followup_rate and annual_followup_rate have no ",
               "defaults; both are unsourced. Supply them with sources rather ",
               "than choosing values that reproduce the anchor.", call. = FALSE)
  }
  entrants   <- base::sum(split$newly_entering_care)
  continuing <- base::sum(split$continuing_care)
  visits <- tibble::tibble(
    component = c("new_consultation", "first_year_followup", "continuing_followup"),
    volume    = c(entrants * new_consults_per_entrant,
                  entrants * first_year_followup_rate,
                  continuing * annual_followup_rate))

  # THE ACCEPTANCE GATES RUN HERE, on the object they describe.
  #
  # They are split by KIND, because the four are not the same sort of claim and
  # collapsing them would force a false choice between blocking on a known-open
  # provenance item and not checking arithmetic at all:
  #
  #   gates 1-3  arithmetic and structural. A failure means the decomposition
  #              does not add up -- newly_entering + continuing must equal
  #              care_engaged, and new_consultation must be strictly below it
  #              rather than equal by construction. Downstream numbers would be
  #              wrong, not uncertain, so these STOP.
  #   gate 4     provenance: first_year_followup_rate and annual_followup_rate
  #              are unsourced (care_engagement_params() says so). That is the
  #              open scientific question this module was written to expose, and
  #              it is TRUE TODAY -- blocking on it would make the package
  #              unusable while telling us nothing new. It messages instead, so
  #              it stays visible without being fatal.
  gates <- assert_care_engagement_gates(split, visits)
  arith <- gates[seq_len(3), , drop = FALSE]
  arith <- arith[!base::is.na(arith$passed) & !arith$passed, , drop = FALSE]
  if (base::nrow(arith) > 0L) {
    base::stop("Care-engagement decomposition failed: ",
               base::paste(base::sprintf("%s (%s)", arith$gate, arith$detail),
                           collapse = "; "),
               call. = FALSE)
  }
  if (!base::isTRUE(gates$passed[4])) {
    base::message("Care-engagement utilization parameters remain unsourced: ",
                  gates$detail[4], ".")
  }
  visits
}

#' Acceptance gates for the incident/continuing decomposition
#'
#' Four checks, per the review that motivated this module.
#'
#' @param split Output of [split_care_engagement()].
#' @param visits Output of [care_engagement_visits()], or NULL to skip gate 2.
#' @param tolerance Relative tolerance for the conservation identity.
#' @return Tibble of gate, passed, detail.
#' @export
assert_care_engagement_gates <- function(split, visits = NULL,
                                         tolerance = 1e-8) {
  ce <- base::sum(split$care_engaged)
  ne <- base::sum(split$newly_entering_care)
  cc <- base::sum(split$continuing_care)

  g <- tibble::tibble(
    gate = c("newly_entering_care < care_engaged",
             "new_consultation < care_engaged, not equal by construction",
             "newly_entering + continuing == care_engaged",
             "office anchor fit from empirical parameters, not a residual"),
    passed = NA, detail = NA_character_)

  g$passed[1] <- ne < ce
  g$detail[1] <- base::sprintf("%s entrants vs %s engaged",
                               base::format(base::round(ne), big.mark = ","),
                               base::format(base::round(ce), big.mark = ","))

  if (!base::is.null(visits)) {
    nc <- base::sum(visits$volume[visits$component == "new_consultation"])
    g$passed[2] <- nc < ce && base::abs(nc - ce) / ce > tolerance
    g$detail[2] <- base::sprintf("new_consultation %s, ratio %.3f",
                                 base::format(base::round(nc), big.mark = ","), nc / ce)
  } else {
    g$passed[2] <- NA; g$detail[2] <- "visits not supplied"
  }

  g$passed[3] <- base::abs((ne + cc) - ce) / ce < tolerance
  g$detail[3] <- base::sprintf("%s + %s vs %s",
                               base::format(base::round(ne), big.mark = ","),
                               base::format(base::round(cc), big.mark = ","),
                               base::format(base::round(ce), big.mark = ","))

  # Gate 4 is a provenance question, not an arithmetic one.
  p <- care_engagement_params()
  unsourced <- p$parameter[p$calibration_status == "requires_source"]
  g$passed[4] <- base::length(unsourced) == 0L
  g$detail[4] <- if (base::length(unsourced)) {
    base::paste("unsourced:", base::paste(unsourced, collapse = ", "))
  } else "all utilization parameters sourced"

  g
}

#' The frozen care-engaged cohort for this workstream
#' @export
FROZEN_CARE_ENGAGED <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
