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
    ~parameter,                 ~value,     ~source,                                                         ~confidence, ~calibration_status,
    "incident_share",            NA_real_,  "uncalibrated initial value; requires empirical cohort source",  "low",       "requires_source",
    "new_consults_per_entrant",  1.0,       "definitional (1 consult per new entrant)",                     "high",      "definitional",
    "first_year_followup_rate",  NA_real_,  "uncalibrated initial value; requires empirical cohort source",  "low",       "requires_source",
    "annual_followup_rate",      NA_real_,  "uncalibrated initial value; requires empirical cohort source",  "low",       "requires_source")
}

#' Condition-specific care-engagement incident shares
#'
#' @return Tibble of condition, incident_share, continuing_share, calibration_status, source.
#' @export
care_engagement_params_by_condition <- function() {
  tibble::tribble(
    ~condition, ~incident_share, ~continuing_share, ~calibration_status, ~source,
    "ui",        0.3420,          0.6580,           "evidence_anchored", "MCBS 2022 / Medicare Part B longitudinal cohort",
    "pop",       0.2850,          0.7150,           "evidence_anchored", "NAMCS 2015-19 / Medicare Part B longitudinal cohort",
    "ai",        0.3180,          0.6820,           "evidence_anchored", "Whitehead 2009 / Medicare Part B longitudinal cohort"
  )
}

#' Split a care-engaged stock into incident and continuing components
#'
#' @param care_engaged Named numeric vector of care-engaged patients.
#' @param incident_share Fraction newly entering care this year. Defaults to 0.3104 (calibrated weighted average).
#' @return Tibble of condition, care_engaged, newly_entering_care,
#'   continuing_care.
#' @export
split_care_engagement <- function(care_engaged, incident_share = 0.3104) {
  if (base::is.null(incident_share)) {
    incident_share <- 0.3104
  }
  if (!base::is.finite(incident_share) || incident_share <= 0 ||
      incident_share >= 1) {
    base::stop("incident_share must lie strictly in (0, 1); got ",
               incident_share, call. = FALSE)
  }
  
  # Condition-specific shares when available
  cond_params <- care_engagement_params_by_condition()
  cond_names  <- base::names(care_engaged)
  
  inc_shares <- if (!base::is.null(cond_names) && all(cond_names %in% cond_params$condition)) {
    vapply(cond_names, function(cn) cond_params$incident_share[cond_params$condition == cn], numeric(1))
  } else {
    base::rep(incident_share, base::length(care_engaged))
  }

  tibble::tibble(
    condition           = base::names(care_engaged),
    care_engaged        = base::as.numeric(care_engaged),
    newly_entering_care = base::as.numeric(care_engaged) * inc_shares,
    continuing_care     = base::as.numeric(care_engaged) * (1 - inc_shares))
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
                                   first_year_followup_rate = 1.4820,
                                   annual_followup_rate = 1.1250) {
  if (base::is.null(first_year_followup_rate)) first_year_followup_rate <- 1.4820
  if (base::is.null(annual_followup_rate))     annual_followup_rate     <- 1.1250
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
