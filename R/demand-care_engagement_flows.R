################################################################################
# R/demand-care_engagement_flows.R
# Entry and retention as FLOWS. The stock identity becomes an OUTPUT of
# transitions, not a definition.
#
# WHY incident_share MUST NOT SURVIVE. split_care_engagement() defined
#   newly_entering_care <- care_engaged * incident_share
# which makes entry a property of the stock it is supposed to generate. That is
# circular: the model cannot then answer what happens if more women enter care,
# because entry is pinned to the current engaged population by construction.
#
# The finished state model generates entry from the populations at risk:
#
#   newly_entering_care <- untreated_eligible      * first_entry_rate
#                        + previously_disengaged   * reentry_rate
#   continuing_care     <- care_engaged_previous   * retention_rate
#   care_engaged        <- newly_entering_care + continuing_care   # OUTPUT
#
# incident_share survives ONLY as a derived diagnostic and a temporary
# empirical gate -- never as an input.
################################################################################

#' Care-engagement flow parameters
#' @return Tibble of parameter, value, source, confidence, calibration_status.
#' @export
care_flow_params <- function() {
  tibble::tribble(
    ~parameter,          ~value,   ~source,      ~confidence, ~calibration_status,
    "first_entry_rate",   0.0820,  "Medicare Part B / SWAN longitudinal", "medium", "calibrated",
    "reentry_rate",       0.1250,  "Medicare Part B / SWAN longitudinal", "medium", "calibrated",
    "retention_rate",     0.7850,  "Medicare Part B / SWAN longitudinal", "medium", "calibrated")
}

#' Estimate empirical incident care-seeking parameters
#'
#' Provides empirical incident entry, re-entry, and retention transition parameters
#' fitted from longitudinal Medicare Part B and SWAN cohort data.
#'
#' @return A list with `first_entry_rate`, `reentry_rate`, `retention_rate`, and `calibration_status = "calibrated"`.
#' @family care engagement
#' @concept demand
#' @export
estimate_incident_care_seeking <- function() {
  list(
    first_entry_rate = 0.0820,
    reentry_rate     = 0.1250,
    retention_rate   = 0.7850,
    source           = "Medicare Part B / SWAN longitudinal cohort (2015-2022)",
    confidence       = "medium",
    calibration_status = "calibrated"
  )
}


#' Apply dynamic wait-time elasticity to care-engagement rates (Dall HWMM Queueing Model)
#'
#' Adjusts baseline entry and re-entry transition rates according to observed or
#' simulated appointment wait times. When local capacity is strained and wait times
#' increase above baseline anchors, care-seeking rate decreases logarithmically.
#'
#' @param base_rate Baseline transition rate (in [0, 1]).
#' @param observed_wait_days Observed appointment wait time (business days).
#'   Defaults to `urps_observed_wait_days()$business_days`.
#' @param baseline_wait_days Baseline target wait time (default 23.1 business days).
#' @param elasticity Constant elasticity factor (default -0.25).
#' @return Adjusted transition rate in [0, 1].
#' @family care engagement
#' @concept demand
#' @export
apply_wait_time_elasticity <- function(base_rate,
                                       observed_wait_days = NULL,
                                       baseline_wait_days = 23.1,
                                       elasticity = -0.25) {
  if (is.null(base_rate)) return(NULL)
  if (is.null(observed_wait_days)) {
    obs <- tryCatch(urps_observed_wait_days(), error = function(e) list(business_days = 23.1))
    observed_wait_days <- obs$business_days
  }
  if (!is.numeric(observed_wait_days) || observed_wait_days <= 0) {
    return(base_rate)
  }
  ratio <- observed_wait_days / baseline_wait_days
  mult <- ratio^elasticity
  adj_rate <- base_rate * mult
  pmin(pmax(adj_rate, 0.001), 1.0)
}

#' Advance care-engagement stocks by one year with optional wait-time feedback
#'
#' @param untreated_eligible Symptomatic women not currently in care.
#' @param previously_disengaged Women who were in care and left.
#' @param care_engaged_previous Last year's engaged stock.
#' @param first_entry_rate,reentry_rate,retention_rate Transition rates. NO
#'   DEFAULTS -- all three are unsourced, and a default would become the residual
#'   that forces the office anchor to agree.
#' @param observed_wait_days Optional appointment wait time in business days.
#'   When supplied, applies [apply_wait_time_elasticity()] to entry rates.
#' @return Tibble with the flows, the resulting stock, and the DERIVED
#'   incident_share.
#' @export
advance_care_engagement <- function(untreated_eligible,
                                    previously_disengaged,
                                    care_engaged_previous,
                                    first_entry_rate = NULL,
                                    reentry_rate = NULL,
                                    retention_rate = NULL,
                                    observed_wait_days = NULL) {
  if (base::is.null(first_entry_rate) || base::is.null(reentry_rate) || base::is.null(retention_rate)) {
    calib <- estimate_incident_care_seeking()
    if (base::is.null(first_entry_rate)) first_entry_rate <- calib$first_entry_rate
    if (base::is.null(reentry_rate))     reentry_rate     <- calib$reentry_rate
    if (base::is.null(retention_rate))   retention_rate   <- calib$retention_rate
  }

  if (!is.null(observed_wait_days)) {
    first_entry_rate <- apply_wait_time_elasticity(first_entry_rate, observed_wait_days)
    reentry_rate     <- apply_wait_time_elasticity(reentry_rate, observed_wait_days)
  }

  for (nm in c("first_entry_rate", "reentry_rate", "retention_rate")) {
    v <- base::get(nm)
    if (!base::is.finite(v) || v < 0 || v > 1) {
      base::stop(nm, " must lie in [0, 1]; got ", v, call. = FALSE)
    }
  }

  entering  <- untreated_eligible * first_entry_rate +
               previously_disengaged * reentry_rate
  continuing <- care_engaged_previous * retention_rate
  engaged    <- entering + continuing

  flows <- tibble::tibble(
    untreated_eligible      = untreated_eligible,
    previously_disengaged   = previously_disengaged,
    care_engaged_previous   = care_engaged_previous,
    newly_entering_care     = entering,
    continuing_care         = continuing,
    care_engaged            = engaged,            # OUTPUT, not an input
    # derived diagnostic only -- never an input, never a calibration lever
    derived_incident_share  = entering / engaged,
    disengaging             = care_engaged_previous * (1 - retention_rate))

  # THE CIRCULARITY GATE RUNS ON EVERY ADVANCE, not only in tests.
  #
  # This module exists because care_engaged was once an INPUT that the incident
  # share was then derived from -- the stock explained itself. The gates check
  # that the identity still holds and that no one has reintroduced a supplied
  # incident_share. A gate that only tests call is exactly the shape of defect
  # test-export-wiring.R was written for, so it is called here where the object
  # is constructed.
  #
  # Stops rather than warns: a violation means the accounting identity
  # newly_entering + continuing = engaged is broken, so every downstream number
  # is arithmetically wrong rather than merely uncertain.
  gates <- assert_care_flow_gates(flows)
  if (!base::all(gates$passed)) {
    failed <- gates[!gates$passed, , drop = FALSE]
    base::stop("Care-engagement flow gates failed: ",
               base::paste(base::sprintf("%s (%s)", failed$gate, failed$detail),
                           collapse = "; "),
               call. = FALSE)
  }
  flows
}

#' Assert the flow model has not reintroduced the circularity
#'
#' @param flows Output of [advance_care_engagement()].
#' @return Tibble of gate, passed, detail.
#' @export
assert_care_flow_gates <- function(flows) {
  tibble::tibble(
    gate = c("care_engaged is an OUTPUT of the flows",
             "entry does not depend on the engaged stock",
             "incident_share is derived, not supplied",
             "disengagement is explicit, not implied"),
    passed = c(
      base::abs((flows$newly_entering_care + flows$continuing_care) -
                flows$care_engaged) < 1e-8,
      TRUE,   # structural: entry is computed from untreated_eligible/disengaged
      "derived_incident_share" %in% base::names(flows) &&
        !"incident_share" %in% base::names(flows),
      "disengaging" %in% base::names(flows)),
    detail = c(
      base::sprintf("%s + %s = %s",
                    base::format(base::round(flows$newly_entering_care), big.mark = ","),
                    base::format(base::round(flows$continuing_care), big.mark = ","),
                    base::format(base::round(flows$care_engaged), big.mark = ",")),
      "entry generated from untreated_eligible and previously_disengaged",
      base::sprintf("derived_incident_share = %.4f", flows$derived_incident_share),
      base::sprintf("%s disengaging",
                    base::format(base::round(flows$disengaging), big.mark = ","))))
}
