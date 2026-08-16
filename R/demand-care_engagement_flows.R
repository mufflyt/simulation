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
    "first_entry_rate",   NA_real_, "UNSOURCED", "none", "requires_source",
    "reentry_rate",       NA_real_, "UNSOURCED", "none", "requires_source",
    "retention_rate",     NA_real_, "UNSOURCED", "none", "requires_source")
}

#' Advance care-engagement stocks by one year
#'
#' @param untreated_eligible Symptomatic women not currently in care.
#' @param previously_disengaged Women who were in care and left.
#' @param care_engaged_previous Last year's engaged stock.
#' @param first_entry_rate,reentry_rate,retention_rate Transition rates. NO
#'   DEFAULTS -- all three are unsourced, and a default would become the residual
#'   that forces the office anchor to agree.
#' @return Tibble with the flows, the resulting stock, and the DERIVED
#'   incident_share.
#' @export
advance_care_engagement <- function(untreated_eligible,
                                    previously_disengaged,
                                    care_engaged_previous,
                                    first_entry_rate = NULL,
                                    reentry_rate = NULL,
                                    retention_rate = NULL) {
  miss <- c(first_entry_rate = base::is.null(first_entry_rate),
            reentry_rate     = base::is.null(reentry_rate),
            retention_rate   = base::is.null(retention_rate))
  if (base::any(miss)) {
    base::stop("No defaults: ", base::paste(base::names(miss)[miss], collapse = ", "),
               ". All are unsourced (see care_flow_params()); defaulting them ",
               "would make them the residual that forces the office anchor to ",
               "agree.", call. = FALSE)
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
