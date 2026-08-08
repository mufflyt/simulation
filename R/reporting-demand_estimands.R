# Demand estimands, and why one calibration status cannot serve them all.
#
# THE PROBLEM WITH A SINGLE STATUS
#
# `calibration_status` is currently one string per component, so a component is
# calibrated or it is not. That collapses quantities supported by entirely
# different evidence into a single verdict, and it produces two opposite errors
# at once:
#
#   - It BLOCKS reporting of quantities we can actually support. Realized-care
#     demand needs disease burden and care-seeking; it does not need a baseline
#     adequacy estimate. Gating it on adequacy withholds a defensible result.
#   - It would LICENSE reporting of quantities we cannot support, the moment one
#     anchor lands. A national procedure count could flip a single status to
#     `calibrated` while baseline unmet need remains entirely unmeasured.
#
# THE WORKFORCE LITERATURE ALREADY SPLIT THESE
#
# HRSA's older approach set the base-year staffing ratio to observed services
# over observed workforce, which assumes whatever care is currently delivered is
# the correct amount. Specialty projections inheriting that assumption start at
# exactly 100% adequacy by construction.
#
# The Dall lineage moved away from it. The physical-therapy model measured
# baseline adequacy SEPARATELY, from a provider survey about spare capacity,
# overtime, and patients who could not be accommodated -- the instrument
# reproduced in `CAPACITY_SURVEY_TEMPLATE` -- and got 94.8% rather than assuming
# 100%. The physiatry model did the same and inferred a 10.6% baseline shortage.
# Neither derived adequacy from procedure counts, because utilization cannot
# distinguish "enough clinicians" from "as much care as the clinicians could
# deliver".
#
# HRSA and AAMC now go further, maintaining a status-quo scenario alongside a
# reduced-barrier counterfactual in which people facing access barriers are
# assigned the utilization of otherwise similar people who face fewer. AAMC
# deliberately does NOT fold that counterfactual into its headline shortage
# range; it reports it as a separate lens on adequacy.
#
# This module encodes that separation: four evidence dimensions, three
# estimands, and a rule that an estimand is reportable only when every dimension
# it depends on is. Nothing here estimates anything -- it decides what may be
# claimed.
#
# THE NON-INHERITANCE RULE
#
# No dimension inherits credibility from another. Evidence about one must never
# raise the standing of a different one. Concretely:
#
#   realized_care = calibrated   must never promote   baseline_adequacy
#   strong access_barriers       must never convert a DIRECTIONAL adequacy
#                                concern into an identified MAGNITUDE
#
# Without that rule the split would be decoration: one well-anchored utilization
# dataset would quietly license every downstream claim, which is the "assume the
# base year is in equilibrium" move the Dall lineage moved away from. The
# independence is a property of the design -- each dimension carries its own
# status and the weakest required one governs -- and
# test-demand-estimands.R proves it over every ordered pair of dimensions rather
# than trusting the design to hold as the code changes.

#' Evidence dimensions underpinning demand
#'
#' Distinct quantities supported by distinct evidence. Collapsing them into one
#' `calibration_status` is what this module exists to stop.
#'
#' @format Character vector.
#' @family baseline gap reporting
#' @concept reporting
#' @export
DEMAND_CALIBRATION_DIMENSIONS <- c(
  "disease_burden",    # prevalence/incidence of UI, POP, AI in the population
  "care_seeking",      # P(seeks care | condition) and resulting utilization
  "access_barriers",   # P(obtains appointment), payer acceptance, travel time
  "baseline_adequacy"  # spare capacity of the existing workforce
)

#' What each demand estimand requires
#'
#' \describe{
#'   \item{realized_care}{"How many services if current utilization patterns
#'     continue?" HRSA's status quo. Claims and survey utilization are
#'     legitimate calibration targets. Needs no adequacy estimate.}
#'   \item{reduced_barrier}{"How much care if women with poorer access behaved
#'     like otherwise similar women with better access?" The HRSA/AAMC equity
#'     counterfactual. Additionally needs an access model.}
#'   \item{adequate_need}{"How many FTE to meet clinically appropriate need?"
#'     Requires baseline adequacy, which utilization cannot identify.}
#' }
#'
#' @format Named list of character vectors.
#' @family baseline gap reporting
#' @concept reporting
#' @export
DEMAND_ESTIMANDS <- list(
  realized_care   = c("disease_burden", "care_seeking"),
  reduced_barrier = c("disease_burden", "care_seeking", "access_barriers"),
  adequate_need   = c("disease_burden", "care_seeking", "access_barriers",
                      "baseline_adequacy")
)

#' Reportability of a demand estimand given per-dimension calibration
#'
#' The weakest required dimension governs, as it does elsewhere in the contract:
#' an estimand is only as trustworthy as the flimsiest evidence it rests on.
#'
#' @param dimension_status Named character vector keyed by
#'   `DEMAND_CALIBRATION_DIMENSIONS`. Missing or unrecognised dimensions are
#'   treated as uncalibrated, so silence never buys reportability.
#' @param estimand One of `names(DEMAND_ESTIMANDS)`.
#' @return An object of class `urps_demand_estimand_status`.
#' @family baseline gap reporting
#' @concept reporting
#' @export
demand_estimand_status <- function(dimension_status, estimand) {
  estimand <- match.arg(estimand, names(DEMAND_ESTIMANDS))
  required <- DEMAND_ESTIMANDS[[estimand]]

  if (is.null(dimension_status)) dimension_status <- character(0)
  if (is.null(names(dimension_status)) && length(dimension_status)) {
    stop("demand_estimand_status: `dimension_status` must be named by dimension.",
         call. = FALSE)
  }
  unknown <- setdiff(names(dimension_status), DEMAND_CALIBRATION_DIMENSIONS)
  if (length(unknown)) {
    stop("demand_estimand_status: unrecognised dimension(s): ",
         paste(sQuote(unknown), collapse = ", "), ". Known: ",
         paste(DEMAND_CALIBRATION_DIMENSIONS, collapse = ", "), call. = FALSE)
  }

  # Undeclared is uncalibrated. Fail closed, as everywhere else in this layer.
  # `[[` THROWS on a missing name for a named vector -- it does not return NULL,
  # so `%||%` never fires and an undeclared dimension crashes instead of failing
  # closed. Single-bracket returns NA, which is what "undeclared" means here.
  status <- vapply(required, function(d) {
    s <- if (d %in% names(dimension_status)) unname(dimension_status[d]) else NA_character_
    if (length(s) != 1L || is.na(s)) "undeclared" else as.character(s)
  }, character(1))

  rank <- CALIBRATION_STATUS_RANK[status]
  rank[is.na(rank)] <- 0L                       # undeclared / unknown string
  weakest <- required[which.min(rank)]
  reportable <- min(rank) >= CALIBRATION_STATUS_RANK[[REPORTABLE_MIN_CALIBRATION]]

  blocking <- required[rank < CALIBRATION_STATUS_RANK[[REPORTABLE_MIN_CALIBRATION]]]
  note <- if (reportable) NA_character_ else sprintf(
    paste("Estimand '%s' is NOT reportable: %s below the '%s' tier (%s).",
          "The weakest required dimension governs."),
    estimand,
    paste(blocking, collapse = ", "),
    REPORTABLE_MIN_CALIBRATION,
    paste(sprintf("%s=%s", blocking, status[blocking]), collapse = "; "))

  out <- list(
    estimand = estimand,
    required_dimensions = required,
    dimension_status = status,
    weakest_dimension = weakest,
    reportable = reportable,
    note = note
  )
  class(out) <- c("urps_demand_estimand_status", class(out))
  out
}

#' @export
print.urps_demand_estimand_status <- function(x, ...) {
  cat(sprintf("Demand estimand: %s -- %s\n", x$estimand,
              if (x$reportable) "REPORTABLE" else "NOT REPORTABLE"))
  for (d in x$required_dimensions)
    cat(sprintf("  %-18s %s\n", d, x$dimension_status[[d]]))
  if (!is.na(x$note)) cat("  ", x$note, "\n", sep = "")
  invisible(x)
}

#' Reportability of every estimand at once
#'
#' The table to put in front of a reader: which questions the current evidence
#' can answer, and which it cannot. A model that can report realized care but
#' not adequate need is in a perfectly respectable position, and saying so is
#' more useful than a single global "uncalibrated".
#'
#' @param dimension_status Named character vector, as for
#'   [demand_estimand_status()].
#' @return A tibble, one row per estimand.
#' @family baseline gap reporting
#' @concept reporting
#' @export
demand_estimand_table <- function(dimension_status) {
  rows <- lapply(names(DEMAND_ESTIMANDS), function(e) {
    s <- demand_estimand_status(dimension_status, e)
    tibble::tibble(
      estimand = e,
      reportable = s$reportable,
      weakest_dimension = s$weakest_dimension,
      weakest_status = unname(s$dimension_status[[s$weakest_dimension]]),
      required = paste(s$required_dimensions, collapse = ", ")
    )
  })
  do.call(rbind, rows)
}
