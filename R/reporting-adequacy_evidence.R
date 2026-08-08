# Base-year adequacy: naming it an assumption, and reporting the evidence that
# bears on it without pretending that evidence identifies it.
#
# WHY THIS FILE EXISTS
#
# 0.948 does more scientific work than its provenance supports. It is the
# base-year adequacy: supply / demand in the base year. Required FTE is
# `anchor x wRVU(t)/wRVU(base)`, so the anchor sets the LEVEL of demand for
# every projected year, and the projected gap is linear in it. It is the only
# input that can change the SIGN of the 2050 conclusion -- the delegation matrix
# and the demand calibration cancel out of the ratio entirely.
#
# And it was never measured in this specialty. It is the physical-therapy value
# from the Zarek 2025 workforce model (the physiatry analogue is 0.894), carried
# across on the assumption that the two specialties are similar enough. That is
# an assumption. Naming it `REFERENCE_ADEQUACY_CALIBRATION` rather than "the
# base-year adequacy estimate" is not cosmetic: an estimate invites a confidence
# interval and a p-value, and this quantity is entitled to neither.
#
# THE DISCIPLINE THIS FILE ENFORCES
#
# Audit evidence -- mystery-caller wait times, appointment obtainment -- can show
# that realized access moved in a direction inconsistent with an assumption. It
# cannot invert to a required-FTE level. Two cross-sectional audits with
# different sampling frames do not identify a capacity parameter, and confounds
# (pandemic backlog, scheduling practice, frame differences) are not separable
# from capacity with two points.
#
# So `adequacy_evidence_table()` refuses free-text interpretation. Every row must
# choose from a controlled vocabulary that is directional or explicitly
# non-identifying. There is deliberately no vocabulary term meaning "therefore
# the anchor is 1.5x": that sentence is not available, because the evidence
# cannot support it.

#' Reference adequacy calibration
#'
#' The base-year adequacy the projection is specified at. NOT an estimate for
#' this specialty: no validated national URPS adequacy figure exists. This is
#' the physical-therapy value from the Zarek 2025 workforce model, adopted by
#' analogy, and should be reported as a calibration choice with a sensitivity
#' analysis attached -- never as a measured property of the URPS workforce.
#'
#' @format Numeric scalar.
#' @family baseline gap reporting
#' @concept reporting
#' @export
REFERENCE_ADEQUACY_CALIBRATION <- 0.948

#' Published adequacy analogues from other specialties
#'
#' The values the reference calibration could have been drawn from. Their spread
#' (0.894 to 0.948) is a floor on the uncertainty in the reference calibration,
#' not the whole of it: both are analogues, so agreement between them is not
#' evidence about URPS.
#'
#' @format Named numeric vector.
#' @family baseline gap reporting
#' @concept reporting
#' @export
ADEQUACY_ANALOGUES <- c(physical_therapy = 0.948, physiatry = 0.894)

# Controlled vocabulary for the interpretation column. Directional or
# non-identifying only -- see the header note on why no term asserts a level.
ADEQUACY_INTERPRETATIONS <- c(
  "consistent_with_under_calibration",
  "consistent_with_over_calibration",
  "consistent_with_reference_calibration",
  "headcount_not_effective_capacity",
  "not_identifiable_from_this_evidence",
  "stress_scenario_not_an_estimate"
)

#' Evidence bearing on the base-year adequacy calibration
#'
#' Assembles the evidence for and against the reference adequacy calibration as
#' a table, rather than collapsing it into a single "gap". The point is that the
#' evidence constrains the DIRECTION of any mis-calibration and not its
#' magnitude, and a single number cannot say that.
#'
#' @param model_implication Character vector: what the model implies under each
#'   row's calibration.
#' @param observed Character vector: the external observation, or an explicit
#'   statement that none is identifiable.
#' @param interpretation Character vector drawn from
#'   `ADEQUACY_INTERPRETATIONS`. Free text is refused.
#' @param evidence Character vector naming each row's evidence stream.
#' @param citation Character vector; required for any row whose `observed` is
#'   not the non-identifiable sentinel `NA_character_`.
#' @return A tibble of class `urps_adequacy_evidence`.
#' @family baseline gap reporting
#' @concept reporting
#' @export
adequacy_evidence_table <- function(evidence, model_implication, observed,
                                    interpretation, citation = NA_character_) {
  n <- length(evidence)
  stopifnot(is.character(evidence), n > 0L)
  recycle <- function(x, nm) {
    if (length(x) == 1L) x <- rep(x, n)
    if (length(x) != n) stop(sprintf("adequacy_evidence_table: `%s` must be length 1 or %d.", nm, n),
                             call. = FALSE)
    x
  }
  model_implication <- recycle(as.character(model_implication), "model_implication")
  observed          <- recycle(as.character(observed), "observed")
  interpretation    <- recycle(as.character(interpretation), "interpretation")
  citation          <- recycle(as.character(citation), "citation")

  bad <- setdiff(interpretation, ADEQUACY_INTERPRETATIONS)
  if (length(bad)) {
    stop("adequacy_evidence_table: interpretation must come from the controlled ",
         "vocabulary, which is directional or non-identifying by design. ",
         "Rejected: ", paste(sQuote(bad), collapse = ", "), ". Allowed: ",
         paste(ADEQUACY_INTERPRETATIONS, collapse = ", "),
         ". If you want to say the anchor IS some value, that sentence is not ",
         "available from access evidence -- field a capacity survey.", call. = FALSE)
  }

  # An observation without attribution is an assertion. The non-identifiable
  # rows are exempt precisely because they claim no observation.
  needs_cite <- !is.na(observed) & is.na(citation)
  if (any(needs_cite)) {
    stop("adequacy_evidence_table: rows with an observation require a citation. ",
         "Offending evidence: ", paste(evidence[needs_cite], collapse = ", "),
         call. = FALSE)
  }

  out <- tibble::tibble(
    evidence = evidence,
    model_implication = model_implication,
    observed_evidence = observed,
    interpretation = interpretation,
    citation = citation
  )
  class(out) <- c("urps_adequacy_evidence", class(out))
  out
}

#' @export
print.urps_adequacy_evidence <- function(x, ...) {
  cat("Evidence bearing on the base-year adequacy calibration\n")
  cat("(direction only; none of this identifies the anchor's level)\n\n")
  NextMethod()
  invisible(x)
}

#' Is any row inconsistent with the reference calibration?
#'
#' @param tab An [adequacy_evidence_table()].
#' @return Logical.
#' @family baseline gap reporting
#' @concept reporting
#' @export
adequacy_evidence_is_discordant <- function(tab) {
  stopifnot(inherits(tab, "urps_adequacy_evidence"))
  any(tab$interpretation %in% c("consistent_with_under_calibration",
                                "consistent_with_over_calibration",
                                "headcount_not_effective_capacity"))
}

#' One-sentence conclusion licensed by an adequacy evidence table
#'
#' Deliberately conditional. The sentence says the long-run result depends on an
#' assumption about current unmet need; it never says what that assumption
#' should be.
#'
#' @param tab An [adequacy_evidence_table()].
#' @param flip_multiplier Anchor multiplier at which the projected conclusion
#'   reverses, from [baseline_anchor_sensitivity()].
#' @return A single sentence.
#' @family baseline gap reporting
#' @concept reporting
#' @export
adequacy_conclusion_sentence <- function(tab, flip_multiplier = NA_real_) {
  stopifnot(inherits(tab, "urps_adequacy_evidence"))
  flip <- if (is.finite(flip_multiplier))
    sprintf(" A calibration %.2fx the reference value reverses the projected sign.",
            flip_multiplier) else ""
  disc <- if (adequacy_evidence_is_discordant(tab))
    paste("External access evidence moved in a direction inconsistent with",
          "treating provider counts alone as establishing adequate effective",
          "capacity, though it does not identify the correct calibration.")
  else
    "External access evidence is not discordant with the reference calibration."
  paste0("The direction of the long-run workforce result is robust only ",
         "conditional on the assumed base-year adequacy; no validated national ",
         "URPS adequacy estimate exists. ", disc, flip)
}
