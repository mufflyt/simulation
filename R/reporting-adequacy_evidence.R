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

#' Provenance classes for an adequacy evidence row
#'
#' Kept separate from the interpretation vocabulary because they answer
#' different questions: interpretation says what a row means, `evidence_type`
#' says where it came from. Only an `empirical_observation` may carry an
#' observation of the world.
#'
#' @format Character vector.
#' @family baseline gap reporting
#' @concept reporting
#' @export
ADEQUACY_EVIDENCE_TYPES <- c("empirical_observation", "model_implication", "assumption")

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
#' @param evidence_type Character vector from `ADEQUACY_EVIDENCE_TYPES`. A
#'   `model_implication` or `assumption` row may NOT carry an observation: that
#'   is how a simulation result gets mistaken for external evidence.
#' @param citation Character vector; required for any row whose `observed` is
#'   not the non-identifiable sentinel `NA_character_`.
#' @return A tibble of class `urps_adequacy_evidence`.
#' @family baseline gap reporting
#' @concept reporting
#' @export
adequacy_evidence_table <- function(evidence, model_implication, observed,
                                    interpretation, citation = NA_character_,
                                    evidence_type = "empirical_observation") {
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
  evidence_type     <- recycle(as.character(evidence_type), "evidence_type")

  bad_type <- setdiff(evidence_type, ADEQUACY_EVIDENCE_TYPES)
  if (length(bad_type)) {
    stop("adequacy_evidence_table: evidence_type must be one of ",
         paste(ADEQUACY_EVIDENCE_TYPES, collapse = ", "), ". Rejected: ",
         paste(sQuote(bad_type), collapse = ", "), call. = FALSE)
  }

  # A model output is not an observation of the world. Without this, a row
  # reading "2050 shortage ~326 FTE" could be filed under observed evidence and
  # the table would appear to corroborate itself.
  self_citing <- evidence_type %in% c("model_implication", "assumption") & !is.na(observed)
  if (any(self_citing)) {
    stop("adequacy_evidence_table: a ", paste(unique(evidence_type[self_citing]), collapse = "/"),
         " row cannot carry an external observation -- that presents a simulation ",
         "result as evidence about the world. Offending evidence: ",
         paste(evidence[self_citing], collapse = ", "), call. = FALSE)
  }

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
    evidence_type = evidence_type,
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
#' Deliberately conditional, and asymmetric. Access evidence can raise concern
#' that the reference calibration understates effective unmet need; it cannot
#' say by how much. The sentence never ends on "consistent with
#' under-calibration" without immediately carrying that limit, because a reader
#' who stops at the comma has been misled.
#'
#' @param tab An [adequacy_evidence_table()].
#' @param flip_multiplier Multiplier at which the projected conclusion reverses,
#'   from [balance_reversal_threshold()]. Reported as a threshold, never as an
#'   estimate of baseline need.
#' @param no_estimate_citation Citation supporting the claim that no validated
#'   national URPS adequacy estimate exists. This is a LITERATURE claim, not a
#'   property of the model, so asserting it flatly requires attribution. Without
#'   one the sentence degrades to a first-person statement of what the authors
#'   are aware of, which is weaker and honest.
#' @return A single sentence, validated to contain no identifying language.
#' @family baseline gap reporting
#' @concept reporting
#' @export
adequacy_conclusion_sentence <- function(tab, flip_multiplier = NA_real_,
                                         no_estimate_citation = NA_character_) {
  stopifnot(inherits(tab, "urps_adequacy_evidence"))

  # Attributed claim vs first-person awareness. "No validated estimate exists"
  # asserts a fact about the literature; "we are not aware of one" asserts a
  # fact about the authors. Only the second is free.
  absence <- if (!is.na(no_estimate_citation) && nzchar(no_estimate_citation))
    sprintf("no validated national URPS adequacy estimate exists (%s)", no_estimate_citation)
  else
    "we are not aware of a validated national URPS adequacy estimate"

  flip <- if (is.finite(flip_multiplier))
    sprintf(paste(" The projected balance changes sign when assumed baseline need",
                  "is approximately %.0f%% greater than the reference calibration;",
                  "that is a threshold, not an estimate of baseline need."),
            100 * (flip_multiplier - 1)) else ""

  # THE ASYMMETRY. Concern and magnitude are separate clauses, joined so that
  # neither can be quoted without the other.
  disc <- if (adequacy_evidence_is_discordant(tab))
    paste("Observed deterioration in appointment access raises concern that the",
          "reference calibration may understate effective unmet need, but it",
          "cannot establish the magnitude of any such under-calibration:",
          "cross-sectional access evidence does not invert to a required-FTE",
          "level.")
  else
    "External access evidence is not discordant with the reference calibration."

  .assert_no_identifying_language(
    paste0("The direction of the long-run workforce result is robust only ",
           "conditional on the assumed base-year adequacy; ", absence, ". ",
           disc, flip),
    "adequacy_conclusion_sentence()")
}

# ---- Balance-reversal threshold -------------------------------------------
#
# The model cannot discover the adequacy parameter. It can answer a cleaner
# question: how wrong would the reference calibration have to be before the
# substantive conclusion changes? That is a tipping point, and it is reportable
# without claiming to know current unmet need.
#
# Three guardrails, all enforced below rather than left to the writer:
#
#   1. CONTINUOUS. The threshold is solved in closed form, never snapped to the
#      nearest prespecified scenario. If the crossing sits at 1.37 and the
#      scenario grid contains 1.25 and 1.5, reporting "1.5x flips it" invites
#      the reader to treat 1.5x as secretly favoured because it happens to sit
#      near the crossing. The scenarios stay prespecified; the threshold is
#      derived and separate.
#   2. EXPRESSED AS A DEVIATION. The output is a percentage departure from the
#      reference calibration, not a new adequacy value. An adequacy number
#      reads as a rival estimate; a deviation reads as what it is -- a statement
#      about how much error the conclusion tolerates.
#   3. NA WITH A REASON. If no sign reversal occurs across the examined range,
#      the threshold is NA and carries an explanation. Silently returning the
#      range endpoint would manufacture a tipping point that does not exist.

#' Adequacy multiplier at which the projected balance changes sign
#'
#' Required FTE is `anchor x demand_growth`, so the projected balance is linear
#' in the anchor and the crossing is closed-form. This is a tipping-point
#' analysis and belongs in main Results, not in a sensitivity appendix.
#'
#' It is NOT an estimate of baseline adequacy. It says how far the reference
#' calibration would have to be wrong, in which direction, before the policy
#' conclusion flips. A result of 1.37 licenses "the balance reversed when
#' assumed baseline need was about 37% greater than the reference calibration",
#' and never "baseline need is 37% greater".
#'
#' @param gap A [baseline_gap()] object supplying the reference anchor.
#' @param supply_at_target Projected supply FTE at the horizon.
#' @param demand_growth Ratio of target-year to base-year demand.
#' @param examined_range Multiplier range searched for a sign change.
#' @return An object of class `urps_balance_reversal`.
#' @family baseline gap reporting
#' @concept reporting
#' @export
balance_reversal_threshold <- function(gap, supply_at_target, demand_growth,
                                       examined_range = c(0.5, 3)) {
  stopifnot(inherits(gap, "urps_baseline_gap"),
            is.numeric(supply_at_target), length(supply_at_target) == 1L,
            is.numeric(demand_growth), length(demand_growth) == 1L, demand_growth > 0,
            is.numeric(examined_range), length(examined_range) == 2L,
            examined_range[1] < examined_range[2], examined_range[1] > 0)

  anchor <- gap$required_fte
  balance_at <- function(mult) supply_at_target - anchor * mult * demand_growth

  # Closed form: supply = anchor * m * growth  =>  m = supply / (anchor * growth)
  mult <- supply_at_target / (anchor * demand_growth)

  at_reference <- balance_at(1)
  in_range <- is.finite(mult) && mult >= examined_range[1] && mult <= examined_range[2]

  direction <- if (!in_range) NA_character_
               else if (at_reference > 0) "surplus_to_shortage"
               else if (at_reference < 0) "shortage_to_surplus"
               else "balanced_at_reference"

  note <- if (in_range) NA_character_ else sprintf(
    paste("No sign reversal occurs for adequacy multipliers in [%.2f, %.2f].",
          "The projected balance is %s across the whole examined range, so there",
          "is no tipping point to report. The crossing implied by the linear",
          "form lies at %.2f, outside the range considered plausible; reporting",
          "it would assert a tipping point the analysis did not examine."),
    examined_range[1], examined_range[2],
    if (at_reference > 0) "a surplus" else "a shortage", mult)

  out <- list(
    reference_anchor_fte = anchor,
    balance_at_reference = at_reference,
    # The headline. NA when no reversal is examined, never snapped to a scenario.
    reversal_multiplier = if (in_range) mult else NA_real_,
    reversal_pct_deviation = if (in_range) 100 * (mult - 1) else NA_real_,
    direction = direction,
    examined_range = examined_range,
    reversal_within_examined_range = in_range,
    # Deliberately named to refuse being read as a rival estimate.
    implied_adequacy_at_reversal_NOT_AN_ESTIMATE =
      if (in_range && is.finite(gap$base_supply_fte)) gap$base_supply_fte / (anchor * mult)
      else NA_real_,
    note = note
  )
  class(out) <- c("urps_balance_reversal", class(out))
  out
}

# Verbs that would convert a conditional statement into an identifying one. The
# sentence builders below refuse to emit any of them, so a later edit to a
# template fails loudly instead of quietly upgrading the claim.
FORBIDDEN_INFERENCE_VERBS <- c(
  "demonstrates", "demonstrate", "proves", "prove", "proven",
  "establishes", "establish", "confirms", "confirm",
  "identifies", "identify", "determines", "determine",
  "requires", "shows that the true", "indicates that the true"
)

.assert_no_identifying_language <- function(sentence, what) {
  # NEGATED USES ARE THE POINT, NOT THE PROBLEM. "does not identify the correct
  # calibration" is exactly the sentence this module exists to produce, and it
  # contains "identify". Neutralise negated occurrences before scanning, so the
  # guard catches an affirmative upgrade ("identifies the calibration") without
  # rejecting the disclaimer it is meant to protect.
  scan <- gsub("\\b(cannot|can not|does not|do not|did not|never|no|not|without)\\s+([a-z]+\\s+){0,2}",
               " <negated> ", sentence, ignore.case = TRUE, perl = TRUE)
  hits <- FORBIDDEN_INFERENCE_VERBS[
    vapply(FORBIDDEN_INFERENCE_VERBS,
           function(v) grepl(v, scan, ignore.case = TRUE), logical(1))]
  if (length(hits)) {
    stop(sprintf(paste("%s produced identifying language (%s). This evidence",
                       "constrains direction, not magnitude; the sentence must",
                       "stay conditional."),
                 what, paste(sQuote(hits), collapse = ", ")), call. = FALSE)
  }
  sentence
}

#' @export
print.urps_balance_reversal <- function(x, ...) {
  cat("Balance-reversal threshold (tipping point, not an adequacy estimate)\n")
  if (is.na(x$reversal_multiplier)) {
    cat("  reversal: NONE in examined range\n  ", x$note, "\n", sep = "")
  } else {
    cat(sprintf("  reversal multiplier : %.3fx the reference calibration\n", x$reversal_multiplier))
    cat(sprintf("  as a deviation      : %+.1f%% baseline need vs reference\n", x$reversal_pct_deviation))
    cat(sprintf("  direction           : %s\n", x$direction))
  }
  invisible(x)
}

#' Manuscript sentence for a balance-reversal threshold
#'
#' @param x A [balance_reversal_threshold()] object.
#' @param horizon Label for the projection horizon, e.g. `2050`.
#' @return A single sentence, validated to contain no identifying language.
#' @family baseline gap reporting
#' @concept reporting
#' @export
balance_reversal_sentence <- function(x, horizon = "the projection horizon") {
  stopifnot(inherits(x, "urps_balance_reversal"))
  if (is.na(x$reversal_multiplier)) {
    return(.assert_no_identifying_language(
      sprintf("The projected %s workforce balance did not change sign for any adequacy calibration in the examined range (%.2fx to %.2fx the reference).",
              horizon, x$examined_range[1], x$examined_range[2]),
      "balance_reversal_sentence()"))
  }
  from_to <- switch(x$direction,
    surplus_to_shortage = "from surplus to shortage",
    shortage_to_surplus = "from shortage to surplus",
    "across zero")
  .assert_no_identifying_language(
    sprintf(paste("The projected %s workforce balance reversed %s when assumed",
                  "baseline need was approximately %.0f%% greater than the",
                  "reference calibration. This is a threshold, not an estimate of",
                  "baseline need."),
            horizon, from_to, x$reversal_pct_deviation),
    "balance_reversal_sentence()")
}
