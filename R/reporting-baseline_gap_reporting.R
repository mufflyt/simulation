# Reporting a Baseline Gap ----
#
# Separating the ARITHMETIC METHOD from the EVIDENCE TIER was the right change,
# and it creates an obligation: a capacity-survey calculation looks identical
# whether the distribution came from urogynaecologists or was borrowed from
# physical therapy, so the number alone no longer says what it means. Every
# headline gap has to travel with the fields that distinguish those two cases.
#
# THE LANGUAGE RULE THIS ENFORCES. "The current shortage is X" is a claim about
# the world and requires a DIRECT EXTERNAL ANCHOR -- an independent measurement
# of base-year adequacy in this specialty. Without one, the only defensible
# phrasing is "the model-implied gap under the specified calibration is X".
# The distinction is not stylistic: the first is falsifiable against reality,
# the second only against the model's own assumptions.
#
# `baseline_gap_claim()` returns the permitted sentence, the same way
# `interval_label()` returns the permitted description of an interval. A caller
# that writes its own prose can still say anything; one that asks gets a
# sentence it can defend.

#' Full provenance for a headline baseline-gap estimate
#'
#' Every field a reader needs to decide what the number means, with `NA`
#' reported as "undeclared" rather than quietly omitted.
#'
#' @param gap A [baseline_gap()] object.
#' @return Tibble of one row per reported field.
#' @family baseline gap reporting
#' @concept reporting
#' @export
baseline_gap_provenance <- function(gap) {
  if (!inherits(gap, "urps_baseline_gap")) {
    stop("baseline_gap_provenance(): not a baseline gap object.", call. = FALSE)
  }
  und <- function(x) {
    if (is.null(x) || length(x) == 0L || all(is.na(x))) "UNDECLARED"
    else paste(as.character(x), collapse = "; ")
  }
  interval <- if (is.null(gap$shortfall_fte_ci)) "UNDECLARED" else
    sprintf("%.1f to %.1f FTE (from adequacy %.3f-%.3f)",
            gap$shortfall_fte_ci[1], gap$shortfall_fte_ci[2],
            gap$adequacy_ci[1], gap$adequacy_ci[2])

  tibble::tribble(
    ~field,                  ~value,
    "method",                und(gap$method),
    "calibration_status",    und(gap$calibration_status),
    "population",            und(gap$population),
    "year",                  und(gap$year),
    "source_specialty",      und(gap$source_specialty),
    "externally_measured",   if (is.na(gap$externally_measured %||% NA)) "UNDECLARED"
                             else as.character(isTRUE(gap$externally_measured)),
    "uncertainty_interval",  interval,
    "base_supply_fte",       sprintf("%.1f", gap$base_supply_fte),
    "required_fte",          sprintf("%.1f", gap$required_fte),
    "shortfall_fte",         sprintf("%.1f", gap$shortfall_fte),
    "evidence",              und(gap$evidence)
  )
}

#' Does this gap rest on a direct external anchor?
#'
#' TRUE only when adequacy was measured externally IN THIS SPECIALTY and the
#' tier says so. `method == "external_anchor"` alone is not enough: the same
#' call can carry a borrowed distribution, which is exactly the case the
#' method/tier split exists to distinguish.
#'
#' @param gap A [baseline_gap()] object.
#' @return Single logical.
#' @family baseline gap reporting
#' @concept reporting
#' @export
has_external_anchor <- function(gap) {
  if (!inherits(gap, "urps_baseline_gap")) return(FALSE)
  tier_ok <- identical(gap$calibration_status, "calibrated")
  measured <- isTRUE(gap$externally_measured)
  # A source specialty that is declared and is NOT this one means the evidence
  # was borrowed, whatever the tier claims.
  spec <- gap$source_specialty
  borrowed <- !is.na(spec) && !grepl("urogyn|urps|fpmrs", tolower(spec))
  tier_ok && measured && !borrowed
}

#' The claim this gap supports, in words
#'
#' @param gap A [baseline_gap()] object.
#' @param digits Rounding for the reported FTE.
#' @return A single sentence.
#' @family baseline gap reporting
#' @concept reporting
#' @export
baseline_gap_claim <- function(gap, digits = 0) {
  n <- format(round(gap$shortfall_fte, digits), big.mark = ",")
  ci <- if (is.null(gap$shortfall_fte_ci)) "" else
    sprintf(" (%s to %s FTE)",
            format(round(gap$shortfall_fte_ci[1], digits), big.mark = ","),
            format(round(gap$shortfall_fte_ci[2], digits), big.mark = ","))

  if (has_external_anchor(gap)) {
    return(sprintf("The current shortage is %s FTE%s, measured against an %s anchor for %s in %s.",
                   n, ci, gap$method,
                   gap$population %||% "the modelled population",
                   gap$year %||% "the base year"))
  }
  sprintf(paste("The model-implied gap under the specified calibration is %s FTE%s",
                "(method %s, calibration %s, evidence from %s). This is not a",
                "measured shortage: base-year adequacy has no direct external",
                "anchor in this specialty."),
          n, ci, gap$method, gap$calibration_status,
          if (is.na(gap$source_specialty %||% NA)) "an undeclared source specialty"
          else gap$source_specialty)
}

#' Refuse shortage language without a direct external anchor
#'
#' Call before publishing any sentence of the form "the current shortage is X".
#'
#' @param gap A [baseline_gap()] object.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when the claim is supportable.
#' @family baseline gap reporting
#' @concept reporting
#' @export
assert_external_anchor <- function(gap, mode = resolve_reproducibility_mode()) {
  if (has_external_anchor(gap)) return(invisible(TRUE))
  msg <- paste(
    "This gap has no direct external anchor, so it cannot support the claim",
    "'the current shortage is X'. Say instead:", sQuote(baseline_gap_claim(gap)),
    "-- an externally measured base-year adequacy IN THIS SPECIALTY is what",
    "would license the stronger sentence.")
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' Sensitivity of a projected gap to the base-year anchor
#'
#' Required FTE is `anchor x wRVU(t)/wRVU(base)`, so the projected gap is linear
#' in the anchor and the break-even is closed-form. This reports where the
#' conclusion flips, which is the number a reader needs in order to judge how
#' much the anchor's provenance matters.
#'
#' THIS IS A MAIN RESULT, NOT A SUPPLEMENTARY CHECK. The anchor is the only
#' input that can change the SIGN of the projected 2050 balance -- the
#' delegation matrix and the demand calibration cancel out of the ratio. A
#' projection reported without this table has concealed its dominant
#' uncertainty. The default multipliers span 0.8x to 2x for that reason: the
#' reference calibration is an analogy (see [REFERENCE_ADEQUACY_CALIBRATION]),
#' so multipliers well beyond the published analogue spread are in play.
#'
#' @param gap A [baseline_gap()] object supplying the anchor.
#' @param supply_at_target Projected supply FTE at the horizon.
#' @param demand_growth Ratio of target-year to base-year demand.
#' @param anchor_multipliers Multipliers on the anchor to tabulate.
#' @return List with the break-even anchor, the adequacy implying it, the
#'   elasticity, and a sensitivity table.
#' @family baseline gap reporting
#' @concept reporting
#' @export
baseline_anchor_sensitivity <- function(gap, supply_at_target, demand_growth,
                                        anchor_multipliers = c(0.8, 0.9, 1, 1.1, 1.25, 1.5, 2)) {
  stopifnot(inherits(gap, "urps_baseline_gap"),
            is.numeric(supply_at_target), is.numeric(demand_growth),
            demand_growth > 0)
  anchor <- gap$required_fte
  gap_at <- function(a) supply_at_target - a * demand_growth

  breakeven <- supply_at_target / demand_growth
  g0 <- gap_at(anchor)
  tab <- tibble::tibble(
    anchor_multiplier = anchor_multipliers,
    anchor_fte = anchor * anchor_multipliers,
    required_at_target = anchor * anchor_multipliers * demand_growth,
    gap_fte = vapply(anchor * anchor_multipliers, gap_at, numeric(1))
  )
  tab$conclusion <- ifelse(tab$gap_fte > 0, "surplus", "shortage")

  list(
    anchor_fte = anchor,
    demand_growth = demand_growth,
    gap_at_anchor = g0,
    breakeven_anchor_fte = breakeven,
    anchor_uplift_to_flip = breakeven / anchor - 1,
    implied_adequacy_at_breakeven = if (is.finite(gap$base_supply_fte))
      gap$base_supply_fte / breakeven else NA_real_,
    # d(gap)/d(anchor) = -demand_growth. As an elasticity: the % change in the
    # gap per 1% change in the anchor.
    elasticity = if (abs(g0) > .Machine$double.eps^0.5)
      -anchor * demand_growth / g0 else NA_real_,
    sensitivity = tab
  )
}

#' @export
print.urps_baseline_gap_claim <- function(x, ...) { cat(x, "\n"); invisible(x) }
