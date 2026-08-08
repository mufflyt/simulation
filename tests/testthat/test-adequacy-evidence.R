# Base-year adequacy evidence table (R/reporting-adequacy_evidence.R).
#
# The scientific claim these tests protect: access evidence constrains the
# DIRECTION of a mis-calibration, never its magnitude. Two cross-sectional
# mystery-caller audits with different sampling frames cannot be inverted into a
# required-FTE level, and pandemic backlog is not separable from capacity with
# two time points. The controlled vocabulary is how that discipline is enforced
# in code rather than left to the care of whoever writes the discussion section.

test_that("the reference calibration is labelled a calibration, not an estimate", {
  expect_equal(REFERENCE_ADEQUACY_CALIBRATION, 0.948)
  # Both published analogues come from OTHER specialties, so their agreement is
  # not evidence about URPS. Their spread is a floor on the uncertainty.
  expect_named(ADEQUACY_ANALOGUES, c("physical_therapy", "physiatry"))
  expect_equal(unname(ADEQUACY_ANALOGUES[["physical_therapy"]]), REFERENCE_ADEQUACY_CALIBRATION)
  expect_lt(ADEQUACY_ANALOGUES[["physiatry"]], ADEQUACY_ANALOGUES[["physical_therapy"]])
})

test_that("a table assembles and keeps its columns", {
  tab <- adequacy_evidence_table(
    evidence = c("Reference calibration", "Provider growth"),
    model_implication = c("~5% baseline shortfall",
                          "Capacity should improve, all else equal"),
    observed = c("Mean prolapse wait 23.1 -> 40.8 business days",
                 "Timely access worsened (30% -> 14% within 10 business days)"),
    interpretation = c("consistent_with_under_calibration",
                       "headcount_not_effective_capacity"),
    citation = "Mystery-caller audits, 2020 and 2026")
  expect_s3_class(tab, "urps_adequacy_evidence")
  expect_equal(nrow(tab), 2L)
  expect_true(all(c("evidence", "model_implication", "observed_evidence",
                    "interpretation", "citation") %in% names(tab)))
})

test_that("free-text interpretation is refused", {
  # The whole point. If any string were allowed, the discussion section would
  # eventually contain "therefore the anchor is 1.5x", which the evidence
  # cannot support.
  expect_error(
    adequacy_evidence_table(
      evidence = "Wait times",
      model_implication = "~5% shortfall",
      observed = "waits rose 77%",
      interpretation = "the anchor should be 1.5x",
      citation = "audit"),
    "controlled vocabulary")
})

test_that("no vocabulary term asserts a level", {
  # A guard on the vocabulary itself, so a future edit cannot quietly add
  # "implies_anchor_of" and reopen the door.
  expect_false(any(grepl("1\\.5|implies_anchor|equals|magnitude|level_of",
                         ADEQUACY_INTERPRETATIONS)))
  expect_true("not_identifiable_from_this_evidence" %in% ADEQUACY_INTERPRETATIONS)
  expect_true("stress_scenario_not_an_estimate" %in% ADEQUACY_INTERPRETATIONS)
})

test_that("an observation without a citation is refused", {
  expect_error(
    adequacy_evidence_table(
      evidence = "Wait times", model_implication = "~5% shortfall",
      observed = "waits rose 77%",
      interpretation = "consistent_with_under_calibration",
      citation = NA_character_),
    "require a citation")
})

test_that("a non-identifiable row needs no citation", {
  # The stress-scenario row claims no observation, so requiring attribution for
  # it would be theatre.
  tab <- adequacy_evidence_table(
    evidence = "1.5x calibration",
    model_implication = "2050 shortage ~326 FTE",
    observed = NA_character_,
    interpretation = "stress_scenario_not_an_estimate")
  expect_true(is.na(tab$observed_evidence))
  expect_false(adequacy_evidence_is_discordant(tab))
})

test_that("discordance is detected only for directional rows", {
  concordant <- adequacy_evidence_table(
    evidence = "Reference calibration", model_implication = "~5% shortfall",
    observed = "waits stable", interpretation = "consistent_with_reference_calibration",
    citation = "audit")
  expect_false(adequacy_evidence_is_discordant(concordant))

  discordant <- adequacy_evidence_table(
    evidence = "Provider growth", model_implication = "capacity should improve",
    observed = "timely access worsened",
    interpretation = "headcount_not_effective_capacity", citation = "audit")
  expect_true(adequacy_evidence_is_discordant(discordant))
})

test_that("the conclusion sentence stays conditional and preserves the asymmetry", {
  tab <- adequacy_evidence_table(
    evidence = "Provider growth", model_implication = "capacity should improve",
    observed = "timely access worsened",
    interpretation = "headcount_not_effective_capacity", citation = "audit")
  s <- adequacy_conclusion_sentence(tab, flip_multiplier = 1.37)

  expect_match(s, "conditional on the assumed base-year adequacy")
  expect_match(s, "raises concern")
  # THE ASYMMETRY: concern and its limit must travel together, so neither can be
  # quoted without the other.
  expect_match(s, "cannot establish the magnitude")
  expect_match(s, "does not invert to a required-FTE level")
  # The threshold may be reported; it must not be offered as an estimate.
  expect_match(s, "37% greater than the reference calibration")
  expect_match(s, "threshold, not an estimate")
  expect_false(grepl("the anchor is|should be|correct value is", s))
})

test_that("the literature claim degrades without a citation", {
  tab <- adequacy_evidence_table(
    evidence = "Reference calibration", model_implication = "~5% shortfall",
    observed = "waits stable", interpretation = "consistent_with_reference_calibration",
    citation = "audit")
  # Unattributed: a claim about the AUTHORS, which is free.
  expect_match(adequacy_conclusion_sentence(tab), "we are not aware of a validated")
  # Attributed: a claim about the LITERATURE, which is not.
  cited <- adequacy_conclusion_sentence(tab, no_estimate_citation = "Zarek 2025; HRSA 2024")
  expect_match(cited, "no validated national URPS adequacy estimate exists \\(Zarek 2025; HRSA 2024\\)")
})

test_that("identifying language is refused, but its negation is not", {
  g <- urpssim:::.assert_no_identifying_language
  # These are the sentences this module exists to write.
  expect_silent(g("It does not identify the correct calibration.", "t"))
  expect_silent(g("Access evidence cannot establish the magnitude.", "t"))
  # These are upgrades the guard must catch.
  for (bad in c("This demonstrates that need is higher.",
                "The audit proves the anchor is too low.",
                "This identifies the calibration.",
                "The evidence establishes baseline unmet need.")) {
    expect_error(g(bad, "t"), "identifying language")
  }
})

test_that("a model implication cannot masquerade as external evidence", {
  # Without this, "2050 shortage ~326 FTE" could be filed under observed
  # evidence and the table would appear to corroborate itself.
  expect_error(
    adequacy_evidence_table(
      evidence = "1.5x calibration", model_implication = "2050 shortage ~326 FTE",
      observed = "shortage of 326 FTE", interpretation = "stress_scenario_not_an_estimate",
      citation = "the model", evidence_type = "model_implication"),
    "cannot carry an external observation")
  expect_error(
    adequacy_evidence_table(
      evidence = "x", model_implication = "y", observed = NA_character_,
      interpretation = "not_identifiable_from_this_evidence",
      evidence_type = "hearsay"),
    "evidence_type must be one of")
})

test_that("evidence_type is recorded even though it is provenance, not meaning", {
  tab <- adequacy_evidence_table(
    evidence = c("Wait times", "1.5x calibration"),
    model_implication = c("~5% shortfall", "2050 shortage ~326 FTE"),
    observed = c("waits rose", NA_character_),
    interpretation = c("consistent_with_under_calibration", "stress_scenario_not_an_estimate"),
    citation = c("audit", NA_character_),
    evidence_type = c("empirical_observation", "model_implication"))
  expect_true("evidence_type" %in% names(tab))
  expect_equal(tab$evidence_type, c("empirical_observation", "model_implication"))
})

# ---- balance-reversal threshold -------------------------------------------

.mk_gap <- function(supply = 1339, adequacy = REFERENCE_ADEQUACY_CALIBRATION)
  baseline_gap(base_supply_fte = supply, adequacy = adequacy, method = "assumed",
               evidence = "test", calibration_status = "derived_by_analogy")

test_that("the threshold is solved continuously, not snapped to a scenario", {
  # The crossing must be able to land between prespecified multipliers.
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 2051, demand_growth = 1.15)
  expect_s3_class(r, "urps_balance_reversal")
  expect_true(is.finite(r$reversal_multiplier))
  # closed form: supply / (anchor * growth)
  expect_equal(r$reversal_multiplier, 2051 / (gap$required_fte * 1.15), tolerance = 1e-9)
  expect_false(r$reversal_multiplier %in% c(0.8, 0.9, 1, 1.1, 1.25, 1.5, 2))
})

test_that("the headline is a deviation, not a rival adequacy estimate", {
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 2051, demand_growth = 1.15)
  expect_equal(r$reversal_pct_deviation, 100 * (r$reversal_multiplier - 1), tolerance = 1e-9)
  # The implied adequacy is retained but named so it cannot be quoted as an estimate.
  expect_true("implied_adequacy_at_reversal_NOT_AN_ESTIMATE" %in% names(r))
})

test_that("no reversal in range returns NA with an explanation, not an endpoint", {
  # Manufacturing a tipping point that was never examined is the failure mode.
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 50, demand_growth = 1.15,
                                  examined_range = c(0.9, 1.1),
                                  demand_calibration_status = "calibrated")
  expect_true(is.na(r$reversal_multiplier))
  expect_true(is.na(r$reversal_pct_deviation))
  expect_false(r$reversal_within_examined_range)
  expect_match(r$note, "No sign reversal occurs")
  expect_match(r$note, "outside the range considered plausible")
  expect_match(balance_reversal_sentence(r, 2050), "did not change sign")
})

test_that("the reversal sentence reports a threshold and refuses to identify", {
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 2051, demand_growth = 1.15,
                                  demand_calibration_status = "calibrated")
  s <- balance_reversal_sentence(r, 2050)
  expect_match(s, "reversed from surplus to shortage")
  expect_match(s, "approximately \\d+% greater than the reference calibration")
  expect_match(s, "threshold, not an estimate of baseline need")
  expect_false(grepl("baseline need is", s))
})

test_that("the anchor sensitivity spans beyond the published analogue spread", {
  # The reference calibration is an analogy, so plausible alternatives are not
  # bounded by the 0.894-0.948 gap between two other specialties.
  defaults <- eval(formals(baseline_anchor_sensitivity)$anchor_multipliers)
  expect_true(all(c(1, 1.25, 1.5, 2) %in% defaults))
  expect_gte(max(defaults), 2)
})

# ---- reportability gate ----------------------------------------------------
#
# A threshold computed from placeholder demand coefficients is a software
# result. The distinction is invisible six months later, when the number has
# been pasted into a figure caption and nobody remembers which run made it.

test_that("undeclared calibration is treated as provisional", {
  # Silence must not buy a clean number. Fail closed.
  expect_true(demand_calibration_is_provisional(NULL))
  expect_true(demand_calibration_is_provisional(NA_character_))
  expect_true(demand_calibration_is_provisional(character(0)))
  # An unrecognised string is not a promotion either.
  expect_true(demand_calibration_is_provisional("looks_fine_to_me"))
})

test_that("derived_by_analogy is NOT sufficient to report", {
  # That is the tier the adequacy anchor itself sits at; accepting it would let
  # the assumption certify itself.
  expect_true(demand_calibration_is_provisional("placeholder_uncalibrated"))
  expect_true(demand_calibration_is_provisional("uncalibrated_illustrative"))
  expect_true(demand_calibration_is_provisional("derived_by_analogy"))
  expect_false(demand_calibration_is_provisional("fitted"))
  expect_false(demand_calibration_is_provisional("calibrated"))
  # The weakest input governs.
  expect_true(demand_calibration_is_provisional(c("calibrated", "placeholder_uncalibrated")))
})

test_that("a provisional threshold refuses to emit a manuscript sentence", {
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 2051, demand_growth = 1.15,
                                  demand_calibration_status = "placeholder_uncalibrated")
  # The number is still computed -- development needs it.
  expect_true(is.finite(r$reversal_multiplier))
  expect_false(r$reportable)
  expect_match(r$reportable_note, "NOT REPORTABLE")
  expect_match(r$reportable_note, "software-validation result")

  expect_error(balance_reversal_sentence(r, 2050), "NOT REPORTABLE")
  # The escape hatch exists, and it labels rather than hides.
  s <- balance_reversal_sentence(r, 2050, allow_provisional = TRUE)
  expect_match(s, "\\[PROVISIONAL, NOT FOR PUBLICATION\\]")
})

test_that("a calibrated threshold reports cleanly and unmarked", {
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 2051, demand_growth = 1.15,
                                  demand_calibration_status = "calibrated")
  expect_true(r$reportable)
  expect_true(is.na(r$reportable_note))
  s <- balance_reversal_sentence(r, 2050)
  expect_false(grepl("PROVISIONAL", s))
  expect_match(s, "threshold, not an estimate of baseline need")
})

test_that("the no-reversal sentence is gated too", {
  # The absence of a tipping point is also a claim about the model.
  gap <- .mk_gap()
  r <- balance_reversal_threshold(gap, supply_at_target = 50, demand_growth = 1.15,
                                  examined_range = c(0.9, 1.1),
                                  demand_calibration_status = "placeholder_uncalibrated")
  expect_error(balance_reversal_sentence(r, 2050), "NOT REPORTABLE")
  expect_match(balance_reversal_sentence(r, 2050, allow_provisional = TRUE),
               "\\[PROVISIONAL, NOT FOR PUBLICATION\\]")
})

test_that("the calibration ranking does not drift from the demand-contract copy", {
  # R/reporting-export_demand_contract.R carries a local status_rank. Two
  # rankings that disagree would mean the two layers disagree about what counts
  # as calibrated -- the same silent-divergence failure as a duplicated function.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(!length(root))
  f <- file.path(root[1], "R", "reporting-export_demand_contract.R")
  skip_if(!file.exists(f))
  src <- paste(readLines(f, warn = FALSE), collapse = " ")
  for (nm in names(CALIBRATION_STATUS_RANK)) {
    expect_match(src, sprintf("%s = %dL", nm, CALIBRATION_STATUS_RANK[[nm]]), fixed = FALSE,
                 info = paste("ranking drifted for", nm))
  }
})
