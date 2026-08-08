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

test_that("the conclusion sentence stays conditional and never names a value", {
  tab <- adequacy_evidence_table(
    evidence = "Provider growth", model_implication = "capacity should improve",
    observed = "timely access worsened",
    interpretation = "headcount_not_effective_capacity", citation = "audit")
  s <- adequacy_conclusion_sentence(tab, flip_multiplier = 1.5)

  expect_match(s, "conditional on the assumed base-year adequacy")
  expect_match(s, "no validated national URPS adequacy estimate exists")
  expect_match(s, "does not identify the correct calibration")
  # It may report WHERE the sign flips; it must not assert that is the truth.
  expect_match(s, "reverses the projected sign")
  expect_false(grepl("the anchor is|should be|correct value is", s))
})

test_that("the anchor sensitivity spans beyond the published analogue spread", {
  # The reference calibration is an analogy, so plausible alternatives are not
  # bounded by the 0.894-0.948 gap between two other specialties.
  defaults <- eval(formals(baseline_anchor_sensitivity)$anchor_multipliers)
  expect_true(all(c(1, 1.25, 1.5, 2) %in% defaults))
  expect_gte(max(defaults), 2)
})
