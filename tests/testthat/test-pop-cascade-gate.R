# The POP anchor constrains a TRANSITION, not the output. These tests exist to
# stop the mismatch being "fixed" by multiplying the final number.
#
# These assertions used to read config/pop_cascade_transitions.yml, which was
# INERT: no production code loaded it, and it restated 0.35/0.55/0.12/0.40 that
# actually live in inst/extdata/pathway/condition_service_pathway.csv. A gate
# watching a file nothing executes is not a gate. Every assertion below now
# derives from the LIVE pathway, so the numbers being guarded are the numbers the
# model uses. The inert YAML was deleted; its anchor-constraint arithmetic is
# preserved here and its narrative in docs/CONFIGURATION_AUTHORITY_INVENTORY.md.

.pop_pathway <- function() {
  pw <- condition_service_pathway()
  pw[pw$condition == "pop", , drop = FALSE]
}
.pop_treated <- function() unname(FROZEN_CARE_ENGAGED[["pop"]])
.pop_anchor <- function() {
  a <- utils::read.csv("../../data/anchors/prolapse_procedure_volume.csv")
  a$observed[a$anchor_id == "prolapse_procedure_volume"][[1]]
}
# valid_pathway(), NOT the shipped table: per_entering = 1.00 on
# new_consultation is a stock-as-flow error that assert_incident_not_prevalent()
# refuses (docs/INCIDENT_ENTRY_ESTIMAND.md).
#
# THE QUANTITY THESE TESTS MEASURE IS PROVABLY UNAFFECTED. per_entering scales
# services WITHIN a stage; prolapse_procedure sits at the procedure stage and
# its volume is set by the p_advance cascade. Checked, not assumed: the
# overstatement is 8.5075 under the fixture and 8.5075 under the shipped table.
# The fixture makes the pathway runnable without perturbing the finding.
.pop_volume <- function(pathway = valid_pathway(), by_stage = FALSE) {
  pathway_service_volumes(treated = c(pop = .pop_treated()), year = 2025L,
                          pathway = pathway, by_stage = by_stage)
}

test_that("the shipped pathway drives prolapse volume (mutation proof)", {
  # THE test this file was missing. The pathway CSV is the most consequential
  # artifact in the repo's configuration inventory and had nothing proving its
  # values reach an output. Halving the FIRST advance probability must halve the
  # procedure stage EXACTLY, because every downstream stage is multiplicative in
  # it -- including recurrence, which derives from the procedure stage. An
  # "output merely changed" assertion would pass even if the value were being
  # partially overwritten downstream; exact 0.5 will not.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  # valid_pathway() for the same reason as .pop_volume()'s default above: the
  # mutation being proved is on p_advance, which the fixture does not touch.
  pw <- valid_pathway()

  base_stage <- .pop_volume(pw, by_stage = TRUE)
  base_tot   <- .pop_volume(pw)

  mut <- pw
  mut$p_advance[mut$condition == "pop" & mut$stage == "conservative"] <- 0.175
  mut_stage <- .pop_volume(mut, by_stage = TRUE)
  mut_tot   <- .pop_volume(mut)

  pick <- function(d, st) sum(d$volume[d$service == "prolapse_procedure" &
                                         d$stage == st])
  tot  <- function(d) sum(d$volume[d$service == "prolapse_procedure"])

  expect_equal(pick(mut_stage, "procedure") / pick(base_stage, "procedure"),
               0.5, tolerance = 1e-9)
  # recurrence is downstream of procedure and multiplicative, so it halves too
  expect_equal(pick(mut_stage, "recurrence") / pick(base_stage, "recurrence"),
               0.5, tolerance = 1e-9)
  expect_equal(tot(mut_tot) / tot(base_tot), 0.5, tolerance = 1e-9)
  # and the direction is down, not merely different
  expect_lt(tot(mut_tot), tot(base_tot))
})

test_that("a large POP mismatch must be resolved upstream, not by a terminal scalar", {
  skip_if_not(file.exists("../../data/anchors/prolapse_procedure_volume.csv"))
  predicted <- sum(.pop_volume()$volume[
    .pop_volume()$service == "prolapse_procedure"])
  anchor <- .pop_anchor()
  overstatement <- predicted / anchor

  # The mismatch is real and large. Recording it here is the point: if someone
  # "fixes" it by scaling the output, this number goes to 1 while every upstream
  # probability stays wrong, and the next assertion catches it.
  # After the estimand restructure the discrepancy is carried by ONE parameter
  # and is therefore LARGER, not smaller: the old 0.55 gate was absorbing part
  # of it. This is the intended state -- one honest parameter at ~8.5x is a
  # better object to source than two unsourced ones multiplying to 4.68x.
  expect_gt(overstatement, 8)
  expect_lt(overstatement, 9)

  # No terminal scaling may be applied to the procedure service. The pathway has
  # exactly one lever per stage (per_entering, p_advance); a scalar smuggled in
  # would have to appear as a per_entering != 1.0 at the procedure stage.
  pr <- .pop_pathway()
  pr <- pr[pr$stage == "procedure" & pr$service == "prolapse_procedure", ]
  expect_equal(pr$per_entering[[1]], 1.0,
               info = "a terminal scalar would hide here as per_entering != 1")
  expect_equal(pr$p_advance[[1]], 1.0)
})

test_that("the back-solved constraint is internally consistent", {
  skip_if_not(file.exists("../../data/anchors/prolapse_procedure_volume.csv"))
  pw <- .pop_pathway()
  p_cons <- unique(pw$p_advance[pw$stage == "conservative"])
  p_test <- unique(pw$p_advance[pw$stage == "testing"])
  p_recur_hazard <- unique(pw$p_advance[pw$stage == "followup"])
  p_reop <- pw$per_entering[pw$stage == "recurrence" &
                              pw$service == "prolapse_procedure"]
  expect_length(p_cons, 1L)   # one advance probability per stage, or the
  expect_length(p_test, 1L)   # cascade is ambiguous
  expect_length(p_recur_hazard, 1L)

  recurrence_multiplier <- 1 + p_recur_hazard * p_reop
  p_combined <- p_cons * p_test
  predicted <- .pop_treated() * p_combined * recurrence_multiplier

  # V = N x p_combined x recurrence_multiplier must reproduce what the pathway
  # engine actually computes, to within rounding. If these diverge, the engine
  # is applying something this arithmetic does not describe.
  engine <- sum(.pop_volume()$volume[
    .pop_volume()$service == "prolapse_procedure"])
  expect_lt(abs(predicted - engine) / engine, 0.01)

  # and the probability required to hit the anchor is the one to source
  required <- .pop_anchor() / (.pop_treated() * recurrence_multiplier)
  expect_lt(required, p_combined)   # the model is high, not low
  expect_gt(p_combined / required, 4)
})

test_that("a low-confidence pathway stage is never presented as calibrated", {
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- .pop_pathway()
  # every POP stage is expert judgement at low confidence; none may claim
  # otherwise while its source still says so
  for (i in seq_len(nrow(pw))) {
    if (identical(pw$confidence[[i]], "low")) {
      expect_false(grepl("calibrated|validated", pw$source[[i]], ignore.case = TRUE),
                   info = paste(pw$stage[[i]], pw$service[[i]],
                                "claims calibration on low confidence"))
    }
  }
})

test_that("every POP pathway row declares its evidence", {
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- .pop_pathway()
  for (col in c("per_entering", "confidence", "source", "notes")) {
    expect_true(col %in% names(pw), info = col)
    expect_true(all(!is.na(pw[[col]]) & nzchar(as.character(pw[[col]]))),
                info = paste("blank", col, "in the POP pathway"))
  }
})

test_that("illustrative predictions never reach a production scalar field", {
  # regression for the episode where 0.963 / 1.408 / 0.790 were reported as
  # calibration scalars but were arithmetic against invented predictions
  illustrative <- list(
    estimand_id = "prolapse_procedure_volume", prediction = 100000,
    model_run_id = "smoke_test", model_version = "test",
    artifact_path = NA_character_, artifact_sha256 = NA_character_,
    generated_utc = NA_character_, prediction_status = "illustrative")
  expect_error(compute_production_scalar(140762, illustrative),
               "non-production prediction")
  # and the readiness report must not name its column a production scalar
  skip_if_not(file.exists("../../scripts/calibration/build_empirical_calibration_targets.R"))
  src <- readLines("../../scripts/calibration/build_empirical_calibration_targets.R")
  expect_true(any(grepl("illustrative_smoke_test_scalar", src, fixed = TRUE)))
})

test_that("the inert cascade config stays deleted", {
  # It restated live values with no execution consumer, so editing it changed
  # nothing while looking authoritative. If it returns, it must come back with a
  # loader and a mutation test, not as documentation shaped like config.
  expect_false(file.exists("../../config/pop_cascade_transitions.yml"))
})

test_that("POP testing is non-gating, and UI/AI were not restructured by analogy", {
  # The estimand restructure applies to POP ONLY. UI's testing stage delivers
  # 1.20 services per entrant -- essentially everyone entering receives a test --
  # so a gate there is defensible and must be left alone. AI has the same defect
  # as POP (0.25 services/entrant) but is a separate question with separate
  # evidence, and is deliberately NOT changed here.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- condition_service_pathway()
  expect_equal(unique(pw$p_advance[pw$condition == "pop" & pw$stage == "testing"]), 1.00)
  expect_equal(unique(pw$p_advance[pw$condition == "ui"  & pw$stage == "testing"]), 0.40)
  expect_equal(unique(pw$p_advance[pw$condition == "ai"  & pw$stage == "testing"]), 0.20)

  # testing services survive as UTILISATION, unchanged in magnitude
  pop_test <- pw[pw$condition == "pop" & pw$stage == "testing", ]
  expect_equal(sum(pop_test$per_entering), 0.50)
  # Assert the machine-readable declaration, not prose wording: a pass-through
  # stage declares itself in `source` and carries no confidence interval,
  # because there is no probability being estimated. Matching on note text
  # instead makes the test fail on a rephrase rather than on a semantic change.
  expect_true(all(grepl("pass-through", pop_test$source, fixed = TRUE)))
  expect_true(all(is.na(pop_test$ci_low) & is.na(pop_test$ci_high)))

  # and the conservative transition now carries the whole cascade
  cons <- unique(pw$p_advance[pw$condition == "pop" & pw$stage == "conservative"])
  proc <- pw[pw$condition == "pop" & pw$stage == "procedure", ]
  expect_equal(cons * 1.00 * proc$per_entering[[1]], cons)
})

test_that("the recurrence limb is one cohort-year, and that is recorded as a defect", {
  # STOCK-VERSUS-FLOW. The engine computes recurrence entrants as THIS YEAR's
  # primary operations x the annual hazard, so it exposes a single cohort-year.
  # Recurrences actually arise from the accumulated stock of everyone previously
  # operated. This test does not assert the behaviour is correct -- it pins the
  # arithmetic so the defect cannot be half-fixed silently. See
  # docs/POP_RECURRENCE_ESTIMAND_AUDIT.md.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- .pop_pathway()
  h    <- unique(pw$p_advance[pw$stage == "followup"])
  reop <- pw$per_entering[pw$stage == "recurrence" &
                            pw$service == "prolapse_procedure"]
  st <- .pop_volume(by_stage = TRUE)
  primary <- sum(st$volume[st$service == "prolapse_procedure" &
                             st$stage == "procedure"])
  recurrent <- sum(st$volume[st$service == "prolapse_procedure" &
                               st$stage == "recurrence"])
  # exactly one cohort-year of exposure: no accumulated stock anywhere
  expect_equal(recurrent, primary * h * reop, tolerance = 1e-6)
  expect_equal(1 + recurrent / primary, 1 + h * reop, tolerance = 1e-9)
})

test_that("the implied cumulative reoperation burden is flagged as too high", {
  # 0.12 x 0.40 = 4.8%/yr implies ~39% of operated women reoperated within a
  # decade, far above published rates. If someone lowers the hazard without
  # also widening the exposure window, this test still passes -- which is why
  # the audit doc requires both to move together. What is pinned here is that
  # the CURRENT values carry an implausible annual burden, so the number cannot
  # quietly be called sourced.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- .pop_pathway()
  annual <- unique(pw$p_advance[pw$stage == "followup"]) *
    pw$per_entering[pw$stage == "recurrence" &
                      pw$service == "prolapse_procedure"]
  expect_gt(1 - (1 - annual)^10, 0.30)   # implausibly high at 10 years
  # and the hazard is still declared low-confidence, i.e. not sourced
  expect_true(all(pw$confidence[pw$stage == "followup"] == "low"))
})

test_that("UI and AI recurrence limbs are not changed by analogy", {
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- condition_service_pathway()
  expect_equal(unique(pw$p_advance[pw$condition == "ui" & pw$stage == "followup"]), 0.08)
  expect_equal(pw$per_entering[pw$condition == "ui" & pw$stage == "recurrence" &
                                 pw$service == "sling_procedure"], 0.35)
  # AI carries a follow-up hazard but NO reoperation row -- a separate gap,
  # recorded so it is not mistaken for a modelling decision
  ai_reop <- pw[pw$condition == "ai" & pw$stage == "recurrence" &
                  grepl("procedure", pw$service), ]
  expect_equal(nrow(ai_reop), 0L)
})
