# The demand-coefficient refusal must sit on the PUBLICATION path, not in the
# engine.
#
# Every POP pathway coefficient is calibration_tier "uncalibrated_illustrative",
# source "placeholder (expert judgement; not evidence-anchored)". Before this
# gate was wired, assert_publishable_demand_coefficients() was reachable only
# from validation_report() -- a report -- so a run could be exported as an
# authoritative result on placeholder coefficients without the gate ever firing.
#
# The contract these tests pin:
#   simulate  -> ALLOWED on uncalibrated coefficients (exploration, sweeps, tests)
#   validate  -> allowed, status stays explicit
#   publish   -> REFUSED unless the evidence tier is acceptable
#
# Wiring the refusal into simulate_lifecourse_demand() instead would break
# exploratory and sensitivity work that legitimately needs illustrative values.

test_that("the shipped coefficients are in fact uncalibrated", {
  # If this ever fails the gate tests below become vacuous, so assert the
  # premise rather than assuming it.
  r <- demand_transition_registry()
  expect_true(any(r$calibration_tier == "uncalibrated_illustrative"))
  pop <- r[r$condition == "pop" &
             r$param %in% c("recognition", "p_seek", "p_referral", "p_treated"), ]
  expect_true(all(pop$calibration_tier == "uncalibrated_illustrative"))
})

test_that("ordinary simulation still runs on uncalibrated coefficients", {
  # The engine must stay composable. A demand simulation with placeholder
  # coefficients is a legitimate exploratory artifact.
  #
  # REWRITTEN, because the original conflated two different claims by passing
  # the SHIPPED pathway. "Uncalibrated" is a PROVENANCE statement -- we do not
  # know the value -- and it must stay runnable, which is what this test is for.
  # per_entering = 1.00 on new_consultation is not that: it is an ARITHMETIC
  # error that turns a prevalence stock into an annual flow, and
  # assert_incident_not_prevalent() now refuses it (see
  # docs/INCIDENT_ENTRY_ESTIMAND.md).
  #
  # The doctrine is preserved by using a pathway that is still uncalibrated --
  # every confidence is "low" -- but arithmetically coherent. Asserting that the
  # INVALID configuration runs is not a defence of composability; it is the
  # defect the guard exists to catch.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- condition_service_pathway()
  pw$per_entering[pw$service == "new_consultation"] <- 0.25
  expect_true(all(pw$confidence == "low"))   # still uncalibrated, deliberately
  expect_no_error(
    suppressMessages(
      pathway_service_volumes(treated = c(pop = 1000), year = 2025L, pathway = pw)))
})

test_that("the SHIPPED pathway is refused, and that is the current known state", {
  # The counterpart to the test above, kept adjacent so the two cannot drift
  # apart. The canonical configuration is exercised end to end by the
  # scientific-readiness gate, which stays red; this asserts the same refusal
  # at unit level. When per_entering is sourced, revisit both -- do not delete.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  expect_error(
    suppressMessages(pathway_service_volumes(treated = c(pop = 1000), year = 2025L)),
    "counted as a NEW patient annually")
})

test_that("the gate itself refuses uncalibrated coefficients in strict mode", {
  expect_error(
    suppressMessages(
      assert_publishable_demand_coefficients(variant = "default", mode = "strict")),
    regexp = "uncalibrated|illustrative|publish")
})

test_that("the publication report carries the demand-coefficient check", {
  # The check must EXIST on the publication path -- this is the assertion that
  # would have caught the original defect, where the gate was callable but not
  # on the path that matters.
  skip_if_not(file.exists("../../R/calibration-validation.R"))
  src <- readLines("../../R/calibration-validation.R")
  body_start <- grep("^publishable_run_report <- function", src)
  body_end <- grep("^assert_publishable_run <- function", src)
  expect_length(body_start, 1L)
  expect_gt(body_end[1], body_start)
  body <- src[body_start:body_end[1]]
  expect_true(any(grepl("assert_publishable_demand_coefficients", body, fixed = TRUE)),
              info = "the publication report must call the demand-coefficient gate")
  expect_true(any(grepl("demand_coefficients_publishable", body, fixed = TRUE)))
})

test_that("the publication check fails while coefficients are placeholders", {
  # A minimal result object: publishable_run_report() should mark the
  # demand-coefficient check FAILED regardless of the other checks' outcomes.
  rep <- suppressMessages(
    publishable_run_report(list(), artifact_path = NA_character_,
                           require_artifact = FALSE))
  row <- rep[rep$check == "demand_coefficients_publishable", ]
  expect_equal(nrow(row), 1L)
  expect_false(row$passed[[1]])
  expect_match(row$detail[[1]], "not publishable")
})

test_that("assert_publishable_run refuses the run in strict mode", {
  expect_error(
    suppressMessages(
      assert_publishable_run(list(), artifact_path = NA_character_,
                             require_artifact = FALSE, mode = "strict")),
    regexp = "not publishable")
})

test_that("an acceptable evidence tier lets the publication check pass", {
  # Proves the gate is a gate and not a wall: with analogy-tier coefficients
  # explicitly opted into, the same assertion succeeds. Without this, a check
  # that always failed would look identical to a working gate.
  expect_no_error(
    suppressMessages(
      assert_publishable_workload(status = "calibrated", what = "fixture",
                                  mode = "strict")))
  expect_no_error(
    suppressMessages(
      assert_publishable_workload(status = "derived_by_analogy", allow_analogy = TRUE,
                                  what = "fixture", mode = "strict")))
  # and analogy WITHOUT the opt-in is still refused
  expect_error(
    suppressMessages(
      assert_publishable_workload(status = "derived_by_analogy", allow_analogy = FALSE,
                                  what = "fixture", mode = "strict")))
})

# ---------------------------------------------------------------------------
# SECOND, DISTINCT GATE: the live condition-service pathway.
#
# The registry gate above covers recognition / seek / referral / eligibility /
# treatment preference / disease coefficients. It does NOT cover the staged
# pathway CSV, which is where the POP conservative (0.35) and testing (0.55)
# p_advance values live -- the two numbers responsible for the 4.68x prolapse
# discrepancy. Without a separate gate, the registry could be calibrated and an
# unsourced POP cascade would still publish.

test_that("the shipped pathway is uncalibrated, and it is the POP one", {
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- condition_service_pathway()
  expect_identical(condition_pathway_status(pw), "uncalibrated_illustrative")
  # specifically the artifact holding 0.35 and 0.55, not the registry
  pop <- pw[pw$condition == "pop", ]
  expect_equal(unique(pop$p_advance[pop$stage == "conservative"]), 0.35)
  # testing is NON-GATING for POP after the estimand restructure: p_advance 1.0
  expect_equal(unique(pop$p_advance[pop$stage == "testing"]), 1.00)
  expect_true(all(pop$confidence == "low"))
})

test_that("low confidence alone does not block simulation; invalid arithmetic does", {
  # SAME REWRITE as above, at production cohort scale. Low confidence must not
  # block -- that is the whole point of the calibration-tier vocabulary, which
  # labels rather than refuses. But the shipped table is refused for a reason
  # that has nothing to do with confidence: at 3,264,807 treated it produces
  # 3,264,807 new consultations, ratio exactly 1.00.
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  pw <- condition_service_pathway()
  pw$per_entering[pw$service == "new_consultation"] <- 0.25
  expect_no_error(
    suppressMessages(
      pathway_service_volumes(treated = c(pop = 3264807), year = 2025L, pathway = pw)))

  # 0.25 is a TEST FIXTURE, not a candidate value. It is chosen only to be a
  # plausible flow rather than a stock, and must never be read as an estimate of
  # the incident-entry parameter -- that estimand is unresolved and its
  # estimator is pre-registered in docs/INCIDENT_ENTRY_ESTIMAND.md.
  expect_error(
    suppressMessages(
      pathway_service_volumes(treated = c(pop = 3264807), year = 2025L,
                              pathway = condition_service_pathway())),
    "counted as a NEW patient annually")
})

test_that("publication refuses the shipped pathway", {
  rep <- suppressMessages(
    publishable_run_report(
      list(scenario_meta = list(demand_coefficient_tier = "calibrated",
                                service_pathway = "condition_staged",
                                pathway_status = condition_pathway_status())),
      artifact_path = NA_character_, require_artifact = FALSE))
  row <- rep[rep$check == "condition_service_pathway_publishable", ]
  expect_equal(nrow(row), 1L)
  expect_false(row$passed[[1]])
  expect_match(row$detail[[1]], "condition-service pathway is not publishable")
  # the registry check must be SEPARATELY satisfied here, proving the two gates
  # are independent and that this refusal comes from the pathway alone
  expect_true(rep$passed[rep$check == "demand_coefficients_publishable"])
})

test_that("assert_publishable_run refuses on the pathway alone", {
  expect_error(
    suppressMessages(
      assert_publishable_run(
        list(scenario_meta = list(demand_coefficient_tier = "calibrated",
                                  service_pathway = "condition_staged",
                                  pathway_status = "uncalibrated_illustrative")),
        artifact_path = NA_character_, require_artifact = FALSE, mode = "strict")),
    regexp = "condition_service_pathway_publishable")
})

test_that("a high-confidence pathway fixture clears the boundary", {
  skip_if_not(file.exists("../../inst/extdata/pathway/condition_service_pathway.csv"))
  # Raise every POP row to high confidence; condition_pathway_status() must then
  # resolve to "calibrated" and the publication check must pass. This proves the
  # gate is a gate, not a wall.
  pw <- condition_service_pathway()
  pw$confidence <- "high"
  expect_identical(condition_pathway_status(pw), "calibrated")

  rep <- suppressMessages(
    publishable_run_report(
      list(scenario_meta = list(demand_coefficient_tier = "calibrated",
                                service_pathway = "condition_staged",
                                pathway_status = condition_pathway_status(pw))),
      artifact_path = NA_character_, require_artifact = FALSE))
  expect_true(rep$passed[rep$check == "condition_service_pathway_publishable"])
})

test_that("the legacy flat service map is never publishable", {
  # per_treated = 0.25 has no provenance columns whatsoever, so the legacy
  # branch must not become a way around the pathway gate.
  rep <- suppressMessages(
    publishable_run_report(
      list(scenario_meta = list(demand_coefficient_tier = "calibrated",
                                service_pathway = "flat_service_map",
                                pathway_status = NA_character_)),
      artifact_path = NA_character_, require_artifact = FALSE))
  row <- rep[rep$check == "condition_service_pathway_publishable", ]
  expect_false(row$passed[[1]])
  expect_match(row$detail[[1]], "legacy flat service map")
})
