# Guards for the demand-transition registry (R/25b) and its publication gate.
#
# The registry is ADDITIVE and output-preserving: lifecourse_risk_params(),
# lifecourse_risk_params_cited() and lifecourse_pathway_params() must return
# structures byte-identical to the pre-registry hardcoded lists. The gate
# assert_publishable_demand_coefficients() must apply the canonical calibration contract.

# Rebuild the exact pre-registry structures as the regression baseline.
.expected_risk <- function() {
  mk <- function(b0, bvag, bage, bysl, bbmi, bhyst, bmeno, bcomorb)
    list(b0 = b0, bvag = bvag, bage = bage, bysl = bysl,
         bbmi = bbmi, bhyst = bhyst, bmeno = bmeno, bcomorb = bcomorb)
  list(status = "placeholder_uncalibrated",
       ui  = mk(-1.60, 0.18, 0.35, 0.05, 0.12, 0.10, 0.25, 0.15),
       pop = mk(-2.40, 0.42, 0.30, 0.08, 0.08, 0.45, 0.20, 0.05),
       ai  = mk(-2.90, 0.22, 0.25, 0.04, 0.06, 0.05, 0.10, 0.20))
}
.expected_cited <- function() {
  base <- .expected_risk(); base$status <- "obstetric_literature_anchored"
  base$ui$bvag <- 0.15; base$ui$bbmi <- 0.26
  base$pop$bvag <- 0.30; base$pop$bbmi <- 0.26; base$ai$bvag <- 0.10
  base
}
.expected_pathway <- function() {
  list(status = "placeholder_uncalibrated",
       recognition = c(ui = 0.55, pop = 0.60, ai = 0.35),
       p_seek      = c(ui = 0.45, pop = 0.50, ai = 0.30),
       p_referral  = c(ui = 0.40, pop = 0.55, ai = 0.45),
       p_treated   = c(ui = 0.70, pop = 0.65, ai = 0.60))
}

test_that("registry reconstructs the risk params byte-identically", {
  expect_identical(lifecourse_risk_params(), .expected_risk())
})

test_that("registry reconstructs the cited risk params byte-identically", {
  expect_identical(lifecourse_risk_params_cited(), .expected_cited())
})

test_that("registry reconstructs the pathway params byte-identically", {
  expect_identical(lifecourse_pathway_params(), .expected_pathway())
})

test_that("the bespoke status strings survive (meta$status keys off them)", {
  expect_identical(lifecourse_risk_params()$status, "placeholder_uncalibrated")
  expect_identical(lifecourse_risk_params_cited()$status, "obstetric_literature_anchored")
})

test_that("the registry is well-formed and uses only canonical tiers", {
  reg <- demand_transition_registry()
  expect_true(all(c("stage", "condition", "param", "variant", "value",
                    "ci_low", "ci_high", "calibration_tier", "source", "notes") %in% names(reg)))
  expect_true(all(reg$calibration_tier %in% CALIBRATION_TIERS))
  # Every transition coefficient in the loaded life-course path is represented.
  expect_setequal(unique(reg$stage[reg$variant == "default"]),
                  c("disease_state", "symptom_severity", "care_seeking", "referral",
                    "treatment_preference"))
  # The cited overrides are exactly the five literature-anchored terms.
  cited <- reg[reg$variant == "cited", ]
  expect_equal(nrow(cited), 5L)
  expect_true(all(cited$calibration_tier == "derived_by_analogy"))
  expect_true(all(nzchar(cited$source)))
})

test_that("placeholder coefficients are refused by the publication gate", {
  # Default variant is all placeholders -> refused in strict, warns in relaxed.
  expect_error(assert_publishable_demand_coefficients("default", mode = "strict"))
  expect_false(assert_publishable_demand_coefficients("default", mode = "relaxed"))
  # allow_analogy cannot rescue a placeholder tier.
  expect_error(assert_publishable_demand_coefficients("default", allow_analogy = TRUE, mode = "strict"))
})

test_that("the cited variant is still refused (pathway probs remain placeholders)", {
  # Surfaces the tier-mixing the bespoke status string hid: cited fixes 5 risk
  # coefficients but the care-pathway probabilities are still uncalibrated.
  expect_error(assert_publishable_demand_coefficients("cited", mode = "strict"))
  expect_error(assert_publishable_demand_coefficients("cited", allow_analogy = TRUE, mode = "strict"))
})
