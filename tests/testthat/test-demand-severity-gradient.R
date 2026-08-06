# Guards for the symptom-severity stage (Phase 3): severity-weighted care-seeking.
#
# The severity stage is ADDITIVE and NEUTRAL by default -- the severity->care-
# seeking multipliers are all 1, so demand is byte-identical to the pre-severity
# flat chain until a gradient is supplied. These tests lock that neutrality, the
# gradient arithmetic, and the registry/tier wiring.

# The pre-severity flat care-seeking model, as the neutrality baseline.
.flat_treated <- function(pop, pathway, access_gain) {
  seek <- function(cond) {
    base <- pathway$p_seek[[cond]]
    ifelse(pop$high_barrier == 1L, pmin(1, base * access_gain), base)
  }
  treated <- function(cond, prev)
    prev * pathway$recognition[[cond]] * seek(cond) *
      pathway$p_referral[[cond]] * pathway$p_treated[[cond]]
  pop$treated_ui  <- treated("ui",  pop$p_ui)
  pop$treated_pop <- treated("pop", pop$p_pop)
  pop$treated_ai  <- treated("ai",  pop$p_ai)
  pop$care_seeking_state <-
    1 - (1 - pop$p_ui  * pathway$recognition[["ui"]]  * seek("ui")) *
        (1 - pop$p_pop * pathway$recognition[["pop"]] * seek("pop")) *
        (1 - pop$p_ai  * pathway$recognition[["ai"]]  * seek("ai"))
  pop
}

.demo_pop <- function(seed = 1L, high_barrier = NULL) {
  set.seed(seed)
  hb <- if (is.null(high_barrier)) rep(c(0L, 1L), 50) else rep(as.integer(high_barrier), 100)
  tibble::tibble(high_barrier = hb,
                 p_ui  = runif(100, 0.10, 0.50),
                 p_pop = runif(100, 0.05, 0.30),
                 p_ai  = runif(100, 0.02, 0.20))
}

test_that("severity params are neutral by default", {
  sp <- lifecourse_severity_params()
  expect_identical(sp$levels, c("slight", "moderate", "severe", "very_severe"))
  for (cc in c("ui", "pop", "ai")) {
    expect_equal(sum(sp$shares[[cc]]), 1, tolerance = 1e-9)
    expect_true(all(sp$seek_multiplier[[cc]] == 1))
  }
})

test_that("neutral severity leaves care-seeking byte-identical to the flat model", {
  pop <- .demo_pop()
  path <- lifecourse_pathway_params()
  new <- .lifecourse_treated(pop, path, access_gain = 1)          # default neutral severity
  old <- .flat_treated(pop, path, access_gain = 1)
  expect_identical(new$treated_ui, old$treated_ui)
  expect_identical(new$treated_pop, old$treated_pop)
  expect_identical(new$treated_ai, old$treated_ai)
  expect_identical(new$care_seeking_state, old$care_seeking_state)
})

test_that("a severity gradient shifts care-seeking by the share-weighted mean, per condition", {
  pop <- .demo_pop(seed = 2L, high_barrier = 0L)   # no access cap -> clean linear scaling
  path <- lifecourse_pathway_params()
  base <- .lifecourse_treated(pop, path, access_gain = 1)
  sp <- lifecourse_severity_params()
  sp$seek_multiplier$ui <- c(slight = 1, moderate = 1, severe = 2, very_severe = 3)  # severe seek more
  up <- .lifecourse_treated(pop, path, access_gain = 1, severity = sp)

  expect_gt(mean(up$treated_ui), mean(base$treated_ui))   # more care-seeking
  expect_identical(up$treated_pop, base$treated_pop)       # UI-only gradient: others untouched
  expect_identical(up$treated_ai, base$treated_ai)
  # Exact: treated scales by weighted_mean(multiplier; shares).
  wm <- sum(sp$shares$ui * sp$seek_multiplier$ui) / sum(sp$shares$ui)
  expect_equal(mean(up$treated_ui) / mean(base$treated_ui), wm, tolerance = 1e-9)

  # A sub-unit gradient (mild dominate) reduces care-seeking.
  sp$seek_multiplier$ui <- c(slight = 0.5, moderate = 1, severe = 1, very_severe = 1)
  down <- .lifecourse_treated(pop, path, access_gain = 1, severity = sp)
  expect_lt(mean(down$treated_ui), mean(base$treated_ui))
})

test_that("the registry exposes the severity stage with canonical tiers", {
  reg <- demand_transition_registry()
  expect_true(all(reg$calibration_tier %in% CALIBRATION_TIERS))
  sev <- reg[reg$stage == "symptom_severity", ]
  expect_equal(nrow(sev), 12L)                                  # 3 conditions x 4 levels
  expect_true(all(sev$calibration_tier[sev$condition == "ui"] == "derived_by_analogy"))
  expect_true(all(sev$calibration_tier[sev$condition %in% c("pop", "ai")] == "uncalibrated_illustrative"))
  mult <- reg[startsWith(reg$param, "seek_mult_"), ]
  expect_equal(nrow(mult), 12L)
  expect_true(all(mult$value == 1))                            # neutral default
})

test_that("simulate_lifecourse_demand exposes severity_params", {
  expect_true("severity_params" %in% names(formals(simulate_lifecourse_demand)))
})
