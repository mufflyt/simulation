# Guards for Phase 4: the treatment-eligibility stage + Sandvik-derived UI shares.
#
# Both are ADDITIVE and NEUTRAL/opt-in by default -- p_eligible = 1 keeps
# p_eligible x p_treated == the old p_treated (byte-identical), and severity
# shares stay placeholders unless a SWAN panel is supplied.

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
  pop
}

.demo_pop <- function(seed = 1L) {
  set.seed(seed)
  tibble::tibble(high_barrier = rep(c(0L, 1L), 50),
                 p_ui = runif(100, 0.10, 0.50), p_pop = runif(100, 0.05, 0.30),
                 p_ai = runif(100, 0.02, 0.20))
}

# ---- Treatment-eligibility stage (4A) --------------------------------------

test_that("eligibility params are neutral by default", {
  el <- lifecourse_eligibility_params()
  expect_true(all(el$p_eligible == 1))
  expect_setequal(names(el$p_eligible), c("ui", "pop", "ai"))
})

test_that("neutral eligibility leaves treated demand byte-identical", {
  pop <- .demo_pop(); path <- urpssim:::lifecourse_pathway_params()
  new <- urpssim:::.lifecourse_treated(pop, path, access_gain = 1)   # neutral severity + eligibility
  old <- .flat_treated(pop, path, access_gain = 1)
  expect_identical(new$treated_ui,  old$treated_ui)
  expect_identical(new$treated_pop, old$treated_pop)
  expect_identical(new$treated_ai,  old$treated_ai)
})

test_that("an eligibility gate scales treated demand, per condition", {
  pop <- .demo_pop(); path <- urpssim:::lifecourse_pathway_params()
  base <- urpssim:::.lifecourse_treated(pop, path, access_gain = 1)
  el <- lifecourse_eligibility_params(); el$p_eligible["ui"] <- 0.5
  gated <- urpssim:::.lifecourse_treated(pop, path, access_gain = 1, eligibility = el)
  expect_equal(gated$treated_ui, base$treated_ui * 0.5, tolerance = 1e-12)
  expect_identical(gated$treated_pop, base$treated_pop)   # UI-only gate
  expect_identical(gated$treated_ai,  base$treated_ai)
})

test_that("the registry exposes a treatment_eligibility stage (neutral placeholder)", {
  reg <- demand_transition_registry()
  el <- reg[reg$stage == "treatment_eligibility", ]
  expect_equal(nrow(el), 3L)
  expect_true(all(el$value == 1))
  expect_true(all(el$calibration_tier == "uncalibrated_illustrative"))
})

test_that("simulate_lifecourse_demand exposes eligibility_params", {
  expect_true("eligibility_params" %in% names(formals(simulate_lifecourse_demand)))
})

# ---- Sandvik-derived UI severity shares (4B) -------------------------------

.demo_swan_panel <- function() {
  # 7 leakage rows spanning the Sandvik index, + a continent row + an NA row
  # (both must be dropped, not counted as "slight").
  tibble::tibble(
    swan_id = 1:9, visit = 1L,
    leakage_ever    = c(1, 1, 1, 1, 1, 1, 1, 0, NA),
    frequency_level = c(1, 1, 2, 2, 3, 4, 4, NA, NA),
    amount_level    = c(1, 2, 1, 3, 3, 4, 4, NA, NA),
    amount_scope    = "any")
}

test_that("severity params are placeholders (uncalibrated) without a SWAN panel", {
  sp <- lifecourse_severity_params()
  expect_identical(sp$status, "placeholder_uncalibrated")
  reg <- demand_transition_registry()
  ui <- reg[reg$stage == "symptom_severity" & reg$condition == "ui", ]
  expect_true(all(ui$calibration_tier == "uncalibrated_illustrative"))
})

test_that("a SWAN panel yields Sandvik-derived UI shares (NA dropped, not folded)", {
  skip_if_not(exists("score_sandvik_severity"), "Sandvik scorer unavailable")
  sp <- lifecourse_severity_params(swan_panel = .demo_swan_panel())
  expect_identical(sp$status, "ui_sandvik_derived")
  expect_equal(sum(sp$shares$ui), 1, tolerance = 1e-9)     # normalised over the 4 levels
  expect_identical(names(sp$shares$ui), c("slight", "moderate", "severe", "very_severe"))
  expect_false(any(is.na(sp$shares$ui)))
  # POP/AI stay placeholders (no instrument).
  expect_equal(unname(sp$shares$pop), c(0.45, 0.35, 0.15, 0.05))
})

test_that("lifecourse_severity_params exposes a swan_panel argument", {
  expect_true("swan_panel" %in% names(formals(lifecourse_severity_params)))
})
