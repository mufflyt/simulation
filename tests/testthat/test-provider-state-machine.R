# Guards for the provider career state machine (R/supply-provider_state_machine) and its engine hook.
#
# The state machine is ADDITIVE and OUTPUT-PRESERVING: it reconstructs the R/16
# physician retirement schedule byte-identically, and turning on state tracking
# in simulate_provider_career_once() must leave the published panel columns and
# the seeded RNG stream untouched. These tests lock that, the state labelling,
# the registry tiers, and the publication gate.

# ---- Registry shape + canonical tiers -------------------------------------

test_that("the registry is well-formed and uses only canonical tiers", {
  reg <- career_transition_registry()
  expect_true(all(c("from_state", "to_state", "trigger", "param", "value",
                    "age_lo", "age_hi", "ci_low", "ci_high", "calibration_tier",
                    "source", "role", "notes") %in% names(reg)))
  expect_true(all(reg$calibration_tier %in% CALIBRATION_TIERS))
  expect_true(all(reg$role %in% c("progression", "departure", "participation",
                                  "scenario_lever")))
  # Progression is definitional (solved); the core hazards are analogy-derived.
  expect_true(all(reg$calibration_tier[reg$role == "progression"] == "solved"))
  expect_true(all(reg$calibration_tier[reg$role == "departure"] == "derived_by_analogy"))
})

test_that("scenario levers are neutral and uncalibrated", {
  reg <- career_transition_registry()
  lev <- reg[reg$role == "scenario_lever", ]
  expect_setequal(lev$param, c("burnout_hazard_multiplier",
                               "parental_leave_fte_multiplier",
                               "medicare_participation_multiplier",
                               "medicaid_participation_multiplier"))
  expect_true(all(lev$value == 1))                              # neutral identity
  expect_true(all(lev$calibration_tier == "uncalibrated_illustrative"))
})

# ---- Byte-identity: the registry is the SSOT for the R/16 schedule --------

test_that("the registry reconstructs RETIREMENT_HAZARD_PHYSICIAN byte-identically", {
  expect_identical(.career_retirement_schedule(), RETIREMENT_HAZARD_PHYSICIAN)
})

test_that("state_departure_hazard is identical to departure_hazard across ages", {
  ages <- 30:92
  for (sx in c("male", "female")) {
    expect_identical(state_departure_hazard(ages, sx), departure_hazard(ages, sx))
  }
})

# ---- Career-state labelling ------------------------------------------------

test_that("career_state_of partitions age and respects the absorbing state", {
  st <- career_state_of(c(38, 47, 62, 80), entered = TRUE, retired = FALSE)
  expect_identical(as.character(st),
                   c("early_career", "mid_career", "late_career", "late_career"))
  expect_identical(levels(st), CAREER_STATES)
  # Retired overrides the age band; pipeline for not-yet-entered.
  expect_identical(as.character(career_state_of(55, entered = TRUE, retired = TRUE)),
                   "retired")
  expect_identical(as.character(career_state_of(33, entered = FALSE)), "fellow")
})

# ---- Engine hook is output-preserving -------------------------------------

.mk_cohort <- function(seed = 11L) {
  set.seed(seed)
  initialize_provider_agents(200, "FPMRS", baseline_year = 2025L)
}

test_that("state tracking leaves the published panel columns byte-identical", {
  years <- 2025:2040
  base <- .mk_cohort()

  set.seed(99L)
  off <- simulate_provider_career_once(base, years, entrants_per_year = 40,
                                       track_career_states = FALSE)
  set.seed(99L)
  on  <- simulate_provider_career_once(base, years, entrants_per_year = 40,
                                       track_career_states = TRUE)

  published <- c("year", "subspecialty", "headcount", "effective_fte", "mean_age")
  expect_identical(off$panel[published], on$panel[published])
  # The agent table's pre-existing columns are untouched (career_state is added).
  expect_identical(off$agents, on$agents[names(off$agents)])
})

test_that("state counts sum to headcount and expose the extra columns", {
  years <- 2025:2035
  set.seed(7L)
  res <- simulate_provider_career_once(.mk_cohort(), years, entrants_per_year = 30,
                                       track_career_states = TRUE)
  p <- res$panel
  expect_true(all(c("n_early_career", "n_mid_career", "n_late_career",
                    "n_retired") %in% names(p)))
  expect_equal(p$n_early_career + p$n_mid_career + p$n_late_career, p$headcount)
  expect_true("career_state" %in% names(res$agents))
  expect_true(all(as.character(res$agents$career_state) %in% CAREER_STATES))
})

# ---- Publication gate ------------------------------------------------------

test_that("the core machine passes only under allow_analogy; levers force refusal", {
  # Core machine worst tier is derived_by_analogy: refused in strict, allowed
  # with allow_analogy.
  expect_error(assert_publishable_supply_transitions(mode = "strict"))
  expect_true(assert_publishable_supply_transitions(allow_analogy = TRUE, mode = "strict"))
  # Including the uncalibrated scenario levers forces a refusal even with opt-in.
  expect_error(assert_publishable_supply_transitions(include_scenario_levers = TRUE,
                                                     allow_analogy = TRUE, mode = "strict"))
  expect_false(assert_publishable_supply_transitions(include_scenario_levers = TRUE,
                                                      allow_analogy = TRUE, mode = "relaxed"))
})
