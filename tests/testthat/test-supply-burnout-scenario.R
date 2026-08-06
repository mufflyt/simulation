# Guards for the burnout-reduction supply scenario.
#
# Burnout acts on the AGE-FLAT early-exit (career-change) hazard, not the
# age-graded retirement curve, so it is expressed as a career_change_multiplier
# and threaded into run_supply_microsimulation()'s career_change_hazard. The
# default multiplier is neutral, so an unscenario'd run is output-preserving.

test_that("the burnout_reduction scenario is registered and contract-valid", {
  reg <- local_supply_scenario_registry(baseline_entrants = 55)
  expect_true("burnout_reduction" %in% names(reg))
  b <- reg$burnout_reduction
  expect_equal(b$career_change_multiplier, 0.75)
  expect_lt(b$career_change_multiplier, 1)                 # fewer early exits
  # It must not smuggle in a forbidden retirement-curve multiplier.
  expect_null(b$hazard_mult)
  expect_equal(b$retirement_shift_years, 0)
  # Passes the supply scenario contract (required fields + no hazard_mult).
  expect_invisible(validate_scenario_registry(reg, kind = "supply"))
})

test_that("run_supply_microsimulation exposes career_change_hazard and responds to it", {
  expect_true("career_change_hazard" %in% names(formals(run_supply_microsimulation)))

  # All agents start at 38 and never reach the age-50 retirement regime across
  # the horizon, so the ONLY exit process is the career-change (burnout) hazard.
  young <- initialize_provider_agents(150, "FPMRS", baseline_year = 2025L,
                                      age_distribution = rep(38, 150))
  run <- function(cc) run_supply_microsimulation(
    young, years = 2025:2035, entrants_per_year = 0, n_iterations = 30,
    career_change_hazard = cc, allow_fixed_parameters = TRUE,
    seed = 5L, verbose = FALSE)

  none <- run(0)      # burnout eliminated -> nobody leaves
  heavy <- run(0.40)  # heavy early attrition

  end_none  <- tail(none$summary$headcount_median, 1)
  end_heavy <- tail(heavy$summary$headcount_median, 1)
  expect_equal(end_none, 150)                # no exits, no entrants
  expect_lt(end_heavy, 150)                  # burnout attrition removed providers
  expect_gt(end_none, end_heavy)             # reducing burnout raises supply
})

test_that("the neutral career-change default leaves supply output-preserving", {
  young <- initialize_provider_agents(120, "FPMRS", baseline_year = 2025L,
                                      age_distribution = rep(40, 120))
  a <- run_supply_microsimulation(young, 2025:2033, entrants_per_year = 0,
                                  n_iterations = 20, allow_fixed_parameters = TRUE,
                                  seed = 9L, verbose = FALSE)
  b <- run_supply_microsimulation(young, 2025:2033, entrants_per_year = 0,
                                  n_iterations = 20,
                                  career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50,
                                  allow_fixed_parameters = TRUE, seed = 9L, verbose = FALSE)
  expect_identical(a$summary$headcount_median, b$summary$headcount_median)
})
