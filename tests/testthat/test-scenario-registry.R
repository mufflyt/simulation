# Guards for the policy-lever scenario extensions added to the supply registry
# (reporting-scenario_registry.R): telemedicine, AI documentation, burnout
# reduction. Each is modeled as a small parameter shift on an axis the engine
# already consumes (hours_multiplier / retirement_shift_years), and each must be
# first-class (present via supply_scenario_registry()) and contract-valid.

NEW_SCENARIOS <- c("telemedicine", "ai_documentation", "burnout_reduction")

test_that("the three policy-lever scenarios are registered and first-class", {
  loc <- local_supply_scenario_registry(55)
  expect_true(all(NEW_SCENARIOS %in% names(loc)))
  # first-class through the public entry point too (SSOT or fallback), never
  # shadowing an existing id.
  reg <- supply_scenario_registry(55)
  expect_true(all(NEW_SCENARIOS %in% names(reg)))
  # extensions are appended, not overriding the reference scenario
  expect_true(any(c("baseline", "status_quo") %in% names(reg)))
})

test_that("each scenario changes only the intended parameters", {
  loc <- local_supply_scenario_registry(55)
  sq <- loc$status_quo
  changed <- function(s) {
    fields <- setdiff(names(s), c("label", "source"))
    sum(vapply(fields, function(f) !isTRUE(all.equal(s[[f]], sq[[f]])), logical(1)))
  }
  # AI documentation and telemedicine: a single hours_multiplier lever.
  expect_gt(loc$ai_documentation$hours_multiplier, 1)
  expect_equal(changed(loc$ai_documentation), 1L)
  expect_gt(loc$telemedicine$hours_multiplier, 1)
  expect_equal(changed(loc$telemedicine), 1L)
  # Burnout reduction: early-career attrition -- a single career_change_multiplier
  # lever (< 1 = fewer age-flat early-career exits), NOT a retirement-curve shift.
  # Burnout attrition concentrates in <50 providers that retirement_shift_years
  # cannot touch, so it acts on the career-change hazard instead.
  expect_equal(loc$burnout_reduction$career_change_multiplier, 0.75)
  expect_lt(loc$burnout_reduction$career_change_multiplier, 1)
  expect_equal(loc$burnout_reduction$retirement_shift_years, 0)
  expect_equal(loc$burnout_reduction$hours_multiplier, 1)
  expect_equal(changed(loc$burnout_reduction), 1L)
})

test_that("the extended registry still satisfies the supply contract", {
  expect_invisible(validate_scenario_registry(local_supply_scenario_registry(55), "supply"))
  expect_invisible(validate_scenario_registry(supply_scenario_registry(55), "supply"))
  # No scalar hazard multiplier crept in (the convention the validator forbids).
  loc <- local_supply_scenario_registry(55)
  expect_true(all(vapply(NEW_SCENARIOS, function(s) is.null(loc[[s]]$hazard_mult), logical(1))))
})

test_that("each new scenario is labeled ASSUMED/ILLUSTRATIVE, not a measured rate", {
  loc <- local_supply_scenario_registry(55)
  for (s in NEW_SCENARIOS) {
    expect_match(loc[[s]]$source, "ASSUMED/ILLUSTRATIVE")
  }
})
