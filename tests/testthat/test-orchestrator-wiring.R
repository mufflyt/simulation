# Guards that exist but are never called are the defect this file exists to
# prevent. Both of these were fully implemented and unreachable from a run:
#
#   * assert_demand_calibrated() -- defined in R/21, called by nothing, so the
#     orchestrator accepted `calibration`, stored it in the metadata, and never
#     checked it. Demand totals anchored to no observed quantity passed silently.
#   * the geography layer in R/20 (opportunity_placement_shares(), entrant
#     placement, mid-career migration) -- reachable only by calling
#     simulate_provider_career_once() directly, because the orchestrator never
#     passed `placement_shares`. Every run was national-headcount-only.
#
# These tests assert the WIRING, not the guards' internals, which are tested
# where they are defined.

ow_run <- function(...) {
  set.seed(1)
  run_workforce_microsimulation(
    baseline_supply = 120,
    use_certification_cohorts = FALSE,
    years = 2025:2027,
    n_iterations = 2,
    supply_scenarios = supply_scenario_registry(20)["baseline"],
    pop_by_band = example_female_population_by_band(2025:2027),
    baseline_gap_estimate = baseline_gap(120, 0.95, method = "assumed",
                                         evidence = "test fixture"),
    verbose = FALSE,
    ...
  )
}

ow_shares <- function() {
  tibble::tibble(geo = c("CO", "NY", "TX"), share = c(0.5, 0.3, 0.2))
}

ow_calibration <- function() {
  fit_calibration_scalars(
    predicted = tibble::tibble(category = c("office", "surgery"),
                              predicted = c(1000, 200)),
    observed  = tibble::tibble(category = c("office", "surgery"),
                              observed  = c(1100, 190))
  )
}

test_that("a run whose demand was never calibrated says so", {
  expect_message(ow_run(), "not calibrated to an independent national anchor")
  expect_false(suppressMessages(ow_run())$scenario_meta$demand_calibrated)
})

test_that("supplying calibration scalars satisfies the gate", {
  cal <- ow_calibration()
  r <- suppressMessages(ow_run(calibration = cal))
  expect_true(r$scenario_meta$demand_calibrated)
  # The HDMM Exhibit 11 scalar is observed / predicted.
  expect_equal(cal$scalar[cal$category == "office"], 1.1, tolerance = 1e-8)
})

test_that("placement shares reach the engine and are recorded on the run", {
  r <- suppressMessages(ow_run(placement_shares = ow_shares(),
                               seed_base_geography = TRUE))
  gp <- r$scenario_meta$geographic_placement
  expect_true(gp$active)
  expect_equal(gp$n_geographies, 3L)
  expect_true(gp$base_geography_seeded)
  # The run still produces a supply panel; geography must not cost the estimand.
  expect_true(nrow(r$supply) > 0)
  expect_true(all(c("effective_fte_median", "headcount_median") %in% names(r$supply)))
})

test_that("shares without a geographic cohort are refused, not silently inert", {
  # The certification contract ships aggregate counts with no state, so passing
  # shares alone would leave entrant placement and migration doing nothing at
  # all -- silently, which is the failure this repository keeps rediscovering.
  expect_message(ow_run(placement_shares = ow_shares()), "would do nothing")
  gp <- suppressMessages(
    ow_run(placement_shares = ow_shares()))$scenario_meta$geographic_placement
  expect_false(gp$active)
  expect_equal(gp$n_geographies, 0L)
})

test_that("seeding the base geography announces itself as an assumption", {
  expect_message(ow_run(placement_shares = ow_shares(), seed_base_geography = TRUE),
                 "DRAWN")
  # A national run records the layer as off rather than omitting the field.
  gp <- suppressMessages(ow_run())$scenario_meta$geographic_placement
  expect_false(gp$active)
})

test_that("entrants are placed by the supplied shares, not uniformly", {
  # Unit level: the orchestrator's job is to pass the shares through, but the
  # placement itself must honour them or the wiring buys nothing.
  set.seed(11)
  shares <- tibble::tibble(geo = c("CO", "NY"), share = c(0.9, 0.1))
  a <- initialize_provider_agents(60, "FPMRS", 2025)
  a$sex <- "female"
  a$state <- assign_entrant_geography(nrow(a), shares)
  sim <- simulate_provider_career_once(a, 2025:2035, 20, placement_shares = shares,
                                       hours_intercept = calibrate_hours_intercept(a$age, a$sex))
  ent <- sim$agents[sim$agents$origin_cohort == "entrant", ]
  expect_gt(nrow(ent), 50)
  expect_setequal(unique(ent$state), c("CO", "NY"))
  expect_gt(mean(ent$state == "CO"), 0.75)   # 0.9 target, multinomial spread
})
