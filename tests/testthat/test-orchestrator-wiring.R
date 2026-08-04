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

test_that("the entrant-policy scenarios are not inert", {
  skip_if_not_installed("mufflyaccess")
  skip_on_cran()
  # THE REGRESSION THIS LOCKS. A single param_spec was shared across scenarios,
  # and `entrant_mean` takes precedence over `entrants_per_year` inside the
  # engine, so every scenario ran at the same entrant rate: "Fellowship output
  # +10%" and "-10%" returned results IDENTICAL TO BASELINE to the last digit.
  # The most policy-relevant lever in the model did nothing.
  r <- run_workforce_microsimulation(
    years = 2025:2030, n_iterations = 12, baseline_entrants = 70,
    baseline_gap_estimate = baseline_gap(
      base_supply_fte = 1306, adequacy = 0.95, method = "capacity_survey",
      evidence = "test"),
    allow_analogy = TRUE, verbose = FALSE
  )
  fin <- r$supply[r$supply$year == max(r$supply$year), ]
  get <- function(pat) fin$effective_fte_median[grepl(pat, fin$scenario_label)]
  base <- get("^Baseline")
  up <- get("Fellowship output \\+10")
  down <- get("Fellowship output constrained")

  expect_length(base, 1); expect_length(up, 1); expect_length(down, 1)
  expect_gt(up, base)
  expect_lt(down, base)
})

test_that("baseline_entrants controls the run rather than being overridden", {
  skip_if_not_installed("mufflyaccess")
  skip_on_cran()
  # The spec's entrant_mean used to win silently, so passing baseline_entrants
  # changed nothing and the run warned about its own inconsistency every time.
  mk <- function(e) {
    r <- run_workforce_microsimulation(
      years = 2025:2030, n_iterations = 12, baseline_entrants = e,
      baseline_gap_estimate = baseline_gap(
        base_supply_fte = 1306, adequacy = 0.95, method = "capacity_survey",
        evidence = "test"),
      allow_analogy = TRUE, verbose = FALSE)
    fin <- r$supply[r$supply$year == max(r$supply$year) &
                      grepl("^Baseline", r$supply$scenario_label), ]
    list(fte = fin$effective_fte_median, meta = r$scenario_meta)
  }
  lo <- mk(40); hi <- mk(100)
  expect_gt(hi$fte, lo$fte)
  expect_equal(lo$meta$baseline_entrants, 40)
  expect_equal(lo$meta$entrants_source, "caller_supplied")
})

test_that("a roster with state does not require placement shares", {
  # THE DEFECT THIS PINS. Carrying `state` turns the engine's geography vector
  # on, but placement_shares is NULL whenever the geographic layer is inactive.
  # Entrant placement then threw an assertion from inside
  # assign_entrant_geography() -- reachable ONLY by a caller supplying a real
  # roster, so it stayed latent until the first production cohort was used.
  agents <- tibble::tibble(
    provider_id = sprintf("P%03d", 1:60),
    subspecialty = "FPMRS",
    sex = rep(c("female", "male"), 30),
    state = rep(c("CO", "TX", "NY"), 20),
    age = seq(40, 69, length.out = 60),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "roster", clinical_fte = 1
  )
  sim <- simulate_provider_career_once(agents, 2025:2028, entrants_per_year = 10,
                                       fte_method = "hours")
  expect_gt(nrow(sim$panel), 0)

  # Existing providers keep their observed state; entrants get NA, because where
  # a future graduate practises is unknown without a placement rule. Inventing
  # one would let a geographic result appear that nobody asked for.
  ent <- sim$agents[sim$agents$origin_cohort == "entrant", ]
  expect_gt(nrow(ent), 0)
  expect_true(all(is.na(ent$state)))
  base <- sim$agents[sim$agents$origin_cohort == "roster", ]
  expect_true(all(base$state %in% c("CO", "TX", "NY")))
})
