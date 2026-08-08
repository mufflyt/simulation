# Guards that exist but are never called are the defect this file exists to
# prevent. Both of these were fully implemented and unreachable from a run:
#
#   * assert_demand_calibrated() -- defined in R/calibration-validation, called by nothing, so the
#     orchestrator accepted `calibration`, stored it in the metadata, and never
#     checked it. Demand totals anchored to no observed quantity passed silently.
#   * the geography layer in R/geography-provider_geography (opportunity_placement_shares(), entrant
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
    # The fixture gap is "assumed_with_evidence", which the gate now asks a
    # caller to declare rather than inherit silently.
    allow_analogy = TRUE,
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
      # baseline_gap() now requires an explicit calibration tier: the same
      # arithmetic is 'calibrated' from a fielded URPS survey and
      # 'derived_by_analogy' from another specialty's published distribution,
      # and it refuses to infer which. These fixtures are neither.
      calibration_status = "uncalibrated_illustrative",
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
        calibration_status = "uncalibrated_illustrative", evidence = "test"),
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

test_that("the geographic access layer is wired into the run, fail-closed", {
  r <- suppressMessages(ow_run())
  # The orchestrator attaches $geographic_access. With no imported membership
  # artifact (data-raw/ is absent under R CMD check) and a synthetic cohort that
  # carries no provider identity, it resolves to FALSE with a reason -- wired,
  # but never computed on fallback geometry (the ordering trap).
  expect_false(is.null(r$geographic_access))
  expect_false(isTRUE(r$geographic_access$resolved))
  expect_true(nzchar(r$geographic_access$reason))
})

test_that("run_geographic_access() fails closed on each missing input, resolves on none", {
  mem <- tibble::tibble(demand_id = c("t1", "t2", "t3"),
                        provider_id = c("A", "A", "B"), band = c(30L, 60L, 30L))
  dem <- tibble::tibble(demand_id = c("t1", "t2", "t3"), population = c(100, 200, 50))
  sup <- tibble::tibble(provider_id = c("A", "B"), supply = c(2, 1))

  # a membership artifact that does not exist -> unresolved, with the reason.
  miss <- run_geographic_access(membership = tempfile(fileext = ".rds"),
                                provider_supply = sup, tract_demand = dem)
  expect_false(miss$resolved)
  expect_match(miss$reason, "membership artifact absent")

  # no per-provider supply -> unresolved (the national aggregate can't be placed).
  nosup <- run_geographic_access(membership = mem, provider_supply = NULL,
                                 tract_demand = dem)
  expect_false(nosup$resolved)
  expect_match(nosup$reason, "per-provider supply")

  # all three real inputs present -> a resolved surface + national roll-up.
  ok <- run_geographic_access(membership = mem, provider_supply = sup, tract_demand = dem)
  expect_true(ok$resolved)
  expect_s3_class(ok$access, "data.frame")
  expect_equal(ok$n_providers, 2L)
  expect_equal(ok$n_tracts, 3L)
  expect_false(is.null(ok$national$mean_access))
})


# ---- Input-publishability guards reach a run ------------------------------
#
# THE DEFECT THIS PINS. assert_publishable_demand_coefficients(),
# assert_publishable_supply_transitions() and unresolved_calibration_items()
# were each defined, tested, documented -- and called by nothing. A guard nobody
# calls does not guard anything, and from the outside it is indistinguishable
# from a guard that passes. They now run inside validation_report(), which the
# orchestrator calls on every projection.

ow_report <- function(gap = NULL) {
  suppressMessages(validation_report(
    tibble::tibble(year = 2025:2027, effective_fte_median = c(1300, 1310, 1320)),
    gap = gap))
}

test_that("the input-publishability guards appear in every validation report", {
  v <- ow_report()
  expect_true(all(c("demand_coefficients_publishable",
                    "supply_transitions_publishable",
                    "calibration_items_resolved") %in% v$check))
})

test_that("the guards report the tier honestly rather than passing by default", {
  # Nothing in the package is calibrated from a fielded URPS survey yet, so all
  # three must be FALSE. A TRUE here means either a survey landed (update this
  # test) or a guard started passing vacuously (the failure mode that matters).
  v <- ow_report()
  expect_false(v$passed[v$check == "demand_coefficients_publishable"])
  expect_false(v$passed[v$check == "supply_transitions_publishable"])
  expect_false(v$passed[v$check == "calibration_items_resolved"])
  # The detail must name what is unresolved, not just say "no".
  expect_match(v$detail[v$check == "calibration_items_resolved"], "capacity_anchor")
})

test_that("the external anchor check distinguishes measured from merely estimated", {
  gap <- baseline_gap(1306, 0.95, method = "capacity_survey",
                      calibration_status = "derived_by_analogy",
                      source = "another specialty", evidence = "test")
  v <- ow_report(gap)
  # base_year_gap_estimated asks whether it was estimated rather than assumed;
  # this asks whether it was MEASURED IN THIS SPECIALTY. An analogy-derived gap
  # can pass the first and must fail the second -- that gap between them is the
  # whole reason the second check exists.
  expect_false(v$passed[v$check == "base_year_gap_externally_anchored"])
  expect_true(any(v$check == "base_year_gap_externally_anchored"))
  # With no gap supplied the row is absent rather than silently FALSE.
  expect_false("base_year_gap_externally_anchored" %in% ow_report()$check)
})

test_that("the new checks are external, so strict mode is not made unusable", {
  # assert_validation_passed() stops in strict mode on any failed INTERNAL
  # check. These four ask whether evidence exists -- a fielded practice survey,
  # an external anchor -- which no code change can produce. Typing them internal
  # would make strict mode impossible for the whole package on conditions the
  # code cannot fix, so they follow base_year_gap_measured and
  # geographic_access_validated in being external and reported.
  v <- ow_report(baseline_gap(1306, 0.95, method = "assumed",
                              calibration_status = "uncalibrated_illustrative",
                              evidence = "test"))
  new <- c("demand_coefficients_publishable", "supply_transitions_publishable",
           "base_year_gap_externally_anchored", "calibration_items_resolved")
  expect_true(all(v$type[v$check %in% new] == "external"))
  # And strict mode still stops only on the pre-existing internal failure.
  failed_internal <- v$check[v$type == "internal" & !is.na(v$passed) & !v$passed]
  expect_false(any(new %in% failed_internal))
})

test_that("a guard that errors is recorded as failed, never as passed", {
  # The wrapper is tryCatch(..., error = FALSE). If a guard throws -- a missing
  # contract, a renamed registry -- the check must read FALSE. Recording an
  # erroring guard as TRUE would be the vacuous-pass failure in its purest form.
  local_mocked_bindings(
    assert_publishable_demand_coefficients = function(...) stop("registry gone"))
  v <- ow_report()
  expect_false(v$passed[v$check == "demand_coefficients_publishable"])
})

# ---- The last seven gates ---------------------------------------------------
#
# The `unwired_gate` register reached zero here. Six of these run inside
# validation_report(); check_legacy_canonical() is a property of the SOURCE TREE
# rather than of a projection, so it runs in scripts/ci/check_suite.R instead --
# wiring it into a model run would have put a repo-structure check on the hot
# path and told a user nothing about their result.

test_that("the reproducibility and definition gates appear in every report", {
  v <- ow_report()
  expect_true(all(c("backtest_record_current", "fte_curve_calibrated",
                    "external_data_present") %in% v$check))
})

test_that("the frozen-record gate passes when reachable and cannot pass vacuously", {
  # A drifted record IS fixable by code -- regenerate the artifact and the
  # record together -- so this is INTERNAL and strict mode should stop on it.
  v <- ow_report()
  expect_equal(v$type[v$check == "backtest_record_current"], "internal")
  # Where the artifact is unreachable, verify_backtest_record() reports
  # checked = FALSE and the assert passes. That is not a vacuous pass: there is
  # nothing to have drifted from. Where it IS reachable it must genuinely match.
  vb <- verify_backtest_record()
  if (isTRUE(vb$checked)) {
    expect_true(v$passed[v$check == "backtest_record_current"])
    expect_false(isFALSE(vb$checksum_matches))
  }
})

test_that("the contract check appears only when the contract is installed", {
  v <- ow_report()
  present <- "mufflyaccess_contract_usable" %in% v$check
  expect_equal(present, requireNamespace("mufflyaccess", quietly = TRUE))
  # A build that is ABSENT is not an internal failure -- adding a FALSE row
  # would make strict mode impossible for every contract-free run. A build that
  # is PRESENT but missing an export this package calls is.
  if (present) expect_equal(v$type[v$check == "mufflyaccess_contract_usable"], "internal")
})

test_that("external data presence is decidable rather than a manual placeholder", {
  v <- ow_report()
  row <- v[v$check == "external_data_present", ]
  expect_equal(row$type, "data")
  expect_false(is.na(row$passed))
  # The detail names WHICH inputs are absent; a bare FALSE would send a reader
  # looking through check_external_data() by hand.
  if (!row$passed) expect_match(row$detail, "absent")
})

test_that("the new external gates do not change what strict mode stops on", {
  # The whole batch is FALSE today and waits on evidence no code change can
  # produce -- a fielded survey, an ascertained attrition series, downloaded
  # archives. Only backtest_record_current and mufflyaccess_contract_usable are
  # internal, and both are conditions the repository CAN satisfy.
  v <- ow_report(baseline_gap(1306, 0.95, method = "assumed",
                              calibration_status = "uncalibrated_illustrative",
                              evidence = "test"))
  failed_internal <- v$check[v$type == "internal" & !is.na(v$passed) & !v$passed]
  expect_setequal(failed_internal, "base_year_gap_estimated")
})

test_that("check_legacy_canonical reports declared-vs-actual ownership", {
  # Wired in scripts/ci/check_suite.R. Asserted here on its contract rather than
  # its wiring, because a test cannot observe the CI script running: an empty
  # frame means every legacy function resolves to the file LEGACY_CANONICAL
  # says owns it.
  r <- check_legacy_canonical()
  expect_s3_class(r, "data.frame")
  expect_equal(nrow(r), 0L)
})
