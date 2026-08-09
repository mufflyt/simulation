# Regression guards for the demand estimands, scenarios, geography and
# calibration/validation machinery.

# ---- Demand estimands ------------------------------------------------------

test_that("the three demand estimands are no longer proportional rescalings", {
  # The previous implementation computed every estimand as
  # population_65plus * constant, making D1/D2/D3 the SAME series up to scale.
  pop <- tidyr::expand_grid(year = 2025:2035, age_band = urpssim:::DEMAND_AGE_BANDS) %>%
    dplyr::mutate(female_pop = dplyr::case_when(
      age_band == "20-39" ~ 43e6 * 1.001^(year - 2025),
      age_band == "40-59" ~ 41.5e6 * 1.004^(year - 2025),
      age_band == "60-64" ~ 11e6 * 1.008^(year - 2025),
      age_band == "65-79" ~ 22e6 * 1.016^(year - 2025),
      TRUE                ~ 7.5e6 * 1.031^(year - 2025)
    ))
  d <- compute_demand_denominators(pop)
  expect_setequal(unique(d$estimand), c("D1", "D2", "D3"))
  expect_equal(nrow(detect_proportional_estimands(d)), 0L)
  expect_true(assert_estimands_independent(d, mode = "strict") |> nrow() == 0L)
})

test_that("the crude single-rate path is detected as tautological", {
  pop65 <- tibble::tibble(year = 2025:2035,
                          population_65_plus = 30e6 * 1.018^(0:10))
  crude <- suppressMessages(compute_demand_denominators_crude(pop65))
  prop <- detect_proportional_estimands(crude)
  expect_equal(nrow(prop), 3L)   # all three pairs proportional
  expect_error(assert_estimands_independent(crude, mode = "strict"),
               "proportional rescalings")
})

test_that("the age profiles genuinely differ in shape", {
  # Surgery peaks at 60-79 and halves at 80+, while prevalence keeps rising.
  # That difference is what makes the estimands informative.
  p <- pfd_prevalence_by_band()
  expect_gt(urpssim:::WU2011_SURGERY_RATE_PER_1000[["65-79"]],
            urpssim:::WU2011_SURGERY_RATE_PER_1000[["80+"]])
  expect_gt(p[["80+"]], p[["65-79"]])
  expect_gt(urpssim:::CONSULT_RATE_BY_AGE[["65-79"]], urpssim:::CONSULT_RATE_BY_AGE[["80+"]])
})

test_that("age bands align exactly with the SSOT prevalence boundary", {
  # mufflyaccess::pfd_prevalence() owns 65-79 and 80+. The old 60-79 band could
  # not take the contract's 65-79 value without a silent wrong-grain error, so
  # 60-64 is split out.
  expect_true(all(c("60-64", "65-79", "80+") %in% urpssim:::DEMAND_AGE_BANDS))
  expect_false("60-79" %in% urpssim:::DEMAND_AGE_BANDS)

  own <- pfd_prevalence_ownership()
  expect_equal(own$owner[own$age_band == "65-79"], "mufflyaccess::pfd_prevalence()")
  expect_equal(own$owner[own$age_band == "80+"], "mufflyaccess::pfd_prevalence()")
  # The contract does NOT cover women under 65; those must stay labelled local.
  expect_true(all(grepl("^local", own$owner[own$age_band %in% c("20-39", "40-59", "60-64")])))
})

test_that("PFD prevalence for 65+ comes from the contract, not a literal", {
  skip_if_not_installed("mufflyaccess")
  p <- pfd_prevalence_by_band()
  ssot <- mufflyaccess::pfd_prevalence()
  expect_equal(unname(p[["65-79"]]), unname(ssot[["65_79"]]))
  expect_equal(unname(p[["80+"]]), unname(ssot[["80plus"]]))
})

test_that("growth adequacy is labelled as relative, and equals 1 at the base year", {
  supply <- tibble::tibble(year = 2025:2030, effective_fte_median = c(1300, 1310, 1320, 1330, 1340, 1350))
  demand <- tibble::tibble(year = rep(2025:2030, 2),
                           estimand = rep(c("D1", "D2"), each = 6),
                           label = rep(c("a", "b"), each = 6),
                           demand_cases = c(100:105, 200, 202, 204, 206, 208, 210))
  g <- compute_growth_adequacy(supply, demand, base_year = 2025)
  expect_true("growth_adequacy" %in% names(g))
  expect_false("coverage_pct" %in% names(g))
  # Base-year adequacy is 1.0 BY CONSTRUCTION -- the whole reason a separate
  # absolute base-year gap estimate is required.
  expect_equal(unique(g$growth_adequacy[g$year == 2025]), 1)
})

test_that("Spearman rho across years is flagged as uninformative for monotone series", {
  supply <- tibble::tibble(year = 2025:2030, effective_fte_median = seq(1300, 1350, 10))
  demand <- tibble::tibble(year = rep(2025:2030, 2),
                           estimand = rep(c("D1", "D2"), each = 6),
                           label = rep(c("a", "b"), each = 6),
                           demand_cases = c(100:105, 200, 203, 206, 209, 212, 215))
  g <- compute_growth_adequacy(supply, demand, base_year = 2025)
  con <- assess_demand_concordance(g, demand)
  expect_true(con$rho_uninformative)
  expect_true(is.numeric(con$final_year_spread))
  expect_true(is.logical(con$conclusion_agrees))
})

# ---- Scenario registry -----------------------------------------------------

test_that("supply scenarios come from the mufflyaccess SSOT registry", {
  skip_if_not_installed("mufflyaccess")
  reg <- supply_scenario_registry(55)
  expect_silent(validate_scenario_registry(reg, "supply"))
  # WHETHER THE LOCAL POLICY LEVERS BELONG IN THIS REGISTRY IS AN OPEN DESIGN
  # QUESTION, not something a test should settle. It has been decided three
  # times in opposite directions inside six hours -- 61128a6 made the SSOT
  # exclusive, e5fb995 restored the append, 00866a8 made it exclusive again --
  # and each flip broke whichever assertion had been written to the previous
  # one. Pinning the id set again would just queue up the next false failure.
  #
  # So this asserts only what is true under BOTH designs, which is also the
  # part that actually protects anything.
  ssot <- mufflyaccess::urps_scenario_ids()
  ext <- urpssim:::SUPPLY_SCENARIO_LOCAL_EXTENSIONS

  # 1. The contract's ids are all present. Losing one is a real regression
  #    under any design.
  expect_true(all(ssot %in% names(reg)))

  # 2. Nothing else is present except, possibly, the declared extensions. This
  #    is what stops an arbitrary id being invented; it permits either design
  #    without permitting a third.
  expect_setequal(setdiff(names(reg), ssot), intersect(names(reg), ext))

  # 3. The extensions are all-in or all-out, never a partial set -- a registry
  #    carrying one of three levers is a bug in either design.
  expect_true(sum(ext %in% names(reg)) %in% c(0L, length(ext)))

  # 4. An SSOT id is never served from the local fallback. THIS is the failure
  #    the module warns about and the only one that is silent: an id defined in
  #    both places takes the local definition and still validates downstream,
  #    because the NAME is on the contract's list while the CONTENT is not.
  from_ssot <- urpssim:::ssot_supply_scenarios(55)
  local_only <- suppressMessages(supply_scenario_registry(55, prefer_ssot = FALSE))
  for (id in intersect(names(local_only), ssot)) {
    expect_identical(reg[[id]], from_ssot[[id]])
  }
  expect_true("baseline" %in% names(reg))
  expect_equal(reg$retire_2yr_later$retirement_shift_years, 2)
  expect_equal(reg$retire_2yr_earlier$retirement_shift_years, -2)
  expect_equal(reg$retire_5yr_earlier$retirement_shift_years, -5)
  # Entrants come through as a multiplier on the baseline count.
  expect_equal(reg$fellowship_plus_10pct$entrants, 55 * 1.10)
  expect_equal(reg$fellowship_constrained$entrants, 55 * 0.90)
  # And the late-career FTE scenario carries an onset age, not a flat multiplier.
  expect_equal(reg$lower_late_career_fte$late_career_fte_factor, 0.75)
  expect_equal(reg$lower_late_career_fte$late_career_fte_onset_age, 60)
})

test_that("the local policy levers stay reachable, and announce that they are not the SSOT", {
  # Dropping the extensions from the SSOT assertion above would otherwise leave
  # them untested entirely -- three scenarios defined, exported and reachable by
  # nobody's test, which is how a capability ends up implemented and never
  # connected. They live in the local fallback until the contract carries them.
  expect_message(supply_scenario_registry(55, prefer_ssot = FALSE),
                 "will not validate against")
  reg <- suppressMessages(supply_scenario_registry(55, prefer_ssot = FALSE))
  expect_true(all(urpssim:::SUPPLY_SCENARIO_LOCAL_EXTENSIONS %in% names(reg)))
  # The warning is the point: a caller who reaches these ids must know the
  # registry they got is no longer the one downstream validation checks against.
})

test_that("an unregistered scenario id is refused", {
  skip_if_not_installed("mufflyaccess")
  expect_silent(assert_scenarios_registered(c("baseline", "retire_2yr_later"),
                                            mode = "strict"))
  expect_error(assert_scenarios_registered("my_made_up_scenario", mode = "strict"),
               "not in the mufflyaccess registry")
})

test_that("the local fallback still rejects scalar hazard multipliers", {
  reg <- local_supply_scenario_registry(55)
  expect_silent(validate_scenario_registry(reg, "supply"))
  expect_equal(reg$retire_2yr_later$retirement_shift_years, 2)
  bad <- reg
  bad$status_quo$hazard_mult <- 0.73
  expect_error(validate_scenario_registry(bad, "supply"), "scalar hazard multiplier")
})

test_that("the local fallback carries the SSOT retirement-shift contract ids", {
  # Regression guard for the scenario-id drift: urps_p_active()/p_active_by_age()
  # fall back to this registry when mufflyaccess is absent (e.g. on CI), so it
  # must answer the SSOT ids `retire_2yr_later`/`retire_2yr_earlier` — not older
  # local names — or the age-axis shift silently resolves to 0.
  reg <- local_supply_scenario_registry(55)
  expect_equal(reg$retire_2yr_later$retirement_shift_years, 2)
  expect_equal(reg$retire_2yr_earlier$retirement_shift_years, -2)
  expect_null(reg$retirement_2_years_later)   # old drifted id is gone
})

test_that("the local registry version cannot collide with the SSOT version", {
  skip_if_not_installed("mufflyaccess")
  # Two different registries sharing "1.0.0" is how silent divergence starts.
  expect_false(identical(urpssim:::SCENARIO_REGISTRY_VERSION,
                         mufflyaccess::URPS_SCENARIO_REGISTRY_VERSION))
})

test_that("scenario registries are contract-checked", {
  reg <- local_supply_scenario_registry()
  no_sq <- reg[setdiff(names(reg), "status_quo")]
  expect_error(validate_scenario_registry(no_sq, "supply"), "reference scenario")

  incomplete <- reg
  incomplete$status_quo$entrants <- NULL
  expect_error(validate_scenario_registry(incomplete, "supply"), "missing field")

  dem <- demand_scenario_registry()
  expect_silent(validate_scenario_registry(dem, "demand"))
  bad <- dem
  bad$status_quo$access_components <- "not_a_component"
  expect_error(validate_scenario_registry(bad, "demand"), "unknown access component")
})

test_that("the reduced-barriers scenario names the components it relaxes", {
  dem <- demand_scenario_registry()
  # Assert against urpssim:::ACCESS_COMPONENTS rather than a hand-copied list: "income"
  # was added to both the constant and the scenario, and this literal was the
  # only place left saying three. A copy of a canonical list goes stale the
  # moment the list grows.
  expect_setequal(dem$reduced_barriers$access_components, urpssim:::ACCESS_COMPONENTS)
  expect_true("income" %in% dem$reduced_barriers$access_components)
  # Urogynaecology-specific lever: only 25-45% of women with UI seek care.
  expect_gt(dem$care_seeking_improved$care_seeking_multiplier, 1)
})

# ---- Geography -------------------------------------------------------------

test_that("opportunity placement follows the HWSM five-step algorithm", {
  growth <- tibble::tibble(geo = c("A", "B", "C"), demand_growth_fte = c(100, 50, 0))
  retire <- tibble::tibble(geo = c("A", "B", "C"), retirements_fte = c(20, 50, 30))
  s <- opportunity_placement_shares(growth, retire)
  # Step 3: requirements = growth + retirements.
  expect_equal(s$requirements_fte, c(120, 100, 30))
  # Step 4: shares sum to 1.
  expect_equal(sum(s$share), 1, tolerance = 1e-12)
  expect_equal(s$share[s$geo == "A"], 120 / 250)
})

test_that("historical placement reproduces existing maldistribution", {
  roster <- tibble::tibble(state = c(rep("A", 80), rep("B", 20)))
  h <- historical_placement_shares(roster, "state")
  expect_equal(h$share[h$geo == "A"], 0.8)
})


test_that("entrant placement is deterministic when asked to be", {
  shares <- tibble::tibble(geo = c("A", "B"), share = c(0.75, 0.25))
  out <- assign_entrant_geography(100, shares, stochastic = FALSE)
  expect_equal(sum(out == "A"), 75L)
  expect_equal(length(out), 100L)
  expect_length(assign_entrant_geography(0, shares), 0L)
})

test_that("benchmark density separates national from geographic shortfall", {
  # Physiatry: 30 per million implies +984 nationally but +1,747 state by state,
  # because surpluses cannot offset deficits elsewhere.
  pc <- tibble::tibble(geo = c("A", "B"), fte = c(200, 10),
                       population = c(4e6, 2e6))
  res <- benchmark_density_shortfall(pc, benchmark = 30)
  expect_lt(res$national_additional, res$geographic_additional)
  expect_equal(res$n_geo_below_benchmark, 1L)
})

# ---- Calibration and validation -------------------------------------------

test_that("calibration scalars follow observed / predicted", {
  pred <- tibble::tibble(category = c("obgyn", "urology"), predicted = c(80804, 35925))
  obs <- tibble::tibble(category = c("obgyn", "urology"), observed = c(73198, 26153))
  s <- fit_calibration_scalars(pred, obs)
  # HDMM Exhibit 11 published values.
  expect_equal(s$scalar[s$category == "obgyn"], 0.906, tolerance = 1e-3)
  expect_equal(s$scalar[s$category == "urology"], 0.728, tolerance = 1e-3)
  expect_false(any(s$flagged))
})

test_that("an implausible calibration scalar is flagged, not silently applied", {
  pred <- tibble::tibble(category = "x", predicted = 100)
  obs <- tibble::tibble(category = "x", observed = 1000)
  expect_message(s <- fit_calibration_scalars(pred, obs), "structural mismatch")
  expect_true(s$flagged)
})

test_that("applying scalars rescales the modelled values", {
  vals <- tibble::tibble(category = "obgyn", predicted = 1000)
  s <- tibble::tibble(category = "obgyn", scalar = 0.906)
  out <- apply_calibration_scalars(vals, s)
  expect_equal(out$predicted, 906)
})

test_that("uncalibrated demand output is refused in strict mode", {
  expect_error(assert_demand_calibrated(NULL, mode = "strict"), "national anchor")
  ok <- tibble::tibble(category = "x", scalar = 1.0)
  expect_true(assert_demand_calibrated(ok, mode = "strict"))
})

test_that("two-method agreement rewards genuine independence", {
  a <- tibble::tibble(geo = c("NY", "MT", "CA", "ND"), adequacy = c(1.3, 0.6, 1.1, 0.5))
  b <- tibble::tibble(geo = c("NY", "MT", "CA", "ND"), adequacy = c(1.2, 0.7, 1.05, 0.55))
  res <- two_method_agreement(a, b)
  expect_equal(res$verdict, "concordant")
  expect_gt(res$spearman_rho, 0.9)
  # Declaring the methods non-independent changes the verdict.
  expect_message(res2 <- two_method_agreement(a, b, independent = FALSE), "NOT independent")
  expect_equal(res2$verdict, "not_independent")
})

test_that("the validation report fails closed on internal checks", {
  supply <- tibble::tibble(year = 2025:2026, scenario = "status_quo",
                           effective_fte_median = c(1300, 1310))
  required <- tibble::tibble(year = 2025:2026, required_fte = c(1400, 1450))
  gap <- baseline_gap(1306, 0.948, method = "capacity_survey",
                      calibration_status = "calibrated",
                      source = "fielded URPS practice-capacity survey")

  rep_ok <- validation_report(supply, required, gap)
  expect_true(all(rep_ok$passed[rep_ok$type == "internal" & !is.na(rep_ok$passed)]))
  expect_silent(assert_validation_passed(rep_ok, mode = "strict"))

  # A run with no base-year gap fails the internal check.
  rep_bad <- validation_report(supply, required, NULL)
  expect_false(rep_bad$passed[rep_bad$check == "base_year_gap_estimated"])
  expect_error(assert_validation_passed(rep_bad, mode = "strict"), "Internal validation failed")

  # Negative supply is caught.
  neg <- supply; neg$effective_fte_median[1] <- -5
  rep_neg <- validation_report(neg, required, gap)
  expect_false(rep_neg$passed[rep_neg$check == "no_negative_supply"])
})

test_that("the report records the validation types that cannot be automated", {
  rep <- validation_report(tibble::tibble(year = 2025, effective_fte_median = 1))
  expect_setequal(unique(rep$type), c("internal", "conceptual", "external", "data"))
  # The placeholders for manual review stay NA. The external checks that ARE
  # decidable from the run carry a real TRUE/FALSE instead. The distinction is
  # the point of the type field: "external" marks a question the code cannot
  # answer by arithmetic, but some of those are still answerable by inspecting
  # the inputs the run was given, and those must not masquerade as unreviewed.
  #
  # The publishability checks are external rather than internal on purpose:
  # assert_validation_passed() stops in strict mode on a failed INTERNAL check,
  # and these ask whether a fielded survey or an external anchor exists yet --
  # which no code change can produce. See test-orchestrator-wiring.R.
  decidable <- c("base_year_gap_measured", "geographic_access_validated",
                 "demand_coefficients_publishable", "supply_transitions_publishable",
                 "base_year_gap_externally_anchored", "calibration_items_resolved",
                 "backtest_attrition_ascertained", "fte_curve_calibrated",
                 "external_data_present")
  manual <- rep$type %in% c("conceptual", "external", "data") &
    !rep$check %in% decidable
  expect_true(all(is.na(rep$passed[manual])))
})

test_that("the report carries a geographic-access gate that fails closed until isochrones land", {
  # A report whose INTERNAL checks all pass, so the only thing under test is that
  # the geographic gate is present, decidable, and non-fatal.
  supply <- tibble::tibble(year = 2025, scenario = "status_quo", effective_fte_median = 1300)
  gap <- baseline_gap(1306, 0.948, method = "capacity_survey",
                      calibration_status = "calibrated",
                      source = "fielded URPS practice-capacity survey")
  rep <- validation_report(supply, gap = gap)

  row <- rep[rep$check == "geographic_access_validated", ]
  expect_equal(nrow(row), 1L)
  expect_equal(row$type, "external")
  # It reads geographic_access_status(): unresolved while drive-time isochrones
  # are absent, so the check is FALSE (never NA) and the run records the gap
  # rather than presenting a headline as if geography had been validated.
  expect_false(is.na(row$passed))
  # The dynamic assertion above IS the contract: the report must mirror the
  # status object. The old hard-coded expect_false() pinned the state of the
  # world in 2026, when no validated surface existed. Resolution is now earned
  # by validate_access_surface() and depends on whether E2SFCA_SURFACE_DIR
  # points at one, so asserting a constant here would test the environment.
  expect_type(row$passed, "logical")
  # An unresolved external check does NOT fail the build (only internal checks do).
  expect_silent(assert_validation_passed(rep, mode = "strict"))
})

test_that("the validation report distinguishes an estimated gap from a measured one", {
  supply <- tibble::tibble(year = 2025, scenario = "status_quo", effective_fte_median = 1300)

  borrowed <- baseline_gap(1306, 0.948, method = "capacity_survey",
                           calibration_status = "derived_by_analogy",
                           source = "Zarek 2025 PTJ (physical therapists)")
  rep <- validation_report(supply, gap = borrowed)

  # It is a real estimate -- the internal check still passes and strict mode runs.
  expect_true(rep$passed[rep$check == "base_year_gap_estimated"])
  expect_silent(assert_validation_passed(rep, mode = "strict"))
  # ...but the artifact can no longer read as a survey fielded in urogynaecology.
  expect_false(rep$passed[rep$check == "base_year_gap_measured"])
  expect_match(rep$detail[rep$check == "base_year_gap_estimated"], "derived_by_analogy")
  expect_match(rep$detail[rep$check == "base_year_gap_measured"], "NOT measured")

  fielded <- baseline_gap(1306, 0.948, method = "capacity_survey",
                          calibration_status = "calibrated",
                          source = "fielded URPS practice-capacity survey")
  rep2 <- validation_report(supply, gap = fielded)
  expect_true(rep2$passed[rep2$check == "base_year_gap_measured"])
  expect_match(rep2$detail[rep2$check == "base_year_gap_measured"], "fielded URPS")

  # An assumption with no ledger fails the internal check outright.
  hollow <- validation_report(supply, gap = baseline_gap(1306, 0.90, method = "assumed"))
  expect_false(hollow$passed[hollow$check == "base_year_gap_estimated"])
  expect_error(assert_validation_passed(hollow, mode = "strict"), "no evidence ledger")
})

# ---- mufflyaccess SSOT hookups ---------------------------------------------

test_that("the supply projection conforms to the URPS projection contract", {
  skip_if_not_installed("mufflyaccess")
  sup <- tibble::tibble(
    year = 2025:2027, scenario = "baseline",
    headcount_median = c(1306, 1300, 1295),
    headcount_lo = c(1290, 1282, 1275), headcount_hi = c(1322, 1318, 1315),
    effective_fte_median = c(1300, 1295, 1290)
  )
  p <- as_urps_projection(sup)
  schema <- mufflyaccess::urps_projection_schema()
  expect_true(all(schema$column[!schema$optional] %in% names(p)))
  expect_silent(mufflyaccess::validate_urps_projection(p))
})

test_that("the 95% bounds bracket headcount, not FTE", {
  # The contract requires lower_95/upper_95 to bracket supply_headcount. Passing
  # FTE bounds fails validation -- a real bug this hookup caught.
  skip_if_not_installed("mufflyaccess")
  sup <- tibble::tibble(
    year = 2025, scenario = "baseline",
    headcount_median = 1306, headcount_lo = 1290, headcount_hi = 1322,
    effective_fte_median = 1200, effective_fte_lo = 1180, effective_fte_hi = 1220
  )
  p <- as_urps_projection(sup)
  expect_lte(p$lower_95, p$supply_headcount)
  expect_gte(p$upper_95, p$supply_headcount)
})

test_that("drive-time bands come from the canonical contract", {
  expect_equal(e2sfca_bands(), c(30L, 60L, 120L, 180L))
  if (requireNamespace("mufflyaccess", quietly = TRUE)) {
    expect_equal(e2sfca_bands(), as.integer(mufflyaccess::get_canonical_bands()))
  }
  # The shipped decay weights must key to the canonical bands.
  expect_setequal(as.integer(names(urpssim:::E2SFCA_DEFAULT_WEIGHTS)), e2sfca_bands())
})

test_that("rurality has an operational definition", {
  skip_if_not_installed("mufflyaccess")
  expect_equal(ssot_rurality(1), "Metropolitan")
  expect_equal(ssot_rurality(10), "Rural")
  # The nonmetro access component is no longer a bare label.
  expect_true("nonmetro" %in% urpssim:::ACCESS_COMPONENTS)
})

test_that("division guards delegate to the contract convention", {
  expect_true(is.na(ssot_safe_divide(1, 0)))
  expect_equal(ssot_safe_divide(10, 2), 5)
  expect_equal(ssot_safe_percent(1, 4), 25)
  expect_equal(ssot_safe_percent(1, 0), 0)
})

# ---- gap projection contract -----------------------------------------------

test_that("as_urps_gap_projection produces all REQUIRED_COLS and correct arithmetic", {
  supply <- tibble::tibble(
    year = 2025:2027, scenario = "baseline",
    headcount_median = c(1300, 1295, 1290),
    headcount_lo = c(1280, 1275, 1270), headcount_hi = c(1320, 1315, 1310),
    effective_fte_median = c(1200, 1195, 1190)
  )
  req <- tibble::tibble(year = 2025:2027, required_fte = c(1300, 1310, 1320))
  gap_tbl <- compute_fte_gap(supply, req)
  gp <- as_urps_gap_projection(supply, gap_tbl)

  expect_true(all(urpssim:::REQUIRED_COLS %in% names(gp)))
  expect_equal(gp$gap_fte, gp$supply_clinical_fte - gp$demand_clinical_fte)
  expect_equal(gp$gap_headcount, gp$supply_headcount - gp$demand_headcount)
  # shortage in all years (supply < demand)
  expect_true(all(gp$gap_fte < 0))
})

test_that("validate_urps_gap_projection errors in strict mode on missing columns", {
  bad <- data.frame(year = 2025L, gap_fte = -100)
  withr::with_options(list(urpssim.mode = "strict"), {
    expect_error(validate_urps_gap_projection(bad, mode = "strict"), "missing required column")
  })
})

test_that("validate_urps_gap_projection errors in strict mode on arithmetic inconsistency", {
  gp <- data.frame(
    year = 2025L, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = 1300, supply_clinical_fte = 1200,
    supply_cohort_basis = "roster",
    demand_headcount = 1350, demand_clinical_fte = 1300,
    gap_fte = -50,       # wrong: should be -100
    gap_headcount = -50
  )
  expect_error(validate_urps_gap_projection(gp, mode = "strict"), "gap_fte does not equal")
})

test_that("an exported gap projection carries what its supply cohort is", {
  # Every column in this table is a supply number or derived from one, so the
  # cohort's provenance conditions the whole row. It used to live only in
  # scenario_meta, which meant the caveat stopped travelling the moment a
  # projection was saved or handed on.
  supply <- tibble::tibble(
    year = 2025:2026, scenario = "baseline",
    headcount_median = c(1300, 1295), effective_fte_median = c(1200, 1195)
  )
  req <- tibble::tibble(year = 2025:2026, required_fte = c(1300, 1310))
  gap_tbl <- compute_fte_gap(supply, req)

  gp <- as_urps_gap_projection(supply, gap_tbl, cohort_basis = "certification_cohorts",
                               observed_share = 0.498)
  expect_true("supply_cohort_basis" %in% urpssim:::REQUIRED_COLS)
  # Recycled down every row, so filtering to one year keeps the basis attached.
  expect_equal(unique(gp$supply_cohort_basis), "certification_cohorts")
  expect_equal(nrow(gp), 2L)
  expect_equal(unique(gp$supply_observed_share), 0.498)
  expect_output(print(structure(gp, class = c("urps_gap_projection", class(gp)))),
                "RECONSTRUCTED COHORT")

  # A measured roster says so, and says it without the warning.
  ros <- as_urps_gap_projection(supply, gap_tbl, cohort_basis = "roster")
  expect_equal(unique(ros$supply_cohort_basis), "roster")
  expect_silent(validate_urps_gap_projection(ros, mode = "strict"))
})

test_that("supply provenance cannot be left undeclared or unreadable", {
  supply <- tibble::tibble(year = 2025L, scenario = "baseline",
                           headcount_median = 1300, effective_fte_median = 1200)
  req <- tibble::tibble(year = 2025L, required_fte = 1300)
  gap_tbl <- compute_fte_gap(supply, req)

  # Silence is the state this column exists to end: without it the numbers
  # export indistinguishable from ones built on a real roster.
  expect_error(
    as_urps_gap_projection(supply, gap_tbl, mode = "strict"),
    "undeclared"
  )
  # Relaxed mode warns rather than stopping, but still says it.
  expect_message(as_urps_gap_projection(supply, gap_tbl, mode = "relaxed"), "undeclared")

  # A basis nobody can interpret is refused for the same reason.
  expect_error(
    as_urps_gap_projection(supply, gap_tbl, cohort_basis = "vibes", mode = "strict"),
    "unrecognised supply_cohort_basis"
  )
})

test_that("validation_report includes gap_projection checks when supplied", {
  supply <- tibble::tibble(
    year = 2025L, scenario = "baseline",
    headcount_median = 1300, effective_fte_median = 1200
  )
  req <- tibble::tibble(year = 2025L, required_fte = 1300)
  gap_tbl <- compute_fte_gap(supply, req)
  gp <- as_urps_gap_projection(supply, gap_tbl)

  rep <- validation_report(supply, req, gap_projection = gp)
  expect_true("gap_projection_cols"       %in% rep$check)
  expect_true("gap_projection_arithmetic" %in% rep$check)
  expect_true(rep$passed[rep$check == "gap_projection_cols"])
  expect_true(rep$passed[rep$check == "gap_projection_arithmetic"])
})

test_that("publishable_run_report passes only manuscript-ready run objects", {
  ok_status <- backtest_status_from_summary(
    tibble::tibble(within_95 = rep(TRUE, 5), percent_error = c(-1, 0, 1, 2, -2)),
    required = 0.8)
  ok_gap <- baseline_gap(
    100, 0.9, method = "external_anchor", calibration_status = "calibrated",
    source = "fielded URPS capacity anchor")
  ok <- list(
    run_id = "publishable_test",
    projection = tibble::tibble(year = 2025L, supply_headcount = 100),
    baseline_gap = ok_gap,
    validation = tibble::tibble(check = "no_negative_supply", type = "internal",
                                passed = TRUE, detail = "ok"),
    scenario_meta = list(
      example_only = FALSE,
      cohort_provenance = list(source = "roster", is_production = TRUE),
      demand_calibrated = TRUE,
      backtest = ok_status
    )
  )
  artifact <- tempfile(fileext = ".rds")
  write_artifact_with_provenance(ok, artifact, inputs = list(seed = 1),
                                 run_id = "publishable_test")
  rep <- publishable_run_report(ok, artifact_path = artifact)
  expect_true(all(rep$passed))
  expect_silent(assert_publishable_run(ok, artifact_path = artifact, mode = "strict"))

  weak <- ok
  weak$projection <- tibble::tibble(year = 2025L, lower_95 = 1, upper_95 = 2)
  weak$baseline_gap <- baseline_gap(
    100, 0.95, method = "capacity_survey",
    calibration_status = "derived_by_analogy",
    source = "stand-in donor specialty")
  weak$scenario_meta$demand_calibrated <- FALSE
  weak$scenario_meta$backtest <- backtest_status()
  bad <- publishable_run_report(weak, artifact_path = artifact)
  expect_false(bad$passed[bad$check == "demand_calibrated"])
  expect_false(bad$passed[bad$check == "base_year_capacity_anchor"])
  expect_false(bad$passed[bad$check == "forecast_intervals"])
  expect_error(assert_publishable_run(weak, artifact_path = artifact, mode = "strict"),
               "Run is not publishable")
})

test_that("gap_projections_all_scenarios covers every scenario", {
  supply <- tibble::tibble(
    year = rep(2025:2026, 2),
    scenario = c("baseline","baseline","expanded","expanded"),
    headcount_median = c(1300, 1295, 1380, 1375),
    effective_fte_median = c(1200, 1195, 1270, 1265)
  )
  req <- tibble::tibble(year = 2025:2026, required_fte = c(1300, 1310))
  gp <- gap_projections_all_scenarios(supply, req)
  expect_setequal(unique(gp$scenario_id), c("baseline", "expanded"))
  expect_true(all(urpssim:::REQUIRED_COLS %in% names(gp)))
})

test_that("SSOT ownership is reported, including what is NOT owned", {
  r <- ssot_coverage_report()
  expect_true(all(r$owner %in% c("mufflyaccess", "local")))
  # These are owned and must not be redefined locally.
  expect_equal(r$owner[r$quantity == "supply scenarios"], "mufflyaccess")
  expect_equal(r$owner[r$quantity == "PFD prevalence 65+"], "mufflyaccess")
  expect_equal(r$owner[r$quantity == "drive-time bands"], "mufflyaccess")
  # These are NOT owned by the contract despite similar-sounding exports.
  expect_equal(r$owner[r$quantity == "PFD prevalence <65"], "local")
  expect_equal(r$owner[r$quantity == "Monte Carlo CI bands"], "local")
  expect_equal(r$owner[r$quantity == "work RVUs"], "local")
})

test_that("the late-career FTE factor applies only from its onset age", {
  agents <- tibble::tibble(
    provider_id = sprintf("p%d", 1:200), subspecialty = "URPS",
    sex = rep(c("female", "male"), 100),
    age = c(rep(45, 100), rep(65, 100)),
    entry_year = 2010, retirement_year = NA_real_, origin_cohort = "baseline"
  )
  set.seed(4)
  full <- simulate_provider_career_once(agents, 2025, 0)$panel$effective_fte
  set.seed(4)
  cut <- simulate_provider_career_once(agents, 2025, 0,
                                       late_career_fte_factor = 0.75,
                                       late_career_fte_onset_age = 60)$panel$effective_fte
  expect_lt(cut, full)
  # Only the 65-year-olds are affected, so the reduction is far less than 25%.
  expect_gt(cut / full, 0.80)
})


# ---- NAMCS URPS visit-rate equations (D5 estimand) -------------------------

test_that("flag_urps_visits identifies correct ICD-10 codes", {
  fake <- tibble::tibble(
    DIAG1 = c("N393", "I10-", "N81-", "E119"),
    DIAG2 = c("-9",   "R32-", "-9",   "-9"),
    DIAG3 = rep("-9", 4)
  )
  result <- flag_urps_visits(fake)
  expect_equal(unname(result$is_urps), c(TRUE, TRUE, TRUE, FALSE))
})

# Helper: locate a data-raw file regardless of test working directory
.data_raw_path <- function(...) {
  rel  <- file.path("data-raw", ...)
  rel2 <- file.path("..", "..", "data-raw", ...)
  if (file.exists(rel)) rel else rel2
}
.namcs_rds_path  <- function() .data_raw_path("namcs",  "namcs2019_clean.rds")
.brfss_rds_path  <- function() .data_raw_path("brfss",  "brfss_2023_women18plus.rds")

test_that("namcs_urps_stratum_visits returns non-negative weighted totals", {
  skip_if_not(file.exists(.namcs_rds_path()), "NAMCS 2019 cleaned file not present")
  namcs <- flag_urps_visits(load_namcs_2019(.namcs_rds_path()))
  sv    <- namcs_urps_stratum_visits(namcs)
  expect_gt(nrow(sv), 0L)
  expect_true(all(sv$visits_weighted > 0))
  expect_true(all(sv$n_visits_unweighted > 0))
})

test_that("compute_urps_visit_rates produces positive rates in plausible range", {
  skip_if_not(file.exists(.namcs_rds_path()), "NAMCS 2019 cleaned file not present")
  # Guard BOTH inputs. Guarding only NAMCS made these tests ERROR rather than
  # skip on any checkout that has NAMCS but not BRFSS -- readRDS() on a missing
  # path, reported as "cannot open the connection", which reads like a broken
  # test rather than an absent optional data file.
  skip_if_not(file.exists(.brfss_rds_path()), "BRFSS 2023 cleaned file not present")
  namcs <- flag_urps_visits(load_namcs_2019(.namcs_rds_path()))
  sv    <- namcs_urps_stratum_visits(namcs)
  brfss <- readRDS(.brfss_rds_path())
  sp    <- brfss_population_by_stratum(brfss)
  rt    <- compute_urps_visit_rates(sv, sp)
  expect_gt(nrow(rt), 0L)
  expect_true(all(rt$visits_per_1000 > 0))
  expect_true(all(rt$visits_per_1000 < 100))
})

test_that("fit_urps_visit_rate_model produces a fitted lm with R2 > 0.5", {
  skip_if_not(file.exists(.namcs_rds_path()), "NAMCS 2019 cleaned file not present")
  # Guard BOTH inputs. Guarding only NAMCS made these tests ERROR rather than
  # skip on any checkout that has NAMCS but not BRFSS -- readRDS() on a missing
  # path, reported as "cannot open the connection", which reads like a broken
  # test rather than an absent optional data file.
  skip_if_not(file.exists(.brfss_rds_path()), "BRFSS 2023 cleaned file not present")
  namcs <- flag_urps_visits(load_namcs_2019(.namcs_rds_path()))
  sv    <- namcs_urps_stratum_visits(namcs)
  brfss <- readRDS(.brfss_rds_path())
  sp    <- brfss_population_by_stratum(brfss)
  rt    <- compute_urps_visit_rates(sv, sp)
  model <- fit_urps_visit_rate_model(rt)
  expect_s3_class(model, "lm")
  expect_gt(summary(model)$r.squared, 0.5)
})

test_that("compute_namcs_demand_estimand returns monotonically growing D5 FTE", {
  skip_if_not(file.exists(.namcs_rds_path()), "NAMCS 2019 cleaned file not present")
  # Guard BOTH inputs. Guarding only NAMCS made these tests ERROR rather than
  # skip on any checkout that has NAMCS but not BRFSS -- readRDS() on a missing
  # path, reported as "cannot open the connection", which reads like a broken
  # test rather than an absent optional data file.
  skip_if_not(file.exists(.brfss_rds_path()), "BRFSS 2023 cleaned file not present")
  namcs <- flag_urps_visits(load_namcs_2019(.namcs_rds_path()))
  sv    <- namcs_urps_stratum_visits(namcs)
  brfss <- readRDS(.brfss_rds_path())
  sp    <- brfss_population_by_stratum(brfss)
  rt    <- compute_urps_visit_rates(sv, sp)
  model <- fit_urps_visit_rate_model(rt)

  pop_proj <- tidyr::expand_grid(
    year = 2024:2034,
    age_band = c("18-34", "35-44", "45-64", "65-74", "75+"),
    sex = "Female",
    race_eth = c("White_NH", "Black_NH", "Hispanic"),
    insurance_2tier = "Insured"
  ) |>
    dplyr::mutate(population = 8e6 * (1 + 0.005 * (year - 2024)))

  d5 <- compute_namcs_demand_estimand(pop_proj, model)
  expect_equal(unique(d5$estimand), "D5")
  expect_equal(nrow(d5), 11L)
  expect_true(all(diff(d5$demand_clinical_fte) > 0))
  expect_gt(min(d5$demand_clinical_fte), 0)
})
