# Regression guards for the demand estimands, scenarios, geography and
# calibration/validation machinery.

# ---- Demand estimands ------------------------------------------------------

test_that("the three demand estimands are no longer proportional rescalings", {
  # The previous implementation computed every estimand as
  # population_65plus * constant, making D1/D2/D3 the SAME series up to scale.
  pop <- tidyr::expand_grid(year = 2025:2035, age_band = DEMAND_AGE_BANDS) %>%
    dplyr::mutate(female_pop = dplyr::case_when(
      age_band == "20-39" ~ 43e6 * 1.001^(year - 2025),
      age_band == "40-59" ~ 41.5e6 * 1.004^(year - 2025),
      age_band == "60-79" ~ 33e6 * 1.014^(year - 2025),
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
  expect_gt(WU2011_SURGERY_RATE_PER_1000[["60-79"]],
            WU2011_SURGERY_RATE_PER_1000[["80+"]])
  expect_gt(PFD_PREVALENCE_BY_AGE[["80+"]], PFD_PREVALENCE_BY_AGE[["60-79"]])
  expect_gt(CONSULT_RATE_BY_AGE[["60-79"]], CONSULT_RATE_BY_AGE[["80+"]])
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

test_that("supply scenarios use age-axis shifts, not hazard multipliers", {
  reg <- supply_scenario_registry(55)
  expect_silent(validate_scenario_registry(reg, "supply"))
  expect_equal(reg$retirement_2_years_later$retirement_shift_years, 2)
  expect_equal(reg$retirement_2_years_earlier$retirement_shift_years, -2)

  bad <- reg
  bad$status_quo$hazard_mult <- 0.73
  expect_error(validate_scenario_registry(bad, "supply"), "scalar hazard multiplier")
})

test_that("scenario registries are contract-checked", {
  reg <- supply_scenario_registry()
  no_sq <- reg[setdiff(names(reg), "status_quo")]
  expect_error(validate_scenario_registry(no_sq, "supply"), "status_quo")

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
  expect_setequal(dem$reduced_barriers$access_components,
                  c("uninsured", "nonmetro", "racial_equity"))
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

test_that("the two placement rules give different answers when demand has moved", {
  roster <- tibble::tibble(state = c(rep("A", 90), rep("B", 10)))
  h <- historical_placement_shares(roster, "state")
  o <- opportunity_placement_shares(
    tibble::tibble(geo = c("A", "B"), demand_growth_fte = c(10, 90)),
    tibble::tibble(geo = c("A", "B"), retirements_fte = c(5, 5))
  )
  expect_gt(o$share[o$geo == "B"], h$share[h$geo == "B"])
  blend <- blend_placement_shares(h, o, weight = 0.5)
  expect_equal(sum(blend$share), 1, tolerance = 1e-12)
  expect_gt(blend$share[blend$geo == "B"], h$share[h$geo == "B"])
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
  gap <- baseline_gap(1306, 0.948, method = "capacity_survey")

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
  expect_true(all(is.na(rep$passed[rep$type %in% c("conceptual", "external", "data")])))
})
