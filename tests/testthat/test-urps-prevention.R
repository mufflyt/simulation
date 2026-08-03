# Tests for R/urps_prevention.R — DPMM-lite conservative management module
#
# Guards:
#  1. Baseline (zero uptake): all multipliers equal 1.
#  2. Surgical services: sling/prolapse/postop reduced by uptake × surgical_reduction.
#  3. Office services: consultations/returns reduced by mean_uptake × office_reduction.
#  4. Conservative-pathway services: pessary_care and ptns multipliers > 1 when uptake > 0.
#  5. Input validation: bad fractions, wrong types, missing columns.
#  6. apply_prevention_multipliers: correct scaling, passthrough for unspecified services.
#  7. apply_named_prevention_scenario: matches manual multiplier application.
#  8. prevention_volume_summary: shape, signs, and pct_change arithmetic.

# ---- Fixtures ----------------------------------------------------------------

make_volumes <- function() {
  tibble::tibble(
    year    = rep(2025L, 11L),
    service = c("new_consultation", "return_visit", "pessary_care",
                "urodynamics", "cystoscopy", "botox_bladder", "ptns",
                "bladder_instillation", "sling_procedure", "prolapse_procedure",
                "postoperative_care"),
    volume  = c(5000, 12000, 350, 1500, 900, 40, 100, 60, 1100, 900, 4000)
  )
}

# ---- 1. Baseline (zero uptake) -----------------------------------------------

test_that("zero uptake produces all multipliers equal 1", {
  m <- conservative_management_multipliers(ui_uptake = 0, pop_uptake = 0)
  # botox_bladder and bladder_instillation are always 1; surgical services should be 1
  expect_equal(m[["sling_procedure"]],   1)
  expect_equal(m[["prolapse_procedure"]], 1)
  expect_equal(m[["new_consultation"]],  1)
  expect_equal(m[["return_visit"]],      1)
  expect_equal(m[["ptns"]],             1)
  expect_equal(m[["pessary_care"]],      1)
})

test_that("apply_prevention_multipliers with all-1 multipliers is a no-op", {
  vols <- make_volumes()
  m    <- conservative_management_multipliers(0, 0)
  out  <- apply_prevention_multipliers(vols, m)
  expect_equal(out$volume, vols$volume)
})

# ---- 2. Surgical services ----------------------------------------------------

test_that("sling_procedure multiplier equals 1 - ui_uptake * surgical_reduction", {
  m <- conservative_management_multipliers(ui_uptake = 0.25, pop_uptake = 0,
                                           surgical_reduction = 0.80)
  expect_equal(m[["sling_procedure"]], 1 - 0.25 * 0.80, tolerance = 1e-10)
})

test_that("prolapse_procedure multiplier equals 1 - pop_uptake * surgical_reduction", {
  m <- conservative_management_multipliers(ui_uptake = 0, pop_uptake = 0.30,
                                           surgical_reduction = 0.80)
  expect_equal(m[["prolapse_procedure"]], 1 - 0.30 * 0.80, tolerance = 1e-10)
})

test_that("sling and prolapse multipliers are independent of each other's uptake", {
  m_ui  <- conservative_management_multipliers(ui_uptake = 0.50, pop_uptake = 0)
  m_pop <- conservative_management_multipliers(ui_uptake = 0,    pop_uptake = 0.50)
  # Only sling changes with ui_uptake
  expect_lt(m_ui[["sling_procedure"]],    1)
  expect_equal(m_ui[["prolapse_procedure"]], 1)
  # Only prolapse changes with pop_uptake
  expect_equal(m_pop[["sling_procedure"]], 1)
  expect_lt(m_pop[["prolapse_procedure"]], 1)
})

test_that("full uptake with full surgical_reduction drives surgical volume to 0", {
  m <- conservative_management_multipliers(1, 1, surgical_reduction = 1.0,
                                           office_reduction = 0)
  expect_equal(m[["sling_procedure"]],    0)
  expect_equal(m[["prolapse_procedure"]], 0)
})

test_that("multipliers are non-negative for extreme uptake", {
  m <- conservative_management_multipliers(1, 1, surgical_reduction = 1,
                                           office_reduction = 1)
  expect_true(all(m >= 0))
})

# ---- 3. Office services ------------------------------------------------------

test_that("consultation multiplier is less than 1 when mean_uptake > 0", {
  m <- conservative_management_multipliers(ui_uptake = 0.25, pop_uptake = 0.25,
                                           office_reduction = 0.40)
  expect_lt(m[["new_consultation"]], 1)
  expect_lt(m[["return_visit"]],     1)
})

test_that("new_consultation and return_visit multipliers are equal", {
  m <- conservative_management_multipliers(0.20, 0.30)
  expect_equal(m[["new_consultation"]], m[["return_visit"]])
})

test_that("office reduction is zero when office_reduction = 0", {
  m <- conservative_management_multipliers(0.50, 0.50, office_reduction = 0)
  expect_equal(m[["new_consultation"]], 1)
})

# ---- 4. Conservative-pathway services ----------------------------------------

test_that("pessary_care multiplier > 1 when pop_uptake > 0", {
  m <- conservative_management_multipliers(ui_uptake = 0, pop_uptake = 0.20)
  expect_gt(m[["pessary_care"]], 1)
})

test_that("ptns multiplier > 1 when ui_uptake > 0", {
  m <- conservative_management_multipliers(ui_uptake = 0.20, pop_uptake = 0)
  expect_gt(m[["ptns"]], 1)
})

test_that("pessary_care multiplier equals 1 when pop_uptake = 0", {
  m <- conservative_management_multipliers(ui_uptake = 0.30, pop_uptake = 0)
  expect_equal(m[["pessary_care"]], 1)
})

test_that("ptns multiplier equals 1 when ui_uptake = 0", {
  m <- conservative_management_multipliers(ui_uptake = 0, pop_uptake = 0.30)
  expect_equal(m[["ptns"]], 1)
})

test_that("botox_bladder and bladder_instillation are unaffected (multiplier = 1)", {
  m <- conservative_management_multipliers(0.50, 0.50,
                                           surgical_reduction = 1, office_reduction = 1)
  expect_equal(m[["botox_bladder"]],       1)
  expect_equal(m[["bladder_instillation"]], 1)
})

# ---- 5. Input validation -----------------------------------------------------

test_that("ui_uptake outside [0, 1] throws an error", {
  expect_error(conservative_management_multipliers(ui_uptake = -0.1, pop_uptake = 0),
               regexp = "ui_uptake")
  expect_error(conservative_management_multipliers(ui_uptake = 1.1, pop_uptake = 0),
               regexp = "ui_uptake")
})

test_that("pop_uptake outside [0, 1] throws an error", {
  expect_error(conservative_management_multipliers(0, pop_uptake = 1.5),
               regexp = "pop_uptake")
})

test_that("surgical_reduction outside [0, 1] throws an error", {
  expect_error(conservative_management_multipliers(0.2, 0.2, surgical_reduction = 1.2),
               regexp = "surgical_reduction")
})

test_that("office_reduction outside [0, 1] throws an error", {
  expect_error(conservative_management_multipliers(0.2, 0.2, office_reduction = -0.1),
               regexp = "office_reduction")
})

test_that("apply_prevention_multipliers errors on missing columns", {
  m <- conservative_management_multipliers(0.25, 0.25)
  expect_error(apply_prevention_multipliers(tibble::tibble(service = "sling_procedure"),
                                            m),
               regexp = "volume")
  expect_error(apply_prevention_multipliers(tibble::tibble(volume = 100), m),
               regexp = "service")
})

test_that("apply_prevention_multipliers errors on negative multipliers", {
  vols <- make_volumes()
  bad  <- c(sling_procedure = -1)
  expect_error(apply_prevention_multipliers(vols, bad), regexp = "non-negative")
})

test_that("apply_prevention_multipliers errors on unnamed multiplier vector", {
  vols <- make_volumes()
  expect_error(apply_prevention_multipliers(vols, c(0.5, 0.6)),
               regexp = "named")
})

# ---- 6. apply_prevention_multipliers -----------------------------------------

test_that("volumes for named services are scaled correctly", {
  vols <- make_volumes()
  m    <- conservative_management_multipliers(0.25, 0.25)
  out  <- apply_prevention_multipliers(vols, m)

  sling_row   <- vols$volume[vols$service == "sling_procedure"]
  sling_mult  <- m[["sling_procedure"]]
  expect_equal(out$volume[out$service == "sling_procedure"],
               sling_row * sling_mult, tolerance = 1e-10)
})

test_that("services absent from multiplier vector pass through unchanged", {
  vols <- tibble::tibble(service = c("sling_procedure", "some_other_service"),
                         volume  = c(1000, 500))
  m    <- c(sling_procedure = 0.8)
  out  <- apply_prevention_multipliers(vols, m)
  expect_equal(out$volume[out$service == "some_other_service"], 500)
})

test_that("apply_prevention_multipliers attaches prevention_multipliers attribute", {
  vols <- make_volumes()
  m    <- conservative_management_multipliers(0.20, 0.20)
  out  <- apply_prevention_multipliers(vols, m)
  expect_false(is.null(attr(out, "prevention_multipliers")))
})

test_that("multi-year volumes are all scaled consistently", {
  vols <- tibble::tibble(
    year    = c(2025L, 2026L, 2027L),
    service = rep("sling_procedure", 3),
    volume  = c(1000, 1050, 1100)
  )
  m   <- conservative_management_multipliers(0.25, 0)
  out <- apply_prevention_multipliers(vols, m)
  expected_mult <- m[["sling_procedure"]]
  expect_equal(out$volume, vols$volume * expected_mult, tolerance = 1e-10)
})

# ---- 7. apply_named_prevention_scenario -------------------------------------

test_that("apply_named_prevention_scenario matches manual multiplier application", {
  vols  <- make_volumes()
  scen  <- URPS_PREVENTION_SCENARIOS[["conservative_25pct"]]
  m     <- conservative_management_multipliers(
    ui_uptake          = scen$ui_uptake,
    pop_uptake         = scen$pop_uptake,
    surgical_reduction = scen$surgical_reduction,
    office_reduction   = scen$office_reduction
  )
  manual  <- apply_prevention_multipliers(vols, m)
  wrapped <- apply_named_prevention_scenario(vols, "conservative_25pct")
  expect_equal(manual$volume, wrapped$volume, tolerance = 1e-10)
})

test_that("apply_named_prevention_scenario errors on unknown scenario_id", {
  vols <- make_volumes()
  expect_error(apply_named_prevention_scenario(vols, "nonexistent_scenario"),
               regexp = "unknown scenario_id")
})

test_that("baseline scenario leaves volumes unchanged", {
  vols <- make_volumes()
  out  <- apply_named_prevention_scenario(vols, "baseline")
  expect_equal(out$volume, vols$volume)
})

test_that("higher uptake leads to lower surgical volumes", {
  vols <- make_volumes()
  s10  <- apply_named_prevention_scenario(vols, "conservative_10pct")
  s50  <- apply_named_prevention_scenario(vols, "conservative_50pct")
  sling_10 <- s10$volume[s10$service == "sling_procedure"]
  sling_50 <- s50$volume[s50$service == "sling_procedure"]
  expect_lt(sling_50, sling_10)
})

# ---- 8. prevention_volume_summary -------------------------------------------

test_that("prevention_volume_summary returns expected columns", {
  demand_long <- compute_demand_denominators(example_female_population_by_band())
  tbl <- prevention_volume_summary(demand_long, scenario_id = "conservative_25pct")
  expect_true(all(c("service", "volume_baseline", "volume_prevention",
                    "volume_delta", "pct_change") %in% names(tbl)))
})

test_that("surgical services have negative pct_change in prevention scenario", {
  demand_long <- compute_demand_denominators(example_female_population_by_band())
  tbl <- prevention_volume_summary(demand_long, "conservative_25pct",
                                   year_filter = min(demand_long$year))
  sling_row <- tbl[tbl$service == "sling_procedure", ]
  expect_lt(sling_row$pct_change, 0)
  prolapse_row <- tbl[tbl$service == "prolapse_procedure", ]
  expect_lt(prolapse_row$pct_change, 0)
})

test_that("conservative-pathway services have non-negative pct_change", {
  demand_long <- compute_demand_denominators(example_female_population_by_band())
  tbl <- prevention_volume_summary(demand_long, "conservative_25pct",
                                   year_filter = min(demand_long$year))
  ptns_row    <- tbl[tbl$service == "ptns", ]
  pessary_row <- tbl[tbl$service == "pessary_care", ]
  expect_gte(ptns_row$pct_change,    0)
  expect_gte(pessary_row$pct_change, 0)
})

test_that("pct_change arithmetic is correct", {
  demand_long <- compute_demand_denominators(example_female_population_by_band())
  tbl <- prevention_volume_summary(demand_long, "conservative_10pct",
                                   year_filter = min(demand_long$year))
  expect_equal(tbl$pct_change,
               100 * tbl$volume_delta / tbl$volume_baseline,
               tolerance = 1e-8)
})

test_that("year_filter subsets to the requested year", {
  demand_long <- compute_demand_denominators(example_female_population_by_band())
  yr  <- min(demand_long$year)
  tbl <- prevention_volume_summary(demand_long, "conservative_25pct",
                                   year_filter = yr)
  expect_true(all(tbl$year == yr))
})
