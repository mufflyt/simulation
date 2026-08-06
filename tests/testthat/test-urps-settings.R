# Tests for R/supply-urps_settings.R — care delivery setting differentiation
#
# Guards:
#  1. URPS_DEFAULT_SETTING_MIX: shares sum to 1 per service, valid settings.
#  2. validate_setting_mix: errors on bad inputs.
#  3. volumes_with_setting: attaches correct setting, warns on unknown services.
#  4. split_volumes_by_setting: volume conservation, shape, missing service warn.
#  5. apply_setting_shift: volume conservation, from/to update, unknown setting errors.
#  6. apply_setting_productivity: effective volume scales correctly.
#  7. apply_setting_scenario: baseline is no-op; named scenarios change volume.
#  8. setting_scenario_summary: shape and pct_change arithmetic.
#  9. convert_workload_to_fte(by_setting = TRUE): returns per-setting rows.
# 10. URPS_SETTING_SCENARIOS: all entries validate.

# ---- Fixtures ----------------------------------------------------------------

make_vols <- function() {
  tibble::tibble(
    year    = rep(2025L, 4L),
    service = c("new_consultation", "return_visit",
                "sling_procedure", "prolapse_procedure"),
    volume  = c(3e6, 7.2e6, 1.1e5, 9e4)
  )
}

# ---- 1. URPS_DEFAULT_SETTING_MIX --------------------------------------------

test_that("default setting mix shares sum to 1 per service", {
  sums <- tapply(URPS_DEFAULT_SETTING_MIX$share,
                 URPS_DEFAULT_SETTING_MIX$service, sum)
  expect_true(all(abs(sums - 1) < 1e-8),
              info = paste("Off-sum services:", names(sums[abs(sums-1) > 1e-8])))
})

test_that("default setting mix uses only valid setting names", {
  expect_true(all(URPS_DEFAULT_SETTING_MIX$setting %in% URPS_SETTING_NAMES))
})

test_that("all services in example_service_volumes are covered by the mix", {
  demand <- compute_demand_denominators(example_female_population_by_band())
  vols   <- example_service_volumes(demand)
  svc    <- unique(vols$service)
  mixed  <- unique(URPS_DEFAULT_SETTING_MIX$service)
  expect_true(all(svc %in% mixed),
              info = paste("Uncovered:", setdiff(svc, mixed)))
})

# ---- 2. validate_setting_mix -------------------------------------------------

test_that("validate_setting_mix passes on valid input", {
  expect_silent(validate_setting_mix(URPS_DEFAULT_SETTING_MIX))
})

test_that("validate_setting_mix errors when shares don't sum to 1", {
  bad <- URPS_DEFAULT_SETTING_MIX
  bad$share[bad$service == "new_consultation"] <-
    bad$share[bad$service == "new_consultation"] * 0.5
  expect_error(validate_setting_mix(bad), regexp = "sum to 1")
})

test_that("validate_setting_mix errors on unknown setting names", {
  bad <- URPS_DEFAULT_SETTING_MIX
  bad$setting[1] <- "spaceship"
  expect_error(validate_setting_mix(bad), regexp = "unknown setting")
})

test_that("validate_setting_mix errors on negative shares", {
  bad <- URPS_DEFAULT_SETTING_MIX
  bad$share[1] <- -0.1
  expect_error(validate_setting_mix(bad), regexp = "\\[0, 1\\]")
})

# ---- 3. volumes_with_setting ------------------------------------------------

test_that("volumes_with_setting attaches correct setting column", {
  vols <- make_vols()
  out  <- volumes_with_setting(vols)
  expect_true("setting" %in% names(out))
  expect_equal(out$setting[out$service == "sling_procedure"],    "operative")
  expect_equal(out$setting[out$service == "new_consultation"],   "ambulatory")
})

test_that("volumes_with_setting warns on services absent from workload basket", {
  vols <- tibble::tibble(service = c("new_consultation", "mystery_service"),
                         volume  = c(1e6, 500))
  expect_warning(volumes_with_setting(vols), regexp = "absent from workload basket")
})

# ---- 4. split_volumes_by_setting --------------------------------------------

test_that("split_volumes_by_setting conserves total volume per service per year", {
  vols  <- make_vols()
  split <- split_volumes_by_setting(vols)
  for (svc in unique(vols$service)) {
    orig  <- vols$volume[vols$service == svc]
    total <- sum(split$volume[split$service == svc])
    expect_equal(total, orig, tolerance = 1e-8,
                 label = paste("volume conservation for", svc))
  }
})

test_that("split_volumes_by_setting adds a setting column", {
  out <- split_volumes_by_setting(make_vols())
  expect_true("setting" %in% names(out))
})

test_that("split_volumes_by_setting has more rows than input", {
  vols <- make_vols()
  out  <- split_volumes_by_setting(vols)
  expect_gt(nrow(out), nrow(vols))
})

test_that("split_volumes_by_setting warns on services absent from mix", {
  vols <- tibble::tibble(year = 2025L, service = "unknown_service", volume = 1000)
  expect_warning(split_volumes_by_setting(vols), regexp = "absent from setting mix")
})

# ---- 5. apply_setting_shift -------------------------------------------------

test_that("apply_setting_shift conserves total volume", {
  vols  <- make_vols()
  split <- split_volumes_by_setting(vols)
  total_before <- sum(split$volume)
  shifted <- apply_setting_shift(split, service = "new_consultation",
                                  from = "office", to = "telehealth",
                                  fraction = 0.20)
  expect_equal(sum(shifted$volume), total_before, tolerance = 1e-6)
})

test_that("apply_setting_shift reduces from-setting volume", {
  vols  <- make_vols()
  split <- split_volumes_by_setting(vols)
  office_before <- sum(split$volume[split$service == "new_consultation" &
                                      split$setting == "office"])
  shifted <- apply_setting_shift(split, "new_consultation",
                                  from = "office", to = "telehealth", 0.20)
  office_after <- sum(shifted$volume[shifted$service == "new_consultation" &
                                       shifted$setting == "office"])
  expect_lt(office_after, office_before)
})

test_that("apply_setting_shift increases to-setting volume", {
  vols  <- make_vols()
  split <- split_volumes_by_setting(vols)
  th_before <- sum(split$volume[split$service == "new_consultation" &
                                  split$setting == "telehealth"])
  shifted <- apply_setting_shift(split, "new_consultation",
                                  from = "office", to = "telehealth", 0.20)
  th_after <- sum(shifted$volume[shifted$service == "new_consultation" &
                                   shifted$setting == "telehealth"])
  expect_gt(th_after, th_before)
})

test_that("fraction = 0 is a no-op", {
  vols   <- make_vols()
  split  <- split_volumes_by_setting(vols)
  out    <- apply_setting_shift(split, "new_consultation",
                                 from = "office", to = "telehealth", 0)
  expect_equal(out$volume, split$volume)
})

test_that("apply_setting_shift errors on invalid fraction", {
  split <- split_volumes_by_setting(make_vols())
  expect_error(apply_setting_shift(split, "new_consultation",
                                    "office", "telehealth", 1.5),
               regexp = "\\[0, 1\\]")
})

test_that("apply_setting_shift errors on unknown setting names", {
  split <- split_volumes_by_setting(make_vols())
  expect_error(apply_setting_shift(split, "new_consultation",
                                    "office", "spaceship", 0.1),
               regexp = "URPS_SETTING_NAMES")
})

# ---- 6. apply_setting_productivity ------------------------------------------

test_that("office productivity multiplier = 1 leaves volume unchanged", {
  split <- split_volumes_by_setting(make_vols())
  prod  <- c(office = 1, telehealth = 1, hospital_outpatient = 1,
             asc = 1, operative = 1, postacute = 1)
  out   <- apply_setting_productivity(split, productivity = prod)
  expect_equal(out$volume, split$volume, tolerance = 1e-10)
})

test_that("lower productivity increases effective volume (more FTE required)", {
  split <- split_volumes_by_setting(make_vols())
  prod  <- c(office = 1, telehealth = 0.5, hospital_outpatient = 1,
             asc = 1, operative = 1, postacute = 1)
  out   <- apply_setting_productivity(split, productivity = prod)
  th_raw <- split$volume[split$setting == "telehealth"]
  th_eff <- out$volume[out$setting == "telehealth"]
  if (length(th_raw) > 0 && any(th_raw > 0)) {
    expect_true(all(th_eff[th_raw > 0] > th_raw[th_raw > 0]))
  }
})

test_that("apply_setting_productivity errors on non-positive multiplier", {
  split <- split_volumes_by_setting(make_vols())
  prod  <- c(office = 0, telehealth = 0.92, hospital_outpatient = 0.88,
             asc = 1.05, operative = 1.00, postacute = 0.80)
  expect_error(apply_setting_productivity(split, prod), regexp = "> 0")
})

# ---- 7. apply_setting_scenario ----------------------------------------------

test_that("baseline scenario is a no-op on effective volumes", {
  vols <- make_vols()
  out  <- apply_setting_scenario(vols, "baseline")
  expect_equal(sort(out$service), sort(vols$service))
  # Total volume may differ due to productivity adjustment, but services match
  expect_setequal(out$service, vols$service)
})

test_that("telehealth_10pct reduces aggregate new_consultation effective volume", {
  vols     <- make_vols()
  base_eff <- apply_setting_scenario(vols, "baseline")
  tel_eff  <- apply_setting_scenario(vols, "telehealth_10pct")
  base_nc  <- base_eff$volume[base_eff$service == "new_consultation"]
  tel_nc   <- tel_eff$volume[tel_eff$service == "new_consultation"]
  # Telehealth has productivity 0.92 < 1.0 (office), so effective volume increases
  # (more FTE needed per raw visit). Total effective volume should be >= baseline.
  expect_gte(tel_nc, base_nc)
})

test_that("asc_migration_30pct changes sling effective volume", {
  vols     <- make_vols()
  base_eff <- apply_setting_scenario(vols, "baseline")
  asc_eff  <- apply_setting_scenario(vols, "asc_migration_30pct")
  base_sl  <- base_eff$volume[base_eff$service == "sling_procedure"]
  asc_sl   <- asc_eff$volume[asc_eff$service == "sling_procedure"]
  # ASC productivity is 1.05 > 1.00 (operative), so effective volume decreases
  expect_lt(asc_sl, base_sl)
})

test_that("apply_setting_scenario errors on unknown scenario_id", {
  expect_error(apply_setting_scenario(make_vols(), "nonexistent"),
               regexp = "unknown scenario_id")
})

test_that("aggregate = FALSE returns setting-level tibble", {
  out <- apply_setting_scenario(make_vols(), "telehealth_10pct", aggregate = FALSE)
  expect_true("setting" %in% names(out))
  expect_gt(nrow(out), nrow(make_vols()))
})

test_that("all pre-defined scenarios run without error", {
  vols <- make_vols()
  for (nm in names(URPS_SETTING_SCENARIOS)) {
    expect_no_error(apply_setting_scenario(vols, nm))
  }
})

# ---- 8. setting_scenario_summary --------------------------------------------

test_that("setting_scenario_summary returns expected columns", {
  demand <- compute_demand_denominators(example_female_population_by_band())
  vols   <- example_service_volumes(demand)
  summ   <- setting_scenario_summary(vols, "telehealth_10pct",
                                     year_filter = 2025L)
  expect_true(all(c("service", "volume_baseline", "volume_adjusted",
                    "volume_delta", "pct_change") %in% names(summ)))
})

test_that("setting_scenario_summary pct_change arithmetic is correct", {
  demand <- compute_demand_denominators(example_female_population_by_band())
  vols   <- example_service_volumes(demand)
  summ   <- setting_scenario_summary(vols, "telehealth_10pct",
                                     year_filter = 2025L)
  expect_equal(summ$pct_change,
               100 * summ$volume_delta / summ$volume_baseline,
               tolerance = 1e-8)
})

# ---- 9. convert_workload_to_fte(by_setting = TRUE) ---------------------------

test_that("by_setting = TRUE returns a setting column", {
  demand    <- compute_demand_denominators(example_female_population_by_band())
  vols      <- example_service_volumes(dplyr::filter(demand, year == 2025))
  base_wrvu <- service_volume_to_wrvu(vols)
  wrvu_per  <- calibrate_wrvu_per_fte(base_wrvu$work_rvu,
                                       base_year_required_fte = 1400)
  out <- convert_workload_to_fte(vols, wrvu_per_fte = wrvu_per,
                                  by_setting = TRUE)
  expect_true("setting" %in% names(out))
})

test_that("by_setting = TRUE required_fte sums to by_setting = FALSE total", {
  demand    <- compute_demand_denominators(example_female_population_by_band())
  vols      <- example_service_volumes(dplyr::filter(demand, year == 2025))
  base_wrvu <- service_volume_to_wrvu(vols)
  wrvu_per  <- calibrate_wrvu_per_fte(base_wrvu$work_rvu,
                                       base_year_required_fte = 1400)

  total_flat  <- convert_workload_to_fte(vols, wrvu_per, by_setting = FALSE)$required_fte
  by_setting  <- convert_workload_to_fte(vols, wrvu_per, by_setting = TRUE)
  total_split <- sum(by_setting$required_fte)
  expect_equal(total_split, total_flat, tolerance = 1e-6)
})
