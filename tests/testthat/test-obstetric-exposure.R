# Regression guards for the obstetric-exposure demand estimand (D4) and the
# birth-cohort vaginal-delivery exposure layer (R/13b-obstetric_exposure.R).

# ---- Cohort exposure --------------------------------------------------------

test_that("cesarean interpolation is a bounded fraction, clamped outside anchors", {
  r <- cesarean_rate_for_year(c(1965, 1996, 2009, 2024))
  expect_true(all(r > 0 & r < 1))
  expect_equal(cesarean_rate_for_year(1900), cesarean_rate_for_year(1965))  # clamp low
  expect_equal(cesarean_rate_for_year(2100), cesarean_rate_for_year(2024))  # clamp high
})

test_that("cohort exposure decomposes parity into vaginal + cesarean", {
  ex <- cohort_vaginal_exposure(c(1935, 1965, 1985))
  expect_equal(ex$mean_vaginal_deliveries + ex$mean_cesarean_deliveries,
               ex$mean_total_parity, tolerance = 1e-6)
  expect_true(all(ex$cohort_cesarean_fraction >= 0 & ex$cohort_cesarean_fraction <= 1))
})

test_that("later cohorts carry less vaginal-delivery exposure (the core signal)", {
  ex <- cohort_vaginal_exposure(c(1935, 1950, 1965, 1975, 1985))
  expect_true(all(diff(ex$mean_vaginal_deliveries) < 0))   # monotone decline
  expect_true(all(diff(ex$cohort_cesarean_fraction) > 0))  # cesarean rises
})

# ---- Exposure multiplier ----------------------------------------------------

test_that("the multiplier equals 1 at the reference cohort and falls for later ones", {
  m <- obstetric_exposure_multiplier(c(2025, 2040, 2055), bands = "65-79")
  ref <- m$exposure_multiplier[m$year == 2025]
  expect_equal(ref, 1, tolerance = 1e-9)                    # 65-79 @ base year = ref
  expect_true(all(diff(m$exposure_multiplier[order(m$year)]) < 0))
  expect_true(all(m$exposure_multiplier > 0))
})

test_that("a larger OR deepens the exposure discount", {
  m_lo <- obstetric_exposure_multiplier(2055, bands = "65-79",
                                        or_per_vaginal_delivery = 1.05)
  m_hi <- obstetric_exposure_multiplier(2055, bands = "65-79",
                                        or_per_vaginal_delivery = 1.40)
  expect_lt(m_hi$exposure_multiplier, m_lo$exposure_multiplier)
})

# ---- D4 estimand ------------------------------------------------------------

test_that("compute_demand_denominators_lifecourse adds D4 alongside D1-D3", {
  pop <- tidyr::expand_grid(year = c(2025, 2040, 2055), age_band = DEMAND_AGE_BANDS) |>
    dplyr::mutate(female_pop = dplyr::case_when(
      age_band == "20-39" ~ 40e6, age_band == "40-59" ~ 41e6,
      age_band == "60-64" ~ 11e6, age_band == "65-79" ~ 24e6, TRUE ~ 9e6))
  d <- compute_demand_denominators_lifecourse(pop)
  expect_setequal(unique(d$estimand), c("D1", "D2", "D3", "D4"))
  expect_true(all(d$demand_cases > 0))
})

test_that("D4 diverges from D1 over time (not a proportional rescaling)", {
  pop <- tidyr::expand_grid(year = c(2025, 2055), age_band = DEMAND_AGE_BANDS) |>
    dplyr::mutate(female_pop = dplyr::case_when(
      age_band == "20-39" ~ 40e6, age_band == "40-59" ~ 41e6,
      age_band == "60-64" ~ 11e6, age_band == "65-79" ~ 24e6, TRUE ~ 9e6))
  d <- compute_demand_denominators_lifecourse(pop)
  ratio <- sapply(c(2025, 2055), function(y) {
    d$demand_cases[d$estimand == "D4" & d$year == y] /
      d$demand_cases[d$estimand == "D1" & d$year == y]
  })
  # the obstetric discount deepens as lower-exposure cohorts age in
  expect_lt(ratio[2], ratio[1])
  expect_true(all(ratio > 0 & ratio <= 1.05))
})
