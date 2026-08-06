# Wu 2011 surgery rates and the age-specific D3 (R/13-demand_urps.R).
#
# These exist because the rates shipped until 2026-08-05 (1.5 / 4.6 / 6.3 / 3.2)
# overstated the source by ~16% and nothing caught it: a constant with a citation
# beside it looks checked. cliff carried a second, component-level table that WAS
# faithful, in a function that was never wired. The duplicate is now resolved in
# favour of the correct numbers, and these hold that.

test_that("the summed rates are derived from the components, never typed", {
  # The failure this prevents: someone edits one and not the other, and the total
  # silently stops matching its own parts.
  expect_equal(
    unname(WU2011_SURGERY_RATE_PER_1000),
    WU2011_SURGERY_RATE_COMPONENTS$sui + WU2011_SURGERY_RATE_COMPONENTS$pop)
  expect_equal(names(WU2011_SURGERY_RATE_PER_1000),
               WU2011_SURGERY_RATE_COMPONENTS$age_band)
  expect_equal(names(WU2011_SURGERY_RATE_PER_1000), DEMAND_AGE_BANDS)
})

test_that("the published 60-79 rate is applied to both sub-bands", {
  # Wu publishes four bands; the package uses five. Splitting 60-79 must not
  # invent a gradient the source does not have.
  cmp <- WU2011_SURGERY_RATE_COMPONENTS
  expect_equal(cmp$sui[cmp$age_band == "60-64"], cmp$sui[cmp$age_band == "65-79"])
  expect_equal(cmp$pop[cmp$age_band == "60-64"], cmp$pop[cmp$age_band == "65-79"])
})

test_that("surgery peaks at 60-79 and falls at 80+, unlike prevalence", {
  # The shape is what makes D3 a different estimand from D1 rather than a
  # rescaling of it. A rate table that rose monotonically would be a red flag.
  expect_gt(WU2011_SURGERY_RATE_PER_1000[["65-79"]],
            WU2011_SURGERY_RATE_PER_1000[["80+"]])
  expect_gt(WU2011_SURGERY_RATE_PER_1000[["65-79"]],
            WU2011_SURGERY_RATE_PER_1000[["40-59"]])
})

test_that("the rates reproduce Wu 2011's published counts", {
  # The regression guard on the +16% error. Rates are anchored to the paper's
  # own absolute counts (SUI 210,700 + POP 166,000 = 376,700 in 2010 ->
  # 555,020 in 2050), interpolated to 2022 and applied to NPP female population.
  # Uses a fixed population so the test does not depend on the Census file.
  pop <- c("20-39" = 44.4e6, "40-59" = 41.4e6, "60-79" = 34.6e6, "80+" = 8.0e6)
  cmp <- WU2011_SURGERY_RATE_COMPONENTS
  r <- function(col, band) cmp[[col]][cmp$age_band == if (band == "60-79") "60-64" else band]

  sui <- sum(pop * vapply(names(pop), function(b) r("sui", b), numeric(1)) / 1000)
  pop_c <- sum(pop * vapply(names(pop), function(b) r("pop", b), numeric(1)) / 1000)

  interp <- function(a, b) a + (2022 - 2010) / 40 * (b - a)
  expect_lt(abs(sui / interp(210700, 310050) - 1), 0.05)
  expect_lt(abs(pop_c / interp(166000, 245970) - 1), 0.05)
  expect_lt(abs((sui + pop_c) / interp(376700, 555020) - 1), 0.05)

  # And the OLD rates must fail the same check, or this test proves nothing.
  old <- c("20-39" = 1.5, "40-59" = 4.6, "60-79" = 6.3, "80+" = 3.2)
  expect_gt(abs(sum(pop * old / 1000) / interp(376700, 555020) - 1), 0.10)
})

test_that("an unrecognised age band is an error, not a silent drop", {
  # cliff labels the top band "80plus" where this table says "80+". Dropping it
  # quietly removed 15% of the cases and understated demand with no warning.
  good <- data.frame(year = 2025L, age_band = DEMAND_AGE_BANDS, female_pop = 1e6)
  bad  <- transform(good, age_band = replace(as.character(age_band), 5, "80plus"))

  expect_error(apply_age_specific_surgery_demand(bad), "80plus")
  expect_error(apply_age_specific_surgery_demand(bad), "understate")

  # The correctly-labelled frame still works, and keeps the band it would have lost.
  ok <- apply_age_specific_surgery_demand(good)
  expect_gt(ok$surgical_cases,
            apply_age_specific_surgery_demand(good[1:4, ])$surgical_cases)
})

test_that("by_condition splits SUI and POP and conserves the total", {
  # The split is the reason cliff's component shape won: R/51 models UI and POP
  # as separate cascades, and a summed rate cannot feed it.
  pop <- data.frame(year = c(2025L, 2030L), age_band = rep(DEMAND_AGE_BANDS, each = 2),
                    female_pop = 1e6)
  split <- apply_age_specific_surgery_demand(pop, by_condition = TRUE)
  total <- apply_age_specific_surgery_demand(pop)

  expect_true(all(c("sui_cases", "pop_cases") %in% names(split)))
  expect_equal(split$sui_cases + split$pop_cases, total$surgical_cases)
  expect_true(all(split$sui_cases > 0), all(split$pop_cases > 0))
})
