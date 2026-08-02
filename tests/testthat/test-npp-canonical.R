# Guards for the Census-NPP canonical demand-population wiring.
# These skip when the data-raw file is absent (e.g. an installed-package check),
# so they never turn red for a legitimate no-data environment.

npp_available <- function() {
  ok <- tryCatch({ resolve_canonical("census_npp_female_mid"); TRUE },
                 error = function(e) FALSE)
  isTRUE(ok)
}

test_that("canonical registry resolves the NPP mid series with a matching checksum", {
  skip_if_not(npp_available(), "Census NPP mid file not present")
  # resolve_canonical verifies the recorded SHA-256 in strict mode; if it
  # returns a path there, the checksum matched.
  path <- resolve_canonical("census_npp_female_mid", mode = "strict")
  expect_true(file.exists(path))
})

test_that("npp_female_by_band returns the model's age bands with sane magnitudes", {
  skip_if_not(npp_available(), "Census NPP mid file not present")
  pop <- npp_female_by_band("mid", years = 2025:2050)

  expect_setequal(unique(pop$age_band), DEMAND_AGE_BANDS)
  expect_true(all(pop$female_pop > 0))
  # One row per (year, band).
  expect_equal(nrow(pop), length(2025:2050) * length(DEMAND_AGE_BANDS))

  # Women 65+ should be ~33M in 2025 and grow toward ~45M by 2050 (Census NPP).
  w65 <- npp_women_65plus("mid", years = c(2025, 2050))
  v2025 <- w65$population_65_plus[w65$year == 2025]
  v2050 <- w65$population_65_plus[w65$year == 2050]
  expect_gt(v2025, 30e6); expect_lt(v2025, 36e6)
  expect_gt(v2050, 42e6); expect_lt(v2050, 48e6)
  expect_gt(v2050, v2025)   # aging population grows
})

test_that("npp_women_65plus equals the sum of the 65-79 and 80+ bands", {
  skip_if_not(npp_available(), "Census NPP mid file not present")
  by_band <- npp_female_by_band("mid", years = 2030)
  from_bands <- sum(by_band$female_pop[by_band$age_band %in% c("65-79", "80+")])
  crude <- npp_women_65plus("mid", years = 2030)$population_65_plus
  expect_equal(crude, from_bands, tolerance = 1e-6)
})

test_that("resolve_demand_population prefers NPP and labels its source", {
  skip_if_not(npp_available(), "Census NPP mid file not present")
  res <- resolve_demand_population(2025:2030, series = "mid", mode = "relaxed")
  expect_equal(res$source, "census_npp_mid")
  expect_true(all(c("year", "age_band", "female_pop") %in% names(res$pop_by_band)))
})

test_that("the demand estimands accept the real NPP population", {
  skip_if_not(npp_available(), "Census NPP mid file not present")
  pop <- npp_female_by_band("mid", years = 2025:2050)
  demand <- compute_demand_denominators(pop)
  expect_setequal(unique(demand$estimand), c("D1", "D2", "D3"))
  expect_true(all(demand$demand_cases > 0))
})
