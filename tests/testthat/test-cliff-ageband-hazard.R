# The retirement hazard's default source is now cliff's exposure-based age-band
# table (person-years at risk + departure events -> annual hazard), vendored
# under inst/extdata/provider_year. These tests pin that wiring: the model reads
# the real calibrated hazard by default, carries exposure-based uncertainty, and
# extrapolates the sparse zero-event tail without letting the hazard fall.

ageband_csv <- function() {
  system.file("extdata", "provider_year", "retirement_hazard_by_ageband.csv",
              package = "urpssim")
}

skip_if_no_ageband <- function() {
  testthat::skip_if_not(nzchar(ageband_csv()) && file.exists(ageband_csv()),
                        "vendored cliff age-band hazard CSV not installed")
}

test_that("the calibrated cliff age-band hazard is the default source", {
  skip_if_no_ageband()
  h <- build_urps_exit_hazard(verbose = FALSE)
  expect_identical(h$source, "cliff_ageband_empirical")
  # 33+5+6+9+13+15+0 = 81 observed departures across the bands.
  expect_equal(h$n_events, 81L)
  # Real, data-driven uncertainty (1/sqrt(events)), not the fixed 0.15 / assumed 0.
  expect_equal(h$hazard_cv, 1 / sqrt(81), tolerance = 1e-6)
  expect_true(all(c("calibrated", "derived_by_analogy") %in%
                    h$exit_probs$calibration_tier))
})

test_that("per-age exit probs are valid and carry exposure-based SE", {
  skip_if_no_ageband()
  h <- build_urps_exit_hazard(smooth = FALSE, verbose = FALSE)
  ep <- h$exit_probs
  expect_setequal(unique(ep$sex), c("Female", "Male"))
  expect_setequal(unique(ep$age), 30:80)
  expect_true(all(ep$prob_exit >= 0 & ep$prob_exit <= 1))
  # Age 67 sits in the 65-69 band: hazard 0.0725 from 15 events, so the Poisson
  # relative SE is 0.0725 / sqrt(15). Unsmoothed, the band value is exact.
  row <- ep[ep$sex == "Female" & ep$age == 67, ]
  expect_equal(row$prob_exit, 0.0725, tolerance = 1e-9)
  expect_equal(row$se_prob_exit, 0.0725 / sqrt(15), tolerance = 1e-6)
  expect_identical(row$calibration_tier, "calibrated")
})

test_that("the zero-event tail is analogy-tier and never falls below the observed peak", {
  skip_if_no_ageband()
  h <- build_urps_exit_hazard(smooth = FALSE, verbose = FALSE)
  fem <- h$exit_probs[h$exit_probs$sex == "Female", ]
  fem <- fem[order(fem$age), ]
  # The 70+ band has zero events; those ages are extrapolated (analogy tier)...
  expect_true(all(fem$calibration_tier[fem$age >= 70] == "derived_by_analogy"))
  # ...and floored at the highest observed-band hazard (0.0725), so the hazard
  # is non-decreasing from the last observed band through the top age.
  expect_true(all(diff(fem$prob_exit[fem$age >= 65]) >= 0))
  expect_gte(min(fem$prob_exit[fem$age >= 70]), 0.0725 - 1e-9)
})

test_that("scale_shift delays retirement (shifts the hazard curve along age)", {
  skip_if_no_ageband()
  base <- build_urps_exit_hazard(smooth = FALSE, verbose = FALSE)
  later <- build_urps_exit_hazard(smooth = FALSE, verbose = FALSE, scale_shift = 5)
  at <- function(h, a) h$exit_probs$prob_exit[h$exit_probs$sex == "Female" &
                                                h$exit_probs$age == a]
  # +5 years reads age 67's hazard from age 62 (the lower 60-64 band), so the
  # hazard at a fixed age drops under a delayed-retirement shift.
  expect_lt(at(later, 67), at(base, 67))
})

test_that("cliff_ageband_csv = NULL restores the Weibull analogy path", {
  h <- build_urps_exit_hazard(cliff_duckdb_path = NULL, cliff_ageband_csv = NULL,
                              verbose = FALSE)
  expect_identical(h$source, "hwsm_weibull_analogy")
  expect_true(all(h$exit_probs$calibration_tier == "derived_by_analogy"))
})
