# Guards for R/61-supply_review_followups.R: entrant staged pipeline,
# uncertainty-driver registry, international-migration assumption.

matches_df <- function() tibble::tibble(year = 2020:2025,
                                        matched = c(55, 58, 60, 60, 62, 64))

# ---- Entrant match -> board -> active pipeline ------------------------------

test_that("staged pipeline applies both conversions and the total lag", {
  p <- entrant_pipeline_transition(matches_df(), p_complete_cert = 0.95,
                                   p_active_practice = 0.90, cert_lag = 2)
  expect_equal(p$active_entrants[p$year == 2022], 55 * 0.95 * 0.90, tolerance = 1e-6)
  # source years before the series start yield NA (leading edge of the lag)
  expect_true(all(is.na(p$active_entrants[p$year %in% c(2020, 2021)])))
})

test_that("attrition is monotone: active <= certified <= source matches", {
  p <- entrant_pipeline_transition(matches_df(), p_complete_cert = 0.9,
                                   p_active_practice = 0.8, cert_lag = 1)
  ok <- !is.na(p$active_entrants)
  expect_true(all(p$active_entrants[ok] <= p$certified[ok] + 1e-9))
  expect_true(all(p$certified[ok] <= p$matched[match(p$year[ok] - 1, p$year)] + 1e-9))
})

test_that("doubling matches doubles active entrants (linearity)", {
  base <- entrant_pipeline_transition(matches_df(), cert_lag = 1)
  dbl  <- entrant_pipeline_transition(
    dplyr::mutate(matches_df(), matched = matched * 2), cert_lag = 1)
  ok <- !is.na(base$active_entrants)
  expect_equal(dbl$active_entrants[ok], 2 * base$active_entrants[ok], tolerance = 1e-6)
})

test_that("a longer lag shifts the active series later", {
  p1 <- entrant_pipeline_transition(matches_df(), cert_lag = 1, active_lag = 0)
  p2 <- entrant_pipeline_transition(matches_df(), cert_lag = 1, active_lag = 2)
  # p2 needs an extra 2 years of source history, so it has more leading NAs
  expect_gt(sum(is.na(p2$active_entrants)), sum(is.na(p1$active_entrants)))
})

test_that("pipeline rejects bad fractions, negative matches, and negative/non-integer lags", {
  expect_error(entrant_pipeline_transition(matches_df(), p_complete_cert = 1.2))
  expect_error(entrant_pipeline_transition(matches_df(), p_active_practice = -0.1))
  expect_error(entrant_pipeline_transition(tibble::tibble(year = 2020, matched = -5)))
  expect_error(entrant_pipeline_transition(matches_df(), cert_lag = -1))
  expect_error(entrant_pipeline_transition(matches_df(), cert_lag = 1.5))
  expect_error(entrant_pipeline_transition(tibble::tibble(y = 1, m = 2)))  # wrong columns
})

# ---- Uncertainty-driver registry --------------------------------------------

test_that("retirement is registered as a weakly-observed, high-priority driver", {
  d <- supply_uncertainty_drivers()
  expect_true(all(c("driver", "observability", "priority", "psa_knob", "rationale") %in% names(d)))
  ret <- d[d$driver == "retirement_hazard", ]
  expect_equal(ret$observability, "weakly_observed")
  expect_equal(ret$priority, "high")
  # every listed driver carries a rationale and an observability label
  expect_true(all(nzchar(d$rationale)))
  expect_true(all(d$observability %in% c("weakly_observed", "partially_observed",
                                         "well_observed", "unquantified")))
})

# ---- International-migration assumption --------------------------------------

test_that("the international-migration assumption defaults to an explicit zero and is adjustable", {
  a0 <- international_migration_assumption()
  expect_equal(a0$net_annual_providers, 0)
  expect_match(a0$calibration_status, "zero")
  a1 <- international_migration_assumption(net_annual = -25)
  expect_equal(a1$net_annual_providers, -25)          # net emigration is representable
  expect_error(international_migration_assumption("x"))
  expect_error(international_migration_assumption(c(1, 2)))
  expect_error(international_migration_assumption(Inf))
})
