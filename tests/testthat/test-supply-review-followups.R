# Guards for R/supply-review_followups.R: entrant staged pipeline,
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

# ---- Entrant pipeline: lag, calibrated conversion, and disruption -----------
#
# Four defects found by scoring the pipeline against the observed certification
# series rather than only checking its internal arithmetic.

test_that("the NRMP series is contiguous, so the pipeline can span the validation window", {
  s <- nrmp_entrant_series()
  # 2021-2024 were missing until 2026-08-05; the series jumped 2020 -> 2025 and
  # could produce nothing for the back-test validation window.
  expect_equal(setdiff(2010:2025, s$appointment_year), integer(0))
  expect_true(all(diff(sort(s$appointment_year)) == 1))
  expect_equal(s$positions_filled[s$appointment_year == 2021], 62L)
  expect_equal(s$positions_filled[s$appointment_year == 2024], 65L)
  # Filled can never exceed offered, in any year.
  expect_true(all(s$positions_filled <= s$positions_offered))
})

test_that("the certification lag defaults to the documented fellowship length", {
  # A 1-year default contradicted the three-year fellowship this package
  # documents, and scored worse against every observed certification year.
  expect_equal(eval(formals(entrant_pipeline_transition)$cert_lag),
               URPS_FELLOWSHIP_YEARS)
  expect_equal(URPS_FELLOWSHIP_YEARS, 3L)
})

test_that("the match-to-cert conversion is estimated, and excludes uninformative years", {
  skip_if_not_installed("mufflyaccess")
  r <- nrmp_match_to_cert_ratio(2020L)
  expect_equal(r$cert_lag, 3L)
  # Backlog years certified an already-practising pool that never passed through
  # the match, and 2020's examination was cancelled. Including either makes the
  # ratio meaningless -- with them the estimate is above 4.0.
  expect_true(all(c(2013L, 2014L, 2015L, 2020L) %in% r$excluded))
  expect_gt(r$ratio, 0.5)
  expect_lt(r$ratio, 1.0)
  expect_gt(nrmp_match_to_cert_ratio(2020L, exclude_disrupted = FALSE)$ratio, 2)

  # The default must be the estimated conversion, not the old 0.95 assumption.
  expect_equal(eval(formals(entrant_pipeline_transition)$p_complete_cert),
               round(r$ratio, 2), tolerance = 0.02)
})

test_that("a per-year conversion represents a cancelled examination", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  sched <- data.frame(year = 2013:2025, p_complete_cert = 0.8)
  sched$p_complete_cert[sched$year == 2020] <- 0.1

  flat <- entrant_pipeline_transition(m, p_complete_cert = 0.8)
  vary <- entrant_pipeline_transition(m, p_complete_cert = sched)

  # A constant conversion cannot express the event at all.
  expect_equal(length(unique(flat$p_complete_cert)), 1L)
  expect_lt(vary$certified[vary$year == 2020], flat$certified[flat$year == 2020])
})

test_that("a suppressed year defers its fellows rather than destroying them", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  sched <- data.frame(year = 2013:2025, p_complete_cert = 0.8)
  sched$p_complete_cert[sched$year == 2020] <- 0.1

  kept <- entrant_pipeline_transition(m, p_complete_cert = sched, defer_shortfall = TRUE)
  lost <- entrant_pipeline_transition(m, p_complete_cert = sched, defer_shortfall = FALSE)

  # The deficit reappears in the following year, and only there.
  expect_gt(kept$deferred_in[kept$year == 2021], 0)
  expect_equal(kept$deferred_in[kept$year == 2022], 0)
  expect_gt(kept$certified[kept$year == 2021], lost$certified[lost$year == 2021])

  # Deferral conserves fellows across the disruption; discarding them does not.
  win <- 2019:2022
  expect_equal(sum(kept$certified[kept$year %in% win]),
               sum(lost$certified[lost$year %in% win]) +
                 50 * (0.8 - 0.1), tolerance = 1e-6)
})

test_that("a scalar conversion is unchanged by the per-year machinery", {
  m <- data.frame(year = 2013:2025, matched = seq(40, 64, length.out = 13))
  a <- entrant_pipeline_transition(m, p_complete_cert = 0.75)
  b <- entrant_pipeline_transition(m, p_complete_cert = rep(0.75, 13))
  expect_identical(a$certified, b$certified)
  expect_true(all(a$deferred_in == 0))   # nothing to defer when nothing varies
})

test_that("malformed conversion schedules are rejected", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  expect_error(entrant_pipeline_transition(m, p_complete_cert = c(0.5, 0.6)),
               "supply a")
  expect_error(entrant_pipeline_transition(m, p_complete_cert = 1.5), "\\[0, 1\\]")
  expect_error(entrant_pipeline_transition(m, p_complete_cert = data.frame(year = 2020)),
               "needs")
  # A partial schedule falls back to its own median rather than blanking years.
  part <- entrant_pipeline_transition(
    m, p_complete_cert = data.frame(year = 2020, p_complete_cert = 0.1))
  expect_true(all(is.finite(part$p_complete_cert)))
})
