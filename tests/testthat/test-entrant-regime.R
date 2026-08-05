# Guards for the entrant regime model.
#
# The 2020->2023 back-test failed twice: the point estimate averaged a cancelled
# examination year into the steady-state rate, and the interval described
# sampling noise rather than forecast uncertainty. These tests pin down the
# mechanism that fixes each, and pin down the limits that remain -- particularly
# that a window containing no disruption simulates no break, which is a real
# blind spot and must not be silently papered over.

# The observed national ABOG+ABU entrant series. 2013-2015 is the initial
# certification backlog; 2020 is the COVID examination collapse (ABOG entrants
# fell 35 -> 3 while ABU held at 13 -> 7); 2021 is the deferred cohort arriving.
OBSERVED_ENTRANTS <- data.frame(
  year  = 2013:2023,
  count = c(655, 175, 102, 36, 33, 40, 48, 10, 81, 54, 72)
)

pre_cutoff <- function(y = 2020L) {
  OBSERVED_ENTRANTS[OBSERVED_ENTRANTS$year <= y, , drop = FALSE]
}

test_that("the initial backlog is classified as a prefix regime", {
  r <- classify_certification_regimes(pre_cutoff(), verbose = FALSE)
  expect_equal(r$year[r$regime == "backlog"], 2013:2015)
  # Backlog cannot resume: the initial pool is certified once. A later year may
  # be disrupted or a release, never backlog.
  expect_false(any(r$regime[r$year > 2015] == "backlog"))
})

test_that("the COVID examination year is flagged as disrupted, and only it", {
  r <- classify_certification_regimes(pre_cutoff(), verbose = FALSE)
  expect_equal(r$year[r$regime == "disrupted"], 2020L)
  expect_equal(sort(r$year[r$regime == "steady"]), 2016:2019)
})

test_that("ordinary year-to-year variation does not trip the disruption screen", {
  # A smooth series with no break must yield no disrupted year, or the screen
  # would manufacture deferred backlog out of noise.
  smooth <- data.frame(year = 2013:2020, count = c(40, 42, 39, 44, 41, 45, 43, 46))
  r <- classify_certification_regimes(smooth, verbose = FALSE)
  expect_false(any(r$regime == "disrupted"))
})

test_that("a disrupted year is excluded from the trend and its deficit deferred", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  expect_equal(m$steady_years, 2016:2019)
  expect_false(2020L %in% m$steady_years)
  expect_gt(m$deferred_backlog, 0)
  # The deficit is the shortfall against what the other steady years predicted,
  # so it must be of the order of a missing cohort rather than a rounding error.
  expect_gt(m$deferred_backlog, 20)
})

test_that("an already-observed release is credited against the deficit", {
  # Fitting through 2021 sees both the 2020 disruption and the 2021 bulge. The
  # deferred candidates have arrived, so scheduling them again as a FUTURE
  # release would count the same people twice -- once inside the fitted trend
  # and once on top of it.
  m21 <- fit_entrant_regime_model(pre_cutoff(2021L), 2021L, verbose = FALSE)
  expect_true(2021L %in% m21$release_observed_years)
  expect_false(2021L %in% m21$steady_years)
  expect_gt(m21$already_released, 0)
  expect_lt(m21$deferred_backlog, m21$gross_deficit)

  m20 <- fit_entrant_regime_model(pre_cutoff(2020L), 2020L, verbose = FALSE)
  expect_lt(m21$deferred_backlog, m20$deferred_backlog)
})

test_that("the model refuses to project years it was fitted on", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  expect_error(project_entrant_path(m, 2019:2023), "at or before the fit cutoff")
  expect_error(draw_entrant_paths(m, 2020:2023, 10L), "at or before the fit cutoff")
  expect_silent(project_entrant_path(m, 2021:2023))
})

test_that("the deterministic path is the steady trend plus the scheduled release", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  p <- project_entrant_path(m, 2021:2023)
  expect_equal(p$expected, p$steady + p$backlog_release)
  # release_years defaults to 2, so the third year gets none of it.
  expect_gt(p$backlog_release[1], 0)
  expect_gt(p$backlog_release[2], 0)
  expect_equal(p$backlog_release[3], 0)
  expect_equal(sum(p$backlog_release), m$deferred_backlog)
})

test_that("each uncertainty component widens the interval, and the trend dominates", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  yrs <- 2021:2023
  set.seed(20260802)
  w <- function(inc) {
    tot <- rowSums(draw_entrant_paths(m, yrs, 2000L, include = inc))
    diff(stats::quantile(tot, c(0.025, 0.975), names = FALSE))
  }
  w_disp  <- w("dispersion")
  w_trend <- w(c("dispersion", "trend"))
  w_full  <- w(c("dispersion", "trend", "break", "release_timing"))

  expect_gt(w_trend, w_disp)
  expect_gt(w_full, w_trend)
  # Coefficient uncertainty is the term the shipped engine omitted entirely, and
  # at a three-year horizon it is the largest single contributor.
  expect_gt(w_trend - w_disp, w_full - w_trend)
})

test_that("a regime break defers entrants rather than multiplying them", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  set.seed(1)
  paths <- draw_entrant_paths(m, 2021:2023, 3000L,
                              include = c("trend", "dispersion", "break"))
  no_break <- draw_entrant_paths(m, 2021:2023, 3000L,
                                 include = c("trend", "dispersion"))
  # A break can only push entrants LATER, and a late one pushes them past the
  # horizon. The median total must therefore fall, never rise: an earlier draft
  # drew the magnitude from a symmetric lognormal, which let a year certify five
  # times its cohort and produced a band wide enough to cover anything.
  expect_lt(stats::median(rowSums(paths)), stats::median(rowSums(no_break)))
  expect_lt(stats::quantile(rowSums(paths), 0.999, names = FALSE),
            3 * stats::median(rowSums(no_break)))
})

test_that("no observed disruption means no break is simulated, and it is announced", {
  # This is a genuine blind spot, not an oversight: fitted through 2019 the
  # model has never seen a break, so it cannot state how deep one would be, and
  # it misses the 2020 collapse. The package's rule is that an unquantified
  # parameter is reported rather than invented.
  smooth <- data.frame(year = 2013:2020, count = c(40, 42, 39, 44, 41, 45, 43, 46))
  expect_message(
    m <- fit_entrant_regime_model(smooth, 2020L, verbose = TRUE),
    "breaks are NOT simulated"
  )
  expect_true(is.na(m$break_surviving_share))

  spec <- supply_parameter_spec(entrant_regime = m)
  expect_false(spec$quantified[["entrant_regime_break"]])

  # Supplying a prior is an explicit modelling choice and does switch it on.
  m2 <- fit_entrant_regime_model(smooth, 2020L, break_surviving_share_prior = 0.2,
                                 verbose = FALSE)
  expect_equal(m2$break_surviving_share, 0.2)
  expect_true(supply_parameter_spec(entrant_regime = m2)$quantified[["entrant_regime_break"]])
})

test_that("break frequency uses a Jeffreys adjustment rather than k / n", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  # One break in five screened years: 1.5 / 6, not 1 / 5. A bare k/n would state
  # 0.2 with false precision and, with no break seen, would state 0.
  expect_equal(m$break_probability, 1.5 / 6)
  expect_gt(m$break_probability, 0)
})

test_that("dispersion is floored at Poisson", {
  # A below-Poisson dispersion estimated from four points is a small-sample
  # artefact; letting it through would make the intervals NARROWER than Poisson,
  # which is the defect this module exists to remove.
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  expect_gte(m$dispersion, 1)
})

test_that("too few steady years falls back to an intercept rate rather than a slope", {
  short <- data.frame(year = 2017:2020, count = c(33, 40, 48, 10))
  m <- fit_entrant_regime_model(short, 2020L, trend_family = "loglinear",
                                verbose = FALSE)
  # 2020 is screened out, leaving three steady years: not enough to identify a
  # log-linear slope that will be extrapolated three years forward.
  expect_length(m$steady_years, 3L)
  expect_equal(m$trend_family, "intercept")
  expect_equal(m$trend_family_requested, "loglinear")

  # Four steady years is enough, and the slope is kept.
  full <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  expect_length(full$steady_years, 4L)
  expect_equal(full$trend_family, "loglinear")
})

test_that("malformed series are rejected rather than silently reordered", {
  expect_error(fit_entrant_regime_model(data.frame(year = 2013:2015), 2015L),
               "missing column")
  expect_error(
    classify_certification_regimes(data.frame(year = c(2013, 2015), count = c(1, 2))),
    "gaps")
  expect_error(
    classify_certification_regimes(data.frame(year = c(2013, 2013), count = c(1, 2))),
    "duplicated")
  expect_error(
    classify_certification_regimes(data.frame(year = 2013:2014, count = c(1, -2))),
    "negative")
})

# ---- Integration with the supply engine ------------------------------------

test_that("the engine accepts a per-transition entrant path and honours its alignment", {
  agents <- initialize_provider_agents(200, "URPS", 2020L)
  years <- 2020:2023

  # A scalar and its constant expansion must be bit-identical, or every existing
  # seeded result would silently change value.
  set.seed(1); a <- simulate_provider_career_once(agents, years, 20)$panel
  set.seed(1); b <- simulate_provider_career_once(agents, years, rep(20, 4))$panel
  expect_identical(a$headcount, b$headcount)

  # Element i is the cohort entering between years[i] and years[i + 1], so a
  # value in slot 1 lands in the SECOND panel row. The final element is never
  # read: entrants after the horizon would claim providers outside it.
  set.seed(2)
  front <- simulate_provider_career_once(agents, years, c(60, 0, 0, 999),
                                         retirement_schedule = RETIREMENT_HAZARD_BY_AGE * 0,
                                         career_change_hazard = 0)$panel
  expect_equal(front$headcount[1], nrow(agents))
  expect_equal(front$headcount[2] - front$headcount[1], 60)
  expect_equal(front$headcount[3], front$headcount[2])    # zero entrants
  expect_equal(front$headcount[4], front$headcount[3])    # final element ignored

  # One value per transition (length(years) - 1) is equally acceptable, and is
  # what draw_supply_parameters() returns for a regime spec.
  set.seed(2)
  short <- simulate_provider_career_once(agents, years, c(60, 0, 0),
                                         retirement_schedule = RETIREMENT_HAZARD_BY_AGE * 0,
                                         career_change_hazard = 0)$panel
  expect_identical(short$headcount, front$headcount)
})

test_that("a mis-sized entrant path is an error, not silent recycling", {
  agents <- initialize_provider_agents(50, "URPS", 2020L)
  expect_error(simulate_provider_career_once(agents, 2020:2023, c(10, 20)),
               "supply 1, 4")
  expect_error(simulate_provider_career_once(agents, 2020:2023, c(10, 20, NA, 5)),
               "finite and non-negative")
})

test_that("a spec carrying a regime model draws a path and demands the years", {
  m <- fit_entrant_regime_model(pre_cutoff(), 2020L, verbose = FALSE)
  spec <- supply_parameter_spec(entrant_regime = m)
  expect_true(spec$quantified[["entrant_rate"]])
  expect_true(spec$quantified[["entrant_regime_break"]])

  # Silently collapsing the path to a scalar would discard the regime structure
  # the model exists to represent, so the omission is an error.
  expect_error(draw_supply_parameters(spec), "`years` must be supplied")

  set.seed(3)
  d <- draw_supply_parameters(spec, RETIREMENT_HAZARD_BY_AGE, years = 2020:2023)
  # One per transition: the base year needs no entrant count, its cohort is the
  # starting agent table.
  expect_length(d$entrants, 3L)
  expect_true(all(d$entrants >= 0))

  set.seed(4); d2 <- draw_supply_parameters(spec, RETIREMENT_HAZARD_BY_AGE, years = 2020:2023)
  expect_false(identical(d$entrants, d2$entrants))  # it actually varies
})

test_that("the regime spec is rejected unless it came from the fitting function", {
  expect_error(supply_parameter_spec(entrant_regime = list(a = 1)),
               "fit_entrant_regime_model")
})

# ---- Rolling-origin validation ---------------------------------------------

test_that("rolling-origin validation reports its fold count and beats the naive rate", {
  cum <- data.frame(
    year = 2013:2023,
    n_active = c(655, 830, 932, 968, 1001, 1041, 1089, 1099, 1180, 1234, 1306)
  )
  set.seed(20260802)
  v <- entrant_regime_rolling_validation(OBSERVED_ENTRANTS, cum, horizon = 3L,
                                         n_draws = 500L, verbose = FALSE)
  expect_gte(v$n_folds, 1L)
  expect_lt(v$median_absolute_percent_error,
            v$naive_median_absolute_percent_error)

  # The series is far too short to establish coverage, and the existing gate
  # must say so rather than be satisfied by a favourable handful of folds.
  expect_error(assert_interval_coverage_publishable(v, verbose = FALSE),
               "rolling-origin folds")
})

# ---- Attrition ascertainment ------------------------------------------------

test_that("the attrition requirement is reported as data, not prose", {
  skip_if_not_installed("mufflyaccess")
  req <- backtest_attrition_requirement()

  # As of contract v3.0.0 retirement is not ascertained. The point of the object
  # is that this flips automatically when the upstream artifact starts
  # populating n_retired, rather than needing a document to be rewritten.
  expect_s3_class(req, "urps_attrition_requirement")
  expect_type(req$ascertained, "logical")
  expect_false(req$ascertained)
  expect_identical(req$retirement_status, "not_ascertained")
  expect_false(req$n_retired_populated)
  expect_true(req$active_equals_ever_certified)
  expect_output(print(req), "NOT AVAILABLE")
})
