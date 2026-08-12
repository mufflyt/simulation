# Fellowship-to-practice conversion.
#
# The estimator is a cohort-level deconvolution, not an individual linkage, and
# these tests pin the properties that make it readable: a conversion above 1 is
# reported rather than clipped, contaminated years are excluded only on request,
# and a caller who mixes strata is stopped rather than handed a number.

fc_filled <- stats::setNames(c(30, 40, 37, 48, 50, 57, 53, 59, 59, 58, 56, 62),
                             2010:2021)
# Outcome generated from the predictor at a known lag and conversion, so the
# estimator is checked against a truth it cannot see.
fc_make_outcome <- function(theta = 0.5, lag = 3L, years = 2015:2023) {
  stats::setNames(theta * as.numeric(fc_filled[as.character(years - lag)]),
                  as.character(years))
}

# ---- recovery ---------------------------------------------------------------

test_that("the estimator recovers a known conversion and lag from a clean series", {
  y <- fc_make_outcome(theta = 0.5, lag = 3L)
  f <- fit_fellowship_conversion(fc_filled, y, lags = 2:5)
  expect_equal(f$conversion, 0.5, tolerance = 1e-3)
  expect_equal(f$modal_lag, 3L)
  expect_equal(unname(f$lag_weights[["3"]]), 1, tolerance = 1e-3)
  expect_gt(f$r_squared, 0.99)
})

test_that("coverage rescales the conversion and nothing else", {
  y <- fc_make_outcome(theta = 0.5, lag = 3L)
  full <- fit_fellowship_conversion(fc_filled, y, lags = 2:5)
  half <- fit_fellowship_conversion(fc_filled, y * 0.8, lags = 2:5, coverage = 0.8)
  expect_equal(half$conversion, full$conversion, tolerance = 1e-3)
  expect_equal(half$modal_lag, full$modal_lag)
})

# ---- the diagnostics must not be suppressed ---------------------------------

test_that("a conversion above 1 is reported, not clipped", {
  # Impossible as a conversion; it is the signature of misaligned series and is
  # the one number that must survive to the caller intact.
  y <- fc_make_outcome(theta = 1.4, lag = 3L)
  f <- fit_fellowship_conversion(fc_filled, y, lags = 2:5)
  expect_gt(f$conversion, 1)
  expect_equal(f$conversion, 1.4, tolerance = 1e-3)
  expect_output(print(f), "above 1.0 is impossible")
})

test_that("lag mass on a boundary is announced", {
  y <- fc_make_outcome(theta = 0.5, lag = 5L)
  f <- fit_fellowship_conversion(fc_filled, y, lags = 3:5)   # truth at the edge
  expect_equal(f$modal_lag, 5L)
  expect_output(print(f), "boundary")
})

# ---- exclusions are visible and never automatic -----------------------------

test_that("contaminated years are dropped only when asked, and are recorded", {
  y <- fc_make_outcome(theta = 0.5, lag = 3L)
  y["2020"] <- 3                                   # the cancelled examination
  kept <- fit_fellowship_conversion(fc_filled, y, lags = 2:5)
  expect_length(kept$excluded_years, 0L)           # nothing dropped by default
  expect_true(2020 %in% as.integer(names(kept$observed)))

  dropped <- fit_fellowship_conversion(fc_filled, y, lags = 2:5, exclude_years = 2020)
  expect_equal(dropped$excluded_years, 2020L)
  expect_false(2020 %in% as.integer(names(dropped$observed)))
  expect_output(print(dropped), "caller-specified, not automatic")
  # Removing the artefact recovers the truth the artefact was hiding.
  expect_equal(dropped$conversion, 0.5, tolerance = 1e-3)
  expect_lt(kept$r_squared, dropped$r_squared)
})

test_that("excluding a year that is not in the series is an error, not a no-op", {
  y <- fc_make_outcome()
  expect_error(fit_fellowship_conversion(fc_filled, y, lags = 2:5,
                                         exclude_years = 1999),
               "1999")
  expect_error(fit_fellowship_conversion(fc_filled, y, lags = 2:5,
                                         exclude_years = 1999), "outcome series")
})

# ---- refusals ---------------------------------------------------------------

test_that("an unnamed or non-year-keyed series is refused by name", {
  y <- fc_make_outcome()
  expect_error(fit_fellowship_conversion(unname(fc_filled), y), "`filled`")
  expect_error(fit_fellowship_conversion(unname(fc_filled), y), "NAMED")
  bad <- y; names(bad)[2] <- "twenty-sixteen"
  expect_error(fit_fellowship_conversion(fc_filled, bad), "not years")
})

test_that("too few usable outcome years is refused rather than underdetermined", {
  # Every lag must have a predictor, so a short outcome window cannot support a
  # wide lag support. Returning a fit here would be fitting noise exactly.
  y <- fc_make_outcome(years = 2015:2017)
  err <- tryCatch(fit_fellowship_conversion(fc_filled, y, lags = 2:8),
                  error = function(e) conditionMessage(e))
  expect_type(err, "character")
  expect_match(err, "underdetermined")
  expect_match(err, "Narrow `lags`")
})

test_that("a coverage outside (0, 1] is refused and names the value", {
  y <- fc_make_outcome()
  for (bad in list(0, -0.2, 1.5, c(0.5, 0.5), NA_real_)) {
    expect_error(fit_fellowship_conversion(fc_filled, y, lags = 2:5, coverage = bad))
  }
  expect_error(fit_fellowship_conversion(fc_filled, y, lags = 2:5, coverage = 1.5),
               "1.5")
})

# ---- the certification series must be one stratum ---------------------------

test_that("differencing a multi-stratum series is refused, not silently negative", {
  # The bug this exists to prevent: stacking ABOG, ABU and combined rows and
  # differencing across them yields negative 'flows' and conversions near 16.
  mixed <- data.frame(
    year = c(2013:2015, 2013:2015), measure = "board_certified_active",
    geography = "national", board_pathway = "ABOG",
    n_active = c(100, 120, 140, 500, 520, 540)
  )
  expect_error(fellowship_certification_series(series = mixed), "not a single monotone stratum")

  clean <- data.frame(year = 2013:2016, measure = "board_certified_active",
                      geography = "national", board_pathway = "ABOG",
                      n_active = c(100, 120, 140, 175))
  flow <- fellowship_certification_series(series = clean)
  expect_equal(unname(flow), c(20, 20, 35))
  expect_equal(names(flow), c("2014", "2015", "2016"))
})

# ---- steady state agrees with the fit when the model is correct -------------

test_that("the steady-state estimator agrees with the fit on a clean series", {
  # Two estimators, one truth. They must not disagree when the model holds, or
  # neither can be trusted when it does not.
  y <- fc_make_outcome(theta = 0.5, lag = 3L)
  ss <- fellowship_conversion_steady_state(fc_filled, y, lag = 3)
  expect_equal(ss$conversion, 0.5, tolerance = 1e-6)
  f <- fit_fellowship_conversion(fc_filled, y, lags = 2:5)
  expect_equal(ss$conversion, f$conversion, tolerance = 1e-3)
})

test_that("a lag with too few usable years is refused with both spans named", {
  y <- fc_make_outcome(years = 2015:2016)
  err <- tryCatch(fellowship_conversion_steady_state(fc_filled, y, lag = 12),
                  error = function(e) conditionMessage(e))
  expect_match(err, "usable outcome year")
  expect_match(err, "2015-2016")
})
