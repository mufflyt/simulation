# The entrant rate, sourced. These tests exist to stop the back-test gap being
# "fixed" by adopting the observed 69/yr, which is a COVID exam-scheduling
# artifact rather than a sustainable rate.

.er <- function() yaml::read_yaml("../../config/entrant_rate_source.yml")

test_that("the entrant estimand is certification, not fellowship entry", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  e <- .er()$estimand
  expect_match(e$definition, "CERTIFICATION, not", fixed = TRUE)
  expect_match(e$not_this, "NOT NRMP filled fellowship positions", fixed = TRUE)
})

test_that("the backlog years are excluded and the reason recorded", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  ex <- .er()$sources$certification_flow$excluded_years[[1]]
  expect_setequal(unlist(ex$years), c(2013, 2014, 2015))
  expect_match(ex$why, "certification began in 2013", fixed = TRUE)
})

test_that("the certification series in config matches the live accessor", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  s <- .er()$sources$certification_flow$series
  live <- tryCatch(as.data.frame(urps_certification_cohorts()), error = function(e) NULL)
  skip_if(is.null(live), "certification cohorts unavailable")
  for (y in names(s)) {
    got <- live$n_certified[live$cert_year == as.integer(y)]
    if (length(got) == 1L) expect_equal(got, s[[y]], info = y)
  }
})

test_that("69/yr is recorded as contaminated, not adopted", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  w <- .er()$window_estimates
  expect_equal(w$`2021-2023`$mean, 69.00)
  expect_match(w$`2021-2023`$note, "CONTAMINATED", fixed = TRUE)
  # smoothing the COVID pair must move it well away from 69
  expect_lt(w$`2021-2023_covid_smoothed`$mean, 60)
  r <- .er()$recommendation
  expect_null(r$value)
  expect_identical(r$status, "NOT_ADOPTED")
})

test_that("two independent series converge once the COVID artifact is removed", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  smoothed <- .er()$window_estimates$`2021-2023_covid_smoothed`$mean
  # NRMP lagged 3 years over the same certification window
  n <- utils::read.csv("../../data-raw/calibration/nrmp_urps_entrants_series.csv")
  nrmp <- mean(n$positions_filled[n$appointment_year >= 2018 & n$appointment_year <= 2020])
  expect_lt(abs(smoothed - nrmp), 2)   # currently 57.17 vs 57.67
})

test_that("the shipped assumption is inside the defensible range", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  r <- .er()$recommendation
  expect_equal(r$shipped_assumption, 55)
  expect_match(r$shipped_assumption_verdict, "DEFENSIBLE", fixed = TRUE)
  w <- .er()$window_estimates
  expect_gte(r$shipped_assumption, w$`2016-2023`$mean)
  expect_lte(r$shipped_assumption, w$`2021-2023_covid_smoothed`$mean)
})

test_that("the back-test must not be closed by adopting the artifact", {
  skip_if_not(file.exists("../../config/entrant_rate_source.yml"))
  # Improving a back-test while making the forecast worse is the failure mode
  # the calibration gates exist to prevent.
  expect_match(.er()$consequences_for_the_backtest,
               "improve the back-test while", fixed = TRUE)
})
