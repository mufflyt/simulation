# Unit tests for Weibull surgical recurrence distributions

test_that("estimate_weibull_recurrence.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_weibull_recurrence.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("recurrence_mass_from_cumulative produces valid probability mass", {
  cum_inc <- c(0.05, 0.12, 0.18, 0.23, 0.27)
  mass <- recurrence_mass_from_cumulative(cum_inc)
  expect_equal(length(mass), 5L)
  expect_equal(sum(mass), 0.27)
  expect_equal(mass[1], 0.05)
  expect_equal(mass[2], 0.07)
})
