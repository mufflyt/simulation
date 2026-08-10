# Adversarial guards: no public numeric path may emit Inf or NaN.
#
# An Inf or NaN that survives into a summary table reads as a real number. Every
# case below was a live defect found by feeding degenerate denominators through
# the public API, so each is a regression guard, not a hypothetical.

# Recursively collect every numeric leaf, so a list-returning function is
# checked as thoroughly as a data frame or a bare vector.
.numeric_leaves <- function(x) {
  if (is.numeric(x)) return(as.numeric(x))
  if (is.list(x)) return(unlist(lapply(x, .numeric_leaves), use.names = FALSE))
  numeric(0)
}

# Returns "guarded" (failed closed -- acceptable), "finite" (numbers, all of
# them finite or NA), "nonfinite" (an Inf/NaN escaped), or "no_numbers".
#
# The last of those is why this returns a LABEL rather than a logical. It used
# to end in all(is.na(v) | is.finite(v)), and all() of a zero-length vector is
# TRUE -- so a case whose result carried no numeric leaf at all passed the
# "no public numeric path emits Inf or NaN" guard having inspected nothing.
# Found by instrumenting all()/any() across the whole suite in adversarial
# cycle 07; case 4 below (assign_entrant_geography with 0 entrants) was the one
# hitting it. A caller must now say which outcome it expects.
.finite_status <- function(expr) {
  r <- tryCatch(eval(expr), error = function(e) structure(list(), class = "guarded"))
  if (inherits(r, "guarded")) return("guarded")
  v <- .numeric_leaves(r)
  if (length(v) == 0L) return("no_numbers")
  if (all(is.na(v) | is.finite(v))) "finite" else "nonfinite"   # NA is a value; Inf/NaN is not
}

.finite_or_error <- function(expr) .finite_status(expr) %in% c("guarded", "finite")

test_that("a zero required-FTE denominator does not produce Inf", {
  g <- compute_fte_gap(tibble::tibble(year = 2025, effective_fte_median = 100),
                       tibble::tibble(year = 2025, required_fte = 0))
  expect_true(is.na(g$gap_pct))
  expect_true(is.na(g$pct_supply_to_demand))
  expect_equal(g$gap_fte, 100)      # the absolute gap is still well defined
  expect_true(.finite_or_error(quote(
    compute_fte_gap(tibble::tibble(year = 2025, effective_fte_median = 0),
                    tibble::tibble(year = 2025, required_fte = 0)))))
})

test_that("zero population does not produce an infinite density", {
  pc <- supply_per_capita(tibble::tibble(geo = "A", fte = 10),
                          tibble::tibble(geo = "A", population = 0))
  expect_true(is.na(pc$fte_per_capita))
  # A benchmark against no population is undefined and must fail closed.
  expect_error(
    benchmark_density_shortfall(tibble::tibble(geo = "A", fte = 0, population = 0),
                                benchmark = 30),
    "total population is zero")
})

test_that("rebasing on a zero base value fails closed", {
  # Silently returning Inf would put an infinite index into the adequacy curve.
  expect_error(suppressMessages(rebase_to_year(2025:2027, c(0, 0, 0), 2025)),
               "cannot be rebased")
  expect_equal(rebase_to_year(2025:2027, c(2, 4, 6), 2025), c(1, 2, 3))
})

test_that("a vanishing capacity-survey denominator fails closed", {
  # A respondent whose entire reported load was overtime gives seen == additional.
  expect_error(capacity_category_adequacy("shortage_hours", seen = 4, additional = 4),
               "cannot yield an adequacy ratio")
  expect_error(capacity_category_adequacy("shortage_unmet", seen = 0, additional = 5),
               "cannot yield an adequacy ratio")
  # The published values still work.
  expect_equal(capacity_category_adequacy("shortage_hours", seen = 31, additional = 4),
               0.852, tolerance = 1e-3)
})

test_that("an empty service basket cannot yield a URPS share", {
  expect_error(
    suppressMessages(implied_urps_share(
      tibble::tibble(service = "new_consultation", volume = 0), 1300)),
    "zero total work RVUs")
})

test_that("productivity and adequacy denominators already fail closed", {
  expect_error(calibrate_wrvu_per_fte(1000, 0))
  expect_error(required_fte_base_year(1306, 0))
})

test_that("no public summary path emits Inf or NaN on degenerate input", {
  skip_if_not_installed("mufflyaccess")
  cases <- list(
    quote(compute_fte_gap(tibble::tibble(year = 2025, effective_fte_median = 100),
                          tibble::tibble(year = 2025, required_fte = 0))),
    quote(supply_per_capita(tibble::tibble(geo = "A", fte = 10),
                            tibble::tibble(geo = "A", population = 0))),
    quote(capacity_survey_adequacy(example_capacity_survey())),
    quote(assign_entrant_geography(0, tibble::tibble(geo = "A", share = 1))),
    quote(series_mean_se(numeric(0)))
  )
  # Each case declares the outcome it is evidence FOR. "no_numbers" is a real
  # outcome (zero entrants produce an empty assignment) but it is not evidence
  # that a numeric path is finite, so it cannot be spelled the same way.
  expected <- c("finite", "finite", "finite", "no_numbers", "finite")
  for (i in seq_along(cases)) {
    expect_equal(.finite_status(cases[[i]]), expected[i],
                 info = paste("case", i, deparse(cases[[i]])[1]))
  }
  # And at least one case must actually have inspected numbers, or the whole
  # block could pass on empties.
  expect_true(sum(vapply(cases, .finite_status, character(1)) == "finite") >= 4L)
})
