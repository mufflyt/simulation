# Adversarial tests for run_supply_microsimulation() (R/12).
#
# Written on the assumption that each input below IS mishandled, and aimed at
# the class of defect this engine is most exposed to: an argument that is
# accepted, silently reinterpreted, and then reported as though it had been
# honoured. The entrant-precedence bug was exactly that shape, so these probe
# the neighbouring arguments for the same thing.

cohort <- function(n = 60) initialize_provider_agents(n, "FPMRS", 2025L)

# Precedence and validation are resolved before any simulating, so a few
# iterations probes them as well as five hundred would.
run <- function(...) run_supply_microsimulation(
  cohort(), years = 2025:2028, n_iterations = 3, verbose = FALSE,
  allow_fixed_parameters = TRUE, ...)

said <- function(expr) {
  msgs <- character(0)
  withCallingHandlers(force(expr),
                      message = function(m) {
                        msgs <<- c(msgs, conditionMessage(m))
                        invokeRestart("muffleMessage")
                      })
  msgs
}

# 1 ---------------------------------------------------------------------------
# A negative entrant count cannot be simulated: the injection block is guarded by
# `if (n_new > 0)`, so a negative rate is silently indistinguishable from zero.
# That is reachable by arithmetic -- a net-flow calculation that comes out
# negative, or a departures-subtracted series -- and the run would report growth
# from a rate the caller believes is shrinking the workforce.
test_that("a negative entrant rate is rejected, not silently treated as zero", {
  # Assert the message NAMES the argument. Pinning exact phrasing would just
  # test the wording; the defect being guarded is an error that identifies
  # nothing, which is what "invalid 'times' argument" did.
  expect_error(run(entrants_per_year = -10), "entrants_per_year")
})

# 2 ---------------------------------------------------------------------------
# conversion_floor is documented as a 0.70-1.0 graduate-to-practice haircut. It
# multiplies the entrant count directly, so a value above 1 MANUFACTURES
# entrants and a negative one flips the sign. Neither is a conversion.
test_that("conversion_floor outside (0, 1] is rejected", {
  expect_error(run(entrants_per_year = 20, conversion_floor = 2.0), "conversion_floor")
  expect_error(run(entrants_per_year = 20, conversion_floor = -0.5), "conversion_floor")
})

# 3 ---------------------------------------------------------------------------
# ci feeds stats::quantile() as (1-ci)/2 and 1-(1-ci)/2. ci = 1.5 gives
# probabilities of -0.25 and 1.25, which dies inside quantile() with a message
# naming neither ci nor this function. ci = 0 is worse: both probabilities
# collapse to 0.5, so lo == hi == median and the run reports a zero-width band
# labelled as a credible interval.
test_that("ci outside (0, 1) is rejected rather than reaching quantile()", {
  expect_error(run(entrants_per_year = 20, ci = 1.5), "ci")
  expect_error(run(entrants_per_year = 20, ci = 0), "ci")
})

# 4 ---------------------------------------------------------------------------
# One replicate has no distribution. Every quantile returns the same value, so
# effective_fte_lo == effective_fte_hi and the run publishes a zero-width
# interval that still calls itself 95%. The uncertainty guard does not catch
# this: it checks whether PARAMETERS vary, not whether there are enough draws.
test_that("a single iteration cannot masquerade as an interval", {
  # Not via run(): that helper fixes n_iterations, and passing it again matches
  # the same formal twice.
  msgs <- said(out <- run_supply_microsimulation(
    cohort(), years = 2025:2028, entrants_per_year = 20, n_iterations = 1,
    verbose = FALSE, allow_fixed_parameters = TRUE))
  final <- out$summary[out$summary$year == 2028, ]
  zero_width <- isTRUE(all.equal(final$effective_fte_lo, final$effective_fte_hi))
  # Either refuse the band or say plainly that it is not one.
  expect_true(any(grepl("iteration", msgs, ignore.case = TRUE)) || !zero_width)
})

# 5 ---------------------------------------------------------------------------
# The engine ages agents once per loop pass and labels each pass with years[i].
# Unsorted years therefore produce a panel whose labels do not match the elapsed
# time: c(2030, 2025) ages the cohort forward while the years count backward.
test_that("unsorted years are rejected", {
  expect_error(
    run_supply_microsimulation(cohort(), years = c(2030L, 2025L, 2028L),
                               entrants_per_year = 20, n_iterations = 2,
                               verbose = FALSE, allow_fixed_parameters = TRUE),
    "sorted|increasing|order")
})

# 6 ---------------------------------------------------------------------------
# A duplicated year is aged twice but summarised once: the group_by collapses
# two different cohort states into a single row, so the reported value for that
# year depends on quantile ordering rather than on the model.
test_that("duplicate years are rejected", {
  expect_error(
    run_supply_microsimulation(cohort(), years = c(2025L, 2025L, 2026L),
                               entrants_per_year = 20, n_iterations = 2,
                               verbose = FALSE, allow_fixed_parameters = TRUE),
    "duplicate|unique")
})

# 7 ---------------------------------------------------------------------------
# A single-year horizon is a legitimate base-year-only run. Entrants are skipped
# on the final year by design, so this must return exactly one row equal to the
# starting cohort -- not an error, and not a row with entrants folded in.
test_that("a single-year horizon returns the base year untouched", {
  out <- run_supply_microsimulation(cohort(60), years = 2025L, entrants_per_year = 20,
                                    n_iterations = 2, verbose = FALSE,
                                    allow_fixed_parameters = TRUE)
  expect_equal(nrow(out$summary), 1L)
  expect_equal(out$summary$headcount_median[1], 60)
})

# 8 ---------------------------------------------------------------------------
# Reproducibility is the whole claim of seed_microsimulation(). Two runs with
# the same seed must agree to the last digit; if they do not, every published
# interval is unreplicable.
test_that("the same seed reproduces the run exactly", {
  a <- run(entrants_per_year = 20, seed = 4242L)
  b <- run(entrants_per_year = 20, seed = 4242L)
  expect_equal(as.data.frame(a$summary), as.data.frame(b$summary))
  c2 <- run(entrants_per_year = 20, seed = 99L)
  expect_false(isTRUE(all.equal(as.data.frame(a$summary), as.data.frame(c2$summary))))
})

# 9 ---------------------------------------------------------------------------
# A 50% band must sit strictly inside a 95% band drawn from the same replicates.
# If ci is ignored, or applied to the wrong tail, the two come out identical.
test_that("a narrower ci nests inside a wider one", {
  wide   <- run(entrants_per_year = 20, ci = 0.95, seed = 7L)
  narrow <- run(entrants_per_year = 20, ci = 0.50, seed = 7L)
  w <- wide$summary[wide$summary$year == 2028, ]
  n <- narrow$summary[narrow$summary$year == 2028, ]
  expect_gte(n$headcount_lo, w$headcount_lo)
  expect_lte(n$headcount_hi, w$headcount_hi)
  expect_equal(n$headcount_median, w$headcount_median)
})

# 10 --------------------------------------------------------------------------
# End-to-end guard on the precedence fix: whichever input wins, the rate the run
# REPORTS must be the rate it SIMULATED. Doubling entrants must grow the
# workforce, and scenario$entrants_per_year must track the winner -- the failure
# mode being a metadata field that says 200 while the engine ran 0.
test_that("the reported entrant rate is the one actually simulated", {
  low  <- run(entrants_per_year = 10, seed = 3L)
  high <- run(entrants_per_year = 40, seed = 3L)
  fin <- function(o) o$summary$headcount_median[o$summary$year == 2028]
  expect_gt(fin(high), fin(low))
  expect_equal(low$scenario$entrants_per_year, 10)
  expect_equal(high$scenario$entrants_per_year, 40)

  # And via the spec, which is the path that used to lie.
  spec <- run(param_spec = supply_parameter_spec(entrant_mean = 40), seed = 3L)
  expect_equal(spec$scenario$entrants_per_year, 40)
  expect_equal(fin(spec), fin(high))
})
