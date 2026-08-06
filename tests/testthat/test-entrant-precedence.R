# Entrant-rate precedence in run_supply_microsimulation() (R/supply-provider_microsimulation).
#
# The iteration loop takes its entrant count from `param_spec` when one is
# supplied. That override used to be unconditional and unannounced, which failed
# two ways -- both reproduced here so neither can come back:
#
#   1. A spec that quantifies nothing carries entrant_mean = NULL. Assigning it
#      propagated numeric(0) into the preallocation arithmetic and died inside
#      simulate_provider_career_once() with "invalid 'times' argument".
#   2. A spec carrying entrant_mean silently beat an explicit entrants_per_year,
#      while the verbose log and the returned metadata both went on reporting
#      the argument. That is the dangerous one: a wrong number with a log line
#      confirming it.

cohort <- function(n = 60) initialize_provider_agents(n, "FPMRS", 2025L)

# Small and cheap: precedence is decided once, before any simulation, so a
# handful of iterations exercises it exactly as well as 500 would.
run <- function(...) run_supply_microsimulation(
  cohort(), years = 2025:2028, n_iterations = 3, verbose = FALSE,
  allow_fixed_parameters = TRUE, ...)

# .msg_warn() is message(), not warning(), so expect_warning() would pass
# vacuously here -- it did while I was writing these. Collect the messages and
# assert on their text instead. Several unrelated warnings fire on this cohort
# (fixed parameters, hours intercept), so "did not warn" has to mean "did not
# emit THIS message", never "emitted nothing".
said <- function(expr) {
  msgs <- character(0)
  withCallingHandlers(force(expr),
                      message = function(m) {
                        msgs <<- c(msgs, conditionMessage(m))
                        invokeRestart("muffleMessage")
                      })
  msgs
}
mentions <- function(msgs, pattern) any(grepl(pattern, msgs, fixed = TRUE))

test_that("a spec that quantifies nothing leaves entrants_per_year in force", {
  # The crash case. supply_parameter_spec() with no arguments is the most
  # natural thing to write and quantifies nothing, so entrant_mean is NULL.
  spec <- supply_parameter_spec()
  expect_null(spec$entrant_mean)

  out <- expect_no_error(run(entrants_per_year = 40, param_spec = spec))
  expect_equal(out$scenario$entrants_per_year, 40)
  expect_equal(out$scenario$entrants_source, "entrants_per_year argument")
})

test_that("entrant_mean beats entrants_per_year, and says so", {
  spec <- supply_parameter_spec(entrant_mean = 200)
  expect_false(spec$quantified[["entrant_rate"]])

  msgs <- said(out <- run(entrants_per_year = 0, param_spec = spec))
  expect_true(mentions(msgs, "takes precedence"))
  # The resolved rate is recorded, not the argument the engine ignored.
  expect_equal(out$scenario$entrants_per_year, 200)
  expect_equal(out$scenario$entrants_source, "param_spec$entrant_mean")

  # And it is the rate actually simulated: 200/yr into a 60-provider cohort has
  # to grow it, which a silent fallback to entrants_per_year = 0 could not.
  final <- out$summary$headcount_median[out$summary$year == 2028]
  expect_gt(final, 60)
})

test_that("agreeing values do not warn", {
  # The existing hours-propagation tests pass entrant_mean = 0 alongside
  # entrants_per_year = 0 deliberately; agreement must stay quiet.
  spec <- supply_parameter_spec(entrant_mean = 0)
  msgs <- said(out <- run(entrants_per_year = 0, param_spec = spec))
  expect_false(mentions(msgs, "takes precedence"))
  expect_equal(out$scenario$entrants_per_year, 0)
})

test_that("an undeclared entrants_per_year does not warn when the spec supplies one", {
  # Taking the default and letting the spec drive is the normal production call;
  # only a CONTRADICTED explicit argument is worth a warning.
  spec <- supply_parameter_spec(entrant_mean = 30)
  expect_false(mentions(said(run(param_spec = spec)), "takes precedence"))
})

test_that("a quantified spec with no entrant_mean fails with a message that names it", {
  # Reachable by hand: entrant_series alone quantifies the rate but leaves no
  # point estimate to draw around. Fail here, naming the spec, rather than
  # several frames down inside the engine.
  spec <- supply_parameter_spec(entrant_series = c(40, 48, 51))
  expect_true(spec$quantified[["entrant_rate"]])
  expect_null(spec$entrant_mean)
  expect_error(run(entrants_per_year = 55, param_spec = spec),
               "entrant_mean")
})

test_that("a drawn entrant rate reports the distribution, not a single value", {
  agents <- cohort()
  spec <- entrant_spec_from_series(agents)
  skip_if(is.null(spec$entrant_mean), "certification series unavailable")

  out <- run_supply_microsimulation(agents, years = 2025:2028, n_iterations = 5,
                                    param_spec = spec, verbose = FALSE)
  expect_equal(out$scenario$entrants_source, "param_spec (drawn per iteration)")
  expect_equal(out$scenario$entrants_per_year, spec$entrant_mean)
})
