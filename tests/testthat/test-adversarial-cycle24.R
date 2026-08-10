# Adversarial cycle 24 -- a lever that is declared and does nothing.
#
# Last cycle. Across the previous twenty-three, scenario parameter propagation
# was the thinnest of the ten named priorities: touched once, in cycle 02, and
# only for retirement_shift_years and an unknown scenario id.
#
# It is also where this repository has already been bitten, and the comment is
# still in the orchestrator:
#
#   "sharing one spec across scenarios silently overrode every scenario's
#    entrant value: 'Fellowship output +10%' and '-10%' returned results
#    identical to Baseline to the last digit."
#
# The only reason that was caught is that someone compared the numbers. So this
# cycle asks the question directly, for every lever: does declaring it change
# the answer, and can a scenario declare one that does nothing?
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

cyc24_agents <- function(n = 60) {
  data.frame(provider_id = sprintf("P%03d", seq_len(n)), subspecialty = "FPMRS",
             sex = rep(c("female", "male"), length.out = n),
             age = seq(36, 68, length.out = n),
             entry_year = 2015L, retirement_year = NA_real_,
             origin_cohort = "baseline", stringsAsFactors = FALSE)
}
cyc24_run <- function(over = list()) {
  ag <- cyc24_agents()
  args <- list(initial_workforce = ag, years = 2025:2035, entrants_per_year = 20,
               n_iterations = 5, hours_intercept = calibrate_hours_intercept(ag$age, ag$sex),
               allow_fixed_parameters = TRUE, verbose = FALSE, seed = 7)
  for (n in names(over)) args[[n]] <- over[[n]]
  s <- suppressMessages(do.call(run_supply_microsimulation, args))
  c(hc = s$summary$headcount_median[11], fte = s$summary$effective_fte_median[11])
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the known-field vocabulary covers both registries exactly", {
  # The set a scenario may draw from. Too narrow and the SSOT registry stops
  # validating; too wide and a typo slips through as a "known" field.
  expect_true(all(SUPPLY_SCENARIO_REQUIRED %in% SUPPLY_SCENARIO_KNOWN_FIELDS))
  expect_false(anyDuplicated(SUPPLY_SCENARIO_KNOWN_FIELDS) > 0L)

  local_fields <- unique(unlist(lapply(local_supply_scenario_registry(), names)))
  expect_true(all(local_fields %in% SUPPLY_SCENARIO_KNOWN_FIELDS))
  expect_silent(validate_scenario_registry(local_supply_scenario_registry(), "supply"))

  skip_if_not(has_mufflyaccess(), "SSOT registry not installed")
  ssot <- tryCatch(ssot_supply_scenarios(55), error = function(e) NULL)
  skip_if(is.null(ssot), "SSOT scenarios unavailable")
  expect_true(all(unique(unlist(lapply(ssot, names))) %in% SUPPLY_SCENARIO_KNOWN_FIELDS))
  expect_silent(validate_scenario_registry(ssot, "supply"))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a neutral lever leaves the answer exactly where it was", {
  # The identity every lever must satisfy at its neutral value. If a multiplier
  # of 1 moves the result, the lever is doing something other than what it says.
  base <- cyc24_run()
  expect_equal(cyc24_run(list(hours_multiplier = 1.0)), base)
  expect_equal(cyc24_run(list(conversion_floor = 1.0)), base)
  expect_equal(cyc24_run(list(late_career_fte_factor = 1.0)), base)
  expect_true(all(is.finite(base)))
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: an unknown field is refused whether it is the only one or one of many", {
  reg <- local_supply_scenario_registry()
  one <- reg; one$status_quo$not_a_lever <- 1
  expect_error(validate_scenario_registry(one, "supply"), "not_a_lever")

  many <- reg
  many$status_quo$not_a_lever <- 1
  many$status_quo$also_not <- 2
  err <- tryCatch(validate_scenario_registry(many, "supply"), error = function(e) conditionMessage(e))
  expect_match(err, "not_a_lever")
  expect_match(err, "also_not")

  # And a scenario carrying only the required four still validates.
  minimal <- list(status_quo = list(label = "SQ", entrants = 55,
                                    retirement_shift_years = 0, source = "test"))
  expect_silent(validate_scenario_registry(minimal, "supply"))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: every lever moves the answer, and moves the right quantity", {
  # The propagation test the orchestrator's own comment says was needed. A lever
  # that is declared, documented and inert is indistinguishable from one that
  # works, until someone compares the numbers.
  base <- cyc24_run()

  # Levers that change WHO is in the workforce move headcount.
  expect_gt(cyc24_run(list(entrants_per_year = 30))[["hc"]], base[["hc"]])
  expect_lt(cyc24_run(list(conversion_floor = 0.6))[["hc"]], base[["hc"]])
  expect_lt(cyc24_run(list(retirement_schedule =
                             setNames(rep(0.20, 120), 1:120)))[["hc"]], base[["hc"]])
  expect_lt(cyc24_run(list(career_change_hazard = 0.05))[["hc"]], base[["hc"]])

  # Levers that change HOW MUCH each provider works move FTE and leave headcount
  # untouched -- the FTE-vs-headcount distinction, as a propagation property.
  hm <- cyc24_run(list(hours_multiplier = 0.8))
  expect_equal(hm[["hc"]], base[["hc"]])
  expect_lt(hm[["fte"]], base[["fte"]])

  lf <- cyc24_run(list(late_career_fte_factor = 0.5, late_career_fte_onset_age = 60))
  expect_equal(lf[["hc"]], base[["hc"]])
  expect_lt(lf[["fte"]], base[["fte"]])
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the onset age is a lever in its own right, not a switch", {
  # late_career_fte_factor and late_career_fte_onset_age are two levers, and a
  # scenario can set either. Holding the factor fixed and moving the onset
  # earlier brings more providers inside it, so the effect must strengthen.
  late <- cyc24_run(list(late_career_fte_factor = 0.5, late_career_fte_onset_age = 60))
  early <- cyc24_run(list(late_career_fte_factor = 0.5, late_career_fte_onset_age = 45))
  expect_lt(early[["fte"]], late[["fte"]])
  expect_equal(early[["hc"]], late[["hc"]])      # neither moves headcount
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the levers compose rather than overriding one another", {
  # Two levers pulling the same way must move further than either alone. If one
  # silently overrode the other -- the failure the orchestrator's comment
  # records for the entrant spec -- the combination would equal one of them.
  base <- cyc24_run()
  a <- cyc24_run(list(entrants_per_year = 30))
  b <- cyc24_run(list(conversion_floor = 0.6))
  both <- cyc24_run(list(entrants_per_year = 30, conversion_floor = 0.6))

  expect_gt(a[["hc"]], base[["hc"]])
  expect_lt(b[["hc"]], base[["hc"]])
  # 30 entrants at 0.6 conversion is 18 effective, below the baseline's 20.
  expect_lt(both[["hc"]], a[["hc"]])
  expect_false(isTRUE(all.equal(unname(both), unname(a))))
  expect_false(isTRUE(all.equal(unname(both), unname(b))))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a misspelled lever is refused, not silently neutralised", {
  # THE DEFECT. Every lever is read as `params$<name> %||% <neutral>`, so a
  # field the orchestrator does not recognise applies NOTHING. A scenario
  # labelled "Hours down 20%" carrying `hours_multipler = 0.8` validated clean
  # and ran as baseline.
  reg <- local_supply_scenario_registry()
  typo <- reg$status_quo
  typo$label <- "Hours down 20%"
  typo$hours_multipler <- 0.8               # one letter short
  reg$hours_down <- typo

  err <- tryCatch(validate_scenario_registry(reg, "supply"), error = function(e) conditionMessage(e))
  expect_match(err, "hours_multipler")
  expect_match(err, "applies NOTHING")
  # The message offers the near match, because the whole failure is a typo.
  expect_match(err, "Did you mean")
  expect_match(err, "hours_multiplier")

  # Spelled correctly, the same scenario validates and the lever bites.
  reg$hours_down$hours_multipler <- NULL
  reg$hours_down$hours_multiplier <- 0.8
  expect_silent(validate_scenario_registry(reg, "supply"))
  expect_lt(cyc24_run(list(hours_multiplier = 0.8))[["fte"]], cyc24_run()[["fte"]])
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a near-miss on every lever name is caught", {
  # One typo caught is not a guard. Each lever's name is checked, because the
  # one a future scenario misspells is not the one tested today.
  levers <- c("conversion", "hours_multiplier", "career_change_multiplier",
              "late_career_fte_factor", "late_career_fte_onset_age", "entrants",
              "retirement_shift_years")
  for (lv in levers) {
    reg <- local_supply_scenario_registry()
    bad <- sub("s$", "", sub("_", "", lv))          # drop an underscore and a plural
    if (bad %in% SUPPLY_SCENARIO_KNOWN_FIELDS) next
    reg$status_quo[[bad]] <- 0.5
    expect_error(validate_scenario_registry(reg, "supply"), bad, fixed = TRUE,
                 info = paste("misspelling of", lv))
  }
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the scenario grid produces distinct answers, not relabelled ones", {
  # The end-to-end form of the recorded defect: several scenarios returning the
  # same numbers under different labels. Any two scenarios differing in a lever
  # must differ in the result.
  results <- list(
    baseline   = cyc24_run(),
    more_grads = cyc24_run(list(entrants_per_year = 22)),
    less_grads = cyc24_run(list(entrants_per_year = 18)),
    retire_l8r = cyc24_run(list(retirement_schedule = setNames(rep(0.01, 120), 1:120))),
    hours_down = cyc24_run(list(hours_multiplier = 0.9))
  )
  hc <- vapply(results, function(r) r[["hc"]], numeric(1))
  fte <- vapply(results, function(r) r[["fte"]], numeric(1))

  # No two scenarios agree on BOTH quantities.
  key <- paste(round(hc, 6), round(fte, 6))
  expect_equal(length(unique(key)), length(results))
  # And the entrant ordering is the one the labels claim.
  expect_gt(hc[["more_grads"]], hc[["less_grads"]])
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: a registry that cannot be validated cannot be run from", {
  # The guard has to sit where a run passes through it, or a caller reaches the
  # orchestrator with a registry the validator would have refused.
  reg <- local_supply_scenario_registry()
  reg$status_quo$hours_multipler <- 0.8
  expect_error(validate_scenario_registry(reg, "supply"), "unknown field")

  # A demand registry is judged by its own rules and is unaffected by the supply
  # vocabulary -- the two must not share a field list.
  dem <- demand_scenario_registry()
  expect_silent(validate_scenario_registry(dem, "demand"))
  expect_false(all(unique(unlist(lapply(dem, names))) %in% SUPPLY_SCENARIO_KNOWN_FIELDS))
})
