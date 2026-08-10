# Adversarial cycle 14 -- providers added to the workforce who were never in it.
#
# Cycle 13 carried forward: a local variable that shadows a package constant.
# Parsed every function body in R/ and matched every assignment target against
# the package-level constants. Result: NONE. The BRFSS shadow fixed in cycle 13
# was the only one, so that class is closed on evidence.
#
# This cycle moved to the untouched geography module and its accounting.
# add_returning_providers() adds rows to the agent table for providers coming
# back into practice, and filled every unset NUMERIC column with 0. One of those
# columns is retirement_year, and the microsimulation's active predicate is
# `is.na(retirement_year) | retirement_year > year` -- so every returner was
# retired before the projection began.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc14_agents <- function(n = 10) {
  data.frame(
    provider_id = sprintf("P%02d", seq_len(n)), subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(40, 55, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "baseline", state = rep(c("CO", "NY"), length.out = n),
    stringsAsFactors = FALSE
  )
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: zero returners is a no-op and fractional counts round, not truncate", {
  a <- cyc14_agents()
  expect_identical(add_returning_providers(a, data.frame(geo = "CO", n_returners = 0),
                                           year = 2026L), a)
  # A negative count is a data error, not "nobody came back".
  expect_error(add_returning_providers(a, data.frame(geo = "CO", n_returners = -1),
                                       year = 2026L), "non-negative")
  expect_error(add_returning_providers(a, data.frame(geo = "CO", n_returners = NA_real_),
                                       year = 2026L), "finite")

  # An expected-value input of 2.5 returners is rounded, and rounding is
  # documented behaviour rather than a silent floor.
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 2.5), year = 2026L)
  expect_equal(nrow(r) - nrow(a), 2L)         # round(2.5) is 2 under R's banker's rounding
  r3 <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 3.5), year = 2026L)
  expect_equal(nrow(r3) - nrow(a), 4L)
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a returner is active in the year they return and not before", {
  # The boundary that matters: entry_year == year means active THIS year, under
  # the same predicate the engine uses for everyone else.
  a <- cyc14_agents()
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 2), year = 2026L)
  new <- r[(nrow(a) + 1L):nrow(r), ]
  expect_true(all(provider_active_in_year(new, 2026L)))
  expect_true(all(provider_active_in_year(new, 2030L)))
  expect_false(any(provider_active_in_year(new, 2025L)))   # not before they return
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: returners across several geographies split by the counts given", {
  a <- cyc14_agents()
  r <- add_returning_providers(a, data.frame(geo = c("CO", "NY", "TX"),
                                             n_returners = c(1, 0, 3)), year = 2027L)
  new <- r[(nrow(a) + 1L):nrow(r), ]
  expect_equal(nrow(new), 4L)
  expect_equal(as.integer(table(new$state)[c("CO", "TX")]), c(1L, 3L))
  expect_false("NY" %in% new$state)           # zero means zero, not one
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: a returner counts toward the active workforce", {
  # THE DEFECT. retirement_year is numeric, the generic fill set every unset
  # numeric column to 0, and the active predicate is
  # `is.na(retirement_year) | retirement_year > year`. 0 > 2026 is FALSE, so
  # every returner was retired before the projection started. Measured before
  # the fix: ten active providers, add three returners, still ten active.
  a <- cyc14_agents()
  before <- sum(provider_active_in_year(a, 2026L))
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 3), year = 2026L)
  expect_equal(sum(provider_active_in_year(r, 2026L)), before + 3L)
  expect_true(all(is.na(r$retirement_year[(nrow(a) + 1L):nrow(r)])))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: zero is the absent value for a counter and for nothing else", {
  # The rule the fix encodes. n_moves counts events, so a returner has had 0.
  # retirement_year records WHEN something happened, so a returner has NA. Using
  # one default for both is what turned a returner into a retiree.
  a <- cyc14_agents()
  a$n_moves <- 2L
  a$left_country <- TRUE
  a$last_seen_year <- 2020
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 2), year = 2026L)
  new <- r[(nrow(a) + 1L):nrow(r), ]

  expect_equal(new$n_moves, rep(0, 2))            # a counter: zero events so far
  expect_equal(new$left_country, rep(FALSE, 2))   # they are back, by definition
  expect_true(all(is.na(new$retirement_year)))    # never retired
  expect_true(all(is.na(new$last_seen_year)))     # unknown, not year zero
  expect_true(all(is.na(new$sex)))                # unknown, not "" or the modal sex
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: every returner is identifiable and cannot collide with the base cohort", {
  # An agent with no id cannot be joined to, deduplicated, or followed across
  # years -- and duplicate ids were already shown (cycle 01) to be treated as
  # distinct clinicians, so a table of NA ids is a table of anonymous rows.
  a <- cyc14_agents()
  r <- add_returning_providers(a, data.frame(geo = c("CO", "NY"), n_returners = c(2, 2)),
                               year = 2026L)
  new_ids <- r$provider_id[(nrow(a) + 1L):nrow(r)]
  expect_false(any(is.na(new_ids)))
  expect_equal(length(unique(new_ids)), 4L)
  expect_equal(length(intersect(new_ids, a$provider_id)), 0L)

  # Two return events in different years cannot collide either.
  r2 <- add_returning_providers(r, data.frame(geo = "CO", n_returners = 2), year = 2027L)
  expect_equal(length(unique(r2$provider_id)), nrow(r2))
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the agent table's schema survives the append", {
  # bind_rows() will happily widen a table. If the returner path adds or drops a
  # column, downstream code that selects by position or asserts on names breaks
  # only for runs that happen to have returners.
  a <- cyc14_agents()
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 3), year = 2026L)
  expect_identical(names(r), names(a))
  for (col in names(a)) {
    expect_identical(class(r[[col]]), class(a[[col]]), info = paste("column", col))
  }
  expect_equal(nrow(r), nrow(a) + 3L)
  # The original rows are untouched.
  expect_equal(r[seq_len(nrow(a)), ], a, ignore_attr = TRUE)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: returners survive a projection instead of vanishing from it", {
  # The end-to-end consequence. If a returner is inactive at birth, the engine
  # carries the row, reports a smaller headcount than the roster contains, and
  # nothing anywhere says so.
  a <- cyc14_agents(n = 20)
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 5), year = 2025L)
  ic <- calibrate_hours_intercept(r$age, r$sex)
  set.seed(14)
  p <- simulate_provider_career_once(r, 2025:2028, entrants_per_year = 0,
                                     retirement_schedule = setNames(rep(0, 100), 1:100),
                                     career_change_hazard = 0,
                                     hours_intercept = ic)$panel
  expect_equal(p$headcount[1], nrow(r))
  expect_true(all(p$headcount == nrow(r)))     # no exits configured

  # And the returners are the difference: the same run without them is smaller.
  ic2 <- calibrate_hours_intercept(a$age, a$sex)
  set.seed(14)
  p2 <- simulate_provider_career_once(a, 2025:2028, entrants_per_year = 0,
                                      retirement_schedule = setNames(rep(0, 100), 1:100),
                                      career_change_hazard = 0,
                                      hours_intercept = ic2)$panel
  expect_equal(p$headcount[1] - p2$headcount[1], 5L)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a returner's sex is unknown, and the engine does not invent one", {
  # sex feeds the hours schedule and the retirement multiplier. Filling it with
  # a value rather than NA would give every returner the same career, and the
  # bias would scale with how many returners a scenario adds.
  a <- cyc14_agents(n = 20)
  r <- add_returning_providers(a, data.frame(geo = "CO", n_returners = 5), year = 2025L)
  new <- r[(nrow(a) + 1L):nrow(r), ]
  expect_true(all(is.na(new$sex)))

  # The engine's own default for a missing sex is applied explicitly and
  # visibly, not smuggled in by the append.
  r$sex[is.na(r$sex)] <- "female"
  expect_false(any(is.na(r$sex)))
  ic <- calibrate_hours_intercept(r$age, r$sex)
  expect_true(is.finite(ic))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: an emigrated provider leaves every state total, and that is visible", {
  # apply_provider_migration_matrix() marks an out-of-country mover with
  # left_country = TRUE and state = NA. NOTHING in R/ reads left_country, so
  # such a provider is absent from every state total while the national active
  # count still includes them. Recorded as a test rather than silently
  # "fixed", because which total is right is a modelling decision: an emigrant
  # may legitimately remain in a national certification count.
  a <- cyc14_agents(n = 20)
  a$left_country <- FALSE
  a$n_moves <- 0L
  a$left_country[1:3] <- TRUE
  a$state[1:3] <- NA_character_

  by_state <- sum(!is.na(a$state))
  national <- sum(provider_active_in_year(a, 2026L))
  expect_equal(by_state, 17L)
  expect_equal(national, 20L)
  # The gap IS the emigrants, and it is exactly recoverable from left_country --
  # so any consumer that wants either total can compute it.
  expect_equal(national - by_state, sum(a$left_country))
})
