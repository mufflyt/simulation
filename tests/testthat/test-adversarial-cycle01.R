# Adversarial cycle 01 -- year indexing, cohort aging, entrant accounting, RNG.
#
# Targets chosen because coverage was thinnest exactly where a test can pass
# while the microsimulation is scientifically wrong: year indexing (3 test files
# touched it), cohort aging (5), FTE-vs-headcount semantics (7).
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc01_agents <- function(n = 40, seed = 101) {
  set.seed(seed)
  data.frame(
    provider_id = sprintf("P%03d", seq_len(n)),
    subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(38, 62, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "baseline", stringsAsFactors = FALSE
  )
}
cyc01_run <- function(agents, years, entrants, ...) {
  ic <- calibrate_hours_intercept(agents$age, agents$sex)
  simulate_provider_career_once(agents, years, entrants, hours_intercept = ic, ...)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: entrant vectors are accepted at exactly ny and ny-1 and refused either side", {
  ag <- cyc01_agents(); yrs <- 2025:2029; ny <- length(yrs)
  expect_silent(invisible(cyc01_run(ag, yrs, rep(5, ny))))       # one per year
  expect_silent(invisible(cyc01_run(ag, yrs, rep(5, ny - 1L))))  # one per transition
  expect_error(cyc01_run(ag, yrs, rep(5, ny - 2L)), "length")
  expect_error(cyc01_run(ag, yrs, rep(5, ny + 1L)), "length")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the recycled final entrant slot is never read", {
  # effective_entrants <- rep_len(entrants_per_year, length(years)) RECYCLES, so a
  # length ny-1 path silently reuses year 1's value at position ny. The loop
  # guards with `if (i < n_years)` so that slot is never read -- today. This pins
  # it: two paths that differ ONLY in the unread slot must be identical, so a
  # future change from `<` to `<=` fails here instead of quietly shifting
  # entrants into the final projection year.
  ag <- cyc01_agents(); yrs <- 2025:2029; ny <- length(yrs)
  path <- c(3, 3, 3, 3)                        # ny-1
  set.seed(7); a <- cyc01_run(ag, yrs, path)$panel
  set.seed(7); b <- cyc01_run(ag, yrs, c(path, 999))$panel   # ny, absurd final slot
  expect_equal(a$headcount, b$headcount,
               info = "the final entrant slot changed the result, so it IS being read")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: conversion_floor boundaries are open at 0 and closed at 1", {
  ag <- cyc01_agents(); yrs <- 2025:2027
  expect_error(cyc01_run(ag, yrs, 5, conversion_floor = 0))
  expect_silent(invisible(cyc01_run(ag, yrs, 5, conversion_floor = 1e-9)))
  expect_silent(invisible(cyc01_run(ag, yrs, 5, conversion_floor = 1)))
  # Above 1 would MANUFACTURE entrants rather than discount them.
  expect_error(cyc01_run(ag, yrs, 5, conversion_floor = 1 + 1e-9))
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: a single-year horizon has no transitions and admits no entrants", {
  ag <- cyc01_agents(); n0 <- nrow(ag)
  p <- cyc01_run(ag, 2025L, 50)$panel     # 50 entrants/yr, but zero transitions
  expect_equal(nrow(p), 1L)
  expect_equal(p$headcount[1], n0,
               info = "entrants appeared in a horizon with no year to enter into")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: entrants land in the year they are indexed to, not shifted", {
  # Year indexing is the failure mode that passes every aggregate test: totals
  # are right, timing is wrong. A spike in exactly one slot must appear in
  # exactly one year.
  ag <- cyc01_agents(); yrs <- 2025:2029
  spike <- c(0, 0, 60, 0)                  # ny-1 transitions; spike on the 3rd
  set.seed(11)
  p <- cyc01_run(ag, yrs, spike, retirement_schedule = setNames(rep(0, 100), 1:100),
                 career_change_hazard = 0)$panel
  d <- diff(p$headcount)
  expect_equal(which(d > 0), 3L,
               info = paste("headcount grew in year(s)", paste(which(d > 0), collapse = ","),
                            "but the spike was indexed to transition 3"))
  expect_equal(unname(d[3]), 60)
  expect_true(all(d[-3] == 0))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: clinical FTE never exceeds headcount", {
  # An FTE is a fraction of one person's clinical time, so supply in FTE cannot
  # exceed supply in heads. If it does, the hours intercept and the FTE
  # definition disagree -- the exact inconsistency the engine warns about.
  ag <- cyc01_agents(); yrs <- 2025:2035
  set.seed(3)
  p <- cyc01_run(ag, yrs, 4)$panel
  expect_true(all(p$effective_fte <= p$headcount + 1e-9),
              info = sprintf("max FTE/head ratio was %.4f",
                             max(p$effective_fte / p$headcount)))
  expect_true(all(p$effective_fte >= 0))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: with no entry and no exit, every survivor ages exactly one year", {
  ag <- cyc01_agents(); yrs <- 2025:2030
  set.seed(5)
  sim <- cyc01_run(ag, yrs, 0,
                   retirement_schedule = setNames(rep(0, 100), 1:100),
                   career_change_hazard = 0)
  p <- sim$panel
  expect_true(all(p$headcount == nrow(ag)),
              info = "headcount moved with zero entrants and zero hazard")
  # Mean age must rise by exactly one per year when the cohort is closed.
  expect_equal(diff(p$mean_age), rep(1, length(yrs) - 1L), tolerance = 1e-8)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: results depend on the seed, not on ambient RNG state", {
  ag <- cyc01_agents(); yrs <- 2025:2030
  set.seed(42); a <- cyc01_run(ag, yrs, 5)$panel$headcount
  set.seed(42); b <- cyc01_run(ag, yrs, 5)$panel$headcount
  expect_equal(a, b, info = "same seed gave different answers")

  set.seed(43); c3 <- cyc01_run(ag, yrs, 5)$panel$headcount
  expect_false(identical(a, c3), info = "different seeds gave identical answers -- RNG inert")

  # Ambient state consumed BEFORE the seed must not leak in.
  set.seed(42); invisible(runif(1000)); d <- cyc01_run(ag, yrs, 5)$panel$headcount
  set.seed(42); e <- cyc01_run(ag, yrs, 5)$panel$headcount
  expect_false(identical(d, e),
               info = "draws ignored ambient RNG state, suggesting an internal re-seed")
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: row order of the agent table does not change the estimand", {
  # A join or an order-dependent index would show up here and nowhere else:
  # the same cohort, shuffled, must give the same aggregate under the same seed.
  ag <- cyc01_agents(); yrs <- 2025:2030
  set.seed(9); a <- cyc01_run(ag, yrs, 0,
                              retirement_schedule = setNames(rep(0, 100), 1:100),
                              career_change_hazard = 0)$panel
  shuffled <- ag[rev(seq_len(nrow(ag))), , drop = FALSE]
  set.seed(9); b <- cyc01_run(shuffled, yrs, 0,
                              retirement_schedule = setNames(rep(0, 100), 1:100),
                              career_change_hazard = 0)$panel
  expect_equal(a$headcount, b$headcount)
  expect_equal(a$mean_age, b$mean_age, tolerance = 1e-9)
  expect_equal(a$effective_fte, b$effective_fte, tolerance = 1e-8)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: duplicated provider ids are not silently collapsed", {
  # Two rows sharing an id are two clinicians as far as supply is concerned. If
  # something de-duplicates on id, headcount silently drops and the shortage
  # grows -- a scientific error with no error message.
  ag <- cyc01_agents(n = 20)
  dup <- rbind(ag, ag[1:5, ])            # 25 rows, 20 distinct ids
  dup$provider_id[21:25] <- ag$provider_id[1:5]
  yrs <- 2025:2028
  set.seed(13)
  p <- cyc01_run(dup, yrs, 0,
                 retirement_schedule = setNames(rep(0, 100), 1:100),
                 career_change_hazard = 0)$panel
  expect_equal(p$headcount[1], nrow(dup),
               info = sprintf("25 rows entered, headcount reported %s", p$headcount[1]))
})
