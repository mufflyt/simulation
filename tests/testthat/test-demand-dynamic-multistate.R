# Dynamic multistate disease model (R/demand-dynamic_multistate.R).
# The engine (simulate_dmdm) is base R; the cohort builder/trajectory use R/demand-lifecourse.
#
# Every test here exercises engine MECHANICS -- row counts, monotonicity,
# determinism, the onset/remission accounting identity -- on the placeholder
# coefficients, because those properties hold whatever the coefficients are.
# assert_calibrated_transitions() refuses placeholder inputs by default, so the
# calls below declare the exploratory override; none of them reads a number as a
# projection. suppressMessages keeps the declaration notice out of the output.

sim_dmdm <- function(...) {
  suppressMessages(simulate_dmdm(..., allow_uncalibrated = TRUE))
}

mk_cohort <- function(vag, n = 12000, seed = 1) {
  set.seed(seed)
  data.frame(
    age = sample(45:70, n, TRUE),
    cumulative_vaginal_deliveries = vag,
    years_since_last_vaginal_birth = sample(5:30, n, TRUE),
    bmi = pmax(16, rnorm(n, 28, 5)),
    hysterectomy = rbinom(n, 1, 0.2),
    menopause_status = rbinom(n, 1, 0.5),
    comorbidity = rbinom(n, 1, 0.3)
  )
}

test_that("the engine returns one row per year with valid prevalence/incidence", {
  out <- sim_dmdm(mk_cohort(2L), 2025, 2040, seed = 42)
  expect_equal(nrow(out), 16L)
  expect_true(all(c("year", "living", "deaths", "prev_ui", "prev_pop", "prev_ai",
                    "inc_ui", "inc_pop", "inc_ai") %in% names(out)))
  expect_true(all(out$prev_pop >= 0 & out$prev_pop <= 1))
})

test_that("a closed cohort shrinks through mortality", {
  out <- sim_dmdm(mk_cohort(1L), 2025, 2045, seed = 42)
  expect_true(all(diff(out$living) <= 0))
  expect_lt(out$living[nrow(out)], out$living[1])
  expect_gt(sum(out$deaths), 0)
})

test_that("cumulative vaginal deliveries drive prolapse onset (the primary exposure)", {
  lo <- sim_dmdm(mk_cohort(0L), 2025, 2040, seed = 42)
  hi <- sim_dmdm(mk_cohort(3L), 2025, 2040, seed = 42)
  expect_gt(hi$prev_pop[10], lo$prev_pop[10])
  expect_gt(hi$inc_pop[1],  lo$inc_pop[1])
})

test_that("remission lowers long-run prevalence", {
  no_rem <- dmdm_default_transitions(); no_rem$remission <- c(ui = 0, pop = 0, ai = 0)
  a <- sim_dmdm(mk_cohort(2L), 2025, 2045, transitions = no_rem, seed = 42)
  b <- sim_dmdm(mk_cohort(2L), 2025, 2045, seed = 42)
  expect_gt(a$prev_ui[nrow(a)], b$prev_ui[nrow(b)])
})

test_that("a case cannot onset and remit within the same year", {
  # Both transitions must be evaluated against the state at the START of the
  # year. Applying remission to the post-onset state let a case be counted in
  # inc_ and vanish before appearing in any prev_, so incidence and prevalence
  # disagreed. With remission = 1 the old code reported ~178 incident UI cases a
  # year against a prevalence that never left zero.
  always <- dmdm_default_transitions()
  always$remission <- c(ui = 1, pop = 1, ai = 1)
  out <- sim_dmdm(mk_cohort(2L), 2025, 2028, transitions = always, seed = 7)

  # Every case incident in year y is prevalent at the start of year y+1, then
  # resolves. So prevalence must track the previous year's incidence exactly.
  # Onsets are drawn among survivors, and prevalence is measured among the living
  # at the start of the next year -- the same set, so the denominator is living[y+1].
  expected <- utils::head(out$inc_ui, -1) / out$living[-1]
  expect_equal(out$prev_ui[-1], expected, tolerance = 1e-9)
  expect_true(all(out$inc_ui > 0))
  expect_true(all(out$prev_ui[-1] > 0))
})

test_that("prevalence and incidence stay mutually consistent without remission", {
  none <- dmdm_default_transitions()
  none$remission <- c(ui = 0, pop = 0, ai = 0)
  out <- sim_dmdm(mk_cohort(2L), 2025, 2035, transitions = none, seed = 3)
  # With no remission and no re-entry, the prevalent count can only grow.
  prevalent <- out$prev_pop * out$living
  expect_true(all(diff(prevalent) >= -1e-9))
})

test_that("the engine is deterministic given a seed", {
  expect_equal(sim_dmdm(mk_cohort(2L), 2025, 2035, seed = 7),
               sim_dmdm(mk_cohort(2L), 2025, 2035, seed = 7))
})

test_that("the trajectory wrapper seeds year-0 prevalence and evolves it", {
  pop_by_age <- tibble::tibble(age = 40:85,
                               population = round(2e6 * exp(-0.02 * (40:85 - 40))))
  tr <- suppressMessages(dmdm_prevalence_trajectory(
    pop_by_age, 2025, 2035, n = 6000, seed = 1, allow_uncalibrated = TRUE))
  expect_equal(nrow(tr), 11L)
  expect_gt(tr$prev_ui[1], 0)                    # seeded from the cross-sectional risk
  expect_true(all(tr$prev_ui >= 0 & tr$prev_ui <= 1))
})
