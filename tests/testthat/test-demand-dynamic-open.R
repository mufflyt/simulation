# Open-population DMDM (R/demand-dynamic_open.R). The engine is deterministic
# base R; the trajectory wrapper uses R/demand-lifecourse.
#
# These test demography and reweighting mechanics, which hold whatever the
# transition coefficients are, so every call declares the exploratory override
# that assert_calibrated_transitions() now requires. Without the declaration the
# calibration gate would pre-empt the conservation gate and the strict-mode test
# below would catch the wrong error.
#
# Only the declaration notice is muffled, not every message: the conservation
# tests assert on messages, so a blanket suppressMessages() would make them
# vacuous. Muffling by pattern keeps "Population conservation" observable.

.muffle_declaration <- function(expr) {
  withCallingHandlers(expr, message = function(m) {
    if (grepl("EXPLORATORY", conditionMessage(m), fixed = TRUE)) {
      invokeRestart("muffleMessage")
    }
  })
}
sim_open  <- function(...) .muffle_declaration(
  simulate_dmdm_open(..., allow_uncalibrated = TRUE))
traj_open <- function(...) .muffle_declaration(
  dmdm_open_prevalence_trajectory(..., allow_uncalibrated = TRUE))

mk_agents <- function(ages, vag, w, seedp = 0.05) {
  data.frame(
    age = ages, cumulative_vaginal_deliveries = vag,
    years_since_last_vaginal_birth = pmax(0, ages - 30),
    bmi = 28, hysterectomy = 0, menopause_status = as.integer(ages >= 51),
    comorbidity = 0, weight = w, p_ui = seedp, p_pop = seedp / 2, p_ai = seedp / 2)
}
entrants_for <- function(vag, w, years = 2026:2050) {
  do.call(rbind, lapply(years, function(y) {
    d <- mk_agents(40, vag, w); d$entry_year <- y; d
  }))
}

test_that("the open population does not collapse and prevalence is valid", {
  out <- sim_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  expect_equal(nrow(out), 26L)
  expect_true(all(out$population > 0.5 * out$population[1]))   # replenished
  expect_true(all(out$prev_pop >= 0 & out$prev_pop <= 1))
  expect_true(all(out$inc_pop > 0))
})

test_that("prevalence reaches a quasi-steady state (unlike a closed cohort)", {
  out <- sim_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  early <- abs(out$prev_pop[6]  - out$prev_pop[2])
  late  <- abs(out$prev_pop[26] - out$prev_pop[22])
  expect_lt(late, early)
})

test_that("more cumulative vaginal deliveries raise population prolapse prevalence", {
  hi <- sim_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  lo <- sim_open(mk_agents(40:84, 0, 1e5), entrants_for(0, 1e5), 2025, 2050)
  expect_gt(hi$prev_pop[26], lo$prev_pop[26])
})

test_that("without entrants the population declines (open vs closed contrast)", {
  open   <- sim_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  closed <- sim_open(mk_agents(40:84, 2, 1e5), NULL, 2025, 2050)
  expect_lt(closed$population[26], open$population[26])
})

test_that("the engine is deterministic (no RNG)", {
  i <- mk_agents(40:84, 2, 1e5); en <- entrants_for(2, 1e5)
  expect_equal(sim_open(i, en, 2025, 2035),
               sim_open(i, en, 2025, 2035))
})

test_that("the trajectory wrapper builds an open population from projections", {
  pop_by_age_year <- do.call(rbind, lapply(2025:2030, function(y)
    data.frame(year = y, age = 40:85,
               population = round(2e6 * exp(-0.02 * (40:85 - 40)) * (1 + 0.01 * (y - 2025))))))
  pop_by_age_year <- tibble::as_tibble(pop_by_age_year)
  tr <- traj_open(pop_by_age_year, 2025, 2030,
                  n_init = 4000, n_entrants = 500, seed = 1)
  expect_equal(nrow(tr), 6L)
  expect_true(all(tr$population > 0))
  expect_true(all(tr$prev_pop >= 0 & tr$prev_pop <= 1))
})

test_that("a single-year window builds no entrant cohorts", {
  # `(start + 1):end` counts DOWN when start == end, so the wrapper built cohorts
  # for start_year + 1 AND start_year -- the latter duplicating the base
  # population's entry-age group. The engine discarded both, but the frame was
  # wrong.
  pop <- do.call(rbind, lapply(2025:2026, function(y)
    data.frame(year = y, age = 40:70, population = 1e5)))
  pop <- tibble::as_tibble(pop)

  one <- traj_open(pop, 2025, 2025, n_init = 800,
                   n_entrants = 100, seed = 1)
  expect_equal(nrow(one), 1L)
  expect_equal(one$year, 2025)
  # The base year's population must be exactly the base population: no entrant
  # cohort may be folded into it.
  init_only <- traj_open(pop, 2025, 2026, n_init = 800,
                         n_entrants = 100, seed = 1)
  expect_equal(one$population, init_only$population[1])
})

# ---- Population-conservation audit ----------------------------------------

open_pop_grid <- function(years, ages, n = 1e5) {
  tibble::as_tibble(do.call(rbind, lapply(years, function(y)
    data.frame(year = y, age = ages, population = n))))
}

test_that("reweighting audits how much population escapes the projection", {
  # The projection stops at 85; the cohort ages past it every year, so the
  # unanchored share must grow monotonically and sit at the OLDEST ages -- the
  # ones carrying the highest prevalence.
  pop <- open_pop_grid(2025:2035, 40:85)
  init <- urpssim:::.dmdm_open_agents(pop[pop$year == 2025, c("age", "population")],
                            2025, 40, 3000, seed = 1)
  # expect_message() returns the CONDITION, not the value, so the run and the
  # message assertion are separate calls.
  expect_message(sim_open(init, NULL, 2025, 2035, pop_by_age_year = pop),
                 "Population conservation")
  r <- suppressMessages(
    sim_open(init, NULL, 2025, 2035, pop_by_age_year = pop))

  aud <- dmdm_population_audit(r)
  expect_equal(nrow(aud), 11L)
  expect_equal(aud$share_unanchored[1], 0)             # year 0: nothing has aged out
  expect_true(all(diff(aud$share_unanchored) > 0))     # leak grows every year
  expect_gt(max(aud$share_unanchored), 0.15)
  expect_true(all(aud$age_min_unanchored[-1] > 85))    # only ages past the top
  # The headline column must be visible on the result, not only the attribute.
  expect_true("share_unanchored" %in% names(r))
  expect_equal(r$share_unanchored, aud$share_unanchored)
  # Anchored + unanchored is the whole simulated population, by construction.
  expect_equal(aud$population_anchored + aud$population_unanchored,
               aud$population_simulated)
})

test_that("a projection covering every simulated age conserves exactly", {
  # Ages 40-120 covers everything an 11-year run can reach from age 40-85.
  pop <- open_pop_grid(2025:2035, 40:120)
  init <- urpssim:::.dmdm_open_agents(pop[pop$year == 2025 & pop$age <= 85, c("age", "population")],
                            2025, 40, 3000, seed = 1)
  r <- sim_open(init, NULL, 2025, 2035, pop_by_age_year = pop)
  aud <- dmdm_population_audit(r)
  expect_true(all(aud$share_unanchored == 0))
  expect_true(all(aud$population_unanchored == 0))
  # Fully anchored means the reported population IS the projection's total over
  # the ages the model spans.
  expect_equal(r$population[1], sum(pop$population[pop$year == 2025 & pop$age %in% 40:85]))
})

test_that("strict mode refuses a leaking reweight, and the tolerance is a knob", {
  pop <- open_pop_grid(2025:2032, 40:85)
  init <- urpssim:::.dmdm_open_agents(pop[pop$year == 2025, c("age", "population")],
                            2025, 40, 2000, seed = 2)
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE")
          else Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)

  Sys.setenv(REPRODUCIBILITY_MODE = "strict")
  expect_error(sim_open(init, NULL, 2025, 2032, pop_by_age_year = pop),
               "Population conservation")
  # Declaring a wider tolerance proceeds knowingly. Assert on the absence of the
  # conservation message specifically rather than on total silence: the run also
  # emits the exploratory-transitions declaration, which expect_silent would
  # catch without saying anything about conservation.
  #
  # Called through simulate_dmdm_open() directly, NOT through sim_open(): the
  # helper muffles the EXPLORATORY declaration, so capturing through it returned
  # ZERO messages and expect_false(any(...)) passed on an empty vector -- for
  # exactly the reason the comment above says it should not. Found by
  # instrumenting all()/any() across the suite in adversarial cycle 07.
  msgs <- capture_messages(
    simulate_dmdm_open(init, NULL, 2025, 2032, pop_by_age_year = pop,
                       conservation_tolerance = 0.9, allow_uncalibrated = TRUE))
  # The capture must have seen SOMETHING, or the absence below proves nothing.
  expect_true(length(msgs) > 0L)
  expect_true(any(grepl("EXPLORATORY", msgs)))
  expect_false(any(grepl("Population conservation", msgs)))
})

test_that("a run without reweighting has no audit and no conservation claim", {
  i <- mk_agents(40:84, 2, 1e5)
  r <- sim_open(i, NULL, 2025, 2030)
  expect_null(dmdm_population_audit(r))
  expect_false("share_unanchored" %in% names(r))
})
