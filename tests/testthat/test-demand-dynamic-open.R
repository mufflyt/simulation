# Open-population DMDM (R/30-demand_dynamic_open.R). The engine is deterministic
# base R; the trajectory wrapper uses R/25.

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
  out <- simulate_dmdm_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  expect_equal(nrow(out), 26L)
  expect_true(all(out$population > 0.5 * out$population[1]))   # replenished
  expect_true(all(out$prev_pop >= 0 & out$prev_pop <= 1))
  expect_true(all(out$inc_pop > 0))
})

test_that("prevalence reaches a quasi-steady state (unlike a closed cohort)", {
  out <- simulate_dmdm_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  early <- abs(out$prev_pop[6]  - out$prev_pop[2])
  late  <- abs(out$prev_pop[26] - out$prev_pop[22])
  expect_lt(late, early)
})

test_that("more cumulative vaginal deliveries raise population prolapse prevalence", {
  hi <- simulate_dmdm_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  lo <- simulate_dmdm_open(mk_agents(40:84, 0, 1e5), entrants_for(0, 1e5), 2025, 2050)
  expect_gt(hi$prev_pop[26], lo$prev_pop[26])
})

test_that("without entrants the population declines (open vs closed contrast)", {
  open   <- simulate_dmdm_open(mk_agents(40:84, 2, 1e5), entrants_for(2, 1e5), 2025, 2050)
  closed <- simulate_dmdm_open(mk_agents(40:84, 2, 1e5), NULL, 2025, 2050)
  expect_lt(closed$population[26], open$population[26])
})

test_that("the engine is deterministic (no RNG)", {
  i <- mk_agents(40:84, 2, 1e5); en <- entrants_for(2, 1e5)
  expect_equal(simulate_dmdm_open(i, en, 2025, 2035),
               simulate_dmdm_open(i, en, 2025, 2035))
})

test_that("the trajectory wrapper builds an open population from projections", {
  pop_by_age_year <- do.call(rbind, lapply(2025:2030, function(y)
    data.frame(year = y, age = 40:85,
               population = round(2e6 * exp(-0.02 * (40:85 - 40)) * (1 + 0.01 * (y - 2025))))))
  pop_by_age_year <- tibble::as_tibble(pop_by_age_year)
  tr <- dmdm_open_prevalence_trajectory(pop_by_age_year, 2025, 2030,
                                        n_init = 4000, n_entrants = 500, seed = 1)
  expect_equal(nrow(tr), 6L)
  expect_true(all(tr$population > 0))
  expect_true(all(tr$prev_pop >= 0 & tr$prev_pop <= 1))
})
