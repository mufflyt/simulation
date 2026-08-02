# Dynamic multistate disease model (R/29-demand_dynamic_multistate.R).
# The engine (simulate_dmdm) is base R; the cohort builder/trajectory use R/25.

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
  out <- simulate_dmdm(mk_cohort(2L), 2025, 2040, seed = 42)
  expect_equal(nrow(out), 16L)
  expect_true(all(c("year", "living", "deaths", "prev_ui", "prev_pop", "prev_ai",
                    "inc_ui", "inc_pop", "inc_ai") %in% names(out)))
  expect_true(all(out$prev_pop >= 0 & out$prev_pop <= 1))
})

test_that("a closed cohort shrinks through mortality", {
  out <- simulate_dmdm(mk_cohort(1L), 2025, 2045, seed = 42)
  expect_true(all(diff(out$living) <= 0))
  expect_lt(out$living[nrow(out)], out$living[1])
  expect_gt(sum(out$deaths), 0)
})

test_that("cumulative vaginal deliveries drive prolapse onset (the primary exposure)", {
  lo <- simulate_dmdm(mk_cohort(0L), 2025, 2040, seed = 42)
  hi <- simulate_dmdm(mk_cohort(3L), 2025, 2040, seed = 42)
  expect_gt(hi$prev_pop[10], lo$prev_pop[10])
  expect_gt(hi$inc_pop[1],  lo$inc_pop[1])
})

test_that("remission lowers long-run prevalence", {
  no_rem <- dmdm_default_transitions(); no_rem$remission <- c(ui = 0, pop = 0, ai = 0)
  a <- simulate_dmdm(mk_cohort(2L), 2025, 2045, transitions = no_rem, seed = 42)
  b <- simulate_dmdm(mk_cohort(2L), 2025, 2045, seed = 42)
  expect_gt(a$prev_ui[nrow(a)], b$prev_ui[nrow(b)])
})

test_that("the engine is deterministic given a seed", {
  expect_equal(simulate_dmdm(mk_cohort(2L), 2025, 2035, seed = 7),
               simulate_dmdm(mk_cohort(2L), 2025, 2035, seed = 7))
})

test_that("the trajectory wrapper seeds year-0 prevalence and evolves it", {
  pop_by_age <- tibble::tibble(age = 40:85,
                               population = round(2e6 * exp(-0.02 * (40:85 - 40))))
  tr <- dmdm_prevalence_trajectory(pop_by_age, 2025, 2035, n = 6000, seed = 1)
  expect_equal(nrow(tr), 11L)
  expect_gt(tr$prev_ui[1], 0)                    # seeded from the cross-sectional risk
  expect_true(all(tr$prev_ui >= 0 & tr$prev_ui <= 1))
})
