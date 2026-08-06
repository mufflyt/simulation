# DMDM refinements: Census reweighting (R/demand-dynamic_open), SWAN-style fitting (R/demand-dmdm_fit_transitions),
# and the DMDM -> demand-contract bridge (R/reporting-export_demand_contract.R).

mk_agents <- function(ages, vag, w) {
  data.frame(age = ages, cumulative_vaginal_deliveries = vag,
             years_since_last_vaginal_birth = pmax(0, ages - 30),
             bmi = 28, hysterectomy = 0, menopause_status = as.integer(ages >= 51),
             comorbidity = 0, weight = w, p_ui = .05, p_pop = .025, p_ai = .025)
}
entrants_for <- function(vag, w, years) {
  do.call(rbind, lapply(years, function(y) { d <- mk_agents(40, vag, w); d$entry_year <- y; d }))
}

# Reweighting arithmetic holds whatever the transition coefficients are, so the
# reweighting tests declare the exploratory override the calibration gate wants.
sim_open <- function(...) {
  suppressMessages(simulate_dmdm_open(..., allow_uncalibrated = TRUE))
}

# ---- 1. Census reweighting ------------------------------------------------

test_that("reweighting makes population match the projection while rates stay model-driven", {
  init <- mk_agents(40:84, 2, 1e5); ent <- entrants_for(2, 1e5, 2026:2030)
  proj <- do.call(rbind, lapply(2025:2030, function(y)
    data.frame(year = y, age = 40:90, population = 1e6)))
  rw <- sim_open(init, ent, 2025, 2030, pop_by_age_year = proj)
  no <- sim_open(init, ent, 2025, 2030)
  expect_equal(rw$population[1], 45e6, tolerance = 1)          # 45 ages x 1e6
  expect_equal(rw$prev_pop[1], no$prev_pop[1], tolerance = 1e-9)  # year-1 uniform mix preserved
  expect_true(rw$population[6] != no$population[6])            # counts track the projection
  expect_true(all(rw$prev_pop >= 0 & rw$prev_pop <= 1))
})

test_that("population follows a rising projection", {
  init <- mk_agents(40:84, 2, 1e5); ent <- entrants_for(2, 1e5, 2026:2030)
  proj <- do.call(rbind, lapply(2025:2030, function(y)
    data.frame(year = y, age = 40:90, population = 1e6 * (1 + 0.05 * (y - 2025)))))
  rw <- sim_open(init, ent, 2025, 2030, pop_by_age_year = proj)
  expect_gt(rw$population[6], rw$population[1])
})

# ---- 2. Fitting transitions (SWAN-style) ----------------------------------

test_that("fit_dmdm_transitions recovers onset coefficients and is engine-usable", {
  set.seed(1); N <- 40000
  age <- sample(40:85, N, TRUE); vag <- rpois(N, 2)
  df <- data.frame(
    condition = "pop", from = 0L, age = age, cumulative_vaginal_deliveries = vag,
    years_since_last_vaginal_birth = pmax(0, age - 30),
    bmi = pmax(16, rnorm(N, 28, 5)), hysterectomy = rbinom(N, 1, .2),
    menopause_status = as.integer(age >= 51), comorbidity = rbinom(N, 1, .3))
  lp <- -3.0 + 0.30 * df$cumulative_vaginal_deliveries + 0.30 * ((df$age - 50) / 10)
  df$event <- rbinom(N, 1, plogis(lp))
  # add a few at-risk==1 rows so remission is estimable
  rem <- df[1:2000, ]; rem$from <- 1L; rem$event <- rbinom(2000, 1, 0.05)
  tr <- fit_dmdm_transitions(rbind(df, rem), conditions = "pop")
  expect_identical(tr$status, "fitted")
  expect_equal(unname(tr$onset$pop["avag"]), 0.30, tolerance = 0.05)
  # expect_equal()'s `tolerance` is RELATIVE, so 0.02 demanded recovery to within
  # 2% of 0.05 -- i.e. +/- 0.001 -- from 2,000 Bernoulli draws whose standard
  # error is 0.0049. The observed 0.056 is 1.2 SE out and the test failed on
  # ordinary sampling variation. State the intended ABSOLUTE bound instead.
  expect_lt(abs(unname(tr$remission[["pop"]]) - 0.05), 0.02)
  # usable directly by the engine
  co <- mk_agents(50:70, 2, 1e5)[, setdiff(names(mk_agents(50, 2, 1)), c("weight","p_ui","p_pop","p_ai"))]
  full <- dmdm_default_transitions(); full$onset$pop <- tr$onset$pop
  # Only the POP onset block was fitted here; UI and AI are still placeholders,
  # so the object's status is unchanged and the run is declared exploratory.
  out <- suppressMessages(
    simulate_dmdm(co, 2025, 2030, transitions = full, seed = 1,
                  allow_uncalibrated = TRUE))
  expect_equal(nrow(out), 6L)
})

test_that("dmdm_transition_data reshapes a state panel into at-risk rows", {
  panel <- tibble::tibble(
    person_id = c(1, 1, 2, 2), year = c(2013L, 2014L, 2013L, 2014L),
    age = c(50, 51, 60, 61), cumulative_vaginal_deliveries = c(2, 2, 0, 0),
    years_since_last_vaginal_birth = c(20, 21, 0, 0), bmi = 28, hysterectomy = 0,
    menopause_status = c(0, 0, 1, 1), comorbidity = 0,
    has_ui = c(0, 1, 0, 0), has_pop = c(0, 0, 1, 1), has_ai = 0)
  td <- dmdm_transition_data(panel, conditions = c("ui", "pop"))
  expect_true(all(c("person_id", "condition", "from", "event") %in% names(td)))
  ui1 <- td[td$condition == "ui" & td$person_id == 1, ]
  expect_equal(ui1$from, 0L); expect_equal(ui1$event, 1L)   # 0 -> 1 onset
})

# ---- 3. DMDM -> demand-contract bridge ------------------------------------

# These assert the contract SCHEMA and the provenance stamping, on deliberately
# placeholder trajectories -- so each export declares itself exploratory, which
# the calibration gate now requires before it will write a contract file.
export_dmdm <- function(...) {
  suppressMessages(export_dmdm_demand_contract(..., allow_uncalibrated = TRUE))
}

test_that("export_dmdm_demand_contract emits tier3 + per-condition tiers", {
  tr <- data.frame(year = 2025:2030, population = seq(45e6, 48e6, length.out = 6),
                   prev_ui = seq(.20, .26, length.out = 6),
                   prev_pop = seq(.08, .14, length.out = 6),
                   prev_ai = seq(.05, .07, length.out = 6))
  out <- export_dmdm(tr, output_directory = tempfile("dm_"), verbose = FALSE)
  dc <- out$data
  expect_setequal(unique(dc$denominator_tier),
                  c("tier3_prevalent_pfd", "dmdm_ui", "dmdm_pop", "dmdm_ai"))
  expect_true(all(dc$model == "DMDM"))
  t3 <- dc[dc$denominator_tier == "tier3_prevalent_pfd", ]
  ui <- dc[dc$denominator_tier == "dmdm_ui", ]
  expect_true(all(t3$prevalence >= ui$prevalence))            # any-PFD is the union
  expect_equal(t3$denominator_index[t3$calendar_year == 2025], 100)
})

test_that("export_dmdm_demand_contract stamps per-tier provenance from `transitions`", {
  tr <- data.frame(year = 2025:2030, population = seq(45e6, 48e6, length.out = 6),
                   prev_ui = seq(.20, .26, length.out = 6),
                   prev_pop = seq(.08, .14, length.out = 6),
                   prev_ai = seq(.05, .07, length.out = 6))
  out <- export_dmdm(
    tr, output_directory = tempfile("dmp_"), verbose = FALSE,
    transitions = dmdm_transitions_with_pop_literature())
  dc <- out$data
  expect_true("tier_calibration_status" %in% names(dc))
  # object-level status comes from the transitions object
  expect_true(all(dc$calibration_status == "derived_by_analogy"))
  status_of <- function(tier) unique(dc$tier_calibration_status[dc$denominator_tier == tier])
  expect_identical(status_of("dmdm_pop"), "derived_by_analogy")     # the POP row is literature-derived
  expect_identical(status_of("dmdm_ui"), "placeholder_uncalibrated")
  expect_identical(status_of("dmdm_ai"), "placeholder_uncalibrated")
  # any-PFD is only as calibrated as its weakest input condition
  expect_identical(status_of("tier3_prevalent_pfd"), "placeholder_uncalibrated")
})

test_that("without `transitions`, the per-tier status mirrors the object status (back-compat)", {
  tr <- data.frame(year = 2025:2027, population = 45e6,
                   prev_ui = .2, prev_pop = .1, prev_ai = .05)
  out <- export_dmdm(tr, output_directory = tempfile("dmn_"), verbose = FALSE)
  expect_true(all(out$data$tier_calibration_status == "placeholder_uncalibrated"))
})
