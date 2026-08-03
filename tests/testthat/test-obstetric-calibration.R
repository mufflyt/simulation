# Guards for the cited birth-cohort obstetric calibration of R/25's
# .lifecourse_population (reuses R/13b cohort_vaginal_exposure()).

test_that("the baseline uses the cited cohort-varying cesarean, not a scalar", {
  pa <- tibble::tibble(age = 40:90, population = rep(1e6, 51))
  out <- simulate_lifecourse_demand(pa, year = 2040, seed = 1, n = 5000)
  expect_match(out$meta$status, "cohort_marginals_cited")
  expect_identical(out$meta$cesarean_rate, "cited_cohort_varying")
  expect_true(all(out$person_years$cumulative_vaginal_deliveries <=
                    out$person_years$parity))
})

test_that("older cohorts carry more vaginal births than younger (falling parity + rising cesarean)", {
  pa_old <- tibble::tibble(age = 85, population = 1e6)   # 2040 -> born 1955
  pa_yng <- tibble::tibble(age = 50, population = 1e6)   # 2040 -> born 1990
  vo <- mean(simulate_lifecourse_demand(pa_old, 2040, seed = 1, n = 8000)$person_years$vaginal_births)
  vy <- mean(simulate_lifecourse_demand(pa_yng, 2040, seed = 1, n = 8000)$person_years$vaginal_births)
  expect_gt(vo, vy)
})

test_that("an explicit cesarean_rate still overrides the cited baseline (scenario lever)", {
  pa <- tibble::tibble(age = 40:90, population = rep(1e6, 51))
  pv <- function(o) sum(o$service_volumes$volume)
  base <- simulate_lifecourse_demand(pa, 2040, seed = 1, n = 5000)
  csec <- simulate_lifecourse_demand(pa, 2040, scenario = "delivery_mode",
                                     cesarean_rate = 0.70, seed = 1, n = 5000)
  expect_lt(pv(csec), pv(base))            # more cesarean -> fewer vaginal births -> less demand
})

test_that("cited risk params anchor the vaginal-delivery effect and flip the status", {
  cited <- lifecourse_risk_params_cited()
  expect_equal(cited$status, "obstetric_literature_anchored")
  expect_gt(cited$pop$bvag, 0)
  expect_true(cited$pop$bvag >= cited$ui$bvag)   # POP the strongest vaginal driver
  pa <- tibble::tibble(age = 40:90, population = rep(1e6, 51))
  out <- simulate_lifecourse_demand(pa, 2040, seed = 1, n = 4000,
                                    risk_params = cited)
  expect_match(out$meta$status, "risk_coeffs_literature_anchored")
})
