# Guards for the twostep-ported access-inference statistics and the M2SFCA toggle.

test_that("weighted_mean_all is sparse-group safe", {
  expect_equal(weighted_mean_all(c(1, 2, 3), c(1, 1, 1)), 2)
  expect_equal(weighted_mean_all(c(5, 10), c(3, 1)), (5 * 3 + 10) / 4)
  expect_true(is.na(weighted_mean_all(c(1, 2), c(0, 0))))   # zero weights -> NA, not NaN
})

test_that("zero_access_share counts EXACTLY-zero weighted population", {
  acc <- c(0, 0, 5, 12)
  w <- c(10, 30, 40, 20)
  expect_equal(zero_access_share(acc, w), 100 * 40 / 100)   # 40 of 100 have access 0
  expect_equal(zero_access_share(c(1, 2, 3), c(1, 1, 1)), 0)
  expect_true(is.na(zero_access_share(c(0, 1), c(0, 0))))
})

test_that("access_moe_ci brackets the point estimate and is reproducible", {
  set.seed(99)
  access <- runif(200, 0, 50)
  est <- runif(200, 1e4, 5e4)
  se <- est * 0.05
  ci <- access_moe_ci(access, est, se, stat = "mean", B = 500, seed = 7)
  expect_named(ci, c("point", "lo", "hi"))
  expect_lte(ci[["lo"]], ci[["point"]])     # unbiased: point inside its interval
  expect_gte(ci[["hi"]], ci[["point"]])
  # Same seed -> identical bounds.
  ci2 <- access_moe_ci(access, est, se, stat = "mean", B = 500, seed = 7)
  expect_equal(ci, ci2)
})

test_that("access_moe_ci restores the caller's RNG stream", {
  set.seed(123)
  before <- .Random.seed
  invisible(access_moe_ci(runif(10), runif(10, 1, 2), rep(0.1, 10), B = 50, seed = 5))
  expect_identical(.Random.seed, before)    # caller's stream untouched
})

test_that("annual_trend recovers a known slope", {
  yr <- 2013:2023
  val <- 3 + 2 * (yr - 2013)                 # exact slope 2
  tr <- annual_trend(yr, val)
  expect_equal(unname(tr[["slope"]]), 2, tolerance = 1e-8)
  expect_lte(tr[["lo"]], 2); expect_gte(tr[["hi"]], 2)
  expect_true(all(is.na(annual_trend(2013:2014, c(1, 2)))))  # < 3 points
})

test_that("wilson_ci matches known values and stays in [0,1]", {
  # 0 successes in 10: interval must include 0 and stay non-negative.
  w0 <- wilson_ci(0, 10)
  expect_equal(w0$estimate, 0)
  expect_gte(w0$lo, 0)
  expect_lt(w0$hi, 0.31)                     # Wilson upper ~0.278 for 0/10
  # Symmetric-ish around 0.5 for 5/10, classic Wilson bounds ~0.2366-0.7634.
  w5 <- wilson_ci(5, 10)
  expect_equal(w5$estimate, 0.5)
  expect_equal(w5$lo, 0.2366, tolerance = 1e-3)
  expect_equal(w5$hi, 0.7634, tolerance = 1e-3)
  # Vectorised.
  wv <- wilson_ci(c(1, 50), c(4, 100))
  expect_equal(nrow(wv), 2L)
})

test_that("M2SFCA toggle penalises maldistribution (mean <= E2SFCA)", {
  membership <- tibble::tibble(
    demand_id   = c("X", "Y", "Y", "Z"),
    provider_id = c("A", "A", "B", "B"),
    band        = c(30, 60, 30, 120)
  )
  supply <- tibble::tibble(provider_id = c("A", "B"), supply = c(10, 5))
  demand <- tibble::tibble(demand_id = c("X", "Y", "Z"), population = c(1000, 2000, 1500))

  cmp <- compare_access_methods(membership, supply, demand)
  expect_equal(cmp$e2sfca$meta$method, "E2SFCA")
  expect_equal(cmp$m2sfca$meta$method, "M2SFCA")
  expect_lte(cmp$mean_m2sfca, cmp$mean_e2sfca + 1e-9)   # penalty only removes access
  expect_gte(cmp$penalty_share, 0)

  # compute_access() dispatches on the method name.
  expect_equal(compute_access(membership, supply, demand, "M2SFCA")$meta$step2_power, 2)
  expect_equal(compute_access(membership, supply, demand, "E2SFCA")$meta$step2_power, 1)
})
