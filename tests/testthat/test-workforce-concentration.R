# Guards for R/27-workforce_concentration.R (Gini / HHI / Lorenz / top-k).

test_that("gini is 0 for equality and (n-1)/n for a monopoly", {
  expect_equal(workforce_gini(c(25, 25, 25, 25)), 0)
  expect_equal(workforce_gini(c(100, 0, 0, 0)), 0.75)
  expect_true(is.na(workforce_gini(c(0, 0, 0))))
  expect_error(workforce_gini(c(-1, 2)))
})

test_that("gini rises as provider mass concentrates", {
  expect_lt(workforce_gini(c(10, 10, 10, 10)), workforce_gini(c(20, 10, 8, 2)))
  expect_lt(workforce_gini(c(20, 10, 8, 2)),   workforce_gini(c(38, 1, 0, 1)))
})

test_that("hhi has known values and bounds", {
  expect_equal(workforce_hhi(c(100)), 1)
  expect_equal(workforce_hhi(c(50, 50)), 0.5)
  expect_equal(workforce_hhi(c(50, 30, 20)), 0.38)
  expect_equal(workforce_hhi(c(10, 10, 10, 10), normalized = TRUE), 0)
})

test_that("lorenz starts at origin, ends at (1,1), bows below equality", {
  lc <- workforce_lorenz(c(1, 1, 1, 7))
  expect_equal(lc$cum_unit_share[1], 0); expect_equal(lc$cum_value_share[1], 0)
  expect_equal(tail(lc$cum_unit_share, 1), 1); expect_equal(tail(lc$cum_value_share, 1), 1)
  interior <- lc[-c(1, nrow(lc)), ]
  expect_true(all(interior$cum_value_share <= interior$cum_unit_share + 1e-9))
})

test_that("top_k_share captures the busiest units", {
  expect_equal(workforce_top_k_share(c(50, 30, 15, 5), k = 2), 0.8)
  expect_equal(workforce_top_k_share(c(1, 1, 1, 1), k = 10), 1)
})

test_that("provider_concentration pads the full geography and flags zero units", {
  s <- provider_concentration(c(10, 5, 3, 1, 1), n_units_total = 100, label = "state")
  expect_equal(s$n_occupied, 5)
  expect_equal(s$pct_units_zero, 95)
  expect_gt(s$gini, 0.9)
})
