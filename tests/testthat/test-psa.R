# Guards for the probabilistic-sensitivity-analysis engine.
# The engine is validated against a SYNTHETIC model whose sensitivities are
# known, so PRCC/SRRC must recover the correct signs and ranking.

test_that("LHS sampling stratifies each input over its range", {
  inputs <- list(psa_uniform("a", 0, 10), psa_normal("b", 5, 2),
                 psa_discrete("g", c("x", "y", "z")))
  s <- psa_sample(inputs, n = 300, seed = 1)
  expect_equal(nrow(s), 300L)
  expect_true(all(s$a >= 0 & s$a <= 10))
  # LHS: the uniform marginal covers the range roughly evenly (mean near midpoint).
  expect_equal(mean(s$a), 5, tolerance = 0.5)
  # Discrete input carries an index column and a mapped-value column.
  expect_true(all(s$g %in% 1:3))
  expect_setequal(unique(s$g_value), c("x", "y", "z"))
  # Reproducible.
  expect_equal(psa_sample(inputs, 300, seed = 1), s)
})

test_that("run_psa evaluates every draw and records inputs + outputs", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) p$a + p$b, n = 50, seed = 2, verbose = FALSE)
  expect_equal(nrow(res$draws), 50L)
  expect_true(all(c("a", "b", "output") %in% names(res$draws)))
  expect_equal(res$draws$output, res$draws$a + res$draws$b, tolerance = 1e-9)
  expect_equal(res$n_failed, 0L)
})

test_that("PRCC recovers known signs and ranking of a monotone model", {
  # y = 3a - 2b + 0.5c + small noise; a strongest (+), b strong (-), c weak (+).
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1), psa_uniform("c", 0, 1))
  evaluate <- function(p) 3 * p$a - 2 * p$b + 0.5 * p$c
  res <- run_psa(inputs, evaluate, n = 400, seed = 7, verbose = FALSE)

  pr <- psa_prcc(res)
  get <- function(v) pr$prcc[pr$input == v]
  expect_gt(get("a"), 0.9)          # strong positive
  expect_lt(get("b"), -0.8)         # strong negative
  expect_gt(get("c"), 0)            # weak positive
  # Ranking by |PRCC|: a > b > c.
  expect_equal(pr$input, c("a", "b", "c"))
  # Significant drivers.
  expect_lt(pr$p_value[pr$input == "a"], 1e-6)
})

test_that("SRRC variance shares are non-negative and sum to the model R^2", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1), psa_uniform("c", 0, 1))
  res <- run_psa(inputs, function(p) 3 * p$a - 2 * p$b + 0.5 * p$c, n = 400,
                 seed = 7, verbose = FALSE)
  s <- psa_srrc(res)
  expect_true(all(s$coefficients$var_share >= 0))
  expect_equal(sum(s$coefficients$var_share), s$model_r2, tolerance = 1e-8)
  expect_gt(s$model_r2, 0.95)                       # near-deterministic model
  expect_equal(s$coefficients$input[1], "a")        # a explains the most variance
})

test_that("tornado table is ranked by magnitude with signed direction", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) 3 * p$a - 2 * p$b, n = 200, seed = 3, verbose = FALSE)
  tor <- psa_tornado(res, method = "prcc")
  expect_equal(tor$input, c("a", "b"))
  expect_equal(tor$direction, c("increases", "decreases"))
  expect_true(abs(tor$index[1]) >= abs(tor$index[2]))
})

test_that("failed evaluations degrade to NA and are counted, not fatal", {
  inputs <- list(psa_uniform("a", 0, 1))
  res <- run_psa(inputs, function(p) if (p$a > 0.5) stop("boom") else p$a,
                 n = 100, seed = 4, verbose = FALSE)
  expect_gt(res$n_failed, 0)
  expect_true(any(is.na(res$draws$output)))
  # PRCC still computes on the complete cases.
  expect_true(is.finite(psa_prcc(res)$prcc[1]))
})

test_that("plot_psa_tornado returns a ggplot", {
  skip_if_not(requireNamespace("ggplot2", quietly = TRUE), "ggplot2 not installed")
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) 3 * p$a - 2 * p$b, n = 100, seed = 5, verbose = FALSE)
  g <- plot_psa_tornado(psa_tornado(res))
  expect_s3_class(g, "ggplot")
})

test_that("named-vector evaluate records multiple outputs", {
  inputs <- list(psa_uniform("a", 0, 1))
  res <- run_psa(inputs, function(p) c(lo = p$a, hi = 2 * p$a), n = 30, seed = 6, verbose = FALSE)
  expect_true(all(c("lo", "hi") %in% names(res$draws)))
  expect_equal(res$output_names, c("lo", "hi"))
})

test_that("psa_workforce_gap wiring needs the contract or an explicit base supply", {
  skip_if(requireNamespace("mufflyaccess", quietly = TRUE), "mufflyaccess installed")
  expect_error(psa_workforce_gap(n = 1, verbose = FALSE), "mufflyaccess")
})
