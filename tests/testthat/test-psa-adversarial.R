# Semantic + adversarial guards for the PSA engine (R/psa.R).
#
# The happy-path recovery tests live in test-psa.R. This file pins two further
# classes of property:
#   * SEMANTIC   -- invariants a rank-based global sensitivity method must hold
#                   (monotone-transform invariance, null inputs read as ~0, a
#                   discrete driver recovers its sign, PRCC and SRRC agree).
#   * ADVERSARIAL -- degenerate / hostile inputs must degrade to NA, never crash
#                   and never fabricate a sensitivity. Two of these lock in fixes
#                   for real flaws: psa_prcc() used to return a spurious non-zero
#                   PRCC for a constant output, and psa_srrc() used to error on it.

# ---- Semantic invariants ---------------------------------------------------

test_that("PRCC is invariant to a monotone transform of the output", {
  # Rank-based => rank(exp(y)) == rank(y), so PRCC is identical. Same seed gives
  # the same input draws, isolating the transform.
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  lin  <- run_psa(inputs, function(p) 3 * p$a - 2 * p$b, n = 300, seed = 11, verbose = FALSE)
  mono <- run_psa(inputs, function(p) exp(3 * p$a - 2 * p$b), n = 300, seed = 11, verbose = FALSE)
  expect_equal(psa_prcc(lin)$input, psa_prcc(mono)$input)
  expect_equal(psa_prcc(lin)$prcc,  psa_prcc(mono)$prcc, tolerance = 1e-8)
})

test_that("an irrelevant input reads as ~0 and well below the real driver", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) 5 * p$a, n = 500, seed = 12, verbose = FALSE)  # ignores b
  pr <- psa_prcc(res)
  a <- abs(pr$prcc[pr$input == "a"]); b <- abs(pr$prcc[pr$input == "b"])
  expect_lt(b, 0.2)          # null input carries essentially no signal
  expect_gt(a - b, 0.7)      # and is dwarfed by the true driver
})

test_that("a discrete input's effect is recovered with the correct sign", {
  inputs <- list(psa_uniform("a", 0, 1), psa_discrete("g", c("lo", "mid", "hi")))
  # Output rises with the ordered category index and with a.
  evaluate <- function(p) 2 * match(p$g, c("lo", "mid", "hi")) + p$a
  res <- run_psa(inputs, evaluate, n = 300, seed = 13, verbose = FALSE)
  pr <- psa_prcc(res)
  expect_gt(pr$prcc[pr$input == "g"], 0.5)   # strong positive driver
})

test_that("PRCC and SRRC agree on the sign of every input", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1), psa_uniform("c", 0, 1))
  res <- run_psa(inputs, function(p) 3 * p$a - 2 * p$b + 0.5 * p$c, n = 400, seed = 7, verbose = FALSE)
  pr <- psa_prcc(res); sr <- psa_srrc(res)$coefficients
  m <- merge(pr[c("input", "prcc")], sr[c("input", "srrc")], by = "input")
  expect_true(all(sign(m$prcc) == sign(m$srrc)))
})

test_that("the triangular inverse-CDF has the right mean and support", {
  s <- psa_sample(list(psa_triangular("t", 0, 3, 6)), n = 2000, seed = 21)
  expect_true(all(s$t >= 0 & s$t <= 6))
  expect_equal(mean(s$t), (0 + 3 + 6) / 3, tolerance = 0.15)   # triangular mean = (a+m+b)/3
})

test_that("discrete selection probabilities are respected", {
  s <- psa_sample(list(psa_discrete("g", c("x", "y", "z"), probs = c(0.6, 0.3, 0.1))),
                  n = 2000, seed = 22)
  fr <- prop.table(table(factor(s$g_value, levels = c("x", "y", "z"))))
  expect_equal(as.numeric(fr), c(0.6, 0.3, 0.1), tolerance = 0.03)
})

# ---- Adversarial / robustness ----------------------------------------------

test_that("every evaluation failing degrades to all-NA, never crashes", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) stop("always"), n = 30, seed = 1, verbose = FALSE)
  expect_equal(res$n_failed, 30L)
  expect_true(all(is.na(res$draws$output)))
  expect_true(all(is.na(psa_prcc(res)$prcc)))          # 0 complete cases -> NA, not error
  sr <- psa_srrc(res)
  expect_true(all(is.na(sr$coefficients$srrc)))
  expect_true(is.na(sr$model_r2))
})

test_that("a constant output yields NA, not a fabricated sensitivity", {
  # Regression guard: psa_prcc() once returned a spurious ~0.13 here (residual
  # noise), and psa_srrc() errored ('0 non-NA cases').
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) 42, n = 100, seed = 2, verbose = FALSE)
  expect_true(all(is.na(psa_prcc(res)$prcc)))
  sr <- psa_srrc(res)
  expect_true(all(is.na(sr$coefficients$srrc)))
  expect_true(is.na(sr$model_r2))
})

test_that("a pinned (constant) input reports NA while the others still resolve", {
  # b is constant (pinned); a and c vary so the fit is not degenerately perfect.
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 5, 5), psa_uniform("c", 0, 1))
  res <- run_psa(inputs, function(p) 3 * p$a - 2 * p$c + p$b, n = 200, seed = 3, verbose = FALSE)
  pr <- psa_prcc(res)
  expect_true(is.na(pr$prcc[pr$input == "b"]))
  expect_false(is.na(pr$prcc[pr$input == "a"]))
  sr <- psa_srrc(res)$coefficients
  expect_true(is.na(sr$srrc[sr$input == "b"]))
  expect_false(is.na(sr$srrc[sr$input == "a"]))
})

test_that("too few complete draws degrade to NA, never crash", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  for (nn in c(1L, 2L)) {
    res <- run_psa(inputs, function(p) p$a - p$b, n = nn, seed = 4, verbose = FALSE)
    expect_true(all(is.na(psa_prcc(res)$prcc)))
    expect_true(is.na(psa_srrc(res)$model_r2))
  }
})

test_that("heavy ties in the output are handled and stay finite", {
  inputs <- list(psa_uniform("a", 0, 1), psa_uniform("b", 0, 1))
  res <- run_psa(inputs, function(p) round(3 * p$a - 2 * p$b), n = 300, seed = 5, verbose = FALSE)
  expect_true(all(is.finite(psa_prcc(res)$prcc)))
})

test_that("psa_sample does not perturb the caller's RNG stream", {
  inputs <- list(psa_uniform("a", 0, 1))
  set.seed(123); x1 <- runif(5); invisible(psa_sample(inputs, 100, seed = 7)); x2 <- runif(5)
  set.seed(123); y1 <- runif(5); y2 <- runif(5)
  expect_identical(c(x1, x2), c(y1, y2))
})

test_that("LHS places exactly one draw in each 1/n stratum", {
  s <- psa_sample(list(psa_uniform("a", 0, 1)), n = 50, seed = 8)
  bin <- findInterval(s$a, seq(0, 1, by = 1 / 50), rightmost.closed = TRUE)
  expect_setequal(sort(bin), 1:50)
})

test_that("malformed input specs are rejected at construction", {
  expect_error(psa_triangular("t", 0, 5, 3))                        # mode > max
  expect_error(psa_discrete("g", c("x", "y"), probs = c(0.5, 0.4))) # probs sum != 1
  expect_error(psa_discrete("g", c("x", "y"), probs = c(0.5, 0.3, 0.2)))  # length mismatch
})
