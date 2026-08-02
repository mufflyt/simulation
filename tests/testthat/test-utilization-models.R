# Corrected utilization models (R/26-utilization_models.R).
# Base R at the core; the survey/negbin paths are exercised only when those
# optional packages are installed.

make_cells <- function(n = 4000, seed = 1) {
  set.seed(seed)
  AgeGrp  <- factor(sample(c("40-59", "60-79", "80+"), n, TRUE))
  Obesity <- rbinom(n, 1, 0.3)
  pop     <- sample(500:5000, n, TRUE)
  lograte <- -6 + 0.5 * Obesity + 0.7 * (AgeGrp == "60-79") + 0.4 * (AgeGrp == "80+")
  tibble::tibble(n_slings = rpois(n, pop * exp(lograte)),
                 AgeGrp = AgeGrp, Obesity = Obesity, pop_at_risk = pop)
}

test_that("sling rate model recovers the known log-rate structure", {
  m <- fit_sling_rate_model(make_cells(), n_slings ~ AgeGrp + Obesity)
  co <- coef(m)
  expect_equal(unname(co[["(Intercept)"]]), -6, tolerance = 0.15)
  expect_equal(unname(co[["Obesity"]]),      0.5, tolerance = 0.15)
  expect_equal(unname(co[["AgeGrp60-79"]]),  0.7, tolerance = 0.2)
  expect_identical(attr(m, "hdmm_offset_col"), "pop_at_risk")
})

test_that("rate model rejects non-positive population at risk", {
  expect_error(
    fit_sling_rate_model(transform(make_cells(), pop_at_risk = 0), n_slings ~ Obesity),
    "population at risk")
})

test_that("rate model accepts weights", {
  cells <- make_cells()
  mw <- fit_sling_rate_model(transform(cells, wt = runif(nrow(cells), 0.5, 2)),
                             n_slings ~ Obesity, weights_col = "wt")
  expect_true(all(is.finite(coef(mw))))
})

test_that("visit model falls back to weighted quasipoisson and warns", {
  set.seed(2); n <- 3000
  dat <- tibble::tibble(
    UI_visits = rpois(n, 0.5), Age = runif(n, 40, 85), Obesity = rbinom(n, 1, .3),
    VaginalParity = rpois(n, 1), ChronicCough = rbinom(n, 1, .2), HeavyJob = rbinom(n, 1, .1),
    Race = factor(sample(c("White", "Black", "Hispanic", "Other"), n, TRUE)),
    Hysterectomy = rbinom(n, 1, .2), wt = runif(n, .5, 2))
  if (!requireNamespace("survey", quietly = TRUE)) {
    expect_warning(vm <- fit_visit_model(dat, weights_col = "wt"),
                   "do NOT reflect the survey design")
    expect_identical(attr(vm, "hdmm_method"), "stats::glm(quasipoisson)")
    expect_true(all(is.finite(coef(vm))))
  } else {
    vm <- fit_visit_model(dat, weights_col = "wt")
    expect_true(grepl("svyglm", attr(vm, "hdmm_method")))
  }
})

test_that("param draws recover the coefficient mean and covariance", {
  m <- fit_sling_rate_model(make_cells(), n_slings ~ AgeGrp + Obesity)
  D <- .param_draw(m, 20000)
  expect_equal(colMeans(D), coef(m)[colnames(D)], tolerance = 0.03)
  expect_equal(cov(D), vcov(m)[colnames(D), colnames(D)], tolerance = 0.03)
})

test_that("prediction with uncertainty is obs x draws, centered on the point estimate", {
  cells <- make_cells()
  m <- fit_sling_rate_model(cells, n_slings ~ AgeGrp + Obesity)
  nd <- tibble::tibble(AgeGrp = factor(c("40-59", "60-79", "80+"), levels = levels(cells$AgeGrp)),
                       Obesity = c(0, 1, 0))
  P  <- predict_count_with_uncertainty(m, nd, n_draws = 4000)
  pt <- .expected_count(m, nd)
  expect_equal(dim(P), c(3L, 4000L))
  expect_equal(rowMeans(P), pt, tolerance = 0.05 * max(pt))
  # offset scales counts: N people => N * per-person rate
  expect_equal(.expected_count(m, nd[2, , drop = FALSE], offset = log(1000)),
               pt[2] * 1000, tolerance = 1e-6)
})
