# Geographic held-out (spatial) cross-validation (R/validation-geographic_holdout.R).
#
# External-validity harness: predicting held-out geographies is out-of-sample
# along a dimension that played no part in the temporal model selection. These
# tests assert the harness (1) recovers a real geographic relationship OOS,
# (2) does NOT reward a non-relationship, (3) is leakage-free, (4) supports
# leave-one-region-out, and (5) guards its inputs.

make_geo <- function(G = 60, seed = 42, depends_on_x = TRUE, lambda0 = 20) {
  set.seed(seed)
  x <- stats::runif(G, 0.5, 5)
  region <- rep(c("NE", "S", "MW", "W"), length.out = G)
  lambda <- if (depends_on_x) exp(1.2 + 0.5 * x) else rep(lambda0, G)
  data.frame(geo = paste0("g", seq_len(G)), region = region, x = x,
             obs = stats::rpois(G, lambda), stringsAsFactors = FALSE)
}

test_that("leave-one-geography-out recovers a real spatial relationship", {
  r <- geographic_holdout_cv(make_geo(), "obs", "x", geo = "geo", scheme = "loo")
  expect_equal(nrow(r$predictions), 60L)
  expect_true(all(!is.na(r$predictions$predicted)))
  expect_gt(r$metrics$spearman, 0.6)
  expect_gt(r$metrics$r2_oos, 0.3)
  expect_lt(abs(r$metrics$calibration_slope - 1), 0.4)        # well-calibrated OOS
})

test_that("the harness does not reward a non-relationship (discrimination)", {
  # observed independent of the predictor -> out-of-sample R^2 near or below 0
  r0 <- geographic_holdout_cv(make_geo(depends_on_x = FALSE), "obs", "x",
                              geo = "geo", scheme = "loo")
  expect_lt(r0$metrics$r2_oos, 0.15)
})

test_that("prediction is leakage-free: a held-out geography cannot predict itself", {
  d <- make_geo(); d$obs[1] <- 100000L                         # wild outlier
  r <- geographic_holdout_cv(d, "obs", "x", geo = "geo", scheme = "loo")
  p1 <- r$predictions$predicted[r$predictions$geo == "g1"]
  # driven by the OTHER geographies' x-relationship, not its own inflated value
  expect_lt(p1, 1000)
})

test_that("leave-one-region-out runs one fold per region", {
  r <- geographic_holdout_cv(make_geo(), "obs", "x", geo = "geo",
                             region = "region", scheme = "region")
  expect_equal(length(unique(r$predictions$fold)), 4L)
  expect_true(all(!is.na(r$predictions$predicted)))
  expect_true(all(r$predictions$region %in% c("NE", "S", "MW", "W")))
})

test_that("k-fold is reproducible under a seed", {
  a <- geographic_holdout_cv(make_geo(), "obs", "x", scheme = "kfold", k = 5, seed = 7)
  b <- geographic_holdout_cv(make_geo(), "obs", "x", scheme = "kfold", k = 5, seed = 7)
  expect_equal(a$predictions$predicted, b$predictions$predicted)
})

test_that("a custom fit_predict is honoured and never sees the held-out target", {
  d <- make_geo()
  # constant predictor built ONLY from training rows: proves test rows are unseen
  const_from_train <- function(train, test) rep(mean(train$obs), nrow(test))
  r <- geographic_holdout_cv(d, "obs", "x", geo = "geo", scheme = "loo",
                             fit_predict = const_from_train)
  expect_equal(nrow(r$predictions), nrow(d))
  # each prediction is the leave-one-out mean of the others, not the row itself
  expect_equal(r$predictions$predicted[1], mean(d$obs[-1]))
})

test_that("geographic_holdout_cv guards its inputs", {
  d <- make_geo()
  expect_error(geographic_holdout_cv(d, "obs", "nope", geo = "geo"), "missing column")
  expect_error(geographic_holdout_cv(d[1:2, ], "obs", "x"), "at least")   # too few geos
  expect_error(geographic_holdout_cv(d, "obs", "x", scheme = "region"),
               "needs a `region`")
  bad_fp <- function(train, test) rep(1, nrow(test) + 1L)
  expect_error(geographic_holdout_cv(d, "obs", "x", fit_predict = bad_fp),
               "returned")
})
