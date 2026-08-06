# Forecast-evaluation scorecard (R/validation-forecast_scorecard.R).
#
# Coverage alone is a broken success measure: a deliberately wide interval passes
# 95% coverage while saying nothing. These tests pin the metric that fixes that
# (the interval score / WIS penalizes width), plus point error, bias, calibration,
# skill vs a benchmark, and rank stability across cutoffs.

test_that("the interval score penalizes width: a wide band cannot beat a sharp one", {
  a <- 0.05; y <- 10
  sharp_cover <- urpssim:::.interval_score(y, 9, 11, a)
  wide_cover  <- urpssim:::.interval_score(y, -140, 152, a)   # 292-wide, still covers
  sharp_miss  <- urpssim:::.interval_score(y, 4, 6, a)
  expect_lt(sharp_cover, wide_cover)    # sharpness rewarded among covering intervals
  expect_lt(sharp_cover, sharp_miss)    # covering rewarded at equal width
  # a covering interval pays exactly its width when it covers
  expect_equal(sharp_cover, 2)
})

test_that("WIS rewards sharpness+calibration; a diffuse forecast scores worse", {
  tau <- c(0.025, 0.25, 0.5, 0.75, 0.975)
  sharp <- weighted_interval_score(10, matrix(c(9, 9.7, 10, 10.3, 11), nrow = 1), tau)
  wide  <- weighted_interval_score(10, matrix(c(-140, -40, 10, 60, 152), nrow = 1), tau)
  expect_lt(sharp, wide)
  # vectorizes over cases
  expect_length(weighted_interval_score(c(10, 10),
    rbind(c(9, 9.7, 10, 10.3, 11), c(-140, -40, 10, 60, 152)), tau), 2)
  # requires a median column
  expect_error(weighted_interval_score(10, matrix(c(1, 2), nrow = 1), c(0.25, 0.75)),
               "median")
})

test_that("forecast_scorecard reports the full metric suite", {
  d <- data.frame(observed = c(100, 110, 120, 130), predicted = c(102, 108, 119, 133),
                  lower = c(90, 95, 110, 120), upper = c(115, 120, 130, 145))
  sc <- forecast_scorecard(d, label = "m1")
  expect_equal(sc$label, "m1")
  expect_equal(sc$coverage, 1)                                # all four covered
  expect_equal(sc$mean_width, mean(d$upper - d$lower))
  expect_equal(sc$signed_bias, mean(d$predicted - d$observed))
  expect_true(all(c("mape", "rmse", "mean_interval_score", "calibration_slope") %in% names(sc)))
  # interval columns are optional
  sc2 <- forecast_scorecard(d[, c("observed", "predicted")], lower = NULL, upper = NULL)
  expect_false("coverage" %in% names(sc2))
})

test_that("compare_forecasts scores skill vs a benchmark and rank stability", {
  cuts <- 2015:2022
  good <- data.frame(model = "good", cutoff = cuts, observed = 100 + (cuts - 2015),
                     predicted = 100 + (cuts - 2015) + rep(c(1, -1), 4) * 0.5)
  good$lower <- good$predicted - 3; good$upper <- good$predicted + 3
  naive <- data.frame(model = "naive", cutoff = cuts, observed = 100 + (cuts - 2015),
                      predicted = 100, lower = 70, upper = 130)   # wide + biased
  cmp <- compare_forecasts(rbind(good, naive), benchmark = "naive")
  # good beats the benchmark on both point and interval score
  expect_gt(cmp$scorecard$mape_skill[cmp$scorecard$model == "good"], 0)
  expect_gt(cmp$scorecard$interval_score_skill[cmp$scorecard$model == "good"], 0)
  # ... and a sharp model is NOT beaten by the wide one despite the wide band's coverage
  gwid <- cmp$scorecard$mean_width[cmp$scorecard$model == "good"]
  nwid <- cmp$scorecard$mean_width[cmp$scorecard$model == "naive"]
  gscore <- cmp$scorecard$mean_interval_score[cmp$scorecard$model == "good"]
  nscore <- cmp$scorecard$mean_interval_score[cmp$scorecard$model == "naive"]
  expect_lt(gwid, nwid)        # good is sharper
  expect_lt(gscore, nscore)    # and wins the interval score anyway
  # rank stability: good is best at every cutoff
  gs <- cmp$rank_stability[cmp$rank_stability$model == "good", ]
  expect_equal(gs$mean_rank, 1)
  expect_equal(gs$best_fraction, 1)
  expect_equal(gs$rank_sd, 0)
})

test_that("compare_forecasts errors on an unknown benchmark", {
  d <- data.frame(model = "a", cutoff = 1:3, observed = 1:3, predicted = 1:3,
                  lower = 0:2, upper = 2:4)
  expect_error(compare_forecasts(d, benchmark = "ghost"), "not among the models")
})
