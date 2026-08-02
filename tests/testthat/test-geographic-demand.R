# Geographic (isochrone) demand (R/32-geographic_demand.R).

geo <- data.frame(
  geo_id = paste0("t", 1:6),
  need = c(100, 200, 50, 400, 150, 100),
  nearest_provider_min = c(15, 45, 90, 150, 200, 240),
  access_ratio = c(3.0, 2.0, 1.0, 0.5, 0.2, 0.1),
  capacity = c(120, 150, 40, 300, 60, 20))

test_that("need is distributed across the 30/60/120/180 travel bands", {
  bb <- demand_by_travel_band(geo)
  expect_equal(bb$threshold_min, c(30, 60, 120, 180))
  expect_equal(bb$need_within[bb$threshold_min == 30], 100)   # t1
  expect_equal(bb$need_within[bb$threshold_min == 60], 300)   # t1+t2
  expect_equal(bb$need_within[bb$threshold_min == 180], 750)  # excludes t5(200),t6(240)
  expect_equal(unname(attr(bb, "beyond")["need"]), 250)       # t5+t6
})

test_that("need-weighted access falls below the unweighted mean when need is in low-access areas", {
  expect_lt(need_weighted_access(geo), mean(geo$access_ratio))
})

test_that("accessible-need-vs-capacity flags underserved need", {
  anc <- accessible_need_vs_capacity(geo)
  expect_equal(anc$national_adequacy, sum(geo$capacity) / sum(geo$need))
  # underserved: t2,t3,t4,t5,t6 (only t1 has capacity >= need) => 900/1000
  expect_equal(anc$underserved_need_share, 0.90)
  expect_true("adequacy" %in% names(anc$by_geo))
})

test_that("the summary rolls band + access + adequacy together", {
  s <- geographic_demand_summary(geo)
  expect_equal(s$total_need, 1000)
  expect_equal(s$beyond_share, 0.25)
  expect_false(is.null(s$need_weighted_access))
  expect_false(is.null(s$adequacy))
})

test_that("access/capacity columns are optional", {
  s <- geographic_demand_summary(geo[, c("geo_id", "need", "nearest_provider_min")])
  expect_null(s$need_weighted_access)
  expect_null(s$adequacy)
  expect_equal(s$total_need, 1000)
})
