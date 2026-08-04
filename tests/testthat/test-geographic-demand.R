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

# ---- tract population -> need bridge (script 08 -> R/32) --------------------

tracts <- data.frame(
  GEOID = c("A", "B", "C"),
  female_20_39  = c(1000, 500, 200),
  female_40_59  = c(2000, 800, 300),
  female_60_64  = c( 500, 200, 100),
  female_65_79  = c( 800, 400, 150),
  female_80plus = c( 300, 150,  50),
  nearest_provider_min = c(15, 90, 240),
  access_ratio = c(3, 0.5, 0.1),
  capacity = c(400, 200, 20))
prev <- c("20-39" = 0.05, "40-59" = 0.20, "60-64" = 0.35, "65-79" = 0.45, "80+" = 0.50)

test_that("tract_need_from_population sums population x age-band prevalence", {
  nt <- tract_need_from_population(tracts, prevalence = prev)
  # tract A: 1000*.05 + 2000*.20 + 500*.35 + 800*.45 + 300*.50 = 1135
  # tract C: 200*.05 + 300*.20 + 100*.35 + 150*.45 + 50*.50 = 197.5
  expect_equal(nt$need[1], 1135)
  expect_equal(nt$need, c(1135, 510, 197.5))
  # other columns are carried through so it flows into the summary
  expect_true(all(c("GEOID", "nearest_provider_min", "access_ratio", "capacity") %in% names(nt)))
})

test_that("tract_need_from_population treats NA population as zero", {
  t2 <- tracts; t2$female_40_59[2] <- NA
  nt <- tract_need_from_population(t2, prevalence = prev)
  expect_equal(nt$need[2], 510 - 800 * 0.20)
})

test_that("tract_need_from_population validates columns and prevalence coverage", {
  expect_error(tract_need_from_population(tracts[, -2], prevalence = prev),
               "missing population column")
  expect_error(tract_need_from_population(tracts, prevalence = prev[1:3]),
               "no value for band")
})

test_that("isochrone_demand_from_tracts builds need then summarises it", {
  s <- isochrone_demand_from_tracts(tracts, prevalence = prev)
  expect_equal(s$total_need, 1135 + 510 + 197.5)
  expect_equal(s$by_band$need_within[s$by_band$threshold_min == 30], 1135)   # tract A only
  expect_false(is.null(s$need_weighted_access))
  expect_false(is.null(s$adequacy))
})
