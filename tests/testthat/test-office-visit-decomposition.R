# A prevalent treated patient cannot also be a NEW patient every year.
# A calibrated pathway assigns an incident fraction for new_consultation
# volume, keeping new consultations strictly below the treated cohort total.

test_that("the conservative stage share is within valid bounds", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  d <- office_visit_decomposition(tn)
  conservative <- sum(d$share[d$component %in% c("new_consultation", "conservative_return")])
  expect_gt(conservative, 0.50)
  expect_lt(conservative, 1.00)
})

test_that("new consultations cannot exceed the treated cohort under a calibrated pathway", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  p <- condition_service_pathway()
  p$per_entering[p$stage == "conservative" & p$service == "new_consultation"] <- 0.25
  expect_true(assert_incident_not_prevalent(tn, p))
  expect_true(assert_incident_not_prevalent(tn, p, strict = TRUE))
})
