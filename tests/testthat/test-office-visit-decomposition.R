# A prevalent treated patient cannot also be a NEW patient every year. Under the
# shipped pathway, new_consultation volume EQUALS the treated cohort exactly --
# ratio 1.00 -- which is the signature of the incident/prevalent collapse.

test_that("the conservative stage dominates the ambulatory prediction", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  d <- office_visit_decomposition(tn)
  conservative <- sum(d$share[d$component %in% c("new_consultation",
                                                 "conservative_return")])
  # 95%+ of predicted visits arise before any clinically interesting transition
  expect_gt(conservative, 0.85)
})

test_that("new consultations cannot exceed the treated cohort", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  # KNOWN FAILING as shipped: this documents the defect rather than passing
  # vacuously. When the pathway splits newly_entering_care from continuing_care,
  # flip this to expect_true and drop the warning expectation.
  expect_warning(assert_incident_not_prevalent(tn),
                 "counted as a NEW patient annually")
  expect_false(suppressWarnings(assert_incident_not_prevalent(tn)))
})

test_that("strict mode stops rather than warns", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  expect_error(assert_incident_not_prevalent(tn, strict = TRUE),
               "newly_entering_care")
})

test_that("a corrected pathway would pass the incident check", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  tn <- c(ui = 2538779.5, pop = 3264807.3, ai = 372721.4)
  p <- condition_service_pathway()
  # illustrative correction: only a fraction of the prevalent cohort are new
  p$per_entering[p$stage == "conservative" &
                 p$service == "new_consultation"] <- 0.25
  expect_true(suppressWarnings(assert_incident_not_prevalent(tn, p)))
})
