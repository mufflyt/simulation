# The URPS access anchors, and the two mistakes they exist to prevent:
# entering a business-day wait into a calendar-day model, and anchoring URPS
# access to another specialty's audit.

test_that("business days convert to calendar days at 7/5", {
  expect_equal(business_days_to_calendar(5), 7)
  expect_equal(business_days_to_calendar(23.1), 23.1 * 7 / 5)
  expect_gt(business_days_to_calendar(23.1), 23.1)
})

test_that("the default anchor is the peer-reviewed Rabice observation", {
  w <- urps_observed_wait_days()
  expect_equal(w$business_days, 23.1)
  expect_equal(w$calendar_days, 32.34, tolerance = 1e-8)
  expect_equal(w$status, "calibrated")
  expect_match(w$citation, "Rabice")
  expect_match(w$citation, "2021")
})

test_that("the anchor is in calendar days, not business days", {
  # The failure this guards: 23.1 entered unconverted would understate the
  # national wait by about nine days and nothing downstream would notice.
  w <- urps_observed_wait_days()
  expect_gt(w$calendar_days - w$business_days, 9)
  expect_lt(w$calendar_days - w$business_days, 10)
})

test_that("the 2026 audit is registered but never reports itself as calibrated", {
  w <- urps_observed_wait_days(study = "Acosta", insurance = "Medicaid")
  expect_equal(w$business_days, 46)
  expect_equal(w$status, "preliminary")
  b <- urps_observed_wait_days(study = "Acosta", insurance = "BCBS")
  expect_equal(b$business_days, 35)
  expect_equal(b$status, "preliminary")
  # Medicaid waits longer than commercial for prolapse; that is the finding.
  expect_gt(w$business_days, b$business_days)
})

test_that("an unknown study or scenario is an error, not a silent default", {
  expect_error(urps_observed_wait_days(study = "Corbisiero"), "no observation")
  expect_error(urps_observed_wait_days(scenario = "sinusitis"), "no observation")
})

test_that("the Medicaid barrier is modelled as refusal, not as a longer queue", {
  expect_equal(urps_insurance_fraction("commercial"), 1)
  expect_equal(urps_insurance_fraction("medicaid"), 0.77)
  expect_lt(urps_insurance_fraction("medicaid"), urps_insurance_fraction("commercial"))
})

test_that("urps_access_targets populates wait_time and leaves panel_size empty", {
  t <- urps_access_targets()
  wait <- t[t$target == "wait_time", ]
  expect_true(is.finite(wait$observed))
  expect_equal(wait$status, "calibrated")
  panel <- t[t$target == "panel_size", ]
  expect_true(is.na(panel$observed))
  expect_equal(panel$status, "target_unpopulated")
})

test_that("an unpopulated target can never be scored as a pass", {
  nat <- tibble::tibble(label = c("wait_time", "panel_size"), value = c(32.34, 1234))
  v <- validate_access_outcomes(nat, urps_access_targets())
  expect_equal(v$status[v$target == "wait_time"], "pass")
  expect_equal(v$status[v$target == "panel_size"], "no_target")
  expect_equal(v$target_status[v$target == "wait_time"], "calibrated")
})

test_that("fitting wait_scale to the anchor reproduces the observed wait", {
  catch <- data.frame(demand_workload = c(800, 600, 400),
                      accessible_capacity = c(1000, 1000, 1000))
  obs <- urps_observed_wait_days()$calendar_days
  f <- fit_wait_scale(catch, observed_wait = obs)
  cl <- clear_access(catch, wait_scale = f$wait_scale)
  d <- cl$demand_workload
  ok <- is.finite(cl$wait_time)
  expect_equal(sum(cl$wait_time[ok] * d[ok]) / sum(d[ok]), obs, tolerance = 1e-8)
})
