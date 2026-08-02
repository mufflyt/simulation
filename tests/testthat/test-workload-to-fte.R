# Regression guards for the workload -> FTE conversion and the FTE gap.
#
# Central assertion: every supply/demand comparison has FTE on BOTH sides.
# Dividing provider FTE by a count of prevalent cases, consultations or
# procedures is dimensionally invalid and is now impossible to do by accident.

test_that("the dimensionally invalid coverage ratio is gone", {
  expect_error(compute_demand_coverage(), "not FTE units")
})

test_that("the delegation matrix is validated and sums to one per service", {
  expect_silent(validate_delegation_matrix())
  bad <- URPS_DELEGATION_MATRIX
  bad$urps_share[1] <- 0.9
  expect_error(validate_delegation_matrix(bad), "sum to 1")
  neg <- URPS_DELEGATION_MATRIX
  neg$app_share[1] <- -0.1; neg$urps_share[1] <- 0.88
  expect_error(validate_delegation_matrix(neg), "sum to 1|negative")
})

test_that("APP substitution is modelled by service, not as a blanket ratio", {
  # Forte 2021 Table 4: NPs and PAs perform 1-3% of injection and diagnostic
  # procedures but 15-20% of outpatient services. A scalar substitution ratio
  # cannot represent that spread.
  m <- URPS_DELEGATION_MATRIX
  app_surgery <- m$app_share[m$service == "sling_procedure"]
  app_routine <- m$app_share[m$service == "ptns"]
  expect_lt(app_surgery, 0.05)
  expect_gt(app_routine, 0.40)
  expect_gt(app_routine / app_surgery, 10)
})

test_that("service volume apportions across provider types and conserves total", {
  vol <- tibble::tibble(service = c("new_consultation", "sling_procedure"),
                        volume = c(1000, 500))
  ap <- apportion_service_volume(vol)
  expect_equal(sum(ap$volume), sum(vol$volume), tolerance = 1e-8)
  expect_setequal(unique(ap$provider_type), c("urps", "app", "other_clinician"))
  urps_consult <- ap$volume[ap$service == "new_consultation" & ap$provider_type == "urps"]
  expect_equal(urps_consult, 1000 * 0.70)
})

test_that("work RVU totals follow the basket and the delegation shares", {
  vol <- tibble::tibble(service = "new_consultation", volume = 100)
  rv <- service_volume_to_wrvu(vol)
  expect_equal(rv$work_rvu, 100 * 0.70 * 2.60, tolerance = 1e-8)
  # With no delegation the full volume is attributed to URPS.
  rv_all <- service_volume_to_wrvu(vol, delegation = NULL)
  expect_equal(rv_all$work_rvu, 100 * 2.60, tolerance = 1e-8)
})

test_that("productivity is calibrated to a base-year anchor, never assumed", {
  expect_error(
    convert_workload_to_fte(tibble::tibble(service = "new_consultation", volume = 10)),
    "wrvu_per_fte is required"
  )
  # The calibration must reproduce the anchor exactly once the indirect-time
  # gross-up is accounted for on BOTH sides.
  vol <- tibble::tibble(year = 2025, service = "new_consultation", volume = 500000)
  base_wrvu <- service_volume_to_wrvu(vol)$work_rvu
  anchor <- 1353.3
  k <- calibrate_wrvu_per_fte(base_wrvu, anchor)
  req <- convert_workload_to_fte(vol, wrvu_per_fte = k)
  expect_equal(req$required_fte, anchor, tolerance = 1e-6)
})

test_that("indirect clinical time is not silently free", {
  vol <- tibble::tibble(service = "new_consultation", volume = 1000)
  with_indirect <- convert_workload_to_fte(vol, wrvu_per_fte = 100)
  without <- convert_workload_to_fte(vol, wrvu_per_fte = 100, indirect_share = 0)
  expect_gt(with_indirect$required_fte, without$required_fte)
  expect_equal(with_indirect$required_fte / without$required_fte,
               1 / (1 - INDIRECT_TIME_SHARE), tolerance = 1e-8)
})

test_that("the staffing-ratio method is available as a second conversion route", {
  # Zarek 2025: staffing ratio = national service volume / providers in setting.
  vol <- tibble::tibble(service = "new_consultation", volume = 10000)
  ratios <- tibble::tibble(service = "new_consultation", volume_per_fte = 2860)
  res <- convert_workload_to_fte(vol, method = "staffing", staffing_ratios = ratios)
  expect_equal(res$method, "staffing")
  expect_equal(res$required_fte,
               10000 * 0.70 / 2860 / (1 - INDIRECT_TIME_SHARE), tolerance = 1e-8)
})

test_that("setting allocation follows a survey time share summing to one", {
  a <- allocate_fte_by_setting(1000)
  expect_equal(sum(a$required_fte), 1000, tolerance = 1e-8)
  expect_error(allocate_fte_by_setting(1000, c(a = 0.5, b = 0.2)), "sum to")
})

test_that("the FTE gap compares FTE with FTE and signs shortfalls negative", {
  supply <- tibble::tibble(year = 2025:2026, effective_fte_median = c(1300, 1320))
  required <- tibble::tibble(year = 2025:2026, required_fte = c(1400, 1500))
  g <- compute_fte_gap(supply, required)
  expect_equal(g$gap_fte, c(-100, -180))
  expect_equal(g$gap_pct, 100 * c(-100 / 1400, -180 / 1500))
  expect_equal(g$pct_supply_to_demand, 100 * c(1300 / 1400, 1320 / 1500))
  # A surplus is positive.
  gs <- compute_fte_gap(tibble::tibble(year = 2025, effective_fte_median = 1500),
                        tibble::tibble(year = 2025, required_fte = 1400))
  expect_gt(gs$gap_fte, 0)
})

test_that("an uncalibrated workload basket cannot be labelled publishable", {
  expect_false(assert_publishable_workload("uncalibrated_illustrative", mode = "relaxed"))
  expect_error(assert_publishable_workload("uncalibrated_illustrative", mode = "strict"),
               "calibration_status")
  expect_true(assert_publishable_workload("calibrated", mode = "strict"))
})
