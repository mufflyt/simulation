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
  # Forte 2021 Table 4: NPs and PAs perform 1-3% of procedures but 15-35% of
  # outpatient care management. A scalar substitution ratio cannot represent a
  # spread that wide, which is the whole argument against "one APP = 0.5 MDs".
  m <- URPS_DELEGATION_MATRIX
  app_surgery <- m$app_share[m$service == "sling_procedure"]
  app_routine <- m$app_share[m$service == "ptns"]
  expect_lt(app_surgery, 0.05)
  expect_gt(app_routine, 0.30)
  expect_gt(app_routine / app_surgery, 10)
  # The shares are transferred from another specialty and must say so.
  expect_equal(URPS_DELEGATION_STATUS, "derived_by_analogy")
  expect_true("forte_analogue" %in% names(m))
})

test_that("service volume apportions across provider types and conserves total", {
  vol <- tibble::tibble(service = c("new_consultation", "sling_procedure"),
                        volume = c(1000, 500))
  ap <- apportion_service_volume(vol)
  expect_equal(sum(ap$volume), sum(vol$volume), tolerance = 1e-8)
  expect_setequal(unique(ap$provider_type), c("urps", "app", "other_clinician"))
  urps_consult <- ap$volume[ap$service == "new_consultation" & ap$provider_type == "urps"]
  expect_equal(urps_consult,
               1000 * URPS_DELEGATION_MATRIX$urps_share[
                 URPS_DELEGATION_MATRIX$service == "new_consultation"])
})

test_that("work RVU totals follow the basket and the delegation shares", {
  vol <- tibble::tibble(service = "new_consultation", volume = 100)
  wl <- urps_service_workload()
  rvu_consult <- wl$work_rvu[wl$service == "new_consultation"]
  share <- URPS_DELEGATION_MATRIX$urps_share[
    URPS_DELEGATION_MATRIX$service == "new_consultation"]

  rv <- service_volume_to_wrvu(vol)
  expect_equal(rv$work_rvu, 100 * share * rvu_consult, tolerance = 1e-8)
  # With no delegation the full volume is attributed to URPS.
  rv_all <- service_volume_to_wrvu(vol, delegation = NULL)
  expect_equal(rv_all$work_rvu, 100 * rvu_consult, tolerance = 1e-8)
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
  share <- URPS_DELEGATION_MATRIX$urps_share[
    URPS_DELEGATION_MATRIX$service == "new_consultation"]
  expect_equal(res$required_fte,
               10000 * share / 2860 / (1 - INDIRECT_TIME_SHARE), tolerance = 1e-8)
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

test_that("the publication gate respects the calibration tier", {
  # Placeholders are always refused.
  expect_false(assert_publishable_workload("uncalibrated_illustrative", mode = "relaxed"))
  expect_error(assert_publishable_workload("uncalibrated_illustrative", mode = "strict"),
               "placeholders")
  # Anchored and solved inputs pass.
  expect_true(assert_publishable_workload("calibrated", mode = "strict"))
  expect_true(assert_publishable_workload("solved", mode = "strict"))
  # Analogy-derived inputs need explicit opt-in.
  expect_error(assert_publishable_workload("derived_by_analogy", mode = "strict"),
               "derived by analogy")
  expect_true(assert_publishable_workload("derived_by_analogy", allow_analogy = TRUE,
                                          mode = "strict"))
  expect_error(assert_publishable_workload("nonsense", mode = "relaxed"),
               "Unknown calibration tier")
})

test_that("the workload basket is calibrated to CMS work RVUs", {
  wl <- urps_service_workload()
  expect_equal(urps_service_workload_status(), "calibrated")
  expect_true(all(c("new_consultation", "sling_procedure", "prolapse_procedure")
                  %in% wl$service))
  # Every billable service carries a positive work RVU...
  billable <- wl[!wl$service %in% c("postoperative_care", "indirect_clinical_work"), ]
  expect_true(all(billable$work_rvu > 0))
  # ...and postoperative care carries ZERO, because every procedure in the
  # basket has a 090-day global period that already includes it. Counting it
  # again would double-count the surgeon.
  expect_equal(wl$work_rvu[wl$service == "postoperative_care"], 0)
  expect_true(all(CMS_WORK_RVU$glob[CMS_WORK_RVU$hcpcs %in%
                                      c("57288", "57282", "57425")] == "090"))
})

test_that("CPT mix weights sum to one and reference known codes", {
  expect_silent(validate_cpt_basket())
  bad <- URPS_CPT_BASKET
  bad$mix[bad$service == "new_consultation"][1] <- 0.9
  expect_error(validate_cpt_basket(bad), "sum to 1")
  unknown <- rbind(URPS_CPT_BASKET,
                   data.frame(service = "x", hcpcs = "00000", mix = 1))
  expect_error(validate_cpt_basket(unknown), "absent from CMS_WORK_RVU|sum to 1")
})

test_that("the shipped work RVUs match published CMS values", {
  # Spot checks against the CMS 2025 Physician Fee Schedule Relative Value File.
  get <- function(code) CMS_WORK_RVU$work_rvu[CMS_WORK_RVU$hcpcs == code]
  expect_equal(get("99204"), 2.60)
  expect_equal(get("99213"), 1.30)
  expect_equal(get("57288"), 12.13)   # sling operation for SUI
  expect_equal(get("52000"), 1.53)    # cystourethroscopy
  expect_equal(get("52287"), 3.20)    # cystoscopy with chemodenervation
  expect_equal(get("57160"), 0.89)    # pessary fitting
  expect_equal(get("64566"), 0.60)    # PTNS
})

test_that("calibration status is reported for every model input", {
  r <- calibration_status_report()
  expect_true(all(r$status %in% CALIBRATION_TIERS))
  expect_true(all(nzchar(r$source)))
  expect_equal(r$status[r$input == "work RVUs"], "calibrated")
  expect_equal(r$status[r$input == "hours intercept"], "solved")
  # The delegation shares must not claim to be calibrated.
  expect_equal(r$status[r$input == "delegation shares"], "derived_by_analogy")
})

# ---- Productivity plausibility and the capacity level correction -----------

test_that("an implausible solved productivity is flagged, not absorbed", {
  # calibrate_wrvu_per_fte() SOLVES the denominator, so it silently absorbs any
  # error in the service volumes. A denominator far above benchmark suppresses
  # projected demand, so it must be caught.
  expect_true(check_productivity_plausible(7500, mode = "relaxed"))
  expect_false(check_productivity_plausible(17707, mode = "relaxed"))
  expect_error(check_productivity_plausible(17707, mode = "strict"), "VOLUMES are wrong")
  expect_error(check_productivity_plausible(500, mode = "strict"), "outside the plausible")
})

test_that("the model's own solved productivity is plausible", {
  pop <- example_female_population_by_band(2025:2030)
  vol <- dplyr::filter(example_service_volumes(compute_demand_denominators(pop)),
                       .data$year == 2025)
  anchor <- required_fte_base_year(1306, 0.948)
  k <- calibrate_wrvu_per_fte(service_volume_to_wrvu(vol)$work_rvu, anchor)
  expect_gte(k, WRVU_PER_FTE_BENCHMARK[["low"]])
  expect_lte(k, WRVU_PER_FTE_BENCHMARK[["high"]])
})

test_that("the capacity correction fixes the level and keeps Forte's shape", {
  raw <- URPS_DELEGATION_FORTE_RAW
  adj <- URPS_DELEGATION_MATRIX
  billable <- raw$service != "indirect_clinical_work"

  # Level: physiatry puts the subspecialist at ~65% of services; a 1,306-person
  # subspecialty cannot deliver that share of national UI care.
  expect_gt(mean(raw$urps_share[billable]), 0.60)
  expect_lt(mean(adj$urps_share[billable]), 0.35)

  # Shape: the relative ordering across services is what transfers.
  expect_equal(order(adj$urps_share[billable]), order(raw$urps_share[billable]))

  # The APP split is what Forte actually measured and is left untouched; the
  # difference goes to other clinicians (generalist OB/GYN, urology, primary care).
  expect_equal(adj$app_share, raw$app_share)
  expect_true(all(adj$other_clinician_share >= raw$other_clinician_share))
  expect_silent(validate_delegation_matrix(adj))
})

test_that("implied_urps_share reports the consistent level", {
  pop <- example_female_population_by_band(2025:2030)
  vol <- dplyr::filter(example_service_volumes(compute_demand_denominators(pop)),
                       .data$year == 2025)
  s <- suppressMessages(implied_urps_share(vol, required_fte_base_year(1306, 0.948)))
  expect_gt(s, 0)
  expect_lt(s, 1)
  # It must agree with the factor actually applied to the shipped matrix.
  expect_equal(s, mean(URPS_DELEGATION_MATRIX$urps_share[
    URPS_DELEGATION_MATRIX$service != "indirect_clinical_work"]), tolerance = 0.05)
})

# ---- Which inputs actually move the deliverable ----------------------------

test_that("required FTE is invariant to the LEVEL of the service volumes", {
  # wrvu_per_fte is SOLVED against the base-year anchor, so a proportional error
  # in every volume cancels exactly. This bounds how much the illustrative
  # volumes can distort the deliverable: for the level, not at all.
  vol <- tibble::tibble(year = c(2025L, 2050L),
                        service = "new_consultation",
                        volume = c(1e6, 1.4e6))
  req <- function(v) {
    k <- calibrate_wrvu_per_fte(
      service_volume_to_wrvu(dplyr::filter(v, .data$year == 2025L))$work_rvu, 1377.3)
    convert_workload_to_fte(v, wrvu_per_fte = k)$required_fte
  }
  base <- req(vol)
  expect_equal(req(dplyr::mutate(vol, volume = .data$volume * 2)), base)
  expect_equal(req(dplyr::mutate(vol, volume = .data$volume * 0.5)), base)
})

test_that("required FTE is invariant to a uniform delegation rescaling", {
  # The 0.434 capacity correction changes wrvu_per_fte and the URPS share by the
  # same factor, so it cancels. It fixes the interpretability of the shares and
  # the plausibility of the productivity denominator -- it does NOT move the gap.
  vol <- tibble::tibble(year = c(2025L, 2050L), service = "new_consultation",
                        volume = c(1e6, 1.4e6))
  scaled <- URPS_DELEGATION_MATRIX
  keep <- scaled$service != "indirect_clinical_work"
  moved <- scaled$urps_share[keep] * 0.2
  scaled$urps_share[keep] <- scaled$urps_share[keep] - moved
  scaled$other_clinician_share[keep] <- scaled$other_clinician_share[keep] + moved

  req <- function(dg) {
    k <- calibrate_wrvu_per_fte(
      service_volume_to_wrvu(dplyr::filter(vol, .data$year == 2025L),
                             delegation = dg)$work_rvu, 1377.3)
    convert_workload_to_fte(vol, wrvu_per_fte = k, delegation = dg)$required_fte
  }
  expect_equal(req(scaled), req(URPS_DELEGATION_MATRIX))
})

test_that("the base-year gap is a pass-through of the adequacy estimate", {
  # gap = supply - supply/adequacy, so base gap% is exactly -(1 - adequacy).
  # The headline base-year number is therefore NOT modelled: it is whatever the
  # capacity survey says, with a coefficient of one.
  for (a in c(0.90, 0.948, 0.97)) {
    g <- baseline_gap(1306, a, method = "capacity_survey",
                      calibration_status = "calibrated")
    expect_equal(100 * (1306 - g$required_fte) / g$required_fte,
                 -100 * (1 - a), tolerance = 1e-8)
  }
})

test_that("an inconsistent hours intercept warns instead of exceeding headcount", {
  skip_if_not_installed("mufflyaccess")
  set.seed(11)
  a <- agents_from_certification_cohorts(2023L)
  # Default intercept: general-internal-medicine hours against a 37.2 hr FTE
  # definition gives more FTE than people.
  expect_message(
    run_supply_microsimulation(a, 2023:2024, 88, "FPMRS", n_iterations = 3,
                               verbose = FALSE),
    "FTE supply will exceed headcount")
  # Calibrated intercept: no warning, and the ratio is 1.
  ic <- calibrate_hours_intercept(a$age, a$sex)
  r <- run_supply_microsimulation(a, 2023:2024, 88, "FPMRS", n_iterations = 3,
                                  hours_intercept = ic, verbose = FALSE)
  expect_equal(r$summary$effective_fte_median[1] / r$summary$headcount_median[1],
               1, tolerance = 0.01)
})
