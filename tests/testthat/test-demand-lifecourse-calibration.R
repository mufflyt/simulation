# Calibration + back-test for the life-course demand pathway (R/calibration-demand_lifecourse).

sv <- tibble::tibble(
  year    = 2025L,
  service = c("new_consultation", "return_visit", "sling_procedure",
              "prolapse_procedure", "urodynamics"),
  volume  = c(1e6, 2e6, 1.2e5, 8e4, 5e5)
)

test_that("anchor predictions aggregate services into the anchor categories", {
  pred <- lifecourse_anchor_predictions(sv, base_year = 2025L)
  expect_named(pred, c("category", "predicted"))
  expect_setequal(pred$category,
                  c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"))
  # new_consultation + return_visit -> urps_office_visits
  expect_equal(pred$predicted[pred$category == "urps_office_visits"], 3e6)
})

test_that("calibration scales anchored services to hit the observed totals", {
  observed <- tibble::tibble(
    category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
    observed = c(3.6e6, 1.0e5, 1.0e5))
  cal <- calibrate_lifecourse_demand(sv, observed, base_year = 2025L)
  expect_true(all(c("category", "scalar") %in% names(cal$scalars)))
  # calibrated office-visit volume equals the anchor
  office <- sum(cal$service_volumes$volume[cal$service_volumes$service %in%
                                             c("new_consultation", "return_visit")])
  expect_equal(office, 3.6e6, tolerance = 1e-6)
  # an unanchored service is left unchanged
  expect_equal(cal$service_volumes$volume[cal$service_volumes$service == "urodynamics"],
               sv$volume[sv$service == "urodynamics"])
})

test_that("back-test reports per-category error and a MAPE summary", {
  sv2 <- dplyr::bind_rows(
    dplyr::mutate(sv, year = 2017L),
    dplyr::mutate(sv, year = 2023L, volume = volume * 1.10)   # +10% by the target year
  )
  observed_fit <- tibble::tibble(
    category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
    observed = c(3e6, 1.2e5, 8e4))                            # equals the 2017 prediction (scalar 1)
  observed_target <- tibble::tibble(
    category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
    observed = c(3.3e6, 1.32e5, 8.8e4))                       # matches +10%, so error ~ 0
  bt <- backtest_lifecourse(sv2, observed_fit, observed_target,
                            fit_through_year = 2017L, target_year = 2023L)
  expect_true(all(c("category", "predicted", "observed", "ratio", "abs_pct_error")
                  %in% names(bt$by_category)))
  expect_equal(bt$summary$target_year, 2023L)
  expect_lt(bt$summary$mape, 1e-6)   # calibrated prediction reproduces the held-out year
})
