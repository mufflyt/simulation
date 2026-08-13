# claims_service_volumes() is the calibrated replacement for the illustrative
# example_service_volumes() ratios. These tests pin its contract: claims
# override the fallback per (service, year), the overall tier is the WEAKEST
# present (so a half-claims basket never reports "calibrated"), and it fails
# closed rather than fabricate.

csv_fallback <- function() {
  tibble::tibble(
    service = rep(c("new_consultation", "sling_procedure", "pessary_care", "cystoscopy"),
                  each = 2),
    year = rep(2020:2021, times = 4),
    volume = rep(c(1000, 90, 300, 180), each = 2))
}

test_that("full claims coverage reports a calibrated basket, no fallback rows", {
  claims <- dplyr::transmute(csv_fallback(),
                             service, year, volume = volume * 1.1,
                             calibration_status = "calibrated", source = "CADR")
  out <- suppressMessages(claims_service_volumes(claims, fallback = csv_fallback()))
  expect_equal(attr(out, "overall_status"), "calibrated")
  expect_false(any(out$source == "illustrative_fallback"))
  expect_setequal(names(out), c("year", "service", "volume", "calibration_status", "source"))
})

test_that("partial claims degrade the overall tier to the weakest present", {
  claims <- tibble::tibble(service = c("sling_procedure", "pessary_care"),
                           year = c(2020L, 2020L), volume = c(95, 310), source = "CADR")
  out <- suppressMessages(claims_service_volumes(claims, fallback = csv_fallback()))
  # weakest present is the illustrative fallback the uncovered services fell back to
  expect_equal(attr(out, "overall_status"), "uncalibrated_illustrative")
  # the two claims cells override the fallback for their (service, year)
  expect_equal(out$volume[out$service == "sling_procedure" & out$year == 2020], 95)
  expect_equal(out$source[out$service == "sling_procedure" & out$year == 2020], "CADR")
  # a service claims did not cover still appears, from the fallback
  expect_true(any(out$service == "cystoscopy" & out$source == "illustrative_fallback"))
})

test_that("require_complete fails closed when the fallback is used", {
  claims <- tibble::tibble(service = "sling_procedure", year = 2020L, volume = 95)
  expect_error(
    claims_service_volumes(claims, fallback = csv_fallback(),
                           require_complete = TRUE, mode = "strict"),
    "not fully claims-calibrated")
  # relaxed warns rather than errors, but still returns the honest weak tier
  out <- suppressWarnings(suppressMessages(
    claims_service_volumes(claims, fallback = csv_fallback(),
                           require_complete = TRUE, mode = "relaxed")))
  expect_equal(attr(out, "overall_status"), "uncalibrated_illustrative")
})

test_that("an unknown calibration_status is rejected, not silently ranked", {
  claims <- tibble::tibble(service = "sling_procedure", year = 2020L, volume = 95,
                           calibration_status = "totally_made_up")
  expect_error(suppressMessages(claims_service_volumes(claims)), "unknown calibration_status")
})

test_that("with no fallback the claims pass through unchanged", {
  claims <- dplyr::transmute(csv_fallback(),
                             service, year, volume,
                             calibration_status = "calibrated", source = "CADR")
  out <- suppressMessages(claims_service_volumes(claims))
  expect_equal(attr(out, "overall_status"), "calibrated")
  expect_equal(nrow(out), nrow(csv_fallback()))
})
