# Guards for the supply-side calibration data (cliff-derived, canonical).
# Skip when the data-raw files are absent (e.g. an installed-package check).

calib_available <- function() {
  isTRUE(tryCatch({ resolve_canonical("urps_hazard_pooled"); TRUE },
                  error = function(e) FALSE))
}

test_that("empirical hazards resolve with matching checksums", {
  skip_if_not(calib_available(), "calibration files not present")
  expect_true(file.exists(resolve_canonical("urps_hazard_pooled", mode = "strict")))
  expect_true(file.exists(resolve_canonical("urps_retirement_hazard_ageband", mode = "strict")))
  expect_true(file.exists(resolve_canonical("urps_nrmp_entrants", mode = "strict")))
})

test_that("the sparse 70+ hazard cell is floored, never zero", {
  skip_if_not(calib_available(), "calibration files not present")
  h <- urps_empirical_hazard_by_ageband("urps")
  expect_setequal(names(h), MICROSIM_AGE_BAND_LABELS)
  expect_gt(h[["70+"]], 0)                 # raw file has 0 events / 16 PY here
  expect_gte(h[["70+"]], h[["65-69"]])     # monotone non-decreasing in the tail
  expect_gte(h[["65-69"]], h[["60-64"]])

  # Without the guard, the raw 70+ hazard is exactly 0.
  raw <- urps_empirical_hazard_by_ageband("urps", floor_sparse = FALSE)
  expect_equal(unname(raw[["70+"]]), 0)
})

test_that("the empirical single-year schedule blends observed 50-69 with the HWSM tail", {
  skip_if_not(calib_available(), "calibration files not present")
  s <- urps_empirical_retirement_schedule("urps")
  ages <- as.integer(names(s))
  expect_equal(min(ages), 50L)
  expect_true(all(s >= 0 & s <= 1))
  # 50-69 come from the empirical bands; 70+ from the HWSM physician tail.
  expect_equal(unname(s[as.character(67)]),
               unname(urps_empirical_hazard_by_ageband("urps")[["65-69"]]))
  expect_equal(unname(s[as.character(72)]),
               unname(RETIREMENT_HAZARD_PHYSICIAN[["72"]]))
  # Hazard rises monotonically across the retirement region.
  expect_true(all(diff(s[as.character(60:85)]) >= 0))
})

test_that("NRMP entrants return the real matched counts", {
  skip_if_not(calib_available(), "calibration files not present")
  expect_equal(nrmp_entrants("URPS"), 70L)
  expect_equal(nrmp_entrants("FPMRS"), 70L)   # FPMRS aliases URPS
  expect_equal(nrmp_entrants("GO"), 86L)
  expect_equal(nrmp_entrants("MIGS"), 47L)
})

test_that("the age-productivity curve peaks in mid-career", {
  skip_if_not(calib_available(), "calibration files not present")
  p <- urps_age_productivity_curve()
  expect_true(all(c("age", "rel_to_peak") %in% names(p)))
  expect_equal(max(p$rel_to_peak), 1, tolerance = 1e-6)
  peak_age <- p$age[which.max(p$rel_to_peak)]
  expect_gte(peak_age, 40); expect_lte(peak_age, 55)
})

test_that("empirical exposure exposes person-years and events for CIs", {
  skip_if_not(calib_available(), "calibration files not present")
  ex <- urps_hazard_exposure()
  expect_true(all(c("age_band", "person_years", "events", "annual_hazard") %in% names(ex)))
  expect_true(all(ex$person_years > 0))
})

test_that("run_workforce_microsimulation accepts retirement_source without error", {
  # A pure argument-validation guard (does not require mufflyaccess): the
  # match.arg happens before any contract call.
  expect_error(
    suppressWarnings(suppressMessages(
      run_workforce_microsimulation(retirement_source = "not_a_source",
                                    n_iterations = 1, verbose = FALSE)
    )),
    "should be one of"
  )
})
