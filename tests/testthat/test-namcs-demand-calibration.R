# NAMCS demand calibration (R/52).
#
# The anchor needs the real NAMCS file, so anything touching it skips when the
# file is absent. The apply-side contracts run on a synthetic frame, because
# "calibration must not touch the procedure rows" is the property most likely
# to be broken by a later edit and it does not need real data to check.

ndc_volumes <- function(scalar_year = 2025) {
  tidyr::expand_grid(
    year = c(scalar_year, scalar_year + 1),
    service = c("new_consultation", "return_visit", "sling_procedure",
                "urodynamics", "pessary_care")
  ) |>
    dplyr::mutate(volume = c(3e6, 7e6, 3e5, 9e5, 1e6, 3.1e6, 7.2e6, 3.1e5, 9.2e5, 1.02e6))
}

ndc_fake_cal <- function(scalar = 0.5, services = NAMCS_COMPARABLE_SERVICES) {
  structure(
    tibble::tibble(category = "ambulatory_visits", predicted = 1e7,
                   observed = 1e7 * scalar, scalar = scalar, flagged = FALSE),
    services = services, anchor_year = 2019L, anchor_records = 55L,
    base_year = 2025, provenance = list(source = "test")
  )
}

test_that("calibration scales visit rows and leaves procedures untouched", {
  v <- ndc_volumes()
  out <- apply_demand_calibration(v, ndc_fake_cal(0.5))

  visits <- out$service %in% NAMCS_COMPARABLE_SERVICES
  expect_true(all(out$calibration_scalar[visits] == 0.5))
  # THE CONTRACT. A visit-count anchor is evidence about encounters and nothing
  # else; scaling sling volume by it would propagate the anchor past its data.
  expect_true(all(out$calibration_scalar[!visits] == 1))
  expect_equal(out$volume[!visits], v$volume[!visits])
  expect_equal(out$volume[visits], v$volume[visits] * 0.5)
})

test_that("calibration applies to every projected year, not just the base year", {
  v <- ndc_volumes()
  out <- apply_demand_calibration(v, ndc_fake_cal(0.4))
  # The scalar corrects a level error in the model, so it must travel with the
  # trajectory. Applying it to the base year alone would put a step in 2026.
  for (y in unique(v$year)) {
    got <- out$volume[out$year == y & out$service == "return_visit"]
    want <- v$volume[v$year == y & v$service == "return_visit"] * 0.4
    expect_equal(got, want)
  }
})

test_that("a missing or malformed calibration leaves volumes alone", {
  v <- ndc_volumes()
  expect_equal(apply_demand_calibration(v, NULL), v)
  expect_equal(apply_demand_calibration(v, tibble::tibble()), v)
  # A table with no `scalar` column must not be silently treated as calibration.
  expect_equal(apply_demand_calibration(v, tibble::tibble(category = "x")), v)
})

test_that("fitting refuses a service set the basket does not contain", {
  skip_if_not_installed("assertthat")
  v <- ndc_volumes()
  expect_error(
    namcs_demand_calibration(v, base_year = 2025,
                             anchor = structure(
                               tibble::tibble(category = "ambulatory_visits",
                                              observed = 5e6, n_records = 55L,
                                              reliable = TRUE),
                               data_year = 2019L),
                             services = "not_a_service"),
    "no rows for year"
  )
})

# ---- Anchor (needs the real NAMCS file) ------------------------------------

ndc_have_namcs <- function() {
  file.exists("data-raw/namcs/namcs2019_clean.rds") ||
    file.exists(file.path("..", "..", "data-raw", "namcs", "namcs2019_clean.rds"))
}

test_that("NAMCS codes SEX 1 as female, verified against sex-specific diagnoses", {
  skip_if_not(ndc_have_namcs())
  path <- if (file.exists("data-raw/namcs/namcs2019_clean.rds")) {
    "data-raw/namcs/namcs2019_clean.rds"
  } else file.path("..", "..", "data-raw", "namcs", "namcs2019_clean.rds")
  n <- load_namcs_2019(path)
  d <- toupper(paste(n$DIAG1, n$DIAG2, n$DIAG3))

  # THE REGRESSION THIS LOCKS. NAMCS reverses the Census/ACS/BRFSS/MEPS
  # convention. The stratum builder had it backwards, so every "female" NAMCS
  # quantity was built from male visits -- for a female-predominant
  # subspecialty that silently substitutes the complement of the estimand.
  male_only <- grepl("N40", d, fixed = TRUE) | grepl("C61", d, fixed = TRUE)
  female_only <- grepl("Z34", d, fixed = TRUE) | grepl("N81", d, fixed = TRUE)
  expect_gt(sum(male_only), 0)
  expect_gt(sum(female_only), 0)
  expect_true(all(n$SEX[male_only] == 2L))
  expect_true(all(n$SEX[female_only] == 1L))

  sv <- namcs_urps_stratum_visits(flag_urps_visits(n))
  # Prolapse and incontinence are overwhelmingly female: if the mapping were
  # inverted the female share of URPS visits would collapse.
  fem <- sum(sv$n_visits_unweighted[sv$sex == "Female"])
  expect_gt(fem / sum(sv$n_visits_unweighted), 0.7)
})

test_that("the anchor reports its record count against the NCHS floor", {
  skip_if_not(ndc_have_namcs())
  path <- if (file.exists("data-raw/namcs/namcs2019_clean.rds")) {
    "data-raw/namcs/namcs2019_clean.rds"
  } else file.path("..", "..", "data-raw", "namcs", "namcs2019_clean.rds")
  a <- namcs_urps_visit_anchor(load_namcs_2019(path))
  expect_equal(a$category, "ambulatory_visits")
  expect_gt(a$observed, 0)
  expect_true(a$reliable)
  expect_gte(a$n_records, NAMCS_MIN_RECORDS)
  p <- attr(a, "provenance")
  expect_equal(attr(a, "data_year"), 2019L)
  expect_match(p$weight, "PATWT")
  expect_match(p$sex_coding, "1 = Female")
})

test_that("an anchor below the NCHS floor is flagged, not silently used", {
  skip_if_not(ndc_have_namcs())
  path <- if (file.exists("data-raw/namcs/namcs2019_clean.rds")) {
    "data-raw/namcs/namcs2019_clean.rds"
  } else file.path("..", "..", "data-raw", "namcs", "namcs2019_clean.rds")
  n <- load_namcs_2019(path)
  expect_message(a <- namcs_urps_visit_anchor(n, min_records = 10000L),
                 "reliability floor")
  expect_false(a$reliable)
})

test_that("the fitted scalar lands inside the published HDMM range", {
  skip_if_not(ndc_have_namcs())
  path <- if (file.exists("data-raw/namcs/namcs2019_clean.rds")) {
    "data-raw/namcs/namcs2019_clean.rds"
  } else file.path("..", "..", "data-raw", "namcs", "namcs2019_clean.rds")
  v <- ndc_volumes()
  cal <- suppressMessages(
    namcs_demand_calibration(v, base_year = 2025,
                             anchor = namcs_urps_visit_anchor(load_namcs_2019(path))))
  pub <- range(published_calibration_scalars()$scalar)
  # A scalar outside the range HDMM reports for whole specialties would mean a
  # structural mismatch in the estimand, not a level offset.
  expect_gte(cal$scalar, pub[1])
  expect_lte(cal$scalar, pub[2])
  expect_false(cal$flagged)
})
