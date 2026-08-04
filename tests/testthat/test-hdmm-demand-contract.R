# HDMM life-course demand contract exporter, tiers 5-6 (R/export_demand_contract.R).

# The exporter now refuses to write a contract on placeholder inputs, so these
# schema and index tests declare themselves exploratory. The missing-column test
# below deliberately calls the real function: that check runs before the
# calibration gate, and it must keep reporting the column problem.
export_hdmm <- function(...) {
  suppressMessages(export_hdmm_demand_contract(..., allow_uncalibrated = TRUE))
}

mk_traj <- function() {
  tibble::tibble(year = 2025:2035,
                 care_seeking_national  = seq(4.0e6, 5.2e6,  length.out = 11),
                 service_units_national = seq(9.0e6, 12.6e6, length.out = 11))
}

test_that("exporter emits tiers 5 and 6 in the shared contract schema", {
  out <- export_hdmm(mk_traj(), output_directory = tempfile("hdmm_"),
                                                  scenario = "baseline", verbose = FALSE)
  dc <- read.csv(out$csv_path, stringsAsFactors = FALSE)
  expect_setequal(unique(dc$denominator_tier), c("tier5_care_seeking", "tier6_procedural"))
  expect_true(all(c("model", "model_version", "calibration_status", "denominator_tier",
                    "calendar_year", "prevalence", "national_cases", "denominator_index")
                  %in% names(dc)))
  expect_true(all(dc$model == "HDMM"))
  expect_true(all(dc$calibration_status == "placeholder_uncalibrated"))
  expect_true(all(is.na(dc$prevalence)))
  expect_true(file.exists(out$manifest_path))
})

test_that("index is rebased to 100 at the base year and tracks each tier's growth", {
  out <- export_hdmm(mk_traj(), output_directory = tempfile("hdmm_"),
                                                  base_year = 2025L, verbose = FALSE)
  dc <- out$data
  base5 <- dc$denominator_index[dc$denominator_tier == "tier5_care_seeking" & dc$calendar_year == 2025]
  expect_equal(base5, 100)
  end6 <- dc$denominator_index[dc$denominator_tier == "tier6_procedural" & dc$calendar_year == 2035]
  expect_equal(end6, 100 * (12.6e6 / 9.0e6), tolerance = 1e-6)
})

test_that("exporter rejects a trajectory missing required columns", {
  bad <- tibble::tibble(year = 2025:2026, care_seeking_national = c(1, 2))
  expect_error(export_hdmm_demand_contract(bad, output_directory = tempfile("hdmm_")),
               "trajectory needs columns")
})

test_that("the life-course trajectory feeds the exporter end to end", {
  pop_by_age_year <- dplyr::bind_rows(lapply(c(2025L, 2026L), function(y)
    tibble::tibble(age = 40:85, population = round(2e6 * exp(-0.02 * (40:85 - 40))), year = y)))
  traj <- lifecourse_demand_trajectory(pop_by_age_year, n = 5000, seed = 1)
  out <- export_hdmm(traj$demand_summary,
                                                  output_directory = tempfile("hdmm_"), verbose = FALSE)
  expect_true(file.exists(out$csv_path))
  expect_setequal(unique(out$data$calendar_year), c(2025L, 2026L))
})
