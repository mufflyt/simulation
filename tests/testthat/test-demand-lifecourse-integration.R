# Integration of the life-course demand pathway with the FTE conversion (R/supply-workload_to_fte)
# and the demand concordance framework (R/demand-urps).

pop_by_age_year <- tidyr::expand_grid(year = c(2025L, 2030L, 2035L), age = 40:85) %>%
  dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))

test_that("lifecourse_required_fte hands service volumes to convert_workload_to_fte", {
  out <- lifecourse_required_fte(pop_by_age_year,
                                 wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]],
                                 n = 4000, seed = 1,
                          pathway = valid_pathway())
  expect_true(all(c("service_volumes", "demand_summary", "required_fte") %in% names(out)))
  expect_true(all(c("year", "required_fte") %in% names(out$required_fte)))
  expect_true(all(out$required_fte$required_fte > 0))
  expect_setequal(out$required_fte$year, c(2025L, 2030L, 2035L))
  # the FTE conversion reports its own calibration status (never a bare number)
  expect_true("calibration_status" %in% names(out$required_fte))
})

test_that("lifecourse_demand_estimand matches the demand_long estimand shape", {
  traj <- lifecourse_demand_trajectory(pop_by_age_year, n = 4000, seed = 1,
                               pathway = valid_pathway())
  d5 <- lifecourse_demand_estimand(traj$demand_summary, measure = "service_units")
  expect_named(d5, c("year", "estimand", "label", "demand_cases"))
  expect_true(all(d5$estimand == "D5_lifecourse_service"))
  expect_true(all(d5$demand_cases > 0))
  # same columns as compute_demand_denominators(), so they bind and flow through
  # compute_growth_adequacy()/assess_demand_concordance() unchanged
  d123 <- compute_demand_denominators(example_female_population_by_band())
  expect_true(all(names(d5) %in% names(d123)))
})

test_that("the life-course estimand is NOT a proportional rescaling of D1/D2/D3", {
  bands <- example_female_population_by_band()
  years <- sort(unique(bands$year))
  pay <- tidyr::expand_grid(year = years, age = 40:85) %>%
    dplyr::mutate(population = round(2e6 * exp(-0.02 * (age - 40))))
  traj <- lifecourse_demand_trajectory(pay, n = 4000, seed = 1,
                               pathway = valid_pathway())
  demand_long <- dplyr::bind_rows(
    compute_demand_denominators(bands),
    lifecourse_demand_estimand(traj$demand_summary))
  supply <- tibble::tibble(year = years,
                           effective_fte_median = seq(1300, 1500, length.out = length(years)))
  conc <- assess_demand_concordance(compute_growth_adequacy(supply, demand_long), demand_long)
  expect_true("D5_lifecourse_service" %in% conc$final_year_adequacy$estimand)
  expect_true(conc$distinct_estimands)   # a genuinely independent estimand
})
