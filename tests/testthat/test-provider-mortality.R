# Fast unit tests: whole file runs in well under one second.

illustrative_test_schedule <- function(ratio = 0.70) {
  build_mortality_schedule(
    life_table_path = NULL,
    physician_mortality_ratio = ratio,
    ratio_evidence = "Kiang MV et al. JAMA Intern Med 2023.",
    max_age = 90L,
    verbose = FALSE
  )
}

test_that("cause-specific exit probabilities sum to the total", {
  exit_roster <- data.frame(
    age = c(35L, 52L, 68L, 84L),
    sex = c("female", "male", "female", "male"),
    retirement_probability = c(0.001, 0.020, 0.190, 0.500),
    career_change_probability = c(0.030, 0.008, 0.002, 0.000)
  )
  allocated <- allocate_annual_exits(
    provider_roster = exit_roster,
    mortality_schedule = illustrative_test_schedule(),
    retirement_probability_column = "retirement_probability",
    career_change_probability_column = "career_change_probability",
    verbose = FALSE
  )
  expect_equal(
    allocated$probability_exit_retirement +
      allocated$probability_exit_career_change +
      allocated$probability_exit_death,
    allocated$probability_exit_any
  )
  expect_true(all(allocated$probability_exit_any <= 1))
  expect_true(all(allocated$probability_exit_death >= 0))
})

test_that("allocation is invariant to the order causes are supplied", {
  base_roster <- data.frame(
    age = c(48L, 66L),
    sex = c("male", "female"),
    hazard_a = c(0.05, 0.22),
    hazard_b = c(0.01, 0.03)
  )
  swapped_roster <- data.frame(
    age = c(48L, 66L),
    sex = c("male", "female"),
    hazard_b = c(0.01, 0.03),
    hazard_a = c(0.05, 0.22)
  )
  mortality_schedule <- illustrative_test_schedule()
  forward_allocation <- allocate_annual_exits(
    base_roster, mortality_schedule, "hazard_a", "hazard_b", verbose = FALSE
  )
  reversed_allocation <- allocate_annual_exits(
    swapped_roster, mortality_schedule, "hazard_a", "hazard_b",
    verbose = FALSE
  )
  expect_equal(forward_allocation$probability_exit_any,
               reversed_allocation$probability_exit_any)
  expect_equal(forward_allocation$probability_exit_death,
               reversed_allocation$probability_exit_death)
})

test_that("mortality is invariant to retirement scenario shifts", {
  expect_true(assert_mortality_scenario_invariant(
    mortality_schedule_baseline = illustrative_test_schedule(),
    mortality_schedule_scenario = illustrative_test_schedule(),
    verbose = FALSE
  ))
  expect_error(
    assert_mortality_scenario_invariant(
      mortality_schedule_baseline = illustrative_test_schedule(0.70),
      mortality_schedule_scenario = illustrative_test_schedule(0.90),
      verbose = FALSE
    ),
    "leaked into the mortality builder"
  )
})

test_that("net retirement hazard never goes negative", {
  expect_warning(
    floored_probability <- net_retirement_probability(
      all_cause_exit_probability = c(0.002, 0.050),
      annual_death_probability = c(0.008, 0.010),
      verbose = FALSE
    ),
    "floored at zero"
  )
  expect_equal(floored_probability[1], 0)
  expect_true(all(floored_probability >= 0))
})

test_that("net retirement plus mortality reconstructs the all-cause hazard", {
  observed_all_cause <- c(0.18, 0.24, 0.40)
  observed_death <- c(0.010, 0.015, 0.030)
  net_probability <- net_retirement_probability(
    observed_all_cause, observed_death, verbose = FALSE
  )
  reconstructed <- 1 - (1 - net_probability) * (1 - observed_death)
  expect_equal(reconstructed, observed_all_cause, tolerance = 1e-12)
})

test_that("uncalibrated schedules are refused, analogy gated", {
  uncalibrated_schedule <- build_mortality_schedule(
    life_table_path = NULL,
    physician_mortality_ratio = 1.0,
    ratio_evidence = "uncalibrated: no physician SMR supplied",
    max_age = 90L,
    verbose = FALSE
  )
  expect_error(
    assert_mortality_publishable(uncalibrated_schedule, TRUE, FALSE),
    "uncalibrated_illustrative"
  )
  expect_error(
    assert_mortality_publishable(illustrative_test_schedule(), FALSE, FALSE),
    "uncalibrated_illustrative"
  )
})

test_that("unmatched age-sex cells fail loudly rather than silently", {
  out_of_range_roster <- data.frame(
    age = 95L,
    sex = "male",
    retirement_probability = 0.6,
    career_change_probability = 0.0
  )
  expect_error(
    allocate_annual_exits(
      out_of_range_roster, illustrative_test_schedule(),
      "retirement_probability", "career_change_probability", verbose = FALSE
    ),
    "matched no age-sex cell"
  )
})

test_that("mortality moves the URPS cohort by a negligible amount", {
  urps_cohort <- data.frame(
    age = c(rep(40L, 651L), rep(54L, 655L)),
    sex = rep(c("female", "male"), length.out = 1306L),
    retirement_probability = 0,
    career_change_probability = 0
  )
  cohort_exits <- allocate_annual_exits(
    urps_cohort, illustrative_test_schedule(),
    "retirement_probability", "career_change_probability", verbose = FALSE
  )
  expected_annual_deaths <- sum(cohort_exits$probability_exit_death)
  expect_lt(expected_annual_deaths, 6)
  expect_lt(expected_annual_deaths / 1306, 0.005)
})

test_that("hall of shame: sequential Bernoulli exits are order-dependent", {
  shame_fixture <- readr::read_csv(
    testthat::test_path("fixtures",
                        "sequential_exit_double_count_hall_of_shame.csv"),
    show_col_types = FALSE
  )
  hazard_allocation <- allocate_annual_exits(
    provider_roster = as.data.frame(shame_fixture),
    mortality_schedule = illustrative_test_schedule(),
    retirement_probability_column = "retirement_probability",
    career_change_probability_column = "career_change_probability",
    verbose = FALSE
  )
  expect_equal(hazard_allocation$probability_exit_any,
               shame_fixture$expected_probability_exit_any,
               tolerance = 1e-5)

  naive_sum <- shame_fixture$retirement_probability +
    shame_fixture$career_change_probability +
    hazard_allocation$annual_death_probability
  expect_false(isTRUE(all.equal(naive_sum,
                                hazard_allocation$probability_exit_any,
                                tolerance = 1e-4)))
})
