testthat::test_that("baseline is an identity transformation in policy migration scenarios", {
  baseline <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    female_older_population = c(4e5, 2e5),
    pfd_demand = c(1000, 800),
    provider_fte = c(50, 40),
    fellowship_applications = c(20, 20)
  )
  evidence <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    older_female_net_migration = c(5000, 1000),
    legislative_climate = c(1, -1)
  )

  simulated <- simulate_policy_migration_scenarios(
    baseline,
    evidence = evidence,
    scenario = "baseline",
    draws = 3L
  )

  testthat::expect_equal(simulated$pfd_demand_scenario, simulated$pfd_demand)
  testthat::expect_equal(simulated$provider_fte_scenario, simulated$provider_fte)
})

testthat::test_that("observed migration scenario scales demand by net migration rate", {
  baseline <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    female_older_population = c(1e5, 1e5),
    pfd_demand = c(1000, 1000),
    provider_fte = c(50, 50),
    fellowship_applications = c(20, 20)
  )
  evidence <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    older_female_net_migration = c(5000, 0),
    legislative_climate = c(0, 0)
  )

  simulated <- simulate_policy_migration_scenarios(
    baseline,
    evidence = evidence,
    scenario = "observed_migration",
    draws = 10L,
    seed = 123L
  )

  fl_demand <- dplyr::filter(simulated, state == "FL")$pfd_demand_scenario
  testthat::expect_true(all(fl_demand == 1050))
})

testthat::test_that("summarize_policy_migration_scenarios returns summary stats", {
  baseline <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    female_older_population = c(1e5, 1e5),
    pfd_demand = c(1000, 1000),
    provider_fte = c(50, 50),
    fellowship_applications = c(20, 20)
  )
  evidence <- tibble::tibble(
    state = c("FL", "CO"),
    year = 2030L,
    older_female_net_migration = c(5000, 0),
    legislative_climate = c(1, -1)
  )

  simulated <- simulate_policy_migration_scenarios(
    baseline,
    evidence = evidence,
    scenario = "combined_stress",
    draws = 20L,
    seed = 456L
  )

  summary_tbl <- summarize_policy_migration_scenarios(simulated)
  testthat::expect_true("mean_demand_change" %in% names(summary_tbl))
  testthat::expect_true("mean_provider_change" %in% names(summary_tbl))
  testthat::expect_equal(nrow(summary_tbl), 2L)
})
