testthat::test_that("probabilities sum to one and demand is conserved", {
  choice_fixture <- tibble::tibble(
    origin_id = base::rep(c("A", "B"), each = 2),
    destination_id = base::rep(c("X", "Y"), times = 2),
    fte = c(1, 2, 1, 2),
    travel_time_min = c(10, 70, 40, 20),
    wait_days = c(90, 10, 20, 20),
    subspecialty = c(TRUE, TRUE, FALSE, TRUE),
    origin_demand = c(100, 100, 200, 200),
    origin_county = base::rep(c("001", "003"), each = 2),
    destination_county = base::rep(c("001", "005"), times = 2),
    origin_hrr = base::rep(c("H1", "H2"), each = 2),
    destination_hrr = base::rep(c("H1", "H2"), times = 2)
  )
  probabilities <- predict_patient_destination_choice(choice_fixture)
  probability_sums <- probabilities |>
    dplyr::group_by(.data$origin_id) |>
    dplyr::summarise(value = base::sum(.data$choice_probability))
  testthat::expect_equal(probability_sums$value, c(1, 1), tolerance = 1e-12)
  allocation <- allocate_patient_destination_flows(probabilities)
  testthat::expect_true(allocation$system_diagnostics$conserved)
  testthat::expect_equal(
    allocation$system_diagnostics$allocated_demand,
    300,
    tolerance = 1e-10
  )
})

testthat::test_that("choices respond monotonically to time, wait, and FTE", {
  base_choice <- tibble::tibble(
    origin_id = c("A", "A"),
    destination_id = c("X", "Y"),
    fte = c(1, 1),
    travel_time_min = c(10, 10),
    wait_days = c(20, 20),
    subspecialty = c(TRUE, TRUE),
    origin_demand = c(100, 100)
  )
  equal_probability <- predict_patient_destination_choice(base_choice)
  testthat::expect_equal(
    equal_probability$choice_probability,
    c(0.5, 0.5)
  )
  higher_fte <- base_choice
  higher_fte$fte[1] <- 2
  testthat::expect_gt(
    predict_patient_destination_choice(higher_fte)$choice_probability[1],
    0.5
  )
  longer_wait <- base_choice
  longer_wait$wait_days[1] <- 60
  testthat::expect_lt(
    predict_patient_destination_choice(longer_wait)$choice_probability[1],
    0.5
  )
  longer_trip <- base_choice
  longer_trip$travel_time_min[1] <- 60
  testthat::expect_lt(
    predict_patient_destination_choice(longer_trip)$choice_probability[1],
    0.5
  )
})

testthat::test_that("capacity accounting preserves served plus unmet demand", {
  choice_fixture <- tibble::tibble(
    origin_id = c("A", "A"),
    destination_id = c("X", "Y"),
    fte = c(1, 1),
    travel_time_min = c(10, 20),
    wait_days = c(10, 10),
    subspecialty = c(TRUE, TRUE),
    origin_demand = c(100, 100)
  )
  capacity_fixture <- tibble::tibble(
    destination_id = c("X", "Y"),
    annual_capacity = c(10, 20)
  )
  allocation <- run_patient_destination_choice(
    choice_fixture,
    destination_capacity = capacity_fixture
  )
  destination_totals <- allocation$destination_totals
  testthat::expect_equal(
    base::sum(destination_totals$served_demand) +
      base::sum(destination_totals$unmet_demand),
    100,
    tolerance = 1e-10
  )
})

testthat::test_that("invalid and incomplete choice sets fail closed", {
  invalid_choice <- tibble::tibble(
    origin_id = "A",
    destination_id = "X",
    fte = 0,
    travel_time_min = 10,
    wait_days = 10,
    subspecialty = TRUE,
    origin_demand = 100
  )
  testthat::expect_error(
    predict_patient_destination_choice(invalid_choice),
    "greater than zero"
  )
})
