test_that("calculate_patient_destination_probabilities returns valid probabilities", {
  dist_mat <- matrix(c(
    10, 30,
    40, 15
  ), nrow = 2, byrow = TRUE)

  dests <- tibble::tribble(
    ~destination_id, ~clinical_fte, ~wait_days, ~has_subspecialist,
    "D1", 1.5, 14, TRUE,
    "D2", 1.0, 45, FALSE
  )

  probs <- calculate_patient_destination_probabilities(dist_mat, dests)

  expect_equal(nrow(probs), 2)
  expect_equal(ncol(probs), 2)
  expect_equal(rowSums(probs), c(1, 1), tolerance = 1e-6)

  # D1 has more FTE, shorter travel from origin 1, shorter wait, and subspecialist -> prob(D1) > prob(D2) for origin 1
  expect_gt(probs[1, 1], probs[1, 2])
})

test_that("predict_patient_destination_choice conserves patient demand and calculates flows", {
  dist_mat <- matrix(c(
    10, 30,
    40, 15
  ), nrow = 2, byrow = TRUE)

  origins <- tibble::tribble(
    ~origin_id, ~county_fips, ~hrr_code, ~patient_demand_n,
    "O1", "08001", "HRR001", 100,
    "O2", "08003", "HRR002", 200
  )

  dests <- tibble::tribble(
    ~destination_id, ~county_fips, ~hrr_code, ~clinical_fte, ~wait_days, ~has_subspecialist, ~capacity_patients_n,
    "D1", "08001", "HRR001", 1.5, 14, TRUE, 200,
    "D2", "08003", "HRR002", 1.0, 45, FALSE, 150
  )

  res <- predict_patient_destination_choice(origins, dests, dist_mat)

  expect_named(res, c("probability_matrix", "destination_summary", "boundary_summary", "severe_access_origins"))
  expect_equal(sum(res$boundary_summary$total_demand), 300)
  expect_equal(sum(res$destination_summary$received_demand_n), 300, tolerance = 1e-5)
})
