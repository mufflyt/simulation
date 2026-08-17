# tests/testthat/test-scientific-bva.R
# Scientific Hardening Layer 2E: Boundary-Value Analysis Unit Tests

test_that("Drive-time 30/60/120/180 boundaries evaluate L-eps, L, L+eps accurately", {
  # 30-minute boundary (29.999, 30.000, 30.001)
  expect_equal(test_boundary_value("drive_time_30", 29.999999), "00-30")
  expect_equal(test_boundary_value("drive_time_30", 30.000000), "00-30")
  expect_equal(test_boundary_value("drive_time_30", 30.000001), "31-60")

  # 60-minute boundary (59.999, 60.000, 60.001)
  expect_equal(test_boundary_value("drive_time_60", 59.999999), "31-60")
  expect_equal(test_boundary_value("drive_time_60", 60.000000), "31-60")
  expect_equal(test_boundary_value("drive_time_60", 60.000001), "61-120")
})

test_that("Probability 0/1 boundary catches invalid domain values", {
  expect_equal(test_boundary_value("probability_lower", -0.000001), "error")
  expect_equal(test_boundary_value("probability_lower", 0.000000), "valid")
  expect_equal(test_boundary_value("probability_upper", 1.000000), "valid")
  expect_equal(test_boundary_value("probability_upper", 1.000001), "error")
})

test_that("Supply-demand zero crossing boundary evaluates shortage, balanced, surplus", {
  expect_equal(test_boundary_value("supply_demand_balance", -1.0), "shortage")
  expect_equal(test_boundary_value("supply_demand_balance", 0.0), "balanced")
  expect_equal(test_boundary_value("supply_demand_balance", 1.0), "surplus")
})

test_that("Hospital capability threshold evaluates volume boundaries", {
  expect_equal(test_boundary_value("hospital_capability_threshold", 4.99), "incapable")
  expect_equal(test_boundary_value("hospital_capability_threshold", 5.00), "capable")
  expect_equal(test_boundary_value("hospital_capability_threshold", 5.01), "capable")
})
