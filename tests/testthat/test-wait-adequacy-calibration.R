# The inverse counterpart to the forward adequacy->access bridge: wait times are
# URPS-specific access evidence, but the clear_access() response function cannot
# identify an adequacy below 1.0 from a finite observed wait. These tests pin
# that identification boundary and the refusal to promote wait evidence to a
# calibrated adequacy.

testthat::test_that(
  "clear_access inverse reproduces the original wait",
  {

    wait_business_days <- 23.1
    wait_scale <- 5

    inverse_tbl <- invert_clear_access_wait(
      wait_business_days = wait_business_days,
      wait_scale = wait_scale
    )

    reconstructed_wait <- wait_scale *
      inverse_tbl$utilization /
      (1 - inverse_tbl$utilization)

    testthat::expect_equal(
      reconstructed_wait,
      wait_business_days,
      tolerance = 1e-12
    )
  }
)


testthat::test_that(
  "finite wait inversion cannot produce shortage adequacy",
  {

    inverse_tbl <- invert_clear_access_wait(
      wait_business_days = c(23.1, 35, 46, 51, 41),
      wait_scale = 5
    )

    testthat::expect_true(
      all(inverse_tbl$implied_adequacy > 1)
    )
  }
)


testthat::test_that(
  "current 0.948 anchor is not identified by finite waits",
  {

    status_obj <- wait_adequacy_identification_status(
      reference_adequacy = 0.948
    )

    testthat::expect_false(
      status_obj$identified_from_finite_wait
    )

    testthat::expect_gt(
      status_obj$reference_utilization,
      1
    )

    testthat::expect_equal(
      status_obj$status,
      "not_identified_saturated_branch"
    )
  }
)


testthat::test_that(
  "wait evidence does not masquerade as calibrated capacity",
  {

    status_obj <- wait_adequacy_identification_status()

    testthat::expect_equal(
      status_obj$calibration_status,
      "measured_input_unvalidated_response"
    )

    testthat::expect_false(
      status_obj$calibration_status == "calibrated"
    )
  }
)


testthat::test_that(
  "registered URPS waits enter the evidence ledger",
  {

    evidence_tbl <- urps_wait_adequacy_evidence()

    testthat::expect_gt(
      nrow(evidence_tbl),
      0
    )

    testthat::expect_true(
      all(
        evidence_tbl$interpretation ==
          "not_identifiable_from_this_evidence"
      )
    )

    testthat::expect_true(
      all(
        evidence_tbl$evidence_type ==
          "empirical_observation"
      )
    )
  }
)
