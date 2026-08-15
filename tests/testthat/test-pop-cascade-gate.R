# The POP anchor constrains a TRANSITION, not the output. These tests exist to
# stop the mismatch being "fixed" by multiplying the final number.

.cascade <- function() yaml::read_yaml("../../config/pop_cascade_transitions.yml")

test_that("a large POP mismatch must be resolved upstream, not by a terminal scalar", {
  skip_if_not(file.exists("../../config/pop_cascade_transitions.yml"))
  a <- .cascade()$anchor_constraint
  expect_false(a$terminal_scalar_applied)
  expect_true(a$requires_pathway_recalibration)
  expect_identical(a$resolution_required, "pathway_recalibration")
})

test_that("every cascade transition declares its evidence", {
  skip_if_not(file.exists("../../config/pop_cascade_transitions.yml"))
  required <- c("probability", "source", "population", "vintage",
                "confidence", "calibration_status")
  for (nm in names(.cascade()$transitions)) {
    t <- .cascade()$transitions[[nm]]
    expect_true(all(required %in% names(t)),
                info = paste("transition", nm, "missing evidence fields"))
  }
})

test_that("the back-solved constraint is internally consistent", {
  skip_if_not(file.exists("../../config/pop_cascade_transitions.yml"))
  a <- .cascade()$anchor_constraint
  # V = N x p_combined x recurrence_multiplier, to within rounding
  implied <- a$treated_population * a$required_combined_probability *
             a$recurrence_multiplier
  expect_lt(abs(implied - a$observed_encounters) / a$observed_encounters, 0.01)
  # and the overstatement factor must match the two probabilities
  expect_lt(abs(a$current_combined_probability / a$required_combined_probability
                - a$overstatement_factor), 0.05)
})

test_that("a low-confidence transition is never marked calibrated", {
  skip_if_not(file.exists("../../config/pop_cascade_transitions.yml"))
  for (nm in names(.cascade()$transitions)) {
    t <- .cascade()$transitions[[nm]]
    if (identical(t$confidence, "low")) {
      expect_false(identical(t$calibration_status, "calibrated"),
                   info = paste(nm, "claims calibrated on low confidence"))
    }
  }
})

test_that("illustrative predictions never reach a production scalar field", {
  # regression for the episode where 0.963 / 1.408 / 0.790 were reported as
  # calibration scalars but were arithmetic against invented predictions
  illustrative <- list(
    estimand_id = "prolapse_procedure_volume", prediction = 100000,
    model_run_id = "smoke_test", model_version = "test",
    artifact_path = NA_character_, artifact_sha256 = NA_character_,
    generated_utc = NA_character_, prediction_status = "illustrative")
  expect_error(compute_production_scalar(140762, illustrative),
               "non-production prediction")
  # and the readiness report must not name its column a production scalar
  skip_if_not(file.exists("../../scripts/calibration/build_empirical_calibration_targets.R"))
  src <- readLines("../../scripts/calibration/build_empirical_calibration_targets.R")
  expect_true(any(grepl("illustrative_smoke_test_scalar", src, fixed = TRUE)))
})
