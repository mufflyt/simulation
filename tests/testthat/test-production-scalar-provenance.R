# A production scalar needs provenance for BOTH sides of the division. The
# external target side was hardened first; the prediction side was not, so a
# bare numeric in a smoke test produced arithmetic that was reported as
# calibration. These tests close that.

.pred <- function(...) {
  base <- list(estimand_id = "ui_prevalence", prediction = 0.30,
               model_run_id = "run_2026_08_14_a", model_version = "0.5.0",
               artifact_path = "artifacts/demand/base_year.rds",
               artifact_sha256 = "abc123", generated_utc = "2026-08-14T18:00:00Z",
               prediction_status = "production")
  utils::modifyList(base, list(...))
}

test_that("illustrative predictions cannot create production scalars", {
  illustrative <- .pred(model_run_id = "smoke_test", model_version = "test",
                        artifact_path = NA_character_,
                        artifact_sha256 = NA_character_,
                        generated_utc = NA_character_,
                        prediction_status = "illustrative")
  expect_error(
    compute_production_scalar(external_target = 0.237,
                              model_prediction = illustrative),
    "non-production prediction")
})

test_that("a bare numeric is not a prediction", {
  expect_error(assert_production_prediction(list(prediction = 0.30)),
               "missing production provenance")
})

test_that("a production prediction without a checksum is refused", {
  expect_error(compute_production_scalar(0.237, .pred(artifact_sha256 = "")),
               "no artifact checksum")
})

test_that("a fully provenanced prediction computes and carries both sides", {
  r <- compute_production_scalar(external_target = 0.237,
                                 model_prediction = .pred())
  expect_equal(r$calibration_scalar, 0.237 / 0.30)
  expect_equal(r$model_run_id, "run_2026_08_14_a")
  expect_equal(r$model_artifact_sha256, "abc123")
  # the calibrated prediction must land on the target by construction
  expect_equal(r$calibrated_prediction, 0.237)
})

test_that("degenerate predictions are refused", {
  expect_error(compute_production_scalar(0.237, .pred(prediction = 0)),
               "finite and > 0")
  expect_error(compute_production_scalar(0, .pred()), "finite and > 0")
})

test_that("calibration_state never claims a scalar exists", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  s <- calibration_state("../../config/calibration_targets.yml")
  expect_true(all(s$production_scalar == "pending_real_model_prediction"))
})
