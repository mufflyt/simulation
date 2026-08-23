test_that("service share pipeline runs end to end cleanly", {
  skip(paste(
    "combine_service_share_evidence() and build_service_share_calibration_bundle()",
    "no longer exist -- calibrate_service_share_model() was redesigned around",
    "Bayesian shrinkage over a real `events` argument (see",
    "test-calibration-service-shares.R for coverage of the current API), and",
    "R/zzz-service_share_runner.R (from the -04-integration stage) is the real",
    "end-to-end wiring this test's name describes. Rewrite once that runner",
    "lands and its own contract is known, rather than guess at it here."
  ))
})
