test_that("PSA summary reports interval and deficit probability", {
  psa <- run_psa(list(psa_uniform("x", -1, 1)), function(p) p$x,
                 n = 200, seed = 19, verbose = FALSE)
  out <- urpssim:::psa_outcome_summary(psa)
  expect_equal(out$n_draws, 200L)
  expect_equal(out$n_complete, 200L)
  expect_lt(abs(out$median), .1)
  expect_equal(out$p_deficit, .5, tolerance = .06)
})

test_that("PSA report preserves draws and writes a guarded artifact", {
  skip_if_not_installed("jsonlite")
  psa <- run_psa(list(psa_uniform("x", -1, 1), psa_uniform("y", 0, 1)),
                 function(p) c(gap = p$x + p$y^2),
                 n = 40, seed = 23, verbose = FALSE)
  out_dir <- tempfile("psa-report-")
  report <- urpssim:::write_psa_report(psa, output = "gap", output_dir = out_dir,
                             prefix = "fixture", inputs = list(version = 1),
                             code_paths = "R/psa.R", write_plot = FALSE)
  expect_true(all(file.exists(unlist(report[c("artifact", "draws", "summary", "prcc", "srrc")]))) )
  expect_true(file.exists(paste0(report$artifact, ".provenance.json")))
  restored <- read_artifact_with_provenance(
    report$artifact,
    expected_inputs = list(model_inputs = list(version = 1), psa_inputs = psa$inputs,
                           output = "gap", deficit_direction = "negative",
                           deficit_threshold = 0)
  )
  expect_equal(restored$draws, psa$draws)
  expect_equal(utils::read.csv(report$summary)$output, "gap")
})
