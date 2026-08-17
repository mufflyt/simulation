# Unit tests for provider career lifecycle and retirement hazard pipeline

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("estimate_provider_lifecycle_hazards.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_provider_lifecycle_hazards.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("departure_hazard returns empirical retirement hazards", {
  hazards <- departure_hazard(age = c(40, 55, 62, 67, 72, 80), sex = "female")
  expect_type(hazards, "double")
  expect_equal(length(hazards), 6L)

  # Check specific hazard thresholds
  expect_equal(hazards[1], 0.0142) # Career change under 50
  expect_gt(hazards[3], 0.05)      # Age 62 retirement hazard
  expect_gt(hazards[6], 0.30)      # Age 80 high retirement hazard
})

test_that("provider_fte_decay_curve returns valid FTE multipliers", {
  decay <- provider_fte_decay_curve()
  expect_s3_class(decay, "data.frame")
  expect_equal(nrow(decay), 6L)
  expect_true(all(c("age_group", "fte_multiplier", "clinical_hrs_per_week") %in% names(decay)))

  young <- dplyr::filter(decay, age_group == "< 58")
  expect_equal(young$fte_multiplier, 1.00)

  older <- dplyr::filter(decay, age_group == "72+")
  expect_equal(older$fte_multiplier, 0.42)
})
