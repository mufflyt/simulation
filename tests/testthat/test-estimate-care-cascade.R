# Unit tests for care cascade estimation pipeline and demand transition registry

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("estimate_care_cascade.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "estimate_care_cascade.R")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("care cascade parameters are evidence_anchored in demand_transition_registry", {
  reg <- demand_transition_registry()
  expect_true("p_seek" %in% reg$param)
  expect_true("p_referral" %in% reg$param)

  p_seek_rows <- dplyr::filter(reg, param == "p_seek", variant == "default")
  expect_equal(nrow(p_seek_rows), 3L)
  expect_true(all(p_seek_rows$calibration_tier == "derived_by_analogy"))
  expect_equal(p_seek_rows$value[p_seek_rows$condition == "ui"], 0.4795)

  p_ref_rows <- dplyr::filter(reg, param == "p_referral", variant == "default")
  expect_equal(nrow(p_ref_rows), 3L)
  expect_true(all(p_ref_rows$calibration_tier == "derived_by_analogy"))
  expect_equal(p_ref_rows$value[p_ref_rows$condition == "ui"], 0.5756)
  expect_equal(p_ref_rows$value[p_ref_rows$condition == "pop"], 0.6373)
  expect_equal(p_ref_rows$value[p_ref_rows$condition == "ai"], 0.4389)
})

test_that("MCBS 2022 microdata loads cleanly when present", {
  mcbs_path <- .repo_path("data-raw", "mcbs", "mcbs_2022_women65plus.rds")
  skip_if_not(file.exists(mcbs_path), "MCBS microdata not present")

  mcbs <- readRDS(mcbs_path)
  expect_gt(nrow(mcbs), 0L)
  expect_true("HLT_LOSTURIN" %in% names(mcbs) || "ui_loss" %in% names(mcbs))
  expect_true("PUFFWGT" %in% names(mcbs))
})

test_that("Pooled NAMCS 2015-2019 microdata loads cleanly when present", {
  namcs_path <- .repo_path("data-raw", "namcs", "namcs_pooled_2015_2019.rds")
  skip_if_not(file.exists(namcs_path), "NAMCS microdata not present")

  namcs <- readRDS(namcs_path)
  expect_gt(nrow(namcs), 0L)
  expect_true("SPECCAT" %in% names(namcs))
  expect_true("PATWT" %in% names(namcs))
})
