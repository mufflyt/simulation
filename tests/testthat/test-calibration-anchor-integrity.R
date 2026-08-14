# The guard exists because a sha256 nothing verifies is worse than no sha256:
# it reads as an integrity guarantee and provides none. These tests assert it
# actually fires, not merely that it runs.

test_that("verify_calibration_anchors passes on unmodified anchors", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  r <- verify_calibration_anchors(root = "../..", strict = FALSE)
  present <- r[!r$state %in% c("missing_declared"), ]
  expect_true(all(present$state == "ok"))
})

test_that("a one-character edit to an anchor is caught", {
  skip_if_not(file.exists("../../data/anchors/prolapse_procedure_volume.csv"))
  d <- file.path(tempdir(), "anchor_tamper")
  unlink(d, recursive = TRUE)
  dir.create(file.path(d, "data/anchors"), recursive = TRUE)
  dir.create(file.path(d, "config"), recursive = TRUE)
  file.copy("../../config/calibration_targets.yml", file.path(d, "config/"))
  for (f in list.files("../../data/anchors", "\\.csv$", full.names = TRUE)) {
    file.copy(f, file.path(d, "data/anchors/"))
  }
  f <- file.path(d, "data/anchors/prolapse_procedure_volume.csv")
  x <- readLines(f); x[2] <- sub("140762", "140763", x[2]); writeLines(x, f)

  r <- verify_calibration_anchors(root = d, strict = FALSE)
  expect_equal(r$state[r$anchor == "prolapse_procedure_volume"], "mismatch")
  expect_error(verify_calibration_anchors(root = d, strict = TRUE),
               "calibration anchor integrity failed")
})

test_that("not-yet-acquired anchors are reported, not failed", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  r <- verify_calibration_anchors(root = "../..", strict = TRUE)
  expect_true("missing_declared" %in% r$state)
})
