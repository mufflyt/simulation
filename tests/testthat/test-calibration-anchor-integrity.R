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
  # Originally this read the live config, which then had an anchor declared but
  # not yet acquired. All four are acquired now, so the live read asserts
  # nothing. The rule being protected -- a DECLARED but MISSING anchor is
  # reported as missing_declared and does NOT fail strict verification, because
  # "not collected yet" is a different condition from "tampered with" -- is
  # tested on a fixture so it cannot decay again as anchors land.
  skip_if_not(file.exists("../../data/anchors/prolapse_procedure_volume.csv"))
  d <- file.path(tempdir(), "anchor_missing")
  unlink(d, recursive = TRUE)
  dir.create(file.path(d, "data/anchors"), recursive = TRUE)
  dir.create(file.path(d, "config"), recursive = TRUE)
  file.copy("../../config/calibration_targets.yml", file.path(d, "config/"))
  for (f in list.files("../../data/anchors", "\\.csv$", full.names = TRUE)) {
    file.copy(f, file.path(d, "data/anchors/"))
  }
  # Mark one anchor as knowingly not-yet-collected AND remove its file. Both
  # halves are required: status "missing" in the config is the declaration that
  # this is a planned gap, which is what separates it from a file that vanished.
  cfgp <- file.path(d, "config/calibration_targets.yml")
  cfg <- yaml::read_yaml(cfgp)
  cfg$anchors$ui_prevalence$status <- "missing"
  yaml::write_yaml(cfg, cfgp)
  unlink(file.path(d, "data/anchors/ui_prevalence.csv"))

  r <- verify_calibration_anchors(root = d, strict = TRUE)   # must NOT error
  expect_equal(r$state[r$anchor == "ui_prevalence"], "missing_declared")
  # the remaining anchors still verify, so a declared gap does not mask tampering
  expect_true(all(r$state[r$anchor != "ui_prevalence"] == "ok"))
})

test_that("an anchor file that vanishes is an error, not a declared gap", {
  # The distinction the state machine turns on: "not collected yet" (declared,
  # tolerated) versus "was here and is gone" (absent, fails strict). Without
  # this, marking status: missing would be a way to silence a deleted anchor.
  skip_if_not(file.exists("../../data/anchors/prolapse_procedure_volume.csv"))
  d <- file.path(tempdir(), "anchor_vanished")
  unlink(d, recursive = TRUE)
  dir.create(file.path(d, "data/anchors"), recursive = TRUE)
  dir.create(file.path(d, "config"), recursive = TRUE)
  file.copy("../../config/calibration_targets.yml", file.path(d, "config/"))
  for (f in list.files("../../data/anchors", "\\.csv$", full.names = TRUE)) {
    file.copy(f, file.path(d, "data/anchors/"))
  }
  unlink(file.path(d, "data/anchors/ui_prevalence.csv"))   # no status change

  r <- verify_calibration_anchors(root = d, strict = FALSE)
  expect_equal(r$state[r$anchor == "ui_prevalence"], "absent")
  expect_error(verify_calibration_anchors(root = d, strict = TRUE),
               "calibration anchor integrity failed")
})

test_that("every declared anchor is currently acquired and verifying", {
  # The complement of the fixture above: a plain status report on the live
  # config. If an anchor is ever added to the YAML without its file, this fails
  # and names it rather than passing quietly.
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  r <- verify_calibration_anchors(root = "../..", strict = TRUE)
  expect_true(all(r$state == "ok"),
              info = paste("not ok:", paste(r$anchor[r$state != "ok"],
                                            collapse = ", ")))
})
