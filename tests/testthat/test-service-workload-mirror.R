# config/service_workload.yml is a PROVENANCE MIRROR, not runtime configuration.
#
# Nothing loads it: urps_service_workload() builds the basket from CMS data in
# code, and the scalars below are independently defined as R constants. That
# creates the dangerous state this file exists to eliminate -- a reviewer edits
# the YAML to correct a value, the edit changes nothing, and they believe
# otherwise. Every duplicated executable value is checked here.

.MIRROR_MSG <- paste(
  "service_workload.yml is a provenance mirror, not runtime configuration.",
  "Update the executable value and its provenance mirror together.")

.wl <- function() yaml::read_yaml("../../config/service_workload.yml")

test_that("the file is a mirror, and says so", {
  skip_if_not(file.exists("../../config/service_workload.yml"))
  hdr <- readLines("../../config/service_workload.yml", n = 20)
  # the header must not claim to be calibrated runtime config, which is what it
  # said before and is how it came to drift unnoticed
  expect_true(any(grepl("PROVENANCE MIRROR", hdr, fixed = TRUE)))
  expect_true(any(grepl("NOT RUNTIME CONFIGURATION", hdr, fixed = TRUE)))
})

test_that("nothing loads it, so it must never be treated as authoritative", {
  # If a loader is ever added, this fails and forces the question: should the
  # YAML become authoritative, or should the loader be removed? Silently gaining
  # a consumer is the failure mode.
  r_src <- unlist(lapply(list.files("../../R", "\\.R$", full.names = TRUE),
                         readLines, warn = FALSE))
  expect_false(any(grepl("service_workload.yml", r_src, fixed = TRUE)),
               info = paste("a loader appeared;", .MIRROR_MSG))
})

test_that("indirect_time_share mirrors INDIRECT_TIME_SHARE", {
  skip_if_not(file.exists("../../config/service_workload.yml"))
  expect_equal(.wl()$indirect_time_share$value, INDIRECT_TIME_SHARE,
               tolerance = 1e-12, info = .MIRROR_MSG)
})

test_that("delegation level_correction mirrors URPS_DELEGATION_CAPACITY_FACTOR", {
  skip_if_not(file.exists("../../config/service_workload.yml"))
  expect_equal(.wl()$delegation_shares$level_correction,
               URPS_DELEGATION_CAPACITY_FACTOR,
               tolerance = 1e-12, info = .MIRROR_MSG)
})

test_that("productivity_benchmark mirrors WRVU_PER_FTE_BENCHMARK", {
  skip_if_not(file.exists("../../config/service_workload.yml"))
  y <- .wl()$productivity_benchmark
  expect_equal(y$low,    unname(WRVU_PER_FTE_BENCHMARK[["low"]]),    info = .MIRROR_MSG)
  expect_equal(y$median, unname(WRVU_PER_FTE_BENCHMARK[["median"]]), info = .MIRROR_MSG)
  expect_equal(y$high,   unname(WRVU_PER_FTE_BENCHMARK[["high"]]),   info = .MIRROR_MSG)
})

test_that("work_rvu status agrees with the executable basket metadata", {
  skip_if_not(file.exists("../../config/service_workload.yml"))
  expect_identical(as.character(.wl()$work_rvu$status),
                   as.character(urps_service_workload_status()),
                   info = .MIRROR_MSG)
})

test_that("a drifted mirror is detected, not tolerated", {
  # Proves the checks above can actually fail. Without this, a mirror test that
  # silently compared a value to itself would look green forever.
  skip_if_not(file.exists("../../config/service_workload.yml"))
  y <- .wl()
  drifted <- y$indirect_time_share$value + 0.01
  expect_false(isTRUE(all.equal(drifted, INDIRECT_TIME_SHARE)))
})
