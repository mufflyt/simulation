#!/usr/bin/env Rscript
# Run one scientific-contract suite and refuse to report success vacuously.
#
# WHY THIS EXISTS RATHER THAN `testthat::test_file()` IN THE WORKFLOW.
#
# A job that exits 0 is not evidence that a law was checked. Three ways a green
# job can mean nothing, all of which have happened in this repository and are
# recorded in docs/HALL_OF_SHAME.md:
#
#   * the file discovered ZERO tests -- a renamed file, a bad glob, a helper
#     that errored during collection (entry 22: a CI sweep labelled all 164
#     test files "ok" unconditionally);
#   * every test SKIPPED -- the gate is dark, and "0 failed" is technically
#     true (entry 26: "full suite green", measured in the one context where it
#     could not fail);
#   * the runner stopped early and reported on a prefix of the suite.
#
# So this asserts positively: tests were discovered, tests actually RAN, and
# none failed. Anything less is a failure, not a pass.
#
# Usage: run-scientific-contract.R <label> <test-file> [<test-file> ...]

arguments <- commandArgs(trailingOnly = TRUE)
if (length(arguments) < 2L) {
  stop("Usage: run-scientific-contract.R <label> <test-file> [...]", call. = FALSE)
}

contract_label <- arguments[[1]]
test_files <- arguments[-1]

cat("== scientific contract:", contract_label, "==\n")

suppressMessages(pkgload::load_all(".", quiet = TRUE))

problems <- character(0)
total_passed <- 0L
total_skipped <- 0L
total_blocks <- 0L

for (test_file in test_files) {
  path <- file.path("tests", "testthat", test_file)
  if (!file.exists(path)) {
    # A contract file that has been renamed or deleted must FAIL the gate. If a
    # missing file were skipped, deleting the test would be the easiest way to
    # make the law stop being enforced.
    problems <- c(problems, sprintf("%s: file not found", test_file))
    next
  }

  result <- testthat::test_file(
    path, reporter = "silent", stop_on_failure = FALSE
  )
  frame <- as.data.frame(result)

  blocks <- nrow(frame)
  passed <- sum(frame$passed)
  failed <- sum(frame$failed)
  errored <- sum(frame$error)
  skipped <- sum(frame$skipped)

  total_blocks <- total_blocks + blocks
  total_passed <- total_passed + passed
  total_skipped <- total_skipped + skipped

  cat(sprintf(
    "  %-46s blocks=%3d passed=%4d failed=%d errors=%d skipped=%d\n",
    test_file, blocks, passed, failed, errored, skipped
  ))

  if (blocks == 0L) {
    problems <- c(problems, sprintf(
      "%s: discovered ZERO test blocks -- the law is not being checked", test_file
    ))
  }
  if (passed == 0L && blocks > 0L) {
    problems <- c(problems, sprintf(
      "%s: not one assertion PASSED; every block skipped or failed, so this gate is dark",
      test_file
    ))
  }
  if (failed > 0L || errored > 0L) {
    problems <- c(problems, sprintf(
      "%s: %d failure(s), %d error(s)", test_file, failed, errored
    ))
    bad <- frame[frame$failed > 0 | frame$error > 0, "test", drop = TRUE]
    for (test_name in bad) cat("    FAILED:", test_name, "\n")
  }
}

cat(sprintf(
  "\n  totals: blocks=%d passed=%d skipped=%d\n",
  total_blocks, total_passed, total_skipped
))

if (length(problems) > 0L) {
  cat("\n")
  for (problem in problems) {
    cat(sprintf("::error::[%s] %s\n", contract_label, problem))
  }
  quit(status = 1L)
}

cat(sprintf("  CONTRACT HELD: %s\n", contract_label))
