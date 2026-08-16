#!/usr/bin/env Rscript
# Partition a failing test run into SCIENTIFIC BLOCKERS and INFRASTRUCTURE
# BREAKAGE.
#
# THE PROBLEM THIS SOLVES. Wiring four dormant guards exposed a real defect:
# per_entering = 1.00 turns a prevalence stock into an annual flow, so every
# prevalent treated patient is counted as newly presenting each year. Thirty-one
# tests now refuse, correctly, and they must stay red until the parameter has a
# defensible estimand and source.
#
# A permanently red suite is worse than useless if it cannot say WHY it is red.
# Within a week everyone reads "31 failures" as background noise and the next
# genuine break lands invisibly. The temptation is then to skip the tests, which
# destroys the finding the guards were wired to surface.
#
# So: classify, do not suppress. The tests run. They fail. This script reads the
# failures and answers the only question that matters operationally --
#
#     is every failure one we already know about and have documented?
#
# NOT A SKIP AND NOT A BASELINE THAT ABSORBS ANYTHING NEW. An undeclared failure
# is an infrastructure break and exits non-zero. A declared blocker that starts
# PASSING is also reported, because that means the science moved and the
# registry is now lying about the size of the debt.
#
# Usage:  Rscript .github/scripts/classify-failures.R
# Exit:   0  all failures declared (scientific blockers only), or suite green
#         1  at least one undeclared failure  -> infrastructure breakage

suppressPackageStartupMessages({
  library(testthat)
})

root <- if (file.exists("DESCRIPTION") && !dir.exists("Meta")) "." else {
  stop("classify-failures.R must run from the repository root (source tree).")
}

reg_path <- file.path(root, "tests", "scientific-blockers.csv")
if (!file.exists(reg_path)) {
  stop("tests/scientific-blockers.csv is missing. A classifier with no registry ",
       "would report every real break as 'known', which is the failure mode it ",
       "exists to prevent.", call. = FALSE)
}
reg <- utils::read.csv(reg_path, stringsAsFactors = FALSE, comment.char = "#")
stopifnot(all(c("file", "test", "blocker") %in% names(reg)))

suppressMessages(pkgload::load_all(root, quiet = TRUE, export_all = TRUE))
res <- testthat::test_dir(file.path(root, "tests", "testthat"),
                          reporter = "silent", stop_on_failure = FALSE)
d <- as.data.frame(res)

failing <- d[d$failed > 0 | d$error, c("file", "test"), drop = FALSE]
key <- function(x) paste(x$file, x$test, sep = "")

declared <- key(reg)
observed <- key(failing)

undeclared <- failing[!(observed %in% declared), , drop = FALSE]
# A declared blocker that no longer fails: the science moved, or the test was
# changed. Either way the registry overstates the debt and must shrink.
resolved <- reg[!(declared %in% observed), , drop = FALSE]

cat("\n=== test run classification ===\n")
cat(sprintf("  total tests      %d\n", nrow(d)))
cat(sprintf("  passing          %d\n", sum(d$passed)))
cat(sprintf("  failing          %d\n", nrow(failing)))
cat(sprintf("  declared blocked %d\n", nrow(failing) - nrow(undeclared)))
cat(sprintf("  UNDECLARED       %d\n", nrow(undeclared)))

if (nrow(failing) > nrow(undeclared)) {
  blocked <- merge(failing, reg, by = c("file", "test"))
  cat("\n--- BLOCKED (scientific, expected) ---\n")
  for (b in sort(unique(blocked$blocker))) {
    cat(sprintf("  %-28s %d test(s)\n", b, sum(blocked$blocker == b)))
  }
}

if (nrow(resolved) > 0) {
  cat("\n--- DECLARED BUT NOW PASSING (registry overstates the debt) ---\n")
  for (i in seq_len(nrow(resolved))) {
    cat(sprintf("  %s :: %s\n", resolved$file[i], resolved$test[i]))
  }
  cat("  Remove these rows from tests/scientific-blockers.csv.\n")
}

if (nrow(undeclared) > 0) {
  cat("\n--- UNDECLARED FAILURES: INFRASTRUCTURE BREAKAGE ---\n")
  for (i in seq_len(nrow(undeclared))) {
    cat(sprintf("  %s :: %s\n", undeclared$file[i], undeclared$test[i]))
  }
  cat("\nThese are not known scientific blockers. Fix them, or -- if one is a\n")
  cat("genuine new scientific refusal -- declare it with its estimand and the\n")
  cat("data that would unblock it, the way docs/INCIDENT_ENTRY_ESTIMAND.md does.\n")
  quit(status = 1L)
}

if (nrow(failing) == 0L) {
  cat("\nSuite green. If tests/scientific-blockers.csv is non-empty, it is stale.\n")
} else {
  cat("\nAll failures are declared scientific blockers. Reporting BLOCKED, not broken.\n")
  cat("See docs/INCIDENT_ENTRY_ESTIMAND.md for what unblocks the POP pathway.\n")
}
quit(status = 0L)
