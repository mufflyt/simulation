# The package is now loaded by tests/testthat.R (or by pkgload during
# devtools::test()), so tests call exported functions directly. This helper only
# attaches the tidyverse verbs that test fixtures use directly, and provides a
# source-tree fallback for running test_dir() outside a package context.

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
})

if (!requireNamespace("urpssim", quietly = TRUE) ||
    !exists("run_workforce_microsimulation", mode = "function")) {
  if (requireNamespace("pkgload", quietly = TRUE)) {
    root <- if (file.exists("DESCRIPTION")) "." else file.path("..", "..")
    suppressMessages(pkgload::load_all(root, quiet = TRUE, export_all = TRUE))
  }
}

# ---------------------------------------------------------------------------
# LOCATING THE SOURCE TREE, WHICH IS NOT THE SAME AS LOCATING A PACKAGE.
#
# Seventeen call sites across fourteen test files walked upward looking for a
# DESCRIPTION and treated the first hit as the repository root. That is wrong
# in exactly the execution contexts CI uses. Under covr and R CMD check the
# suite runs from inside the INSTALLED package, which also carries a
# DESCRIPTION -- so the walk stopped there, the guard `skip_if(length(root) ==
# 0)` never fired, and the test proceeded to look for R/*.R sources, scripts/
# and docs/ in a tree that ships none of them.
#
# The failures that produced were not "source tree absent"; they were assertion
# errors that read like real defects: "status text names non-existent path(s):
# R/geography-demand.R". A guard that cannot fire is worse than no guard,
# because it converts a missing precondition into a false accusation against
# the code.
#
# Meta/ is the discriminator: R's installer creates it, and it never exists in
# a source checkout. DESCRIPTION says "a package lives here"; DESCRIPTION
# without Meta/ says "the SOURCES live here", which is the actual precondition
# every one of those tests needs.
#
# Returns character(0) rather than erroring, so the existing call-site idiom --
# skip_if(length(root) == 0, ...) then root[1] -- keeps working unchanged.
.source_tree_root <- function() {
  candidates <- c(".", "..", file.path("..", ".."),
                  file.path("..", "..", ".."), file.path("..", "..", "..", ".."))
  for (p in candidates) {
    if (file.exists(file.path(p, "DESCRIPTION")) && !dir.exists(file.path(p, "Meta"))) {
      return(p)
    }
  }
  character(0)
}
