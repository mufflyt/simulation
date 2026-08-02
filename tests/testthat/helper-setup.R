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
