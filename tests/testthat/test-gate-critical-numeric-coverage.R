# Gate: the critical numeric API keeps dedicated BVA + adversarial coverage.
#
# The scoring and probability functions are the ones a reader ACTS on -- a wrong
# weighted interval score or a mis-signed exceedance probability changes a
# decision, and (unlike a pipeline stage) they are called directly with
# user-supplied numbers, so a boundary or a malformed input is a real input, not
# a hypothetical. This gate names those functions and requires each to be
# exercised by the numeric-core boundary/adversarial suite
# (test-bva-numeric-core.R, test-adversarial-numeric-core.R).
#
# It is a ratchet in the export-registry spirit: add a function to the list and
# it must acquire tests; ship a new critical scorer and add it to the list. The
# gate scans the test text for a `name(` call, so a function merely mentioned in
# a comment does not count as covered.

CRITICAL_NUMERIC_API <- c(
  "weighted_interval_score",
  "forecast_scorecard",
  "forecast_probabilities",
  "workforce_gap_probabilities",
  "series_mean_se",
  "calculate_proportion_ci",
  "career_state_of",
  "haversine_km"
)

gcnc_root <- function() {
  # Sources, not just "a package" -- see .source_tree_root() in helper-setup.R.
  # This gate reads tests/testthat/*.R as text; an installed tree keeps its
  # tests elsewhere, so a DESCRIPTION-only match sent it to an empty corpus.
  r <- .source_tree_root()
  if (length(r) == 0) NULL else r
}

gcnc_corpus <- function(root) {
  d <- file.path(root, "tests", "testthat")
  files <- c(file.path(d, "test-bva-numeric-core.R"),
             file.path(d, "test-adversarial-numeric-core.R"))
  files <- files[file.exists(files)]
  unlist(lapply(files, function(f) sub("#.*$", "", readLines(f, warn = FALSE))))
}

test_that("every critical numeric function is exercised by the BVA/adversarial suite", {
  root <- gcnc_root()
  skip_if(is.null(root), "repository root not reachable")
  hay <- gcnc_corpus(root)
  skip_if(length(hay) == 0, "numeric-core test files absent")
  called <- function(fn) any(grepl(paste0("(^|[^A-Za-z0-9._])",
                                          gsub("([.])", "[.]", fn), "[[:space:]]*[(]"), hay))
  uncovered <- CRITICAL_NUMERIC_API[!vapply(CRITICAL_NUMERIC_API, called, logical(1))]
  expect_equal(uncovered, character(0),
               info = paste("Critical numeric functions with no BVA/adversarial test:",
                            paste(uncovered, collapse = ", ")))
})

test_that("every critical numeric function is an actual package export", {
  # A stale name in the list (renamed or removed function) would make the
  # coverage check pass vacuously; pin the list to the real export surface.
  root <- gcnc_root()
  skip_if(is.null(root), "repository root not reachable")
  ns <- readLines(file.path(root, "NAMESPACE"), warn = FALSE)
  exports <- gsub("[`\"]", "", sub("^export[(](.*)[)]$", "\\1",
                                   grep("^export[(]", ns, value = TRUE)))
  missing <- setdiff(CRITICAL_NUMERIC_API, exports)
  expect_equal(missing, character(0),
               info = paste("Listed but not exported:", paste(missing, collapse = ", ")))
})
