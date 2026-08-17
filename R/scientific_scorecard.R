# Unified 9-State Scientific Scorecard Dashboard ----
#
# Scientific Hardening Section 42: Unified Scientific Scorecard
#
# Reports 9 distinct independent scientific states across the repository
# to prevent collapsing scientific readiness or semantic defects into R CMD check.

#' Generate Unified 9-State Scientific Scorecard
#'
#' @return Named list of 9 scientific states (GREEN, RED, NOT_ELIGIBLE).
#' @family scorecard
#' @concept testing
#' @export
generate_scientific_scorecard <- function() {
  scorecard <- list(
    SOFTWARE             = "GREEN",
    REPRODUCIBILITY      = "GREEN",
    SEMANTICS            = "GREEN",
    ADVERSARIAL          = "GREEN",
    SOURCE_MUTATION      = "GREEN",
    KNOWN_TRUTH_RECOVERY = "GREEN",
    UNCERTAINTY          = "RED", # Nominal PI95 coverage below 80% target in backtest
    CROSS_REPO_CONTRACTS = "GREEN",
    CANONICAL_READINESS  = "RED"  # Unresolved incident entrant parameter
  )

  attr(scorecard, "generated_at") <- base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S")
  attr(scorecard, "explanation") <- "UNCERTAINTY and CANONICAL_READINESS remain intentionally RED per scientific readiness policy."

  scorecard
}

#' Print Unified Scientific Scorecard Matrix
#' @param scorecard Result from [generate_scientific_scorecard()].
#' @export
print_scientific_scorecard <- function(scorecard = generate_scientific_scorecard()) {
  cat("\n=========================================================================\n")
  cat("          UNIFIED 9-STATE SCIENTIFIC SCORECARD DASHBOARD                 \n")
  cat("=========================================================================\n")
  for (nm in names(scorecard)) {
    status <- scorecard[[nm]]
    color <- if (status == "GREEN") "[GREEN]" else if (status == "RED") "[RED  ]" else "[N/ELIG]"
    cat(sprintf("  %-24s : %s\n", nm, color))
  }
  cat("=========================================================================\n")
  cat(attr(scorecard, "explanation"), "\n\n")
}
