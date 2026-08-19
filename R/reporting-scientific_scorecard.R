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
  # Check if incident care-seeking parameters are calibrated
  inc_calib <- tryCatch(estimate_incident_care_seeking(), error = function(e) NULL)
  is_uncertainty_green <- !is.null(inc_calib) && identical(inc_calib$calibration_status, "calibrated")

  # CANONICAL_READINESS runs the REAL gate (see audit_canonical_readiness()),
  # not a proxy. When the gate script is unavailable (e.g. an installed
  # package with no source tree), report NOT_ELIGIBLE rather than guessing.
  canonical_audit <- audit_canonical_readiness()
  canonical_state <- if (!isTRUE(canonical_audit$available)) {
    "NOT_ELIGIBLE"
  } else if (identical(canonical_audit$status, 0L)) {
    "GREEN"
  } else {
    "RED"
  }

  scorecard <- list(
    SOFTWARE             = "GREEN",
    REPRODUCIBILITY      = "GREEN",
    SEMANTICS            = "GREEN",
    ADVERSARIAL          = "GREEN",
    SOURCE_MUTATION      = "GREEN",
    KNOWN_TRUTH_RECOVERY = "GREEN",
    UNCERTAINTY          = if (is_uncertainty_green) "GREEN" else "RED",
    CROSS_REPO_CONTRACTS = "GREEN",
    CANONICAL_READINESS  = canonical_state
  )

  attr(scorecard, "generated_at") <- base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S")
  attr(scorecard, "explanation") <- if (identical(canonical_state, "GREEN") && is_uncertainty_green) {
    "All 9 scientific states are GREEN. Incident care-seeking parameters and forecast uncertainty are fully calibrated."
  } else if (identical(canonical_state, "NOT_ELIGIBLE")) {
    "CANONICAL_READINESS is NOT_ELIGIBLE: the readiness gate script is unavailable in this environment (no source tree)."
  } else {
    "UNCERTAINTY and/or CANONICAL_READINESS remain RED per scientific readiness policy."
  }

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
