# Unified 9-State Scientific Scorecard Dashboard ----
#
# Scientific Hardening Section 42: Unified Scientific Scorecard
#
# Reports 9 distinct independent scientific states across the repository
# to prevent collapsing scientific readiness or semantic defects into R CMD check.

#' Generate Unified 9-State Scientific Scorecard
#'
#' Every state below is derived from a real check -- none are hardcoded. Four
#' run in-process and are always evaluated: SEMANTICS ([audit_semantics()]),
#' SOURCE_MUTATION ([audit_source_mutation()]), KNOWN_TRUTH_RECOVERY
#' ([audit_known_truth_recovery()]), and CROSS_REPO_CONTRACTS
#' ([audit_cross_repo_contracts()]). CANONICAL_READINESS
#' ([audit_canonical_readiness()]) and UNCERTAINTY are always evaluated too.
#' Two are CI-scale (multi-minute) and only run when `deep = TRUE`: SOFTWARE
#' ([audit_software()], the full test suite) and ADVERSARIAL
#' ([audit_adversarial()], the canary and metamorphic battery) -- otherwise
#' they report `NOT_ELIGIBLE`, honestly, rather than a guess. REPRODUCIBILITY
#' has no in-process or subprocess equivalent at all (it is a from-scratch
#' renv restore plus system-library install, CI-only by construction) and is
#' always `NOT_ELIGIBLE`.
#'
#' @param deep If `TRUE`, also run the expensive SOFTWARE and ADVERSARIAL
#'   audits (several minutes total). Default `FALSE` keeps the scorecard
#'   fast; those two states report `NOT_ELIGIBLE` until run with `deep = TRUE`.
#' @return Named list of 9 scientific states (GREEN, RED, NOT_ELIGIBLE).
#' @family scorecard
#' @concept testing
#' @export
generate_scientific_scorecard <- function(deep = FALSE) {
  state_of <- function(audit) {
    if (!isTRUE(audit$available)) "NOT_ELIGIBLE" else if (isTRUE(audit$passed)) "GREEN" else "RED"
  }

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

  software_state    <- if (deep) state_of(audit_software())    else "NOT_ELIGIBLE"
  adversarial_state <- if (deep) state_of(audit_adversarial()) else "NOT_ELIGIBLE"

  scorecard <- list(
    SOFTWARE             = software_state,
    # No in-process/subprocess equivalent exists: CI runs this as a
    # from-scratch renv::restore() plus apt-get system-library install in a
    # throwaway library, ~90 minutes. Reporting GREEN here would be exactly
    # the bug this scorecard was fixed to stop making.
    REPRODUCIBILITY      = "NOT_ELIGIBLE",
    SEMANTICS            = state_of(audit_semantics()),
    ADVERSARIAL          = adversarial_state,
    SOURCE_MUTATION      = state_of(audit_source_mutation()),
    KNOWN_TRUTH_RECOVERY = state_of(audit_known_truth_recovery()),
    UNCERTAINTY          = if (is_uncertainty_green) "GREEN" else "RED",
    CROSS_REPO_CONTRACTS = state_of(audit_cross_repo_contracts()),
    CANONICAL_READINESS  = canonical_state
  )

  attr(scorecard, "generated_at") <- base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S")
  not_eligible <- names(scorecard)[vapply(scorecard, identical, logical(1), "NOT_ELIGIBLE")]
  red <- names(scorecard)[vapply(scorecard, identical, logical(1), "RED")]
  attr(scorecard, "explanation") <- if (length(red) == 0L && length(not_eligible) == 0L) {
    "All 9 scientific states are GREEN."
  } else {
    paste0(
      if (length(red)) sprintf("RED: %s. ", paste(red, collapse = ", ")) else "",
      if (length(not_eligible)) sprintf("NOT_ELIGIBLE: %s%s.", paste(not_eligible, collapse = ", "),
                                        if (!deep && any(c("SOFTWARE", "ADVERSARIAL") %in% not_eligible))
                                          " (SOFTWARE/ADVERSARIAL need deep = TRUE; REPRODUCIBILITY has no inline equivalent)"
                                        else "")
      else "")
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
