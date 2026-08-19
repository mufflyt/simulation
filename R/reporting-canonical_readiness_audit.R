################################################################################
# R/reporting-canonical_readiness_audit.R
# Runs the SAME canonical-readiness gate the nightly workflow and
# test-readiness-protocol.R run, so the scorecard's CANONICAL_READINESS state
# reflects the real gate instead of an unrelated calibration flag.
#
# WHY A SUBPROCESS, NOT A SHARED FUNCTION. assert-canonical-science.R's
# stdout text and exit codes (0/1/2) are pinned byte-for-byte by
# tests/testthat/test-readiness-protocol.R. Re-implementing its logic inline
# here would create a second copy that can silently drift from the tested
# one. Shelling out to the actual script, exactly as those tests do via
# system2(), reuses the one tested contract instead of duplicating it.
################################################################################

#' Locate the repository source tree
#'
#' `.github/`, like `config/` and `scripts/`, is excluded from the built
#' package (see .Rbuildignore) and only exists in a source checkout. Returns
#' `NA_character_` when running from an installed package with no source tree
#' available, so callers can degrade gracefully instead of misreporting.
#'
#' Shared by every scorecard audit_*() function that needs to shell out to a
#' `.github/scripts/` or `scripts/` gate -- defined once here rather than per
#' file, so it cannot drift into two different resolutions of "the repo root".
#'
#' @return Absolute path to the repository root, or `NA_character_`.
#' @keywords internal
.repo_source_root <- function() {
  candidates <- c(".", "..", file.path("..", ".."),
                  file.path("..", "..", ".."), file.path("..", "..", "..", ".."))
  for (p in candidates) {
    if (file.exists(file.path(p, "DESCRIPTION")) &&
        dir.exists(file.path(p, ".github"))) {
      return(normalizePath(p, mustWork = TRUE))
    }
  }
  NA_character_
}

#' Audit canonical scientific readiness
#'
#' Runs `.github/scripts/assert-canonical-science.R` as a subprocess and
#' reports its exit status, exactly as the nightly CI job and
#' test-readiness-protocol.R do. This is the SAME gate, not a re-derived
#' approximation of it -- see the "WHY A SUBPROCESS" note in this file.
#'
#' @return A list with `available` (whether the gate script could be found
#'   and run), `status` (its exit code: 0 runnable, 1 known blocker, 2
#'   unexpected/infrastructure failure), and `text` (its combined stdout).
#' @family scorecard
#' @concept testing
#' @export
audit_canonical_readiness <- function() {
  root <- .repo_source_root()
  gate <- if (is.na(root)) NA_character_ else
    file.path(root, ".github", "scripts", "assert-canonical-science.R")

  if (is.na(gate) || !file.exists(gate)) {
    return(list(available = FALSE, status = NA_integer_, text = NA_character_))
  }

  out <- suppressWarnings(
    system2(file.path(R.home("bin"), "Rscript"), gate,
            stdout = TRUE, stderr = TRUE))
  status <- attr(out, "status")
  list(available = TRUE,
       status = if (is.null(status)) 0L else as.integer(status),
       text = paste(out, collapse = "\n"))
}
