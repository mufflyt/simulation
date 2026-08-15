################################################################################
# R/calibration-clinical_review_gate.R
# Make it structurally impossible for a clinically unreviewed procedure family
# to produce a production calibration scalar.
#
# `production_scalar_eligible: false` in a config file is metadata. Metadata is
# a convention, and conventions drift -- somebody flips a flag to unblock a
# render. This makes the requirement a precondition the calibration path cannot
# route around: eligibility requires a NAMED reviewer and a DATE, not a boolean.
#
# The clinical rules the reviewer is signing off on live in
# config/chia_urps_inpatient_codes.yml under `clinical_review_criteria`. They
# are definitions, not string translations -- each side of an ICD era seam must
# independently mean the same clinical procedure family, and matching rates
# across the seam is validation of that definition, never the definition itself.
################################################################################

#' Assert an anchor's own clinical dependencies are reviewed
#'
#' Enforces ONLY the dependencies that anchor names. The governing rule:
#' an anchor is blocked solely by unresolved assumptions that can change THAT
#' anchor's estimand. A global review switch is both stricter than necessary
#' and destructive -- it blocked the NAMCS office-visit anchor on
#' urogynaecologic procedure-family definitions NAMCS does not use.
#'
#' @param anchor_specification Named list for one anchor, carrying a
#'   `clinical_review` block with `status`, `scope`, and optionally `blockers`.
#' @return Invisibly, TRUE. Stops otherwise, naming the blockers.
#' @export
assert_anchor_reviewed <- function(anchor_specification) {
  review <- anchor_specification$clinical_review

  if (base::is.null(review)) {
    base::stop("Anchor has no clinical-review specification.", call. = FALSE)
  }

  if (!base::identical(review$status, "approved")) {
    blockers <- review$blockers %||% base::character()
    blocker_text <- if (base::length(blockers) > 0L) {
      base::paste0(" Blocker(s): ", base::paste(blockers, collapse = ", "), ".")
    } else ""
    scope_text <- if (base::length(review$scope %||% base::character()) > 0L) {
      base::paste0(" Review scope: ",
                   base::paste(review$scope, collapse = ", "), ".")
    } else ""
    base::stop("Clinical review is not approved for this anchor.",
               blocker_text, scope_text, call. = FALSE)
  }

  if (!base::nzchar(review$reviewer %||% "")) {
    base::stop("Approved review has no named reviewer.", call. = FALSE)
  }
  if (!base::nzchar(review$date %||% "")) {
    base::stop("Approved review has no date.", call. = FALSE)
  }

  base::invisible(TRUE)
}

#' Assert an anchor may produce a production calibration scalar
#'
#' Fails unless the anchor is flagged eligible AND carries an approved clinical
#' review with an attributable reviewer and date.
#'
#' @param anchor_specification Named list for one anchor, from the calibration
#'   YAML. Must carry `anchor_id`, `production_scalar_eligible`,
#'   `clinical_review_status`, `clinical_reviewer`, `clinical_review_date`.
#' @return Invisibly, TRUE. Stops otherwise.
#' @examples
#' \dontrun{
#' cfg <- yaml::read_yaml("config/calibration_targets.yml")
#' assert_production_scalar_eligible(cfg$anchors$urps_office_visits)
#' }
#' @export
assert_production_scalar_eligible <- function(anchor_specification) {
  base::message("Checking production-scalar eligibility for: ",
                anchor_specification$anchor_id)

  required_names <- c("production_scalar_eligible", "clinical_review_status",
                      "clinical_reviewer", "clinical_review_date")
  missing_names <- base::setdiff(required_names,
                                 base::names(anchor_specification))
  if (base::length(missing_names) > 0L) {
    base::stop("Anchor specification is missing: ",
               base::paste(missing_names, collapse = ", "), call. = FALSE)
  }

  if (!base::isTRUE(anchor_specification$production_scalar_eligible)) {
    base::stop("Anchor is not eligible for a production calibration scalar: ",
               anchor_specification$anchor_id, call. = FALSE)
  }
  if (!base::identical(anchor_specification$clinical_review_status,
                       "approved")) {
    base::stop("Clinical review is not approved for: ",
               anchor_specification$anchor_id, call. = FALSE)
  }
  if (!base::nzchar(anchor_specification$clinical_reviewer)) {
    base::stop("Clinical reviewer is missing for: ",
               anchor_specification$anchor_id, call. = FALSE)
  }
  if (!base::nzchar(anchor_specification$clinical_review_date)) {
    base::stop("Clinical review date is missing for: ",
               anchor_specification$anchor_id, call. = FALSE)
  }

  base::message("Production-scalar eligibility passed for: ",
                anchor_specification$anchor_id)
  base::invisible(TRUE)
}

#' Report clinical-review status across every anchor and procedure family
#'
#' A standing view of what is blocked and why, so "awaiting clinical review" is
#' a visible state rather than a note somebody remembers.
#'
#' @param calibration_config Calibration YAML.
#' @param family_config Procedure-family YAML.
#' @return Tibble: item, kind, eligible, review status, reviewer, blocked.
#' @export
clinical_review_status <- function(
    calibration_config = "config/calibration_targets.yml",
    family_config = "config/chia_urps_inpatient_codes.yml") {

  rows <- base::list()

  if (base::file.exists(calibration_config)) {
    cfg <- yaml::read_yaml(calibration_config)
    for (nm in base::names(cfg$anchors)) {
      a <- cfg$anchors[[nm]]
      ok <- !base::inherits(base::try(assert_anchor_reviewed(a),
                                      silent = TRUE), "try-error")
      review <- a$clinical_review %||% base::list()
      rows[[base::length(rows) + 1L]] <- tibble::tibble(
        item = nm, kind = "production_anchor",
        eligible = base::isTRUE(a$production_scalar_eligible),
        review_status = base::as.character(review$status %||% "not_recorded"),
        reviewer = base::as.character(review$reviewer %||% ""),
        blockers = base::paste(review$blockers %||% base::character(),
                               collapse = ", "),
        blocked = !ok)
    }
  }

  if (base::file.exists(family_config)) {
    fam <- yaml::read_yaml(family_config)
    status <- base::as.character(fam$meta$status %||% "unknown")
    for (nm in base::names(fam$families)) {
      rows[[base::length(rows) + 1L]] <- tibble::tibble(
        item = nm, kind = "procedure_family",
        eligible = FALSE,
        review_status = status,
        reviewer = base::as.character(fam$meta$clinical_reviewer %||% ""),
        blockers = "",
        blocked = !base::identical(status, "approved"))
    }
  }

  out <- dplyr::bind_rows(rows)
  n_blocked <- base::sum(out$blocked)
  base::message(base::sprintf(
    "%d of %d items blocked pending clinical review.", n_blocked,
    base::nrow(out)))
  out
}

`%||%` <- function(x, y) if (base::is.null(x)) y else x
