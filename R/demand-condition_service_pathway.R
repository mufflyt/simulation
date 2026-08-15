# Condition-specific service pathways ---------------------------------------
#
# lifecourse_service_map() (R/demand-lifecourse) already separated UI, POP and
# AI -- the model has never used one pooled "PFD demand" rate. What it did not
# have was PATHWAY STRUCTURE. The map was a flat annual rate per treated patient,
# so a UI patient contributed 0.8 PTNS and 0.12 slings in the same year as
# PARALLEL draws, with no dependency between them. Nothing represented staged
# care, recurrence, or post-operative follow-up.
#
# This module replaces the flat map with an explicit ordered pathway:
#
#   conservative -> testing -> procedure -> followup -> recurrence
#
# Each stage carries one `p_advance`, the probability of reaching the NEXT
# stage, so the number entering stage k+1 is the number entering stage k times
# that stage's p_advance. Service volume within a stage is
# `entering[stage] * per_entering`.
#
# IMPORTANT: an ordered stage need not be a clinical attrition gate. A stage
# whose services are selective utilization within the same clinical episode can
# use `p_advance = 1` as a structural pass-through. POP testing is the canonical
# example: urodynamics and cystoscopy are emitted as utilization, but receiving
# neither cannot prevent progression to the POP procedure stage. UI retains a
# genuine testing transition pending its own estimand audit.
#
# WHAT THIS CHANGES AND WHAT IT DOES NOT
#
# It changes the FTE conversion, because the workload mix shifts: procedures move
# down the pathway while follow-up appears for the first time. It does NOT make
# any of it publishable. Every empirical `per_entering` and non-structural
# `p_advance` is expert judgement carrying confidence = "low", exactly as
# unsourced as the flat literals it replaces -- the difference is that the
# structure is explicit and provenance is declared per row. Structural
# pass-through values (`p_advance = 1`) are identities, not empirical estimates.
# The basket-level status stays "uncalibrated_illustrative", so the publication
# boundary continues to refuse it.
#
# TWO HONEST GAPS IN THE AI PATHWAY
#
# Anorectal manometry and endoanal ultrasound are the real AI diagnostics, and
# sacral neuromodulation and sphincteroplasty are the real AI procedures. None
# are in URPS_CPT_BASKET. The table uses stand-ins so the stages are not silently
# empty, and both rows say so in their `notes`. AI procedural workload is
# understated until those CPT codes are added.

#' The pathway stages, in cascade order
#'
#' Order is semantic, not alphabetical: each stage feeds the next, so the
#' sequence determines how `p_advance` compounds.
#'
#' @format Character vector.
#' @keywords internal
PATHWAY_STAGES <- c("conservative", "testing", "procedure", "followup", "recurrence")

#' Condition-specific service pathway table
#'
#' One row per condition-stage-service, giving the expected service units per
#' patient ENTERING that stage (`per_entering`) and the probability of advancing
#' to the next stage (`p_advance`, constant within a condition-stage).
#'
#' @param path Optional CSV path; defaults to the shipped table.
#' @return A tibble with `condition`, `stage`, `service`, `per_entering`,
#'   `p_advance`, `ci_low`, `ci_high`, `confidence`, `source`, `notes`.
#' @seealso [pathway_service_volumes()], [validate_condition_pathway()]
#' @family condition service pathway
#' @concept demand
#' @export
condition_service_pathway <- function(path = NULL) {
  if (is.null(path)) {
    path <- system.file("extdata", "pathway", "condition_service_pathway.csv",
                        package = "urpssim")
    if (!nzchar(path)) {
      path <- file.path("inst", "extdata", "pathway", "condition_service_pathway.csv")
    }
  }
  if (!file.exists(path)) {
    stop("Condition service pathway table not found: ", path, call. = FALSE)
  }
  out <- utils::read.csv(path, stringsAsFactors = FALSE)
  tibble::as_tibble(out)
}

#' Check a pathway table's internal consistency
#'
#' Four ways this table can be wrong without any downstream error, each checked
#' here because each produces plausible-looking FTE:
#'
#' * an unknown stage name silently drops out of the cascade;
#' * `p_advance` differing between rows of the same condition-stage makes the
#'   number entering the next stage ambiguous;
#' * a stage appearing after a missing upstream stage inherits no entrants;
#' * a service name absent from the workload basket is dropped by the
#'   `min_match_rate = 1.0` join in [convert_workload_to_fte()], which errors
#'   late and far from the cause.
#'
#' @param pathway A table from [condition_service_pathway()].
#' @param known_services Service names the workload basket recognises; `NULL`
#'   skips that check (used when the basket is not loaded).
#' @return (Invisibly) TRUE; errors on any inconsistency.
#' @family condition service pathway
#' @concept demand
#' @export
validate_condition_pathway <- function(pathway = condition_service_pathway(),
                                       known_services = NULL) {
  need <- c("condition", "stage", "service", "per_entering", "p_advance")
  if (!all(need %in% names(pathway))) {
    stop("Pathway table needs columns: ", paste(need, collapse = ", "), call. = FALSE)
  }
  bad_stage <- setdiff(unique(pathway$stage), PATHWAY_STAGES)
  if (length(bad_stage)) {
    stop("Unknown pathway stage(s): ", paste(bad_stage, collapse = ", "),
         ". Expected: ", paste(PATHWAY_STAGES, collapse = " -> "), call. = FALSE)
  }
  if (any(pathway$per_entering < 0, na.rm = TRUE)) {
    stop("per_entering must be non-negative.", call. = FALSE)
  }
  adv <- stats::na.omit(pathway$p_advance)
  if (length(adv) && (any(adv < 0) || any(adv > 1))) {
    stop("p_advance must be a probability in [0, 1].", call. = FALSE)
  }

  # p_advance is a property of the STAGE, repeated on each of its service rows
  # for editability. Divergent values would make the cascade ambiguous.
  by_stage <- split(pathway, list(pathway$condition, pathway$stage), drop = TRUE)
  for (nm in names(by_stage)) {
    vals <- unique(by_stage[[nm]]$p_advance)
    if (length(vals) > 1L) {
      stop(sprintf("p_advance is not constant within %s: %s", nm,
                   paste(vals, collapse = ", ")), call. = FALSE)
    }
  }

  # A stage with no upstream stage receives no entrants and contributes nothing,
  # which reads as "this condition has no surgery" rather than "the table is
  # missing a row".
  for (cond in unique(pathway$condition)) {
    have <- unique(pathway$stage[pathway$condition == cond])
    idx <- match(have, PATHWAY_STAGES)
    if (length(idx) && max(idx) > 1L) {
      expected <- PATHWAY_STAGES[seq_len(max(idx))]
      missing_upstream <- setdiff(expected, have)
      if (length(missing_upstream)) {
        stop(sprintf(paste("Condition '%s' has stage(s) %s but is missing upstream",
                           "stage(s) %s, so nothing would enter them."),
                     cond, paste(setdiff(have, expected[1]), collapse = ", "),
                     paste(missing_upstream, collapse = ", ")), call. = FALSE)
      }
    }
  }

  if (!is.null(known_services)) {
    unknown <- setdiff(unique(pathway$service), known_services)
    if (length(unknown)) {
      stop("Pathway services absent from the workload basket: ",
           paste(unknown, collapse = ", "),
           ". convert_workload_to_fte() requires every service to match.",
           call. = FALSE)
    }
  }
  invisible(TRUE)
}

#' Number of patients entering each pathway stage
#'
#' Compounds `p_advance` along [PATHWAY_STAGES]: everyone treated enters
#' `conservative`, and each subsequent stage receives the previous stage's
#' entrants multiplied by that stage's advance probability.
#'
#' @param treated Named numeric vector of treated patients per condition.
#' @param pathway A table from [condition_service_pathway()].
#' @return A tibble of `condition`, `stage`, `entering`.
#' @family condition service pathway
#' @concept demand
#' @export
pathway_stage_entrants <- function(treated, pathway = condition_service_pathway()) {
  validate_condition_pathway(pathway)
  if (!is.numeric(treated) || is.null(names(treated)) || any(!nzchar(names(treated))))
    stop("pathway_stage_entrants(): `treated` must be a NAMED numeric vector ",
         "(condition -> count); an unnamed vector iterates zero times and returns ",
         "empty, i.e. zero downstream demand with no error.", call. = FALSE)
  rows <- list()
  for (cond in names(treated)) {
    p_cond <- pathway[pathway$condition == cond, , drop = FALSE]
    if (!nrow(p_cond)) next
    stages <- PATHWAY_STAGES[PATHWAY_STAGES %in% p_cond$stage]
    n_entering <- as.numeric(treated[[cond]])
    for (st in stages) {
      rows[[length(rows) + 1L]] <- tibble::tibble(
        condition = cond, stage = st, entering = n_entering)
      # Advance to the next stage. NA marks a terminal stage; treat it as 0 so a
      # missing value cannot silently carry the full cohort forward.
      adv <- unique(p_cond$p_advance[p_cond$stage == st])[1]
      n_entering <- n_entering * (if (is.na(adv)) 0 else adv)
    }
  }
  if (!length(rows)) {
    return(tibble::tibble(condition = character(), stage = character(),
                          entering = numeric()))
  }
  dplyr::bind_rows(rows)
}

#' Service volumes from the condition-specific pathway
#'
#' Drop-in replacement for the flat `condition x service x per_treated` join:
#' takes the same treated-patient counts and returns the same
#' `year`/`service`/`volume` shape [convert_workload_to_fte()] expects.
#'
#' @param treated Named numeric vector of treated patients per condition.
#' @param year Calendar year stamped on the result.
#' @param pathway A table from [condition_service_pathway()].
#' @param by_stage Return `condition` and `stage` alongside `service` instead of
#'   collapsing to service totals. Useful for auditing where workload arises.
#' @return A tibble of `year`, `service`, `volume` (plus `condition` and `stage`
#'   when `by_stage = TRUE`).
#' @family condition service pathway
#' @concept demand
#' @export
pathway_service_volumes <- function(treated, year,
                                    pathway = condition_service_pathway(),
                                    by_stage = FALSE) {
  entrants <- pathway_stage_entrants(treated, pathway)
  if (!nrow(entrants)) {
    return(tibble::tibble(year = numeric(), service = character(), volume = numeric()))
  }
  vols <- dplyr::inner_join(pathway, entrants, by = c("condition", "stage"))
  vols <- dplyr::mutate(vols, volume = .data$entering * .data$per_entering,
                        year = year)
  if (isTRUE(by_stage)) {
    return(dplyr::summarise(
      dplyr::group_by(vols, .data$year, .data$condition, .data$stage, .data$service),
      volume = sum(.data$volume), .groups = "drop"))
  }
  dplyr::summarise(dplyr::group_by(vols, .data$year, .data$service),
                   volume = sum(.data$volume), .groups = "drop")
}

#' Calibration status of the condition pathway
#'
#' Every row is expert judgement, so the table as a whole is
#' `"uncalibrated_illustrative"`. Reported through the same vocabulary as the
#' rest of the model so [calibration_status_report()] and
#' [assert_publishable_workload()] see it.
#'
#' @param pathway A table from [condition_service_pathway()].
#' @return A length-one character calibration tier.
#' @family condition service pathway
#' @concept demand
#' @export
condition_pathway_status <- function(pathway = condition_service_pathway()) {
  conf <- unique(pathway$confidence)
  if (all(conf %in% c("high")) ) "calibrated"
  else if (all(conf %in% c("high", "medium"))) "derived_by_analogy"
  else "uncalibrated_illustrative"
}
