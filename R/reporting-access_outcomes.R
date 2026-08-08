# Access outcomes reporting (A1-A5) ------------------------------------------
#
# Rolls the per-catchment clearing table (clear_access(), geography-access_
# clearing.R) up to the national A-series access outcomes, and guards that no
# outcome is published without a calibration label. Distinct from the D1-D5
# demand estimands and the four supply-capacity tiers: these describe what
# demand and supply produce WHEN CLEARED against each other.

.access_dwmean <- function(x, w) {
  ok <- is.finite(x) & is.finite(w)
  if (!any(ok) || sum(w[ok]) == 0) return(NA_real_)
  sum(x[ok] * w[ok]) / sum(w[ok])
}

#' National roll-up of access outcomes (A1-A5)
#'
#' Aggregates a per-catchment clearing table to national access outcomes:
#' demand-weighted wait and appointment probability, total-based utilization and
#' unmet demand, and a pooled panel size. Wait is averaged over catchments with a
#' finite (non-censored) wait only; the demand share sitting in unbounded queues
#' is reported separately as `wait_censored_share` so a saturated system is never
#' hidden behind a finite-looking mean.
#'
#' @param cleared A tibble from [clear_access()] (needs `demand_workload`,
#'   `accessible_capacity`, `served`, `unmet_demand`, `utilization`, `wait_time`,
#'   `wait_censored`, `p_appointment`, and, for panel size,
#'   `accessible_population`/`accessible_fte`).
#' @return A tidy tibble: `estimand` (A1-A5, with `A1b`/`A5b` auxiliary shares),
#'   `label`, `value`, `unit`, `calibration_status`. When `cleared` carries
#'   spatial-overflow columns (from [overflow_access()]), two more rows are
#'   appended: `A6` (`spilled_share`) and `A7` (`overflow_travel_time`).
#' @family access outcomes
#' @concept reporting
#' @examples
#' catchments <- tibble::tibble(
#'   catchment           = c("A", "B", "C"),
#'   demand_workload     = c(1200, 800, 400),
#'   accessible_capacity = c(900, 900, 500)
#' )
#' access_outcomes_national(clear_access(catchments))
#' @export
access_outcomes_national <- function(cleared) {
  need <- c("demand_workload", "accessible_capacity", "served", "unmet_demand",
            "utilization", "wait_time", "wait_censored", "p_appointment")
  if (!is.data.frame(cleared) || !all(need %in% names(cleared))) {
    stop("access_outcomes_national(): `cleared` must come from clear_access() ",
         "(missing: ", paste(setdiff(need, names(cleared)), collapse = ", "), ").",
         call. = FALSE)
  }
  d <- cleared$demand_workload
  tot_demand <- sum(d, na.rm = TRUE)
  tot_cap    <- sum(cleared$accessible_capacity, na.rm = TRUE)
  tot_served <- sum(cleared$served, na.rm = TRUE)
  tot_unmet  <- sum(cleared$unmet_demand, na.rm = TRUE)

  finite_wait <- is.finite(cleared$wait_time) & !.access_is_true(cleared$wait_censored)
  wait_nat <- .access_dwmean(ifelse(finite_wait, cleared$wait_time, NA_real_), d)
  censored_share <- if (tot_demand > 0) {
    sum(d[.access_is_true(cleared$wait_censored)], na.rm = TRUE) / tot_demand
  } else NA_real_
  p_appt_nat <- .access_dwmean(cleared$p_appointment, d)

  pop <- if ("accessible_population" %in% names(cleared)) sum(cleared$accessible_population, na.rm = TRUE) else NA_real_
  fte <- if ("accessible_fte" %in% names(cleared)) sum(cleared$accessible_fte, na.rm = TRUE) else NA_real_
  panel_nat <- if (!is.na(fte) && fte > 0) pop / fte else NA_real_

  util_nat <- if (tot_cap > 0) tot_served / tot_cap else NA_real_
  unmet_frac <- if (tot_demand > 0) tot_unmet / tot_demand else NA_real_

  status <- unique(stats::na.omit(cleared$calibration_status))
  status <- if (length(status) == 1L) status else paste(status, collapse = "+")

  out <- tibble::tibble(
    estimand = c("A1", "A1b", "A2", "A3", "A4", "A5", "A5b"),
    label = c("wait_time", "wait_censored_share", "p_appointment",
              "panel_size", "utilization", "unmet_demand", "unmet_fraction"),
    value = c(wait_nat, censored_share, p_appt_nat, panel_nat,
              util_nat, tot_unmet, unmet_frac),
    unit = c("time", "fraction", "probability", "patients_per_fte",
             "fraction", "workload", "fraction"),
    calibration_status = status
  )

  # A6/A7: spatial-overflow outcomes, present only when `cleared` came from
  # overflow_access() (Phase 2). Demand that was met only by sending the patient
  # to another catchment, and the mean extra travel that cost -- computed from
  # columns so they survive a trajectory roll-up (attr() would not).
  if (all(c("spilled_out", "overflow_travel_time") %in% names(cleared))) {
    base_demand <- if ("demand_workload_pre_overflow" %in% names(cleared)) {
      sum(cleared$demand_workload_pre_overflow, na.rm = TRUE)
    } else tot_demand
    tot_spilled <- sum(cleared$spilled_out, na.rm = TRUE)
    spilled_share <- if (base_demand > 0) tot_spilled / base_demand else NA_real_
    ovt_nat <- .access_dwmean(cleared$overflow_travel_time, cleared$spilled_out)
    out <- rbind(out, tibble::tibble(
      estimand = c("A6", "A7"),
      label = c("spilled_share", "overflow_travel_time"),
      value = c(spilled_share, ovt_nat),
      unit = c("fraction", "time"),
      calibration_status = status
    ))
  }
  out
}

# TRUE for TRUE, FALSE for FALSE or NA -- so an NA censored flag never counts as
# censored and never poisons a sum via subsetting.
.access_is_true <- function(x) !is.na(x) & x

#' Assert every access outcome carries a calibration label
#'
#' Governance guard, mirroring the demand/supply layers: an access outcome must
#' never be published without a `calibration_status`. With
#' `require_calibrated = TRUE` it further refuses an outcome still stamped with an
#' `assumed`/`illustrative` status.
#'
#' @param x A clearing table ([clear_access()]) or a national roll-up
#'   ([access_outcomes_national()]); either way it must have `calibration_status`.
#' @param require_calibrated If `TRUE`, also error when any status matches
#'   "assumed" or "illustrative". Default `FALSE`.
#' @return `x`, invisibly, if it passes.
#' @family access outcomes
#' @concept reporting
#' @export
assert_access_outcomes_labeled <- function(x, require_calibrated = FALSE) {
  if (!is.data.frame(x) || !"calibration_status" %in% names(x)) {
    stop("assert_access_outcomes_labeled(): no `calibration_status` column.", call. = FALSE)
  }
  s <- x$calibration_status
  if (any(is.na(s) | !nzchar(s))) {
    stop("assert_access_outcomes_labeled(): every row must carry a non-empty ",
         "calibration_status.", call. = FALSE)
  }
  if (isTRUE(require_calibrated) && any(grepl("assumed|illustrative", s))) {
    stop("assert_access_outcomes_labeled(): outcomes are still ",
         "assumed/illustrative; fit wait_scale and panel benchmarks before ",
         "publishing (see access_validation_targets()).", call. = FALSE)
  }
  invisible(x)
}

#' National access outcomes as a year-by-year time series
#'
#' Applies [access_outcomes_national()] to each year of a multi-year clearing
#' table (from [clear_access_trajectory()]) and stacks the results, so the A1-A5
#' outcomes can be read as trajectories (e.g. a wait time that climbs as a
#' shortfall compounds under backlog).
#'
#' @param cleared A tibble from [clear_access_trajectory()] (needs a `year`
#'   column plus the [clear_access()] outputs).
#' @return The [access_outcomes_national()] tibble per year, stacked, with a
#'   leading `year` column.
#' @family access outcomes
#' @concept reporting
#' @export
access_outcomes_trajectory <- function(cleared) {
  if (!is.data.frame(cleared) || !"year" %in% names(cleared)) {
    stop("access_outcomes_trajectory(): `cleared` needs a `year` column ",
         "(from clear_access_trajectory()).", call. = FALSE)
  }
  years <- sort(unique(cleared$year))
  parts <- lapply(years, function(y) {
    nat <- access_outcomes_national(cleared[cleared$year == y, , drop = FALSE])
    nat$year <- y
    nat
  })
  res <- dplyr::bind_rows(parts)
  res[, c("year", setdiff(names(res), "year")), drop = FALSE]
}

#' National access outcomes stratified by severity class
#'
#' Applies [access_outcomes_national()] within each severity class of a
#' severity-stratified clearing table (from [clear_access_by_severity()]) and
#' stacks the results, so the A-series can be read per severity (e.g. an urgent
#' wait that must clear a 7-day window vs a routine 30-day one).
#'
#' @param cleared A tibble from [clear_access_by_severity()].
#' @param severity_col Name of the severity column. Default `"severity"`.
#' @return The [access_outcomes_national()] tibble per severity class, stacked,
#'   with a leading severity column.
#' @family access outcomes
#' @concept reporting
#' @export
access_outcomes_by_severity <- function(cleared, severity_col = "severity") {
  if (!is.data.frame(cleared) || !severity_col %in% names(cleared)) {
    stop("access_outcomes_by_severity(): `cleared` needs a `", severity_col,
         "` column (from clear_access_by_severity()).", call. = FALSE)
  }
  levels <- unique(cleared[[severity_col]])
  parts <- lapply(levels, function(s) {
    nat <- access_outcomes_national(cleared[cleared[[severity_col]] == s, , drop = FALSE])
    nat[[severity_col]] <- s
    nat
  })
  res <- dplyr::bind_rows(parts)
  res[, c(severity_col, setdiff(names(res), severity_col)), drop = FALSE]
}

#' Roll up and publish access outcomes, guarding calibration
#'
#' The single publish-path seam for the access layer: it produces the national
#' A-series roll-up and then refuses to hand it back unless every outcome carries
#' a calibration label and, by default, is actually calibrated (not still
#' `assumed`/`illustrative`). Wiring [assert_access_outcomes_labeled()] in HERE
#' means an un-calibrated access number cannot leave the layer as a published
#' figure by accident -- the same governance the demand/supply layers use.
#'
#' @param cleared A per-catchment clearing table from [clear_access()] (or a
#'   spatial-overflow clearing).
#' @param require_calibrated If `TRUE` (default, the publishing posture), error
#'   when any outcome is still `assumed`/`illustrative`. Set `FALSE` to roll up
#'   an explicitly-labeled draft without the calibration gate.
#' @return The [access_outcomes_national()] tibble, invisibly returned only after
#'   it passes the guard.
#' @family access outcomes
#' @concept reporting
#' @export
publish_access_outcomes <- function(cleared, require_calibrated = TRUE) {
  national <- access_outcomes_national(cleared)
  assert_access_outcomes_labeled(national, require_calibrated = require_calibrated)
  national
}
