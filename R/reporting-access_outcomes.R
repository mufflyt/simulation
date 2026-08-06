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
#'   `label`, `value`, `unit`, `calibration_status`.
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

  tibble::tibble(
    estimand = c("A1", "A1b", "A2", "A3", "A4", "A5", "A5b"),
    label = c("wait_time", "wait_censored_share", "p_appointment",
              "panel_size", "utilization", "unmet_demand", "unmet_fraction"),
    value = c(wait_nat, censored_share, p_appt_nat, panel_nat,
              util_nat, tot_unmet, unmet_frac),
    unit = c("time", "fraction", "probability", "patients_per_fte",
             "fraction", "workload", "fraction"),
    calibration_status = status
  )
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
