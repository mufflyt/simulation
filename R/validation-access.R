# Access-outcome validation targets ------------------------------------------
#
# The access-clearing outputs (A1-A5) are only as trustworthy as the wait/panel
# calibration behind them. This registers the two external anchors the repository
# already names in data-practice_survey -- observed specialty WAIT TIME (the only
# direct observable of unmet demand) and PANEL SIZE (an independent capacity
# check) -- as the targets to fit `wait_scale` and sanity-check panel size
# against. Targets ship UNPOPULATED: a real observed value must be supplied
# before an outcome may be published as calibrated.

#' Access-outcome external validation targets
#'
#' The named anchors the access-clearing layer calibrates against. Ship with no
#' observed values (`observed = NA`, `status = "target_unpopulated"`); populate
#' `observed` from a specialty survey (see data-practice_survey) before treating
#' the corresponding outcome as calibrated.
#'
#' @return A tibble: `target` (matches an `access_outcomes_national()` `label`),
#'   `observed`, `unit`, `rel_tol`, `status`, `note`.
#' @family access
#' @concept validation
#' @export
access_validation_targets <- function() {
  tibble::tribble(
    ~target,        ~observed,  ~unit,              ~rel_tol, ~status,              ~note,
    "wait_time",    NA_real_,   "time",             0.25,     "target_unpopulated", "Observed specialty wait time; primary external anchor (fits wait_scale). See data-practice_survey.",
    "panel_size",   NA_real_,   "patients_per_fte", 0.25,     "target_unpopulated", "Observed panel size; independent capacity check on the wRVU path."
  )
}

#' Compare national access outcomes to external targets
#'
#' Joins the `A`-series roll-up to [access_validation_targets()] by outcome name
#' and, for every target that has a populated `observed` value, reports the
#' relative difference and whether it falls within `rel_tol`. Targets with no
#' observed value are returned with `status = "no_target"` and never counted as a
#' pass -- silence is not validation.
#'
#' @param national A tibble from [access_outcomes_national()].
#' @param targets A targets tibble; defaults to [access_validation_targets()].
#' @return A tibble: `target`, `predicted`, `observed`, `rel_diff`, `rel_tol`,
#'   `status` (`pass` / `fail` / `no_target`).
#' @family access
#' @concept validation
#' @export
validate_access_outcomes <- function(national, targets = access_validation_targets()) {
  if (!is.data.frame(national) || !all(c("label", "value") %in% names(national))) {
    stop("validate_access_outcomes(): `national` must come from access_outcomes_national().",
         call. = FALSE)
  }
  stopifnot(is.data.frame(targets),
            all(c("target", "observed", "rel_tol") %in% names(targets)))
  pred <- national$value[match(targets$target, national$label)]
  obs  <- targets$observed
  rel  <- ifelse(is.finite(pred) & is.finite(obs) & obs != 0,
                 abs(pred - obs) / abs(obs), NA_real_)
  status <- ifelse(!is.finite(obs), "no_target",
                   ifelse(is.finite(rel) & rel <= targets$rel_tol, "pass", "fail"))
  tibble::tibble(
    target = targets$target,
    predicted = pred,
    observed = obs,
    rel_diff = rel,
    rel_tol = targets$rel_tol,
    status = status,
    # Carry the TARGET's own calibration state (target_unpopulated / assumed /
    # calibrated) alongside the pass/fail, so a "pass" against an ASSUMED
    # benchmark is never mistaken for a pass against a measured anchor.
    target_status = if ("status" %in% names(targets)) targets$status else NA_character_
  )
}

# ---- Calibration mechanisms ------------------------------------------------
#
# The two knobs the A-series depends on -- the wait mapping's constant and the
# panel benchmark -- are fit HERE, not hard-coded. Both ship uncalibrated; these
# turn a supplied observation into a fitted constant / a populated target,
# without inventing the observation itself.

#' Fit the wait-mapping constant `wait_scale` to an observed wait
#'
#' The national demand-weighted wait is LINEAR in `wait_scale`
#' (`wait = wait_scale * rho/(1-rho)`), so the fit is closed-form: clear the
#' catchments at `wait_scale = 1`, take the demand-weighted mean wait over the
#' catchments with a finite (non-censored) wait, and scale it to the observation.
#' The result makes `access_outcomes_national()`'s `A1` (`wait_time`) equal
#' `observed_wait` by construction. Supply `observed_wait` from a specialty wait
#' survey (see [access_validation_targets()]); this does not invent it.
#'
#' @param catchments A per-catchment table for [clear_access()] (needs
#'   `demand_workload` and `accessible_capacity`).
#' @param observed_wait The observed national wait to fit to. Positive scalar,
#'   same time unit as the appointment window.
#' @return A list: `wait_scale` (the fitted constant), `unit_wait` (national wait
#'   at `wait_scale = 1`), `observed_wait`, `n_catchments_used`, and
#'   `censored_demand_share` (demand share in saturated queues the fit cannot
#'   see). Feed `wait_scale` back into [clear_access()].
#' @family access
#' @concept validation
#' @export
fit_wait_scale <- function(catchments, observed_wait) {
  stopifnot(length(observed_wait) == 1L, is.finite(observed_wait), observed_wait > 0)
  cl <- clear_access(catchments, wait_scale = 1)
  d <- cl$demand_workload
  censored <- !is.na(cl$wait_censored) & cl$wait_censored
  ok <- is.finite(cl$wait_time) & !censored & is.finite(d)
  if (!any(ok) || sum(d[ok]) == 0) {
    stop("fit_wait_scale(): no finite-wait catchment carries demand to fit ",
         "against (every catchment is saturated, rho >= 1).", call. = FALSE)
  }
  unit_wait <- sum(cl$wait_time[ok] * d[ok]) / sum(d[ok])
  tot <- sum(d, na.rm = TRUE)
  list(
    wait_scale = observed_wait / unit_wait,
    unit_wait = unit_wait,
    observed_wait = observed_wait,
    n_catchments_used = sum(ok),
    censored_demand_share = if (tot > 0) sum(d[censored], na.rm = TRUE) / tot else NA_real_
  )
}

#' Populate an access validation target with an observed (or benchmark) value
#'
#' The targets from [access_validation_targets()] ship UNPOPULATED. This fills
#' one in -- a wait from [fit_wait_scale()]/a survey, or a published panel-size
#' benchmark -- and stamps the target's `status`. Use `status = "assumed"` for a
#' benchmark that stands in for a measured value (a `pass` against it is then
#' visibly assumed in [validate_access_outcomes()], never mistaken for a
#' measured-anchor pass); `status = "calibrated"` for a cited observation.
#'
#' @param targets A targets tibble; defaults to [access_validation_targets()].
#' @param target Which target to populate (e.g. `"wait_time"`, `"panel_size"`).
#' @param observed The observed/benchmark value. Finite scalar.
#' @param status Calibration status to stamp. Default `"assumed"`.
#' @param rel_tol Optional replacement relative tolerance for this target.
#' @return The targets tibble with that row populated.
#' @family access
#' @concept validation
#' @export
set_access_target <- function(targets = access_validation_targets(), target, observed,
                              status = "assumed", rel_tol = NULL) {
  stopifnot(is.data.frame(targets),
            all(c("target", "observed", "rel_tol", "status") %in% names(targets)))
  i <- match(target, targets$target)
  if (is.na(i)) {
    stop(sprintf("set_access_target(): unknown target '%s'. Known: %s",
                 target, paste(targets$target, collapse = ", ")), call. = FALSE)
  }
  stopifnot(length(observed) == 1L, is.finite(observed),
            length(status) == 1L, is.character(status), nzchar(status))
  targets$observed[i] <- observed
  targets$status[i]   <- status
  if (!is.null(rel_tol)) {
    stopifnot(length(rel_tol) == 1L, is.finite(rel_tol), rel_tol >= 0)
    targets$rel_tol[i] <- rel_tol
  }
  targets
}
