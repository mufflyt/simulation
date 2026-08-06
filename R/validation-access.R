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
    status = status
  )
}
