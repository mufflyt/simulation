# Access clearing (Phase 1) --------------------------------------------------
#
# The step that turns "demand vs accessible capacity" into what a patient
# actually experiences: a wait, a chance of getting an appointment, a panel
# size, unmet demand. See docs/ACCESS_CLEARING_SPEC.md.
#
# This is the JOIN between engines that already exist -- it does not rebuild
# either. Demand (completed-visit workload) comes from the demand chain
# (calculate_visit_based_demand() / the D-series, allocated to geography by
# isochrone_demand_from_tracts()); accessible capacity comes from
# supply_capacity_hierarchy() tier 4, distributed by compute_e2sfca_access().
# One row per catchment x year in, one row out.
#
# Phase 1 is a PURE, STATELESS transform: no backlog carry-forward and no
# spatial overflow (those are Phase 2/3, and are opt-in by design so the default
# never hides state). The queue is a steady-state approximation, not a
# discrete-event simulation.

#' Clear demand against accessible capacity into patient-experienced outcomes
#'
#' For each catchment, utilization is `rho = demand / capacity`. Unmet demand is
#' `max(0, demand - capacity)`. Wait time follows a labeled, monotone
#' heavy-traffic mapping `wait = wait_scale * rho / (1 - rho)` for `rho < 1`, and
#' is reported as censored (at `wait_ceiling`) once the queue is unbounded
#' (`rho >= 1`) -- never `NaN`, never negative. The probability of obtaining an
#' appointment within `appointment_window` uses an exponential-delay
#' approximation `P(wait <= W) = 1 - exp(-W / wait)`, multiplied by the share of
#' capacity that will accept the patient (`insurance_fraction`). Panel size is
#' `accessible_population / accessible_fte`.
#'
#' @param catchments A data frame with numeric `demand_workload` and
#'   `accessible_capacity` in the SAME currency (e.g. annual wRVU-equivalent),
#'   and optional `accessible_population`, `accessible_fte`,
#'   `median_travel_time`, and `insurance_fraction` (in `[0, 1]`, default 1). Any
#'   id columns (e.g. `catchment`, `year`) are carried through untouched. `NA`
#'   demand or capacity marks an empty/unknown catchment and yields `NA`
#'   outcomes (not an error).
#' @param appointment_window Window `W` within which an appointment "counts",
#'   in the same time unit as the wait (e.g. days). Positive scalar. Default 30.
#' @param wait_scale Proportionality constant `k` in `wait = k * rho/(1-rho)`,
#'   same time unit as `appointment_window`. A calibration knob fit to observed
#'   waits (see [access_validation_targets()]). Positive scalar. Default 30
#'   (labeled illustrative).
#' @param wait_ceiling Value reported for `wait_time` when `rho >= 1`. Default
#'   `Inf`; set finite to saturate. Rows at the ceiling are flagged by
#'   `wait_censored`.
#' @param status Calibration status stamped on every row. Default
#'   "assumed_illustrative" -- fit `wait_scale` before publishing.
#' @return A tibble: the input columns, plus `utilization`, `served`,
#'   `unmet_demand`, `wait_time`, `wait_censored`, `p_appointment`,
#'   `panel_size`, `median_travel_time`, `calibration_status`.
#' @export
clear_access <- function(catchments,
                         appointment_window = 30,
                         wait_scale = 30,
                         wait_ceiling = Inf,
                         status = "assumed_illustrative") {
  if (!is.data.frame(catchments) ||
      !all(c("demand_workload", "accessible_capacity") %in% names(catchments))) {
    stop("clear_access(): `catchments` needs numeric columns `demand_workload` ",
         "and `accessible_capacity`.", call. = FALSE)
  }
  n <- nrow(catchments)
  col <- function(nm, default) if (nm %in% names(catchments)) catchments[[nm]] else rep(default, n)
  d   <- catchments$demand_workload
  cap <- catchments$accessible_capacity
  ins <- col("insurance_fraction", 1)
  pop <- col("accessible_population", NA_real_)
  fte <- col("accessible_fte", NA_real_)
  mtt <- col("median_travel_time", NA_real_)

  stopifnot(
    is.numeric(d), is.numeric(cap), is.numeric(ins), is.numeric(pop), is.numeric(fte),
    length(appointment_window) == 1L, is.finite(appointment_window), appointment_window > 0,
    length(wait_scale) == 1L, is.finite(wait_scale), wait_scale > 0,
    length(wait_ceiling) == 1L, !is.na(wait_ceiling), wait_ceiling > 0
  )
  bad_d   <- !is.na(d)   & (!is.finite(d)   | d   < 0)
  bad_cap <- !is.na(cap) & (!is.finite(cap) | cap < 0)
  bad_ins <- !is.na(ins) & (!is.finite(ins) | ins < 0 | ins > 1)
  if (any(bad_d) || any(bad_cap) || any(bad_ins)) {
    stop("clear_access(): `demand_workload`/`accessible_capacity` must be finite ",
         "and >= 0 where present, and `insurance_fraction` in [0, 1].", call. = FALSE)
  }

  util <- served <- unmet <- wait <- p_appt <- panel <- rep(NA_real_, n)
  censored <- rep(NA, n)

  known <- !is.na(d) & !is.na(cap)
  # rho: Inf when capacity is 0 with positive demand; the 0/0 catchment stays NA.
  rho <- rep(NA_real_, n)
  rho[known] <- ifelse(cap[known] > 0, d[known] / cap[known],
                       ifelse(d[known] > 0, Inf, NA_real_))

  served[known] <- pmin(d[known], cap[known])
  unmet[known]  <- pmax(0, d[known] - cap[known])

  has_rho <- known & !is.na(rho)
  util[has_rho]     <- pmin(1, rho[has_rho])
  censored[has_rho] <- rho[has_rho] >= 1
  wait[has_rho] <- ifelse(rho[has_rho] < 1,
                          wait_scale * rho[has_rho] / (1 - rho[has_rho]),
                          wait_ceiling)
  # P(wait <= W): wait 0 -> 1; finite wait -> 1 - exp(-W/wait); infinite wait -> 0.
  p_wait <- rep(NA_real_, n)
  p_wait[has_rho] <- ifelse(wait[has_rho] <= 0, 1,
                            ifelse(is.finite(wait[has_rho]),
                                   1 - exp(-appointment_window / wait[has_rho]), 0))
  p_appt[has_rho] <- pmin(1, pmax(0, p_wait[has_rho] * ins[has_rho]))

  ok_fte <- !is.na(fte) & fte > 0
  panel[ok_fte] <- pop[ok_fte] / fte[ok_fte]

  out <- catchments
  out$utilization        <- util
  out$served             <- served
  out$unmet_demand       <- unmet
  out$wait_time          <- wait
  out$wait_censored      <- as.logical(censored)
  out$p_appointment      <- p_appt
  out$panel_size         <- panel
  out$median_travel_time <- mtt
  out$calibration_status <- status
  tibble::as_tibble(out)
}
