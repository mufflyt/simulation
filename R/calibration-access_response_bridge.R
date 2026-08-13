# Adequacy -> access response bridge ----------------------------------------
#
# THE MISSING LINK, made explicit.
#
# Two engines already exist and never touched:
#
#   * baseline_gap() speaks in FTE. It turns a base-year adequacy ratio into a
#     required-FTE level: required = base_supply_fte / adequacy
#     (required_fte_base_year(), R/reporting-baseline_gap.R).
#   * clear_access() speaks in demand-vs-capacity. It turns a
#     (demand_workload, accessible_capacity) pair into a patient-experienced
#     wait and appointment-obtainment probability via a steady-state queue
#     (R/geography-access_clearing.R), rho = demand / capacity.
#
# Nothing converted a scalar adequacy into the (demand, capacity) pair the
# queue consumes, so no code path ran "adequacy -> simulated appointment wait".
# capacity_status() and the Lizeth limitation note both name this gap: the
# "validated response function linking effective URPS capacity to appointment
# access" that does not yet exist.
#
# This module is that bridge -- but only its FORWARD, ASSUMED half. Because
# clear_access() depends on demand and capacity only through their ratio, the
# bridge needs no currency conversion: in FTE units the queue utilization is
#
#     rho = required / supply = (supply / adequacy) / supply = 1 / adequacy
#
# so the mapping is the faithful COMPOSITION of two functions the package
# already ships, not a new free assumption. What IS assumed is that the
# steady-state queue shape describes URPS access at all; that assumption stays
# labeled "assumed_illustrative" and is exactly what a later fit against
# observed waits (Rabice 2019 / Lizeth) would test.
#
# CONSEQUENCE, SURFACED NOT HIDDEN: adequacy < 1 gives rho > 1, so a single
# national cell SATURATES -- the steady-state wait is infinite. That is not a
# bug; it is the honest statement that a below-one national mean adequacy cannot
# produce a finite national wait under this queue without heterogeneity. A
# finite observed wait (Rabice: 23.1 business days) therefore implies either a
# distribution of catchment adequacies with mass above 1, or an effective
# utilization below the raw 1/adequacy. The bridge converts whatever
# granularity of adequacy it is handed (one national cell, or a vector of
# catchment adequacies) and refuses to invent the dispersion itself.
#
# This module does NOT wire into run_workforce_microsimulation() and does NOT
# change capacity_status()$resolved. It is a scaffold for a calibration that has
# not happened, not a claim that it has.

.access_bridge_recycle <- function(x, n, what) {
  if (base::length(x) == n) {
    return(x)
  }
  if (base::length(x) == 1L) {
    return(base::rep(x, n))
  }
  base::stop(
    "`", what, "` must have length 1 or ", n, ", not ", base::length(x), ".",
    call. = FALSE
  )
}

#' Convert a base-year adequacy into a clear_access() load table
#'
#' Bridges the FTE-denominated base-year gap to the demand-vs-capacity currency
#' that [clear_access()] consumes, so a proposed adequacy can be pushed through
#' the access queue. Demand is the required FTE from
#' [required_fte_base_year()] (`required = base_supply_fte / adequacy`) and
#' capacity is `base_supply_fte`, so utilization is `rho = 1 / adequacy`.
#'
#' @details
#' Because [clear_access()] depends on demand and capacity only through their
#' ratio, working in FTE units is exact and needs no wRVU conversion. The
#' mapping is the composition of two shipped functions, not a new assumption;
#' the only thing assumed is that the steady-state queue describes access, which
#' the returned table leaves labeled `assumed_illustrative`.
#'
#' `adequacy < 1` yields `rho > 1`, which [clear_access()] reports as a
#' saturated (infinite-wait) catchment. This is surfaced by a warning, not
#' hidden: a finite national wait under this queue requires a catchment-level
#' adequacy distribution with mass above 1, which the caller must supply (this
#' function does not invent the dispersion). `adequacy`, `base_supply_fte` and
#' `insurance_fraction` are recycled to a common length.
#'
#' @param adequacy Numeric, all `> 0`. Base-year adequacy ratio(s); scalar for a
#'   single national cell or a vector for catchment-level heterogeneity.
#' @param base_supply_fte Numeric, all `> 0`. Base-year clinical FTE, used as the
#'   accessible capacity in the same currency as demand.
#' @param insurance_fraction Share of capacity that accepts the patient, in 0 to
#'   1. Default 1.
#' @param catchment Optional id vector carried through to [clear_access()]. When
#'   `NULL`, ids are `"national"` (one cell) or `c1..cn`.
#' @param status Calibration status recorded as the `response_assumption`
#'   attribute. Default "assumed_illustrative".
#' @return A tibble with `catchment`, `demand_workload`, `accessible_capacity`
#'   and `insurance_fraction`, ready for [clear_access()], carrying a
#'   `response_assumption` attribute naming the mapping and its status.
#'
#' @concept calibration
#' @family access response bridge
#' @seealso [required_fte_base_year()] for the demand side, [clear_access()] for
#'   the queue it feeds, [simulate_access_for_adequacy()] for the composed map.
#' @export
adequacy_access_load <- function(adequacy,
                                 base_supply_fte,
                                 insurance_fraction = 1,
                                 catchment = NULL,
                                 status = "assumed_illustrative") {
  base::message("Bridging base-year adequacy to a clear_access() load table.")
  if (!base::is.numeric(adequacy) || base::anyNA(adequacy) ||
        base::any(adequacy <= 0)) {
    base::stop("`adequacy` must be numeric, non-missing and > 0.", call. = FALSE)
  }
  if (!base::is.numeric(base_supply_fte) || base::anyNA(base_supply_fte) ||
        base::any(base_supply_fte <= 0)) {
    base::stop(
      "`base_supply_fte` must be numeric, non-missing and > 0.",
      call. = FALSE
    )
  }
  if (!base::is.numeric(insurance_fraction) ||
        base::anyNA(insurance_fraction) ||
        base::any(insurance_fraction < 0 | insurance_fraction > 1)) {
    base::stop(
      "`insurance_fraction` must be numeric and in 0 to 1.",
      call. = FALSE
    )
  }
  n <- base::max(
    base::length(adequacy),
    base::length(base_supply_fte),
    base::length(insurance_fraction)
  )
  adequacy <- .access_bridge_recycle(adequacy, n, "adequacy")
  base_supply_fte <- .access_bridge_recycle(base_supply_fte, n, "base_supply_fte")
  insurance_fraction <-
    .access_bridge_recycle(insurance_fraction, n, "insurance_fraction")
  if (base::is.null(catchment)) {
    catchment <- if (n == 1L) "national" else base::paste0("c", base::seq_len(n))
  } else {
    catchment <- .access_bridge_recycle(catchment, n, "catchment")
  }

  # Demand is the required FTE from the shipped single-source-of-truth, one cell
  # at a time (required_fte_base_year() asserts a scalar adequacy).
  demand_workload <- base::vapply(
    base::seq_len(n),
    function(i) required_fte_base_year(base_supply_fte[i], adequacy[i]),
    numeric(1)
  )

  n_saturating <- base::sum(adequacy < 1)
  if (n_saturating > 0L) {
    .msg_warn(
      "adequacy_access_load(): ", n_saturating, " of ", n,
      " cell(s) have adequacy < 1, so rho = 1/adequacy > 1 and clear_access() ",
      "will report them as saturated (infinite wait). A finite national wait ",
      "requires a catchment adequacy distribution with mass above 1; this ",
      "bridge does not invent that dispersion."
    )
  }

  load_tbl <- tibble::tibble(
    catchment = catchment,
    demand_workload = demand_workload,
    accessible_capacity = base_supply_fte,
    insurance_fraction = insurance_fraction
  )
  base::attr(load_tbl, "response_assumption") <- base::list(
    mapping = "rho = required/supply = 1/adequacy (FTE currency, ratio-invariant)",
    status = status,
    identifies_capacity_adequacy = FALSE
  )
  base::message(
    "Built ", n, " catchment cell(s); utilization rho = 1/adequacy."
  )
  load_tbl
}

#' Simulate national access outcomes implied by a base-year adequacy
#'
#' Composes the forward response map end to end: [adequacy_access_load()] turns
#' the adequacy into a [clear_access()] load table, the queue clears it, and
#' [access_outcomes_national()] rolls it up to the national A-series (wait,
#' appointment probability, unmet demand, utilization). This is the
#' "adequacy -> simulated appointment wait" path that did not previously exist.
#'
#' @details
#' The result is an ASSUMED response, not a fielded one: it inherits
#' `clear_access()`'s `calibration_status` (illustrative until `wait_scale` is
#' fitted) and does not resolve [capacity_status()]. It is the forward half of a
#' calibration loop -- vary `adequacy` (or `wait_scale`) and compare the A1 wait
#' against an observed distribution -- but performs no fitting itself. A scalar
#' `adequacy < 1` saturates (A1 wait `Inf`); pass a catchment-level adequacy
#' vector for a finite national wait.
#'
#' @param adequacy Numeric, all `> 0`; see [adequacy_access_load()].
#' @param base_supply_fte Numeric, all `> 0`; base-year clinical FTE.
#' @param wait_scale Proportionality constant `k` in `wait = k * rho/(1-rho)`,
#'   passed to [clear_access()]. Default 30 (illustrative; fit before
#'   publishing).
#' @param appointment_window Window within which an appointment counts, same
#'   time unit as `wait_scale`. Default 30.
#' @param wait_ceiling Finite value to censor saturated waits at, passed to
#'   [clear_access()]. Default `Inf`.
#' @param insurance_fraction Share of capacity accepting the patient, 0 to 1.
#'   Default 1.
#' @param catchment Optional id vector; see [adequacy_access_load()].
#' @return The national A-series tibble from [access_outcomes_national()]
#'   (`estimand`, `label`, `value`, `unit`, `calibration_status`).
#'
#' @concept calibration
#' @family access response bridge
#' @seealso [adequacy_access_load()], [clear_access()],
#'   [access_outcomes_national()], [fit_wait_scale()].
#' @export
simulate_access_for_adequacy <- function(adequacy,
                                         base_supply_fte,
                                         wait_scale = 30,
                                         appointment_window = 30,
                                         wait_ceiling = Inf,
                                         insurance_fraction = 1,
                                         catchment = NULL) {
  base::message("Simulating access outcomes implied by base-year adequacy.")
  load_tbl <- adequacy_access_load(
    adequacy = adequacy,
    base_supply_fte = base_supply_fte,
    insurance_fraction = insurance_fraction,
    catchment = catchment
  )
  cleared <- clear_access(
    load_tbl,
    appointment_window = appointment_window,
    wait_scale = wait_scale,
    wait_ceiling = wait_ceiling
  )
  outcomes <- access_outcomes_national(cleared)
  base::message("Access outcomes complete (assumed_illustrative until fitted).")
  outcomes
}
