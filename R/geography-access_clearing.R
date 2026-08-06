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
#'   `median_travel_time`, and `insurance_fraction` (in 0 to 1, default 1). Any
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
         "and >= 0 where present, and `insurance_fraction` in 0 to 1.", call. = FALSE)
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

# Phase 4: dynamic multi-year clearing ---------------------------------------
#
# "Supply and demand interact every simulated year." Runs the Phase-1 static
# clearing across a trajectory of years -- a supply-capacity path and a demand
# path per catchment -- and (optionally) carries each year's UNMET demand
# forward into the next year's queue. Backlog carry-forward is the Phase-3 lever,
# OFF by default so the baseline stays the stateless per-year clearing; spatial
# overflow (Phase 2) composes underneath, one year at a time, when added.

#' Clear access across a multi-year trajectory (optionally carrying backlog)
#'
#' Applies [clear_access()] to each year of a catchment x year panel in
#' ascending year order. With `carry_backlog = TRUE`, a fraction of each
#' catchment's unmet demand is added to that catchment's demand in the following
#' year (matched by `catchment`), so a persistent shortfall compounds instead of
#' vanishing at the year boundary.
#'
#' @param panel A data frame with `year` plus the per-catchment [clear_access()]
#'   inputs (`demand_workload`, `accessible_capacity`, and the optional
#'   population/fte/insurance/travel columns). A `catchment` id is required when
#'   `carry_backlog = TRUE`.
#' @param carry_backlog If `TRUE`, carry unmet demand forward (Phase-3 behaviour).
#'   Default `FALSE` (each year cleared independently).
#' @param backlog_fraction Fraction of a year's unmet demand carried into the
#'   next year. In 0 to 1. Default 1 (all unmet persists).
#' @param neighbors Optional overflow edge list (see [overflow_access()]). When
#'   supplied, each year is cleared with spatial overflow composed underneath
#'   the backlog step: demand (plus any backlog) is reallocated across catchments
#'   before the year's outcomes are computed, and the backlog carried into the
#'   next year is the POST-overflow unmet demand. Needs a `catchment` id. Default
#'   `NULL` (no overflow; Phase-1 clearing each year).
#' @param max_travel_penalty Passed to [overflow_access()] when `neighbors` is
#'   supplied. Default `Inf`.
#' @param appointment_window,wait_scale,wait_ceiling,status Passed through to
#'   [clear_access()] / [overflow_access()].
#' @return A tibble: every [clear_access()] output column across all years, plus
#'   `backlog_in` (demand carried in from the prior year) and
#'   `demand_workload_base` (this year's demand before any backlog). With
#'   `neighbors`, also the [overflow_access()] accounting columns.
#' @export
clear_access_trajectory <- function(panel,
                                    carry_backlog = FALSE,
                                    backlog_fraction = 1,
                                    neighbors = NULL,
                                    max_travel_penalty = Inf,
                                    appointment_window = 30,
                                    wait_scale = 30,
                                    wait_ceiling = Inf,
                                    status = "assumed_illustrative") {
  if (!is.data.frame(panel) || !"year" %in% names(panel) ||
      !all(c("demand_workload", "accessible_capacity") %in% names(panel))) {
    stop("clear_access_trajectory(): `panel` needs `year`, `demand_workload`, ",
         "and `accessible_capacity`.", call. = FALSE)
  }
  stopifnot(
    length(carry_backlog) == 1L, is.logical(carry_backlog), !is.na(carry_backlog),
    length(backlog_fraction) == 1L, is.finite(backlog_fraction),
    backlog_fraction >= 0, backlog_fraction <= 1
  )
  overflow <- !is.null(neighbors)
  if ((carry_backlog || overflow) && !"catchment" %in% names(panel)) {
    stop("clear_access_trajectory(): carry_backlog = TRUE or a `neighbors` edge ",
         "list needs a `catchment` id to match catchments across years.", call. = FALSE)
  }
  years <- sort(unique(panel$year))
  prev_unmet <- NULL                       # named by catchment, from the prior year
  out <- vector("list", length(years))
  for (i in seq_along(years)) {
    slice <- panel[panel$year == years[i], , drop = FALSE]
    backlog_in <- rep(0, nrow(slice))
    if (carry_backlog && !is.null(prev_unmet)) {
      m <- match(slice$catchment, names(prev_unmet))
      backlog_in <- ifelse(is.na(m), 0, prev_unmet[m] * backlog_fraction)
    }
    augmented <- slice
    augmented$demand_workload <- slice$demand_workload + backlog_in
    cl <- if (overflow) {
      overflow_access(augmented, neighbors, max_travel_penalty = max_travel_penalty,
                      appointment_window = appointment_window, wait_scale = wait_scale,
                      wait_ceiling = wait_ceiling, status = status)
    } else {
      clear_access(augmented, appointment_window = appointment_window,
                   wait_scale = wait_scale, wait_ceiling = wait_ceiling,
                   status = status)
    }
    cl$backlog_in <- backlog_in
    cl$demand_workload_base <- slice$demand_workload
    # attr() (overflow system totals) does not survive bind_rows; drop it here so
    # the stacked result is a clean tibble (per-catchment overflow columns remain).
    attr(cl, "overflow") <- NULL
    out[[i]] <- cl
    if (carry_backlog) {
      # POST-overflow unmet is what actually persists into next year's queue.
      prev_unmet <- stats::setNames(cl$unmet_demand, slice$catchment)
    }
  }
  dplyr::bind_rows(out)
}

# Phase 2: spatial overflow (demand-conserving reallocation) -----------------
#
# "Unmet demand in a catchment spills to the next-nearest catchment with a
# travel penalty, raising its rho -- or is censored as lost demand." A scenario
# switch, not a default: Phase-1 clearing never moves demand across space.
#
# The reallocation is a greedy nearest-spare-capacity fill. A shortage
# catchment's residual demand (demand - capacity) is offered, cheapest edge
# first, to neighbours that still have spare capacity, until either the demand
# is placed or no reachable neighbour has room -- and the remainder is censored
# as lost/unmet. Demand is CONSERVED. Per origin,
#   demand = served_local + spilled_out + unmet,
# and across the system every unit spilled out of one catchment is a unit
# spilled into another, so sum(served) + sum(unmet) = sum(demand). Waits and
# appointment probabilities are then recomputed by clear_access() on each
# catchment's POST-OVERFLOW effective demand
#   demand_effective = demand - spilled_out + spilled_in,
# reusing the tested Phase-1 queue math rather than re-deriving it: an origin
# that sheds its excess relaxes toward rho = 1, and a receiver that absorbs
# spill has its rho (and wait) pushed up toward -- but never past -- saturation.

#' Reallocate unmet demand to neighbouring catchments with spare capacity
#'
#' The Phase-2 spatial-overflow step. Each catchment is first cleared locally
#' (as in [clear_access()]); a shortage catchment's residual demand is then
#' offered to neighbouring catchments with spare capacity, cheapest edge first,
#' until it is placed or no reachable neighbour has room. Demand is conserved:
#' `demand = served_local + spilled_out + unmet` per catchment, and
#' `sum(spilled_out) == sum(spilled_in)` across the system. Post-overflow waits,
#' appointment probabilities and utilization are recomputed by [clear_access()]
#' on each catchment's effective demand (`demand - spilled_out + spilled_in`).
#'
#' @param catchments A data frame with a `catchment` id (unique), numeric
#'   `demand_workload` and `accessible_capacity` (same currency), and the
#'   optional [clear_access()] columns. `NA` demand/capacity marks a
#'   non-participating catchment (it neither spills nor absorbs).
#' @param neighbors A data frame of directed overflow edges: `from` and `to`
#'   catchment ids and a non-negative `travel_penalty` (extra travel to reach
#'   the neighbour, in the wait/appointment time unit). Self-edges are rejected;
#'   every id must be present in `catchments`.
#' @param max_travel_penalty Edges with `travel_penalty` above this are dropped
#'   (demand that can reach no neighbour within budget is censored as unmet).
#'   Positive scalar. Default `Inf`.
#' @param appointment_window,wait_scale,wait_ceiling,status Passed through to
#'   [clear_access()] for the post-overflow recompute.
#' @return A tibble: the [clear_access()] outputs computed on the post-overflow
#'   effective demand, plus `demand_workload_pre_overflow`, `served_local`
#'   (each catchment's own demand served at home), `spilled_out`, `spilled_in`,
#'   `unmet_before_overflow` (the Phase-1 residual), and `overflow_travel_time`
#'   (mean total travel borne by demand spilled out of the catchment; `NA` where
#'   none spilled). System-level overflow totals are attached as
#'   `attr(x, "overflow")`.
#' @export
overflow_access <- function(catchments, neighbors,
                            max_travel_penalty = Inf,
                            appointment_window = 30,
                            wait_scale = 30,
                            wait_ceiling = Inf,
                            status = "assumed_illustrative") {
  if (!is.data.frame(catchments) ||
      !all(c("catchment", "demand_workload", "accessible_capacity") %in% names(catchments))) {
    stop("overflow_access(): `catchments` needs `catchment` (id), `demand_workload`, ",
         "and `accessible_capacity`.", call. = FALSE)
  }
  if (!is.data.frame(neighbors) ||
      !all(c("from", "to", "travel_penalty") %in% names(neighbors))) {
    stop("overflow_access(): `neighbors` needs `from`, `to`, `travel_penalty`.", call. = FALSE)
  }
  stopifnot(
    length(max_travel_penalty) == 1L, !is.na(max_travel_penalty), max_travel_penalty > 0
  )
  ids <- as.character(catchments$catchment)
  if (anyDuplicated(ids)) {
    stop("overflow_access(): `catchment` ids must be unique (one row per catchment).",
         call. = FALSE)
  }
  n <- nrow(catchments)
  d   <- catchments$demand_workload
  cap <- catchments$accessible_capacity
  bad_d   <- !is.na(d)   & (!is.finite(d)   | d   < 0)
  bad_cap <- !is.na(cap) & (!is.finite(cap) | cap < 0)
  if (any(bad_d) || any(bad_cap)) {
    stop("overflow_access(): `demand_workload`/`accessible_capacity` must be finite ",
         "and >= 0 where present.", call. = FALSE)
  }

  nb <- neighbors
  nb$from <- as.character(nb$from)
  nb$to   <- as.character(nb$to)
  if (!all(c(nb$from, nb$to) %in% ids)) {
    stop("overflow_access(): every neighbour `from`/`to` must be a `catchment` id.",
         call. = FALSE)
  }
  if (any(nb$from == nb$to)) {
    stop("overflow_access(): a catchment cannot be its own overflow neighbour.",
         call. = FALSE)
  }
  if (any(!is.finite(nb$travel_penalty) | nb$travel_penalty < 0)) {
    stop("overflow_access(): `travel_penalty` must be finite and >= 0.", call. = FALSE)
  }

  known <- !is.na(d) & !is.na(cap)
  resid_demand <- stats::setNames(rep(0, n), ids)
  resid_cap    <- stats::setNames(rep(0, n), ids)
  resid_demand[known] <- pmax(0, d[known] - cap[known])
  resid_cap[known]    <- pmax(0, cap[known] - d[known])
  spilled_out <- stats::setNames(rep(0, n), ids)
  spilled_in  <- stats::setNames(rep(0, n), ids)
  spill_travel <- stats::setNames(rep(0, n), ids)  # per-origin sum of moved*(travel)
  mtt <- if ("median_travel_time" %in% names(catchments)) {
    stats::setNames(catchments$median_travel_time, ids)
  } else stats::setNames(rep(NA_real_, n), ids)
  travel_load <- 0                       # sum of moved * (origin travel + penalty)

  # Greedy fill, cheapest edge first; deterministic tie-break on (from, to).
  nb <- nb[nb$travel_penalty <= max_travel_penalty, , drop = FALSE]
  nb <- nb[order(nb$travel_penalty, nb$from, nb$to), , drop = FALSE]
  for (e in seq_len(nrow(nb))) {
    i <- nb$from[e]
    j <- nb$to[e]
    move <- min(resid_demand[[i]], resid_cap[[j]])
    if (move <= 0) next
    resid_demand[[i]] <- resid_demand[[i]] - move
    resid_cap[[j]]    <- resid_cap[[j]] - move
    spilled_out[[i]]  <- spilled_out[[i]] + move
    spilled_in[[j]]   <- spilled_in[[j]] + move
    origin_travel <- if (is.na(mtt[[i]])) 0 else mtt[[i]]
    moved_travel  <- move * (origin_travel + nb$travel_penalty[e])
    travel_load       <- travel_load + moved_travel
    spill_travel[[i]] <- spill_travel[[i]] + moved_travel
  }

  eff <- catchments
  eff$demand_workload <- ifelse(known, d - spilled_out[ids] + spilled_in[ids], d)
  cleared <- clear_access(eff, appointment_window = appointment_window,
                          wait_scale = wait_scale, wait_ceiling = wait_ceiling,
                          status = status)
  cleared$demand_workload_pre_overflow <- d
  cleared$served_local          <- ifelse(known, pmin(d, cap), NA_real_)
  cleared$spilled_out           <- unname(spilled_out[ids])
  cleared$spilled_in            <- unname(spilled_in[ids])
  cleared$unmet_before_overflow <- ifelse(known, pmax(0, d - cap), NA_real_)
  # Mean total travel (origin median + edge penalty) borne by demand spilled OUT
  # of this catchment; NA where nothing spilled. Survives a trajectory roll-up as
  # a plain column (the system-level scalar on attr() would not).
  so <- unname(spilled_out[ids])
  cleared$overflow_travel_time  <- ifelse(so > 0, unname(spill_travel[ids]) / so, NA_real_)

  spilled_total <- sum(unname(spilled_out), na.rm = TRUE)
  tot_demand    <- sum(d, na.rm = TRUE)
  attr(cleared, "overflow") <- list(
    spilled_total       = spilled_total,
    spilled_share       = if (tot_demand > 0) spilled_total / tot_demand else NA_real_,
    overflow_travel_time = if (spilled_total > 0) travel_load / spilled_total else NA_real_
  )
  cleared
}

#' Check the demand-conservation invariants of an overflow_access() result
#'
#' Verifies the Phase-2 accounting from the returned columns alone: the
#' per-catchment identity `demand = served_local + spilled_out + unmet`, the
#' transfer balance `sum(spilled_out) = sum(spilled_in)`, and the system
#' identity `sum(served) + sum(unmet) = sum(demand)`. Returns the residuals so a
#' caller (or a test) can assert they are ~0 rather than trusting a boolean.
#'
#' @param x A tibble returned by [overflow_access()].
#' @param tol Absolute tolerance for the "conserved" flags. Default 1e-8.
#' @return A list: `per_catchment_resid` (numeric, one per row), `transfer_resid`
#'   and `system_resid` (scalars), and `conserved` (all within `tol`).
#' @export
overflow_conservation <- function(x, tol = 1e-8) {
  need <- c("demand_workload_pre_overflow", "served_local", "spilled_out",
            "unmet_demand", "spilled_in", "served")
  if (!is.data.frame(x) || !all(need %in% names(x))) {
    stop("overflow_conservation(): `x` must come from overflow_access() (missing: ",
         paste(setdiff(need, names(x)), collapse = ", "), ").", call. = FALSE)
  }
  known <- !is.na(x$demand_workload_pre_overflow)
  per <- (x$served_local + x$spilled_out + x$unmet_demand) -
    x$demand_workload_pre_overflow
  per[!known] <- 0
  transfer_resid <- sum(x$spilled_out, na.rm = TRUE) - sum(x$spilled_in, na.rm = TRUE)
  system_resid <- (sum(x$served, na.rm = TRUE) + sum(x$unmet_demand, na.rm = TRUE)) -
    sum(x$demand_workload_pre_overflow, na.rm = TRUE)
  list(
    per_catchment_resid = per,
    transfer_resid = transfer_resid,
    system_resid = system_resid,
    conserved = max(abs(per), na.rm = TRUE) <= tol &&
      abs(transfer_resid) <= tol && abs(system_resid) <= tol
  )
}

#' Build a k-nearest-neighbour overflow edge list from catchment coordinates
#'
#' Convenience constructor for the `neighbors` argument of [overflow_access()]:
#' links each catchment to its `k` nearest others by great-circle distance
#' ([haversine_km()]), turning distance into a `travel_penalty` at a flat
#' `penalty_per_km`. A stand-in for a real drive-time matrix when one is not to
#' hand; supply a measured matrix directly for production use.
#'
#' @param coords A data frame with `catchment`, `lat`, `lon` (decimal degrees).
#' @param k Number of nearest neighbours to link per catchment. Default 3.
#' @param penalty_per_km Travel penalty per kilometre. Non-negative. Default 1.
#' @return A tibble of `from`, `to`, `travel_penalty` edges.
#' @export
catchment_neighbors_from_coords <- function(coords, k = 3, penalty_per_km = 1) {
  if (!is.data.frame(coords) || !all(c("catchment", "lat", "lon") %in% names(coords))) {
    stop("catchment_neighbors_from_coords(): `coords` needs `catchment`, `lat`, `lon`.",
         call. = FALSE)
  }
  stopifnot(
    length(k) == 1L, is.finite(k), k >= 1,
    length(penalty_per_km) == 1L, is.finite(penalty_per_km), penalty_per_km >= 0
  )
  ids <- as.character(coords$catchment)
  n <- length(ids)
  if (n < 2) {
    return(tibble::tibble(from = character(0), to = character(0),
                          travel_penalty = numeric(0)))
  }
  rows <- lapply(seq_len(n), function(i) {
    dkm <- haversine_km(coords$lat[i], coords$lon[i], coords$lat, coords$lon)
    dkm[i] <- Inf                                  # never a neighbour of itself
    o <- order(dkm)
    take <- o[seq_len(min(as.integer(k), n - 1L))]
    take <- take[is.finite(dkm[take])]
    if (!length(take)) return(NULL)
    tibble::tibble(from = ids[i], to = ids[take],
                   travel_penalty = dkm[take] * penalty_per_km)
  })
  dplyr::bind_rows(rows)
}
