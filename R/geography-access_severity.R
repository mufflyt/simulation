# Severity-stratified access clearing (Phase 3) ------------------------------
#
# Phase 1/2 clear a single undifferentiated service class. But a woman with an
# acute complication and one due for a routine follow-up do not wait the same
# amount of time, and they do not compete for a slot as equals: the urgent case
# is seen first. This stratifies the clearing by severity class (the hook is the
# #41 symptom-severity care-seeking stage, demand-severity_sandvik.R):
#
#   * capacity is SHARED across severities but consumed in PRIORITY order --
#     higher-severity demand books slots first, so a flood of routine demand can
#     never starve urgent care;
#   * each severity carries its OWN appointment window (an urgent "counts" only
#     if seen within days; a routine visit within weeks), so p_appointment is
#     judged against the clinically relevant deadline.
#
# It reuses the Phase-1 queue math (clear_access): each tier is cleared against
# the capacity LEFT after higher-priority tiers took theirs.

# Illustrative severity -> appointment-window map (same time unit as the waits).
# ASSUMED, not a measured standard: replace with a cited access-target schedule.
URPS_SEVERITY_WINDOWS <- c(urgent = 7, routine = 30)

#' Clear demand against shared capacity, stratified by severity and priority
#'
#' For each catchment, capacity is offered to severity classes in `priority`
#' order (most urgent first): a class is cleared with [clear_access()] against
#' the capacity remaining after higher-priority classes have taken theirs, using
#' that class's appointment window. Higher-severity demand therefore cannot be
#' displaced by lower-severity volume, and each class's `p_appointment` is judged
#' against its own clinically relevant deadline.
#'
#' @param catchments A LONG data frame: one row per catchment x severity, with a
#'   `catchment` id, a severity column, `demand_workload` (that class's demand),
#'   and `accessible_capacity` (the catchment's shared capacity, constant across
#'   its severity rows). Optional `insurance_fraction` per row. Demand and
#'   capacity must be finite and >= 0.
#' @param severity_windows Named positive numeric: appointment window per
#'   severity class (names are the severity levels). Default
#'   `URPS_SEVERITY_WINDOWS` (illustrative).
#' @param severity_col Name of the severity column. Default `"severity"`.
#' @param priority Severity levels ordered highest-priority (served first) to
#'   lowest. Default: shortest window first (most urgent first).
#' @param wait_scale,wait_ceiling,status Passed through to [clear_access()].
#' @return A long tibble: every [clear_access()] output column, plus the severity
#'   column and `appointment_window`, one row per catchment x severity.
#' @export
clear_access_by_severity <- function(catchments,
                                     severity_windows = URPS_SEVERITY_WINDOWS,
                                     severity_col = "severity",
                                     priority = NULL,
                                     wait_scale = 30,
                                     wait_ceiling = Inf,
                                     status = "assumed_illustrative") {
  need <- c("catchment", severity_col, "demand_workload", "accessible_capacity")
  if (!is.data.frame(catchments) || !all(need %in% names(catchments))) {
    stop("clear_access_by_severity(): `catchments` needs `catchment`, `",
         severity_col, "`, `demand_workload`, `accessible_capacity` (long form).",
         call. = FALSE)
  }
  if (!is.numeric(severity_windows) || is.null(names(severity_windows)) ||
      any(!nzchar(names(severity_windows))) ||
      any(!is.finite(severity_windows) | severity_windows <= 0)) {
    stop("clear_access_by_severity(): `severity_windows` must be a named vector ",
         "of positive appointment windows.", call. = FALSE)
  }
  sev_levels <- names(severity_windows)
  if (is.null(priority)) priority <- names(sort(severity_windows))
  if (!setequal(priority, sev_levels)) {
    stop("clear_access_by_severity(): `priority` must be a permutation of ",
         "names(severity_windows).", call. = FALSE)
  }
  sv <- as.character(catchments[[severity_col]])
  if (!all(sv %in% sev_levels)) {
    stop("clear_access_by_severity(): every `", severity_col, "` value must be ",
         "named in `severity_windows`.", call. = FALSE)
  }
  d <- catchments$demand_workload
  cap_all <- catchments$accessible_capacity
  if (any(!is.finite(d) | d < 0) || any(!is.finite(cap_all) | cap_all < 0)) {
    stop("clear_access_by_severity(): `demand_workload`/`accessible_capacity` ",
         "must be finite and >= 0 (severity clearing shares a known capacity).",
         call. = FALSE)
  }
  # Capacity is shared, so it must be one value per catchment.
  n_cap <- tapply(cap_all, catchments$catchment, function(x) length(unique(x)))
  if (any(n_cap > 1)) {
    stop("clear_access_by_severity(): `accessible_capacity` must be constant ",
         "within a catchment (it is the shared capacity all severities draw on).",
         call. = FALSE)
  }
  ids <- unique(catchments$catchment)
  cap <- stats::setNames(cap_all[match(ids, catchments$catchment)], ids)
  ins_all <- if ("insurance_fraction" %in% names(catchments)) {
    stats::setNames(catchments$insurance_fraction, paste(catchments$catchment, sv, sep = "\r"))
  } else NULL
  lookup <- stats::setNames(d, paste(catchments$catchment, sv, sep = "\r"))
  cell <- function(tab, id, s, default) {
    v <- tab[[paste(id, s, sep = "\r")]]
    if (is.null(v) || is.na(v)) default else v
  }

  consumed <- stats::setNames(rep(0, length(ids)), ids)  # capacity taken by higher tiers
  out_list <- vector("list", length(priority))
  for (k in seq_along(priority)) {
    s <- priority[k]
    d_s <- vapply(ids, function(id) cell(lookup, id, s, 0), numeric(1))
    eff_cap <- pmax(0, cap[ids] - consumed[ids])
    tier <- tibble::tibble(catchment = ids,
                           demand_workload = unname(d_s),
                           accessible_capacity = unname(eff_cap))
    if (!is.null(ins_all)) {
      tier$insurance_fraction <- vapply(ids, function(id) cell(ins_all, id, s, 1), numeric(1))
    }
    cl <- clear_access(tier, appointment_window = severity_windows[[s]],
                       wait_scale = wait_scale, wait_ceiling = wait_ceiling,
                       status = status)
    cl[[severity_col]] <- s
    cl$appointment_window <- severity_windows[[s]]
    out_list[[k]] <- cl
    consumed[ids] <- consumed[ids] + cl$served         # served slots are now taken
  }
  dplyr::bind_rows(out_list)
}
