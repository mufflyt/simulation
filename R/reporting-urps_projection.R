# Extended supply-demand gap projection contract ----
#
# The mufflyaccess supply-side contract (validate_urps_projection()) validates
# 13 supply columns but carries no demand side, making gap calculation
# impossible from the projection object alone. IHS Markit / Dall-family
# practice: the shortage estimate IS the central deliverable. Keeping it in a
# separate fte_gap object disconnected from the projection forces downstream
# consumers to join two objects, and the join is silent when one is missing.
#
# This module defines a LOCAL extended contract that adds four demand/gap
# columns on top of the supply contract, validates the arithmetic internally,
# and is produced by as_urps_gap_projection().
#
# Column semantics (sign convention: negative = shortage, positive = surplus):
#   demand_headcount     Providers needed (demand_clinical_fte / fte_per_provider)
#   demand_clinical_fte  Required FTE from convert_workload_to_fte()
#   gap_fte              supply_clinical_fte - demand_clinical_fte
#   gap_headcount        supply_headcount    - demand_headcount
#
# The fte_per_provider ratio comes from the supply side itself
# (headcount_median / effective_fte_median), so headcount units stay
# internally consistent.

URPS_GAP_PROJECTION_CONTRACT_VERSION <- "0.1.0"

REQUIRED_COLS <- c(
  "year",
  "scenario_id",
  "specialty",
  "geography_type",
  "geography_id",
  "supply_headcount",
  "supply_clinical_fte",
  "demand_headcount",
  "demand_clinical_fte",
  "gap_fte",
  "gap_headcount"
)

OPTIONAL_COLS <- c(
  "lower_95", "upper_95",
  "demand_headcount_lo", "demand_headcount_hi",
  "demand_clinical_fte_lo", "demand_clinical_fte_hi",
  "gap_fte_lo", "gap_fte_hi",
  "gap_headcount_lo", "gap_headcount_hi",
  "gap_pct",
  "gap_pct_lo", "gap_pct_hi",
  "scenario_label",
  "certification_pathway"
)

#' Validate a gap projection data frame against the extended contract
#'
#' @param x Data frame produced by [as_urps_gap_projection()].
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) `x` when valid.
#' @export
validate_urps_gap_projection <- function(x,
                                         mode = resolve_reproducibility_mode()) {
  missing <- setdiff(REQUIRED_COLS, names(x))
  if (length(missing)) {
    msg <- sprintf(
      "Gap projection is missing required column(s): %s. Contract v%s requires: %s.",
      paste(missing, collapse = ", "),
      URPS_GAP_PROJECTION_CONTRACT_VERSION,
      paste(REQUIRED_COLS, collapse = ", ")
    )
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  # Arithmetic guard: gap = supply - demand on both sides.
  if (all(c("supply_clinical_fte", "demand_clinical_fte", "gap_fte") %in% names(x))) {
    residual <- abs(x$gap_fte - (x$supply_clinical_fte - x$demand_clinical_fte))
    if (any(residual > 0.01, na.rm = TRUE)) {
      msg <- "gap_fte does not equal supply_clinical_fte - demand_clinical_fte (tolerance 0.01 FTE)."
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }
  if (all(c("supply_headcount", "demand_headcount", "gap_headcount") %in% names(x))) {
    residual_hc <- abs(x$gap_headcount - (x$supply_headcount - x$demand_headcount))
    if (any(residual_hc > 0.01, na.rm = TRUE)) {
      msg <- "gap_headcount does not equal supply_headcount - demand_headcount (tolerance 0.01)."
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }

  invisible(x)
}

#' Build a validated gap projection from supply and demand outputs
#'
#' Joins the mufflyaccess supply contract columns with the demand FTE series
#' produced by [convert_workload_to_fte()] / [compute_fte_gap()], computing
#' the explicit shortage/surplus on both FTE and headcount axes.
#'
#' The headcount conversion for demand uses the ratio
#' `supply_headcount / supply_clinical_fte` from the supply side; this keeps
#' the two axes internally consistent without requiring a separate staffing
#' ratio assumption.
#'
#' @param supply Supply panel from `run_workforce_microsimulation()$supply`, or
#'   a projection data frame from [as_urps_projection()].
#' @param fte_gap Gap tibble from [compute_fte_gap()]; needs `year`,
#'   `required_fte`, and optionally `supplied_fte`.
#' @param specialty,geography_type,geography_id Contract identifiers.
#' @param scenario_col Column in `supply` holding the scenario id.
#' @param headcount_col Column in `supply` holding median headcount.
#' @param fte_col Column in `supply` holding median clinical FTE.
#' @param mode Reproducibility mode; passed to [validate_urps_gap_projection()].
#' @return Data frame conforming to the gap projection contract (REQUIRED_COLS +
#'   optional `lower_95`, `upper_95`, `gap_pct`, `scenario_label`).
#' @export
as_urps_gap_projection <- function(supply,
                                   fte_gap,
                                   specialty = "FPMRS",
                                   geography_type = "national",
                                   geography_id = "US",
                                   scenario_col = "scenario",
                                   headcount_col = "headcount_median",
                                   fte_col = "effective_fte_median",
                                   mode = resolve_reproducibility_mode()) {
  assertthat::assert_that(is.data.frame(supply), "year" %in% names(supply))
  assertthat::assert_that(is.data.frame(fte_gap),
                          all(c("year", "required_fte") %in% names(fte_gap)))

  # Normalise supply columns: accept either the raw microsim panel or an
  # already-shaped as_urps_projection() output.
  hc_col_actual <- if (headcount_col %in% names(supply)) headcount_col
                   else if ("supply_headcount" %in% names(supply)) "supply_headcount"
                   else NULL
  fte_col_actual <- if (fte_col %in% names(supply)) fte_col
                    else if ("supply_clinical_fte" %in% names(supply)) "supply_clinical_fte"
                    else NULL
  if (is.null(hc_col_actual) || is.null(fte_col_actual)) {
    stop(sprintf(
      "as_urps_gap_projection(): cannot find headcount ('%s') or FTE ('%s') column in supply.",
      headcount_col, fte_col), call. = FALSE)
  }

  scen <- if (scenario_col %in% names(supply)) supply[[scenario_col]]
          else if ("scenario_id" %in% names(supply)) supply$scenario_id
          else "baseline"

  s_hc  <- as.numeric(supply[[hc_col_actual]])
  s_fte <- as.numeric(supply[[fte_col_actual]])

  # FTE-to-headcount ratio from the supply side. Guard against degenerate zeros.
  fte_per_provider <- ssot_safe_divide(s_fte, s_hc)

  # Demand: join required_fte by year.
  demand <- dplyr::select(fte_gap, "year", demand_clinical_fte = "required_fte")
  joined <- safe_left_join(
    data.frame(year = as.integer(supply$year), row_idx = seq_len(nrow(supply))),
    demand,
    by = "year",
    min_match_rate = 0.5
  )
  d_fte <- joined$demand_clinical_fte[order(joined$row_idx)]

  # Demand headcount uses the supply-side FTE-per-provider ratio.
  d_hc <- ssot_safe_divide(d_fte, fte_per_provider)

  out <- data.frame(
    year                = as.integer(supply$year),
    scenario_id         = as.character(scen),
    specialty           = specialty,
    geography_type      = geography_type,
    geography_id        = geography_id,
    supply_headcount    = s_hc,
    supply_clinical_fte = s_fte,
    demand_headcount    = d_hc,
    demand_clinical_fte = d_fte,
    gap_fte             = s_fte - d_fte,
    gap_headcount       = s_hc  - d_hc,
    stringsAsFactors    = FALSE
  )

  # Carry optional CI and label columns when present.
  if ("headcount_lo" %in% names(supply)) out$lower_95 <- as.numeric(supply$headcount_lo)
  if ("headcount_hi" %in% names(supply)) out$upper_95 <- as.numeric(supply$headcount_hi)
  if ("scenario_label" %in% names(supply)) out$scenario_label <- supply$scenario_label
  if ("gap_pct" %in% names(fte_gap)) {
    pct_joined <- safe_left_join(
      data.frame(year = as.integer(supply$year), row_idx = seq_len(nrow(supply))),
      dplyr::select(fte_gap, "year", "gap_pct"),
      by = "year", min_match_rate = 0.5
    )
    out$gap_pct <- pct_joined$gap_pct[order(pct_joined$row_idx)]
  }

  validate_urps_gap_projection(out, mode = mode)
  out
}

#' Build gap projections for all supply scenarios
#'
#' Thin wrapper over [as_urps_gap_projection()] that iterates across the
#' scenario column of the full supply panel, joining each scenario to the
#' same `required_fte` series (demand is scenario-independent in the status-
#' quo case; pass scenario-specific demand in `fte_gap_by_scenario` if needed).
#'
#' @param supply_by_scenario Full supply panel from
#'   `run_workforce_microsimulation()$supply`.
#' @param required_fte Required-FTE tibble from `convert_workload_to_fte()`.
#' @param fte_gap_by_scenario Named list of gap tibbles keyed by scenario id.
#'   When NULL, `compute_fte_gap()` is called once per scenario against
#'   `required_fte`.
#' @inheritParams as_urps_gap_projection
#' @return Long data frame of gap projections for all scenarios.
#' @export
gap_projections_all_scenarios <- function(supply_by_scenario,
                                          required_fte,
                                          fte_gap_by_scenario = NULL,
                                          specialty = "FPMRS",
                                          geography_type = "national",
                                          geography_id = "US",
                                          mode = resolve_reproducibility_mode()) {
  assertthat::assert_that(is.data.frame(supply_by_scenario),
                          "scenario" %in% names(supply_by_scenario))
  scenarios <- unique(supply_by_scenario$scenario)

  purrr::map_dfr(scenarios, function(scen) {
    s <- dplyr::filter(supply_by_scenario, .data$scenario == scen)
    gap_tbl <- if (!is.null(fte_gap_by_scenario) && scen %in% names(fte_gap_by_scenario)) {
      fte_gap_by_scenario[[scen]]
    } else {
      compute_fte_gap(s, required_fte, supply_col = "effective_fte_median")
    }
    as_urps_gap_projection(s, gap_tbl,
                           specialty = specialty,
                           geography_type = geography_type,
                           geography_id = geography_id,
                           mode = mode)
  })
}

#' @export
print.urps_gap_projection <- function(x, ...) {
  final_year <- max(x$year, na.rm = TRUE)
  fin <- x[x$year == final_year, , drop = FALSE][1, , drop = FALSE]
  cat(sprintf("URPS gap projection (contract v%s)\n", URPS_GAP_PROJECTION_CONTRACT_VERSION))
  cat(sprintf("  scenarios: %s\n", paste(sort(unique(x$scenario_id)), collapse = ", ")))
  cat(sprintf("  years:     %d - %d\n", min(x$year, na.rm = TRUE), final_year))
  cat(sprintf("  %d supply: %.0f hc / %.0f FTE\n",
              final_year, fin$supply_headcount, fin$supply_clinical_fte))
  cat(sprintf("  %d demand: %.0f hc / %.0f FTE\n",
              final_year, fin$demand_headcount, fin$demand_clinical_fte))
  g_sign <- if (!is.na(fin$gap_fte) && fin$gap_fte < 0) "shortage" else "surplus"
  cat(sprintf("  %d gap:    %.0f hc / %.0f FTE (%s)\n",
              final_year, fin$gap_headcount, fin$gap_fte, g_sign))
  invisible(x)
}
