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

# v0.2.0 ADDS supply_cohort_basis, AND IT IS REQUIRED.
#
# Every number in this table is a supply number or is derived from one:
# demand_headcount is demand FTE divided by the supply side's FTE-per-provider
# ratio, and both gap columns are differences against supply. What the supply
# cohort actually IS therefore conditions the whole row.
#
# Today it is usually reconstructed rather than observed. The contract ships
# aggregate certification counts with no age, sex or state, so
# agents_from_certification_cohorts() derives ages from certification year for
# the fellowship cohorts and ASSUMES them for the 2013 backlog -- 655 of 1,306
# providers, 50.2% of the base cohort. `cohort_provenance()` has recorded that
# since it was written, and the orchestrator has carried it in
# `scenario_meta$cohort_provenance`. This table did not, so the moment a gap
# projection was exported, saved or handed on, the caveat stopped travelling
# with the numbers it qualifies.
#
# That is the failure mode this repository keeps rediscovering under other
# names: a qualification that lives beside the artifact rather than inside it.
# Required, not optional, so an export cannot quietly omit it.
URPS_GAP_PROJECTION_CONTRACT_VERSION <- "0.2.0"

# Cohort bases, from cohort_provenance()$source. Only "roster" is a measured
# workforce; the rest are reconstructions of one.
GAP_PROJECTION_MEASURED_BASIS <- "roster"
GAP_PROJECTION_COHORT_BASES <- c("roster", "certification_cohorts", "synthetic",
                                 "unknown", "undeclared")

REQUIRED_COLS <- c(
  "year",
  "scenario_id",
  "specialty",
  "geography_type",
  "geography_id",
  "supply_headcount",
  "supply_clinical_fte",
  "supply_cohort_basis",
  "demand_headcount",
  "demand_clinical_fte",
  "gap_fte",
  "gap_headcount"
)

OPTIONAL_COLS <- c(
  "supply_observed_share",
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

# Tolerance on the gap identity, in FTE. ONE constant because the same check is
# made in two places -- here and in validation_report() -- and two copies of a
# tolerance is how one of them starts accepting what the other rejects.
GAP_IDENTITY_TOLERANCE_FTE <- 0.01

#' Validate a gap projection data frame against the extended contract
#'
#' @param x Data frame produced by [as_urps_gap_projection()].
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) `x` when valid.
#' @family urps projection
#' @concept reporting
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

  # Provenance guard. A basis of "undeclared" means the caller never said what
  # the supply cohort was, which is the state this column exists to end -- the
  # numbers would export looking exactly like measured ones. An unrecognised
  # basis is refused for the same reason: it cannot be read.
  if ("supply_cohort_basis" %in% names(x)) {
    basis <- unique(as.character(x$supply_cohort_basis))
    unknown <- setdiff(basis, GAP_PROJECTION_COHORT_BASES)
    if (length(unknown)) {
      msg <- sprintf(
        "Gap projection has unrecognised supply_cohort_basis: %s. Expected one of: %s.",
        paste(unknown, collapse = ", "), paste(GAP_PROJECTION_COHORT_BASES, collapse = ", "))
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
    if ("undeclared" %in% basis) {
      msg <- paste(
        "Gap projection carries supply_cohort_basis = 'undeclared': the supply",
        "cohort's provenance was never stated, so these numbers export",
        "indistinguishable from ones built on a real roster. Pass",
        "cohort_basis = cohort_provenance(agents)$source."
      )
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    } else if (!identical(basis, GAP_PROJECTION_MEASURED_BASIS)) {
      # Not a failure -- a reconstructed cohort is a legitimate run, and the
      # whole point of the column is that it can say so out loud.
      .msg_info(sprintf(paste(
        "Gap projection supply is a RECONSTRUCTED cohort (basis: %s), not a",
        "measured roster. Report these as reconstructed cohort estimates."),
        paste(basis, collapse = "/")))
    }
  }

  # Completeness guard, and it has to come BEFORE the arithmetic guard, which
  # uses na.rm = TRUE and therefore cannot see this.
  #
  # The demand series is joined to the supply years at min_match_rate = 0.5, so
  # a projection whose demand covers exactly half the horizon joins without even
  # a warning (0.5 is not below 0.5) and exports NA demand and NA gap for the
  # other half. Those rows validated clean in STRICT mode: the arithmetic held
  # vacuously, no other check looked at NA, and the contract went out with a
  # missing gap for half its years.
  na_cols <- intersect(c("supply_headcount", "supply_clinical_fte",
                         "demand_headcount", "demand_clinical_fte",
                         "gap_fte", "gap_headcount"), names(x))
  n_na <- vapply(na_cols, function(cl) sum(!is.finite(x[[cl]])), integer(1))
  if (any(n_na > 0L)) {
    hit <- n_na[n_na > 0L]
    msg <- sprintf(paste(
      "Gap projection has non-finite values in %s (of %d row(s)). A missing gap",
      "is not a gap of zero and must not export as one; the usual cause is a",
      "demand series that does not cover every projection year."),
      paste(sprintf("%s (%d)", names(hit), hit), collapse = ", "), nrow(x))
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  # Interval guard. The ten optional bound pairs in OPTIONAL_COLS were checked
  # for nothing: each column is individually finite, and the RELATION between
  # them -- which is the whole content of an interval -- went unexamined. An
  # inverted lower_95/upper_95 validated clean in strict mode.
  bound_problems <- .interval_bound_problems(x)
  if (length(bound_problems)) {
    msg <- sprintf(paste("Gap projection has malformed interval bound(s): %s.",
                         "A lower bound above its upper bound is not a wide",
                         "interval, it is a swapped one, and every coverage and",
                         "width computed from it is wrong in a way no summary",
                         "statistic flags."),
                   paste(bound_problems, collapse = "; "))
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  # Arithmetic guard: gap = supply - demand on both sides.
  if (all(c("supply_clinical_fte", "demand_clinical_fte", "gap_fte") %in% names(x))) {
    residual <- abs(x$gap_fte - (x$supply_clinical_fte - x$demand_clinical_fte))
    if (any(residual > GAP_IDENTITY_TOLERANCE_FTE, na.rm = TRUE)) {
      msg <- "gap_fte does not equal supply_clinical_fte - demand_clinical_fte (tolerance 0.01 FTE)."
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }
  if (all(c("supply_headcount", "demand_headcount", "gap_headcount") %in% names(x))) {
    residual_hc <- abs(x$gap_headcount - (x$supply_headcount - x$demand_headcount))
    if (any(residual_hc > GAP_IDENTITY_TOLERANCE_FTE, na.rm = TRUE)) {
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
#' @param cohort_basis What the supply cohort is, from
#'   [cohort_provenance()]`$source`. Only `"roster"` is a measured workforce.
#'   Defaults to `"undeclared"`, which [validate_urps_gap_projection()] refuses
#'   in strict mode: silence here is what let reconstructed supply export
#'   looking measured.
#' @param observed_share Share of the cohort with an observed certification
#'   year, from [cohort_provenance()]`$observed_share`. Optional.
#' @param mode Reproducibility mode; passed to [validate_urps_gap_projection()].
#' @return Data frame conforming to the gap projection contract (REQUIRED_COLS +
#'   optional `lower_95`, `upper_95`, `gap_pct`, `scenario_label`).
#' @family urps projection
#' @concept reporting
#' @export
as_urps_gap_projection <- function(supply,
                                   fte_gap,
                                   specialty = "FPMRS",
                                   geography_type = "national",
                                   geography_id = "US",
                                   scenario_col = "scenario",
                                   headcount_col = "headcount_median",
                                   fte_col = "effective_fte_median",
                                   cohort_basis = "undeclared",
                                   observed_share = NA_real_,
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
    # Recycled down every row on purpose. A reader filtering to one year, or
    # binding several projections together, keeps the basis attached to the
    # numbers rather than having to carry a separate scalar alongside.
    supply_cohort_basis = as.character(cohort_basis %||% "undeclared"),
    demand_headcount    = d_hc,
    demand_clinical_fte = d_fte,
    gap_fte             = s_fte - d_fte,
    gap_headcount       = s_hc  - d_hc,
    stringsAsFactors    = FALSE
  )

  if (is.finite(observed_share)) out$supply_observed_share <- as.numeric(observed_share)

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
#' @family urps projection
#' @concept reporting
#' @examples
#' \dontrun{
#' # supply_by_scenario: named list of supply tibbles (one per scenario);
#' # required_fte: required FTE by year. Returns one validated gap projection
#' # per scenario, ready for the downstream contract.
#' gap_projections_all_scenarios(supply_by_scenario, required_fte)
#' }
#' @export
gap_projections_all_scenarios <- function(supply_by_scenario,
                                          required_fte,
                                          fte_gap_by_scenario = NULL,
                                          specialty = "FPMRS",
                                          geography_type = "national",
                                          geography_id = "US",
                                          cohort_basis = "undeclared",
                                          observed_share = NA_real_,
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
                           cohort_basis = cohort_basis,
                           observed_share = observed_share,
                           mode = mode)
  })
}

#' @export
print.urps_gap_projection <- function(x, ...) {
  final_year <- max(x$year, na.rm = TRUE)
  fin <- x[x$year == final_year, , drop = FALSE][1, , drop = FALSE]
  cat(sprintf("URPS gap projection (contract v%s)\n", URPS_GAP_PROJECTION_CONTRACT_VERSION))
  if ("supply_cohort_basis" %in% names(x)) {
    basis <- paste(sort(unique(as.character(x$supply_cohort_basis))), collapse = "/")
    cat(sprintf("  supply:    %s%s\n", basis,
                if (identical(basis, GAP_PROJECTION_MEASURED_BASIS)) " (measured roster)"
                else " -- RECONSTRUCTED COHORT, not a measured roster"))
    if ("supply_observed_share" %in% names(x)) {
      cat(sprintf("             %.1f%% of the base cohort has an observed certification year\n",
                  100 * x$supply_observed_share[1]))
    }
  }
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
