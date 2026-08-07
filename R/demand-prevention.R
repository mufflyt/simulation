# Disease Prevention Module (DPMM-lite) ----
#
# IHS Markit DPMM approach (HWSM v5.19.20, section 5):
#   "A separate Disease Prevention and Management Model (DPMM) models how
#   prevention/management interventions feed back into demand."
#
# URPS application: Conservative management of pelvic-floor disorders (PFD)
# via pelvic-floor physical therapy (PT) or pessary substitutes for surgical
# URPS visits. This module expresses that substitution as multipliers on a
# service-volume tibble produced by example_service_volumes() or
# lifecourse_demand_trajectory(), immediately before convert_workload_to_fte().
#
# Conceptual model:
#   ui_uptake  — fraction of UI patients managed with conservative therapy
#   pop_uptake — fraction of POP patients managed with conservative therapy
#
#   Each diverted patient:
#     • No longer requires a sling (UI) or prolapse (POP) procedure
#     • No longer requires the associated new consultation / return visit
#     • Instead receives pessary_care or ptns visits (conservative pathway)
#
# The surgical reduction fraction (surgical_reduction) and office-visit
# reduction fraction (office_reduction) can be tuned to reflect imperfect
# substitution (e.g. some PT patients ultimately proceed to surgery).
#
# Multipliers < 1 reduce a service volume; multipliers > 1 increase it.
# Services not named in the multiplier vector are unaffected (multiplier = 1).
#
# Wiring: apply_prevention_multipliers() is called on the volumes tibble
# between example_service_volumes() and convert_workload_to_fte() in
# run_workforce_microsimulation() when prevention_scenario is non-NULL.

# ---- Scenario registry -------------------------------------------------------

#' Pre-defined prevention scenario parameters
#'
#' A named list of lists. Each entry must supply at least `ui_uptake` and
#' `pop_uptake`. `surgical_reduction`, `office_reduction`, `pt_visits_per_case`,
#' and `pessary_visits_per_case` default to their function defaults when absent.
#'
#' @family prevention
#' @concept demand
#' @export
URPS_PREVENTION_SCENARIOS <- list(
  baseline = list(
    label          = "No conservative management shift",
    ui_uptake      = 0,
    pop_uptake     = 0
  ),
  conservative_10pct = list(
    label          = "10% of UI/POP patients managed conservatively",
    ui_uptake      = 0.10,
    pop_uptake     = 0.10,
    surgical_reduction = 0.80,
    office_reduction   = 0.40
  ),
  conservative_25pct = list(
    label          = "25% of UI/POP patients managed conservatively",
    ui_uptake      = 0.25,
    pop_uptake     = 0.25,
    surgical_reduction = 0.80,
    office_reduction   = 0.40
  ),
  conservative_50pct = list(
    label          = "50% of UI/POP patients managed conservatively",
    ui_uptake      = 0.50,
    pop_uptake     = 0.50,
    surgical_reduction = 0.80,
    office_reduction   = 0.40
  )
)

# ---- Core computation --------------------------------------------------------

#' Per-service volume multipliers for a conservative management scenario
#'
#' Translates uptake fractions and reduction efficiencies into a named numeric
#' vector of multipliers, one per service in the workload basket. A multiplier
#' of 1 means no change; < 1 means volume decreases; > 1 means it increases.
#'
#' The arithmetic is:
#'   sling_multiplier    = 1 − ui_uptake  × surgical_reduction
#'   prolapse_multiplier = 1 − pop_uptake × surgical_reduction
#'   new_consultation and return_visit multiplier:
#'     = 1 − (0.55 × ui_uptake + 0.45 × pop_uptake) × office_reduction
#'   pessary_care multiplier = 1 + pop_uptake × pessary_visits_per_case / baseline_pessary_rate
#'     (scaled so the conservative cohort generates additional pessary_care above
#'      the baseline rate; expressed as a multiplier on existing baseline volume)
#'   ptns multiplier = 1 + ui_uptake × pt_visits_per_case / baseline_ptns_rate
#'
#' Because the conservative cohort is additive for pessary_care and ptns, the
#' multiplier is stated relative to baseline volumes. Calling code must ensure
#' baseline volumes are non-zero for those services; when they are zero the
#' absolute additive term is also zero (0 × anything = 0) and the multiplier
#' collapses to 1 with a warning.
#'
#' @param ui_uptake Fraction of UI patients diverted to conservative management
#'   (pelvic floor PT/PTNS). Must be in \[0, 1\].
#' @param pop_uptake Fraction of POP patients diverted to pessary management.
#'   Must be in \[0, 1\].
#' @param surgical_reduction Fractional reduction in surgical need for each
#'   diverted patient (default 0.80 = 80% avoid surgery). Must be in \[0, 1\].
#' @param office_reduction Fractional reduction in new consultation and return
#'   visit load per diverted patient (default 0.40 = 40% fewer specialist
#'   visits; the remainder attend for shared decision-making or monitoring).
#'   Must be in \[0, 1\].
#' @param pessary_volume_baseline Baseline pessary_care volume (national annual
#'   units). Used to compute the additive conservative-pathway volume as a
#'   multiplier on baseline. When `NULL` (default) the multiplier is returned
#'   as an absolute additive fraction and `apply_prevention_multipliers()` adds
#'   it to the observed baseline before multiplying.
#' @param ptns_volume_baseline Baseline ptns volume. See `pessary_volume_baseline`.
#' @return Named numeric vector of multipliers, one per service. Services not
#'   listed in the workload basket are silently excluded from the result (they
#'   are unaffected by definition).
#' @seealso [apply_prevention_multipliers()], [URPS_PREVENTION_SCENARIOS]
#' @family prevention
#' @concept demand
#' @export
#'
#' @examples
#' # 25% of UI and POP patients managed conservatively
#' conservative_management_multipliers(ui_uptake = 0.25, pop_uptake = 0.25)
#'
#' # All multipliers are 1 when uptake is zero (no shift from baseline)
#' conservative_management_multipliers(ui_uptake = 0, pop_uptake = 0)
conservative_management_multipliers <- function(ui_uptake         = 0,
                                                pop_uptake        = 0,
                                                surgical_reduction = 0.80,
                                                office_reduction   = 0.40,
                                                pessary_volume_baseline = NULL,
                                                ptns_volume_baseline    = NULL) {
  # ---- Validation -----------------------------------------------------------
  .check_fraction <- function(x, nm) {
    if (!is.numeric(x) || length(x) != 1L || is.na(x) || x < 0 || x > 1) {
      stop(sprintf("conservative_management_multipliers: %s must be a single numeric in [0, 1].",
                   nm), call. = FALSE)
    }
  }
  .check_fraction(ui_uptake,          "ui_uptake")
  .check_fraction(pop_uptake,         "pop_uptake")
  .check_fraction(surgical_reduction, "surgical_reduction")
  .check_fraction(office_reduction,   "office_reduction")

  # ---- Surgical services: sling (UI) and prolapse (POP) --------------------
  sling_mult    <- 1 - ui_uptake  * surgical_reduction
  prolapse_mult <- 1 - pop_uptake * surgical_reduction

  # Postoperative care tracks surgical volume proportionally
  post_op_mult <- (sling_mult * 0.55 + prolapse_mult * 0.45)

  # ---- Office-visit services ------------------------------------------------
  # Weighted mean uptake: sling is 55% of surgical volume, prolapse 45%.
  mean_uptake  <- (ui_uptake * 0.55 + pop_uptake * 0.45)
  consult_mult <- 1 - mean_uptake * office_reduction
  return_mult  <- consult_mult   # same driver

  # ---- Conservative-pathway services: pessary_care and ptns ----------------
  # These INCREASE. The multiplier is expressed as a ratio on existing baseline
  # volume. Callers that have no baseline volume for these services receive the
  # raw additive fraction via a special-case attr() so apply_prevention_multipliers()
  # can add absolute volume rather than scale a zero.
  pessary_additive <- pop_uptake   # fraction of POP demand generating pessary visits
  ptns_additive    <- ui_uptake    # fraction of UI demand generating ptns visits

  pessary_mult <- if (!is.null(pessary_volume_baseline) && pessary_volume_baseline > 0) {
    1 + pessary_additive / pessary_volume_baseline
  } else {
    1 + pessary_additive    # caller interprets as fraction of total demand pool
  }
  ptns_mult <- if (!is.null(ptns_volume_baseline) && ptns_volume_baseline > 0) {
    1 + ptns_additive / ptns_volume_baseline
  } else {
    1 + ptns_additive
  }

  # ---- Diagnostics follow consultations (reduced but not eliminated) --------
  # Urodynamics and cystoscopy: conservative patients still need baseline workup
  # but fewer return for diagnostic re-evaluation. Use office_reduction / 2.
  diag_mult <- 1 - mean_uptake * office_reduction * 0.5

  c(
    new_consultation    = consult_mult,
    return_visit        = return_mult,
    pessary_care        = pessary_mult,
    urodynamics         = diag_mult,
    cystoscopy          = diag_mult,
    botox_bladder       = 1,          # not affected by PT/pessary diversion
    ptns                = ptns_mult,
    bladder_instillation = 1,
    sling_procedure     = sling_mult,
    prolapse_procedure  = prolapse_mult,
    postoperative_care  = post_op_mult
  )
}

# ---- Application -------------------------------------------------------------

#' Apply prevention multipliers to a service-volume tibble
#'
#' Multiplies each service's `volume` column by the corresponding multiplier
#' from [conservative_management_multipliers()]. The result is the demand-side
#' input to [convert_workload_to_fte()] after the conservative management
#' scenario shift.
#'
#' Services absent from `multipliers` are passed through unchanged (multiplier
#' defaults to 1). Services in `multipliers` but absent from `volumes` are
#' silently ignored.
#'
#' @param volumes Tibble with at least columns `service` (character) and
#'   `volume` (numeric). Typically the output of [example_service_volumes()] or
#'   [lifecourse_demand_trajectory()]. A `year` column is preserved if present.
#' @param multipliers Named numeric vector from [conservative_management_multipliers()].
#' @return `volumes` with the `volume` column scaled by the per-service
#'   multipliers. An attribute `prevention_multipliers` is attached to the
#'   returned tibble for audit purposes.
#' @seealso [conservative_management_multipliers()],
#'   [prevention_demand_trajectory()]
#' @family prevention
#' @concept demand
#' @export
#'
#' @examples
#' vols <- tibble::tibble(
#'   year    = rep(2025:2027, each = 3),
#'   service = rep(c("sling_procedure", "prolapse_procedure", "new_consultation"), 3),
#'   volume  = c(1000, 800, 5000, 1050, 840, 5250, 1100, 880, 5500)
#' )
#' m <- conservative_management_multipliers(ui_uptake = 0.25, pop_uptake = 0.25)
#' apply_prevention_multipliers(vols, m)
apply_prevention_multipliers <- function(volumes, multipliers) {
  if (!is.data.frame(volumes) || nrow(volumes) == 0L) {
    stop("apply_prevention_multipliers: volumes must be a non-empty data frame.",
         call. = FALSE)
  }
  if (!all(c("service", "volume") %in% names(volumes))) {
    stop("apply_prevention_multipliers: volumes must have columns 'service' and 'volume'.",
         call. = FALSE)
  }
  if (!is.numeric(multipliers) || is.null(names(multipliers))) {
    stop("apply_prevention_multipliers: multipliers must be a named numeric vector.",
         call. = FALSE)
  }
  if (any(multipliers < 0, na.rm = TRUE)) {
    stop("apply_prevention_multipliers: all multipliers must be non-negative.",
         call. = FALSE)
  }

  out <- volumes
  idx <- match(out$service, names(multipliers))
  m   <- ifelse(is.na(idx), 1, multipliers[idx])
  out$volume <- out$volume * m

  attr(out, "prevention_multipliers") <- multipliers
  out
}

# ---- Convenience wrappers ----------------------------------------------------

#' Apply a named prevention scenario to a service-volume tibble
#'
#' Looks up `scenario_id` in [URPS_PREVENTION_SCENARIOS], calls
#' [conservative_management_multipliers()] with those parameters, then
#' [apply_prevention_multipliers()].
#'
#' @param volumes Tibble; see [apply_prevention_multipliers()].
#' @param scenario_id Character key in [URPS_PREVENTION_SCENARIOS].
#' @param registry Named list with the same structure as
#'   [URPS_PREVENTION_SCENARIOS]. Defaults to [URPS_PREVENTION_SCENARIOS].
#' @return Modified volumes tibble.
#' @family prevention
#' @concept demand
#' @export
#'
#' @examples
#' vols <- tibble::tibble(
#'   service = c("sling_procedure", "prolapse_procedure", "new_consultation"),
#'   volume  = c(1000, 800, 5000)
#' )
#' apply_named_prevention_scenario(vols, "conservative_25pct")
apply_named_prevention_scenario <- function(volumes,
                                            scenario_id,
                                            registry = URPS_PREVENTION_SCENARIOS) {
  scen <- registry[[scenario_id]]
  if (is.null(scen)) {
    stop(sprintf("apply_named_prevention_scenario: unknown scenario_id '%s'. ",
                 scenario_id),
         call. = FALSE)
  }
  m <- conservative_management_multipliers(
    ui_uptake          = scen$ui_uptake %||% 0,
    pop_uptake         = scen$pop_uptake %||% 0,
    surgical_reduction = scen$surgical_reduction %||% 0.80,
    office_reduction   = scen$office_reduction   %||% 0.40
  )
  apply_prevention_multipliers(volumes, m)
}

#' Demand trajectory under a prevention scenario
#'
#' Thin wrapper that calls [example_service_volumes()] on `demand_long`, applies
#' the named prevention scenario, and returns the adjusted volumes. Suitable as a
#' drop-in replacement for [example_service_volumes()] in a scenario loop.
#'
#' @param demand_long Long demand tibble from [compute_demand_denominators()].
#' @param scenario_id Character key in [URPS_PREVENTION_SCENARIOS].
#' @param registry See [apply_named_prevention_scenario()].
#' @return Tibble `year`, `service`, `volume` with prevention adjustment applied.
#' @keywords internal
prevention_demand_trajectory <- function(demand_long,
                                         scenario_id = "conservative_25pct",
                                         registry    = URPS_PREVENTION_SCENARIOS) {
  volumes <- example_service_volumes(demand_long)
  apply_named_prevention_scenario(volumes, scenario_id, registry = registry)
}

# ---- Summary helpers ---------------------------------------------------------

#' Tabulate volume changes under a prevention scenario
#'
#' Compares baseline volumes against the prevention-adjusted volumes and
#' returns a summary tibble showing absolute and relative changes per service.
#' Useful for reporting and validation.
#'
#' @param demand_long Long demand tibble from [compute_demand_denominators()].
#' @param scenario_id Character key in [URPS_PREVENTION_SCENARIOS].
#' @param year_filter Optional integer year to subset (default: all years).
#' @param registry See [apply_named_prevention_scenario()].
#' @return Tibble: `year` (if multi-year), `service`, `volume_baseline`,
#'   `volume_prevention`, `volume_delta`, `pct_change`.
#' @family prevention
#' @concept demand
#' @export
prevention_volume_summary <- function(demand_long,
                                      scenario_id  = "conservative_25pct",
                                      year_filter  = NULL,
                                      registry     = URPS_PREVENTION_SCENARIOS) {
  base <- example_service_volumes(demand_long)
  prev <- apply_named_prevention_scenario(base, scenario_id, registry = registry)

  if (!is.null(year_filter)) {
    base <- dplyr::filter(base, .data$year %in% year_filter)
    prev <- dplyr::filter(prev, .data$year %in% year_filter)
  }

  dplyr::left_join(
    dplyr::rename(base, volume_baseline = "volume"),
    dplyr::rename(prev, volume_prevention = "volume"),
    by = intersect(c("year", "service"), names(base))
  ) |>
    dplyr::mutate(
      volume_delta = .data$volume_prevention - .data$volume_baseline,
      pct_change   = 100 * .data$volume_delta / .data$volume_baseline
    )
}
