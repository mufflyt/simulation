# Versioned Scenario Registry ----
#
# The previous scenario menu mixed entrant counts, a conversion haircut, and a
# scalar hazard multiplier. Every Dall-family study instead expresses retirement
# scenarios as a SHIFT IN THE AGE AXIS -- "retire one or two years earlier or
# later" -- because that is interpretable and preserves the shape of the
# retirement curve. HWSM v5.19.20 (HWSM "Scenarios"): "Scenarios simulating a one-
# or two-year shift in retirement patterns can make it easier to understand the
# effect this may have on the overall supply of a health profession."
#
# The supply-side scenario set across HWSM, Dall 2013, Dall 2021 and Zarek 2025
# is remarkably uniform: status quo, +/-10% new graduates, retire +/-2 years, and
# a shift in hours-worked patterns. HWSM adds an "hours worked cohort effect"
# scenario: newly entering cohorts may systematically work different hours than
# today's providers of the same age.
#
# Demand scenarios come from HDMM: status quo, increased insurance coverage,
# reducing barriers to accessing care ("health care utilization equity"),
# increased managed care, greater APP use, population health goals, and an
# evolving-care-delivery scenario that combines several "with attention paid to
# not double counting the effects that overlapping scenarios might have".

# The local registry version is deliberately NOT "1.0.0": mufflyaccess already
# ships a URPS scenario registry at 1.0.0 with a `retirement_shift_years` field
# of identical name and semantics, and two different registries sharing a
# version string is how silent divergence starts. The SSOT registry is now the
# default (see ssot_supply_scenarios()); what remains here is a local fallback
# and the extensions the contract does not carry.
SCENARIO_REGISTRY_VERSION <- "2.0.0-local-fallback"

# Policy-lever scenarios modeled as parameter shifts on the supply axes the
# engine already consumes (clinical hours via `hours_multiplier`, retirement age
# via `retirement_shift_years`). The mufflyaccess SSOT projection contract does
# not carry them, so supply_scenario_registry() APPENDS them onto the SSOT
# registry (never overriding a contract id) so they are first-class in both SSOT
# and local-fallback modes. Each is ASSUMED/ILLUSTRATIVE until its magnitude is
# calibrated; the source string says so.
SUPPLY_SCENARIO_LOCAL_EXTENSIONS <- c("telemedicine", "ai_documentation", "burnout_reduction")

# Access components a demand scenario can relax. Named so the base-year gap can
# declare which of them it already contains (see assert_access_not_double_counted).
ACCESS_COMPONENTS <- c("uninsured", "nonmetro", "racial_equity", "income")

# ---- Supply scenarios -----------------------------------------------------

#' Supply scenario registry
#'
#' Returns the mufflyaccess registry when available -- it is the SSOT and
#' downstream consumers validate scenario ids against it. The local definitions
#' are a fallback for when the contract is absent, plus the one extension it
#' does not carry (graduate-to-practice conversion).
#'
#' @param baseline_entrants Baseline annual entrants to practice.
#' @param prefer_ssot Use `mufflyaccess::urps_scenarios()` when available.
#' @return Named list of supply scenario definitions.
#' @export
supply_scenario_registry <- function(baseline_entrants = 55, prefer_ssot = TRUE) {
  if (isTRUE(prefer_ssot) && has_mufflyaccess()) {
    base <- ssot_supply_scenarios(baseline_entrants)
    # Keep the local policy-lever scenarios first-class in SSOT mode too, without
    # ever shadowing a contract id.
    ext <- local_supply_scenario_registry(baseline_entrants)[SUPPLY_SCENARIO_LOCAL_EXTENSIONS]
    return(c(base, ext[setdiff(names(ext), names(base))]))
  }
  .msg_warn("Using the LOCAL scenario fallback; ids will not validate against ",
            "the mufflyaccess projection contract.")
  local_supply_scenario_registry(baseline_entrants)
}

#' Local fallback scenario definitions
#' @param baseline_entrants Baseline annual entrants.
#' @return Named list of scenario definitions.
#' @export
local_supply_scenario_registry <- function(baseline_entrants = 55) {
  list(
    status_quo = list(
      label = "Status quo",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM scenarios: current data on entrants, hours, retirement"
    ),
    graduates_plus_10 = list(
      label = "10% more graduates",
      entrants = round(baseline_entrants * 1.10),
      retirement_shift_years = 0,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM / Dall 2013 / Dall 2021 / Zarek 2025"
    ),
    graduates_minus_10 = list(
      label = "10% fewer graduates",
      entrants = round(baseline_entrants * 0.90),
      retirement_shift_years = 0,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM / Dall 2013 / Dall 2021 / Zarek 2025"
    ),
    # Retirement-shift scenarios use the mufflyaccess SSOT contract ids
    # (`retire_2yr_later` / `retire_2yr_earlier`) so urps_p_active() and
    # p_active_by_age() resolve them identically whether the SSOT registry is
    # installed or this local fallback is in use. The reference scenario stays
    # `status_quo` (the fallback's own reference name; see validate_scenario_registry).
    retire_2yr_later = list(
      label = "Retire 2 years later",
      entrants = baseline_entrants,
      retirement_shift_years = 2,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM: one- or two-year shift in retirement patterns"
    ),
    retire_2yr_earlier = list(
      label = "Retire 2 years earlier",
      entrants = baseline_entrants,
      retirement_shift_years = -2,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM: burnout-driven earlier retirement"
    ),
    reduced_clinical_hours = list(
      label = "Reduced clinical hours",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 0.95,
      conversion = 1.00,
      source = "HWSM hours-worked cohort effect: entering cohorts work fewer hours"
    ),
    conversion_70pct = list(
      label = "70% graduate-to-practice conversion",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 1.00,
      conversion = 0.70,
      source = "cliff WORKFORCE_CONVERSION_FLOOR"
    ),
    # ---- Policy-lever scenarios (SUPPLY_SCENARIO_LOCAL_EXTENSIONS) -----------
    # Each changes only a handful of parameters on the existing supply axes.
    # Magnitudes are ASSUMED/ILLUSTRATIVE placeholders, not measured rates.
    telemedicine = list(
      label = "Telemedicine expansion",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 1.05,
      conversion = 1.00,
      source = paste(
        "ASSUMED/ILLUSTRATIVE (magnitude not yet calibrated): telehealth raises",
        "effective clinical throughput ~5% (fewer no-shows, no room turnover).",
        "Its GEOGRAPHIC-REACH effect is modeled separately as accessible_fraction",
        "in the access-clearing layer (clear_access()), not as a supply multiplier here.")
    ),
    ai_documentation = list(
      label = "AI documentation support",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 1.08,
      conversion = 1.00,
      source = paste(
        "ASSUMED/ILLUSTRATIVE (magnitude not yet calibrated): ambient AI",
        "documentation frees ~8% of clinical time from clerical burden, modeled",
        "as an effective clinical-hours gain.")
    ),
    # Burnout reduction acts on the AGE-FLAT early-exit (career-change) hazard,
    # not the age-graded retirement curve -- so it uses `career_change_multiplier`
    # (< 1 = fewer early exits) rather than a `retirement_shift_years` / hours
    # retention lever. Burnout attrition concentrates in EARLY-career (<50)
    # providers, which a retirement-curve shift (only touches 50+) cannot
    # represent; the career-change hazard is the correct locus. There is no
    # in-domain URPS burnout->attrition estimate, so the 0.75 level is
    # ILLUSTRATIVE: a scenario range, not a calibrated effect. Functional
    # counterpart of the neutral `burnout_hazard_multiplier` row in
    # career_transition_registry() (R/supply-provider_state_machine.R).
    burnout_reduction = list(
      label = "Burnout reduction (fewer early-career exits)",
      entrants = baseline_entrants,
      retirement_shift_years = 0,
      hours_multiplier = 1.00,
      conversion = 1.00,
      career_change_multiplier = 0.75,
      source = paste("ASSUMED/ILLUSTRATIVE (magnitude not yet calibrated): 25% fewer",
                     "age-flat early-career exits (burnout attrition, which concentrates",
                     "in <50 providers a retirement-curve shift cannot capture). No",
                     "in-domain URPS burnout->attrition estimate exists; a range, not a",
                     "calibrated point (HWSM burnout narrative; Zarek 2025",
                     "occupational-separation mechanism).")
    )
  )
}

# ---- Demand scenarios -----------------------------------------------------

#' Default demand scenario registry
#'
#' `access_components` names which barriers the scenario relaxes, so the
#' double-count guard can compare them against the base-year gap.
#'
#' @return Named list of demand scenario definitions.
#' @export
demand_scenario_registry <- function() {
  list(
    status_quo = list(
      label = "Status quo",
      access_components = character(0),
      utilization_multiplier = 1.00,
      care_seeking_multiplier = 1.00,
      source = "HDMM status quo: demographics change, use patterns held constant"
    ),
    insurance_equity = list(
      label = "Insurance equity",
      access_components = "uninsured",
      # project_urps_demand() access_scenario = "insurance_equity" raises
      # uninsured cells from 0.58× to 1.0× care-seeking (Richter 2007 AJOG).
      # utilization_multiplier is documented for reporting; the actual lever is
      # the per-cell barrier multiplier in project_urps_demand().
      utilization_multiplier = 1.0 / CARE_SEEKING_BY_INSURANCE[["Uninsured"]],
      care_seeking_multiplier = 1.00,
      access_scenario = "insurance_equity",
      source = "HDMM increased medical insurance coverage; Dall 2021 Improved Access 1; Richter 2007 AJOG insurance-access gap"
    ),
    income_equity = list(
      label = "Income equity",
      access_components = "income",
      # Raises LT25k cells from 0.72× to 1.0× care-seeking (MEPS 2020).
      utilization_multiplier = 1.0 / CARE_SEEKING_BY_INCOME[["LT25k"]],
      care_seeking_multiplier = 1.00,
      access_scenario = "income_equity",
      source = "MEPS 2020 specialty visit rate by income quartile"
    ),
    reduced_barriers = list(
      label = "Reduced barriers to care",
      access_components = c("uninsured", "nonmetro", "racial_equity", "income"),
      # full_equity removes both insurance and income barriers simultaneously.
      utilization_multiplier = NA_real_,
      care_seeking_multiplier = 1.00,
      access_scenario = "full_equity",
      source = "HDMM utilization equity; Zarek 2025 Reduced Barriers (+12% by 2037)"
    ),
    care_seeking_improved = list(
      label = "Improved care-seeking for incontinence",
      access_components = character(0),
      utilization_multiplier = 1.00,
      care_seeking_multiplier = 1.30,
      source = "Only ~25-45% of women with UI seek care; the urogynaecology-specific lever"
    ),
    app_substitution = list(
      label = "Greater APP service substitution",
      access_components = character(0),
      utilization_multiplier = 1.00,
      care_seeking_multiplier = 1.00,
      delegation_shift = 0.10,
      source = "HDMM increased use of APRNs and PAs; modelled by service, not headcount"
    )
  )
}

# ---- Contract validation --------------------------------------------------

SUPPLY_SCENARIO_REQUIRED <- c("label", "entrants", "retirement_shift_years", "source")
DEMAND_SCENARIO_REQUIRED <- c("label", "access_components", "source")

#' Validate a scenario registry against its contract
#'
#' @param registry Named list of scenario definitions.
#' @param kind "supply" or "demand".
#' @return (Invisibly) the registry.
#' @export
validate_scenario_registry <- function(registry, kind = c("supply", "demand")) {
  kind <- match.arg(kind)
  required <- if (kind == "supply") SUPPLY_SCENARIO_REQUIRED else DEMAND_SCENARIO_REQUIRED

  if (!is.list(registry) || is.null(names(registry)) || any(!nzchar(names(registry)))) {
    stop("validate_scenario_registry: registry must be a named list", call. = FALSE)
  }
  # The reference scenario is "baseline" in the mufflyaccess registry and
  # "status_quo" in the local fallback; either satisfies the contract.
  if (!any(c("baseline", "status_quo") %in% names(registry))) {
    stop("validate_scenario_registry: every registry needs a reference scenario ",
         "named 'baseline' (mufflyaccess) or 'status_quo' (local fallback)",
         call. = FALSE)
  }
  for (nm in names(registry)) {
    missing <- setdiff(required, names(registry[[nm]]))
    if (length(missing) > 0) {
      stop(sprintf("validate_scenario_registry: scenario '%s' missing field(s): %s",
                   nm, paste(missing, collapse = ", ")), call. = FALSE)
    }
  }
  if (kind == "supply") {
    for (nm in names(registry)) {
      s <- registry[[nm]]
      if (!is.numeric(s$retirement_shift_years) || abs(s$retirement_shift_years) > 10) {
        stop(sprintf("validate_scenario_registry: '%s' retirement_shift_years must be a plausible year shift",
                     nm), call. = FALSE)
      }
      if (!is.null(s$hazard_mult)) {
        stop(sprintf(paste("validate_scenario_registry: '%s' uses a scalar hazard multiplier.",
                           "Express retirement scenarios as a shift in the age axis",
                           "(retirement_shift_years); a multiplier distorts the shape of the",
                           "retirement curve and is not used by any published Dall-family study."),
                     nm), call. = FALSE)
      }
    }
  } else {
    for (nm in names(registry)) {
      bad <- setdiff(registry[[nm]]$access_components, ACCESS_COMPONENTS)
      if (length(bad) > 0) {
        stop(sprintf("validate_scenario_registry: '%s' names unknown access component(s): %s",
                     nm, paste(bad, collapse = ", ")), call. = FALSE)
      }
    }
  }
  invisible(registry)
}

#' Resolve the retirement schedule implied by a supply scenario
#'
#' When `use_weibull = TRUE` (the default), the schedule is built from the
#' Weibull survival curves in [urps_weibull_exit_probs()], with
#' `retirement_shift_years` used directly as the Weibull `scale_shift` (±2 yr
#' = scale ± 2).  When `use_weibull = FALSE`, the legacy `shift_retirement_schedule()`
#' deterministic age-axis shift is applied instead.
#'
#' @param scenario A supply scenario definition (must have `retirement_shift_years`).
#' @param schedule Base retirement hazard schedule (used only when
#'   `use_weibull = FALSE`).
#' @param use_weibull Logical; use Weibull survival curves (default TRUE).
#' @return A hazard schedule data frame with columns `age`, `sex`,
#'   `prob_exit`, `se_prob_exit`, `calibration_tier`.
#' @keywords internal
scenario_retirement_schedule <- function(scenario,
                                          schedule   = RETIREMENT_HAZARD_BY_AGE,
                                          use_weibull = TRUE) {
  shift <- scenario$retirement_shift_years %||% 0
  if (use_weibull) {
    h <- build_urps_exit_hazard(cliff_duckdb_path = NULL,
                                scale_shift = shift,
                                verbose = FALSE)
    ep <- h$exit_probs
    if (is.data.frame(ep)) {
      # Average across sex strata, key by age string (departure_hazard contract)
      ep <- tapply(ep$prob_exit, as.character(ep$age), mean)
    }
    return(ep)
  }
  shift_retirement_schedule(shift, schedule)
}

#' Summarise a registry as a tibble for reporting
#' @param registry Scenario registry.
#' @return Tibble with one row per scenario.
#' @keywords internal
scenario_registry_table <- function(registry) {
  dplyr::bind_rows(lapply(names(registry), function(nm) {
    s <- registry[[nm]]
    tibble::tibble(
      scenario = nm,
      label = s$label,
      detail = paste(
        vapply(setdiff(names(s), c("label", "source")), function(f) {
          v <- s[[f]]
          sprintf("%s=%s", f, paste(format(v), collapse = "/"))
        }, character(1)),
        collapse = ", "
      ),
      source = s$source
    )
  }))
}
