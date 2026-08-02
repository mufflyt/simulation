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

SCENARIO_REGISTRY_VERSION <- "1.0.0"

# Access components a demand scenario can relax. Named so the base-year gap can
# declare which of them it already contains (see assert_access_not_double_counted).
ACCESS_COMPONENTS <- c("uninsured", "nonmetro", "racial_equity", "income")

# ---- Supply scenarios -----------------------------------------------------

#' Default supply scenario registry
#'
#' @param baseline_entrants Baseline annual entrants to practice.
#' @return Named list of supply scenario definitions.
#' @export
supply_scenario_registry <- function(baseline_entrants = 55) {
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
    retirement_2_years_later = list(
      label = "Retire 2 years later",
      entrants = baseline_entrants,
      retirement_shift_years = 2,
      hours_multiplier = 1.00,
      conversion = 1.00,
      source = "HWSM: one- or two-year shift in retirement patterns"
    ),
    retirement_2_years_earlier = list(
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
      utilization_multiplier = NA_real_,
      care_seeking_multiplier = 1.00,
      source = "HDMM increased medical insurance coverage; Dall 2021 Improved Access 1"
    ),
    reduced_barriers = list(
      label = "Reduced barriers to care",
      access_components = c("uninsured", "nonmetro", "racial_equity"),
      utilization_multiplier = NA_real_,
      care_seeking_multiplier = 1.00,
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

SUPPLY_SCENARIO_REQUIRED <- c("label", "entrants", "retirement_shift_years",
                              "hours_multiplier", "conversion", "source")
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
  if (!"status_quo" %in% names(registry)) {
    stop("validate_scenario_registry: every registry needs a 'status_quo' scenario",
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
#' @param scenario A supply scenario definition.
#' @param schedule Base retirement hazard schedule.
#' @return Shifted hazard schedule.
#' @export
scenario_retirement_schedule <- function(scenario, schedule = RETIREMENT_HAZARD_BY_AGE) {
  shift_retirement_schedule(scenario$retirement_shift_years %||% 0, schedule)
}

#' Summarise a registry as a tibble for reporting
#' @param registry Scenario registry.
#' @return Tibble with one row per scenario.
#' @export
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
