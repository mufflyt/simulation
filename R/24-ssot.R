# mufflyaccess SSOT Hookups ----
#
# Single place where this package reaches into the `mufflyaccess` contract, so a
# quantity that mufflyaccess owns can never be independently redefined here.
# That divergence is exactly what the HDMM improvement plan warned about
# ("do not build a fifth demand model"), and it had already happened: this
# package shipped its own scenario registry, also versioned 1.0.0, with a
# `retirement_shift_years` field of identical name and semantics.
#
# WHAT MUFFLYACCESS ACTUALLY OWNS (verified against the installed package, not
# inferred from exported names):
#
#   urps_count()                base-year supply           ALREADY WIRED (R/16)
#   urps_scenarios()            9-scenario registry v1.0.0  WIRED HERE
#   urps_projection_schema()    13-column output contract   WIRED HERE
#   validate_urps_projection()  contract validator          WIRED HERE
#   pfd_prevalence()            PFD prevalence, 65+ ONLY    WIRED HERE (partial)
#   get_canonical_bands()       c(30, 60, 120, 180)         WIRED HERE
#   rurality_from_ruca()        RUCA >= 4 is rural          WIRED HERE
#   urps_provenance()           artifact lineage            WIRED HERE
#   safe_divide()/safe_rate()   division guards             WIRED HERE
#
# THREE THINGS IT DOES NOT OWN, despite the names suggesting otherwise:
#
#   pfd_prevalence() returns only `65_79` and `80plus`. It does NOT cover women
#   under 65, who are a large share of urogynaecologic demand. Its 65-79 band is
#   also NOT this model's old 60-79 band -- applying one to the other would be a
#   silent wrong-grain error. The demand age bands have been restructured to
#   match the SSOT boundary exactly (see R/13).
#
#   pfd_prevalence_acs_bands() returns the same 65+ values keyed by ACS variable
#   name (a65_66E, a67_69E, ...). It solves ACS table joins; it does not supply
#   a younger-age prevalence.
#
#   mc_weighted_ci(access, est, se, ...) propagates ACS margins of error through
#   an access surface. It is NOT a Monte Carlo replicate summariser, so it does
#   not replace the quantile bands this package computes over simulation
#   replicates. Different quantity; left alone.

#' Is the mufflyaccess contract available?
#' @return Logical.
#' @export
has_mufflyaccess <- function() requireNamespace("mufflyaccess", quietly = TRUE)

.require_mufflyaccess <- function(what) {
  if (!has_mufflyaccess()) {
    stop(sprintf("%s is owned by the mufflyaccess contract, which is not installed. ",
                 what),
         "Install it rather than redefining the quantity here.", call. = FALSE)
  }
}

# ---- Scenario registry -----------------------------------------------------

#' Supply scenarios from the mufflyaccess registry
#'
#' Adopts `mufflyaccess::urps_scenarios()` as the single source of truth and
#' translates it into the shape this package's engine consumes. The registry
#' carries `entrant_multiplier`, `retirement_shift_years`,
#' `late_career_fte_factor` and `late_career_fte_onset_age`.
#'
#' Two mapping notes:
#'  * the registry expresses entrants as a MULTIPLIER on the modelled rate, so a
#'    baseline entrant count must be supplied to turn it into a count;
#'  * `late_career_fte_factor` is applied only from `late_career_fte_onset_age`,
#'    which is more specific than a uniform hours multiplier and is honoured as
#'    such by [simulate_provider_career_once()].
#'
#' @param baseline_entrants Annual entrants under the baseline scenario.
#' @param ids Scenario ids to include; defaults to the whole registry.
#' @return Named list of scenario definitions.
#' @export
ssot_supply_scenarios <- function(baseline_entrants = 55, ids = NULL) {
  .require_mufflyaccess("The URPS scenario registry")
  reg <- mufflyaccess::urps_scenarios()
  if (!is.null(ids)) reg <- reg[reg$scenario_id %in% ids, , drop = FALSE]

  out <- lapply(seq_len(nrow(reg)), function(i) {
    r <- reg[i, ]
    list(
      label = r$label,
      family = r$family,
      entrants = baseline_entrants * r$entrant_multiplier,
      entrant_multiplier = r$entrant_multiplier,
      retirement_shift_years = r$retirement_shift_years,
      late_career_fte_factor = r$late_career_fte_factor,
      late_career_fte_onset_age = r$late_career_fte_onset_age,
      requires_fte_model = r$requires_fte_model,
      hours_multiplier = 1.0,
      conversion = 1.0,
      source = sprintf("mufflyaccess::urps_scenarios() v%s",
                       mufflyaccess::URPS_SCENARIO_REGISTRY_VERSION)
    )
  })
  stats::setNames(out, reg$scenario_id)
}

#' Registry version in force
#' @return Character version string.
#' @export
ssot_scenario_registry_version <- function() {
  if (!has_mufflyaccess()) return(NA_character_)
  mufflyaccess::URPS_SCENARIO_REGISTRY_VERSION
}

#' Refuse a scenario id the SSOT registry does not know
#'
#' @param ids Scenario ids used by a run.
#' @param mode Reproducibility mode; strict errors on an unregistered id.
#' @return (Invisibly) the unregistered ids.
#' @export
assert_scenarios_registered <- function(ids, mode = resolve_reproducibility_mode()) {
  if (!has_mufflyaccess()) return(invisible(character(0)))
  known <- mufflyaccess::urps_scenario_ids()
  unknown <- setdiff(ids, known)
  if (length(unknown)) {
    msg <- sprintf(paste(
      "Scenario id(s) %s are not in the mufflyaccess registry (v%s). Downstream",
      "consumers validate against urps_scenarios(), so an unregistered id will",
      "fail validate_urps_projection(). Registered ids: %s."),
      paste(unknown, collapse = ", "), ssot_scenario_registry_version(),
      paste(known, collapse = ", "))
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }
  invisible(unknown)
}

# ---- Projection contract ---------------------------------------------------

#' Emit a supply projection into the mufflyaccess projection contract
#'
#' The package previously defined its own output shape. `mufflyaccess` already
#' owns a 13-column projection contract that downstream consumers read through
#' `read_urps_projection()`, so the supply panel is reshaped into it and
#' validated before anything downstream sees it.
#'
#' @param supply Supply panel with `year`, `scenario`, and the FTE columns.
#' @param specialty Subspecialty label.
#' @param certification_pathway One of ABOG, ABU_NET_NEW, ABOG_PLUS_ABU.
#' @param geography_type,geography_id Geography identifiers.
#' @param validate Validate against the contract before returning.
#' @return A data frame conforming to `mufflyaccess::urps_projection_schema()`.
#' @export
as_urps_projection <- function(supply,
                               specialty = "URPS",
                               certification_pathway = "ABOG_PLUS_ABU",
                               geography_type = "national",
                               geography_id = "US",
                               validate = TRUE) {
  .require_mufflyaccess("The URPS projection contract")
  assertthat::assert_that(is.data.frame(supply), "year" %in% names(supply))

  scen <- if ("scenario" %in% names(supply)) supply$scenario else "baseline"
  out <- data.frame(
    year = as.integer(supply$year),
    scenario_id = as.character(scen),
    specialty = specialty,
    certification_pathway = certification_pathway,
    geography_type = geography_type,
    geography_id = geography_id,
    supply_headcount = as.numeric(supply$headcount_median %||% supply$headcount),
    supply_clinical_fte = as.numeric(supply$effective_fte_median %||% NA_real_),
    # The contract requires lower_95/upper_95 to BRACKET supply_headcount, so
    # these are the headcount bounds, not the FTE bounds. Supplying FTE bounds
    # here fails validation -- which is precisely why this is validated.
    lower_95 = as.numeric(supply$headcount_lo %||% NA_real_),
    upper_95 = as.numeric(supply$headcount_hi %||% NA_real_),
    stringsAsFactors = FALSE
  )
  if (isTRUE(validate)) {
    mufflyaccess::validate_urps_projection(out)
    .msg_info(sprintf("Supply projection validates against the URPS contract v%s.",
                      mufflyaccess::URPS_PROJECTION_CONTRACT_VERSION))
  }
  out
}

# ---- PFD prevalence --------------------------------------------------------

#' PFD prevalence for the 65+ bands, from the SSOT
#'
#' `mufflyaccess::pfd_prevalence()` covers `65_79` and `80plus` ONLY. The
#' younger bands are not in the contract and remain local to this package; the
#' return value labels which is which so a consumer can never mistake a local
#' literal for a contract value.
#'
#' @param condition Condition passed through to the contract.
#' @return Tibble of `age_band`, `prevalence`, `owner`.
#' @export
ssot_pfd_prevalence <- function(condition = "any_PFD") {
  .require_mufflyaccess("PFD prevalence")
  p <- mufflyaccess::pfd_prevalence(condition)
  tibble::tibble(
    age_band = c("65-79", "80+"),
    prevalence = unname(p[c("65_79", "80plus")]),
    owner = "mufflyaccess::pfd_prevalence()"
  )
}

# ---- Geographic access -----------------------------------------------------

#' Canonical drive-time bands
#'
#' Sourced from `mufflyaccess::get_canonical_bands()` so the access layer cannot
#' diverge from `twostep` and `isochrones`. Falls back to the historical values,
#' which are identical, when the contract is unavailable.
#'
#' @return Integer vector of drive-time bands in minutes.
#' @export
ssot_access_bands <- function() {
  if (!has_mufflyaccess()) return(c(30L, 60L, 120L, 180L))
  as.integer(mufflyaccess::get_canonical_bands())
}

#' Primary access band
#' @param units "min" or "sec".
#' @return Numeric band.
#' @export
ssot_primary_access_band <- function(units = c("min", "sec")) {
  units <- match.arg(units)
  if (!has_mufflyaccess()) return(if (units == "min") 60 else 3600)
  mufflyaccess::get_primary_access_band(units)
}

#' Classify a RUCA code as metropolitan or rural
#'
#' Gives the `nonmetro` access component in [demand_scenario_registry()] an
#' operational definition instead of leaving it a bare label.
#'
#' @param code RUCA code(s).
#' @return Character classification.
#' @export
ssot_rurality <- function(code) {
  .require_mufflyaccess("RUCA rurality classification")
  mufflyaccess::rurality_from_ruca(code)
}

# ---- Provenance ------------------------------------------------------------

#' URPS artifact provenance for the run manifest
#'
#' Records which URPS artifact a simulation output was built from, including the
#' active-in-year definition and the deduplication rule, so a downstream reader
#' can tell whether two outputs are comparable.
#'
#' @param detailed Pass through to the contract.
#' @return List of provenance fields, or NULL when unavailable.
#' @export
ssot_provenance <- function(detailed = FALSE) {
  if (!has_mufflyaccess()) return(NULL)
  p <- mufflyaccess::urps_provenance(detailed = detailed)
  p[intersect(names(p), c("artifact_version", "contract_version", "artifact_source",
                          "canonical_release", "suitable_for_release",
                          "snapshot_date", "source_sha256", "boards",
                          "canonical_2023_estimand", "active_in_year_definition",
                          "deduplication_rule"))]
}

# ---- Division guards -------------------------------------------------------

#' Guarded division
#'
#' Delegates to `mufflyaccess::safe_divide()` when available so this package
#' uses the house convention for divide-by-zero rather than producing `Inf`.
#'
#' @param numerator,denominator Numeric vectors.
#' @param default Value returned when the denominator is effectively zero.
#' @return Numeric vector.
#' @export
ssot_safe_divide <- function(numerator, denominator, default = NA_real_) {
  if (has_mufflyaccess()) {
    return(mufflyaccess::safe_divide(numerator, denominator, default = default))
  }
  ifelse(abs(denominator) < 1e-10, default, numerator / denominator)
}

#' Guarded percentage
#' @param part,total Numeric vectors.
#' @param digits Rounding digits.
#' @return Numeric percentage.
#' @export
ssot_safe_percent <- function(part, total, digits = 1) {
  if (has_mufflyaccess()) {
    return(mufflyaccess::safe_percent(part, total, digits = digits))
  }
  round(ifelse(abs(total) < 1e-10, 0, 100 * part / total), digits)
}

# ---- Coverage report -------------------------------------------------------

#' Which model quantities are owned by the SSOT, and which are local
#'
#' @return Tibble of `quantity`, `owner`, `note`.
#' @export
ssot_coverage_report <- function() {
  tibble::tribble(
    ~quantity,                    ~owner,          ~note,
    "base-year supply",           "mufflyaccess",  "urps_count(); national and CONUS kept distinct",
    "certification cohorts",      "mufflyaccess",  "urps_counts_long(); yields the base-cohort age structure and the entrant rate",
    "individual provider roster", "local",         "NOT in the contract: aggregate counts only, no age/sex/state. use_urps_artifact() hook exists but no artifact is configured",
    "supply scenarios",           "mufflyaccess",  "urps_scenarios() v1.0.0; replaces the local registry",
    "projection output shape",    "mufflyaccess",  "urps_projection_schema(); validated before export",
    "PFD prevalence 65+",         "mufflyaccess",  "pfd_prevalence(): 65-79 and 80plus only",
    "PFD prevalence <65",         "local",         "NOT in the contract; Nygaard-derived literals in R/13",
    "drive-time bands",           "mufflyaccess",  "get_canonical_bands()",
    "rurality",                   "mufflyaccess",  "rurality_from_ruca(); RUCA >= 4 is rural",
    "artifact provenance",        "mufflyaccess",  "urps_provenance() folded into the run manifest",
    "division guards",            "mufflyaccess",  "safe_divide()/safe_percent()",
    "work RVUs",                  "local",         "CMS PFS RVU file; not a mufflyaccess quantity",
    "consultation rates",         "local",         "Kirby-scale literals; no contract equivalent",
    "surgery rates",              "local",         "Wu 2011 literals; no contract equivalent",
    "delegation shares",          "local",         "Forte-derived; no contract equivalent",
    "Monte Carlo CI bands",       "local",         "mc_weighted_ci() propagates ACS MOE, a different quantity"
  )
}
