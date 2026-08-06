# Supply-model scientific-review follow-ups ----
#
# Three additions from the supply-side scientific review. Each is a small,
# isolated, labeled artifact; none re-implements an existing engine.
#
#   1. entrant_pipeline_transition()  -- an explicit NRMP match -> board cert ->
#      active-practice staged pipeline (with lags), replacing a single scalar
#      match-to-active conversion. Fellowship matches are NOT eventual active US
#      clinical entrants; this makes each stage's conversion an explicit, labeled
#      parameter. (Complements the certification-regime model in R/49, which
#      works from the observed cert series; this projects FORWARD from the NRMP
#      leading indicator.)
#
#   2. supply_uncertainty_drivers()  -- a first-class registry of the supply-side
#      uncertainty drivers with an observability/priority label, so retirement
#      (weakly observed) is a PROMINENTLY LABELED driver rather than a routine
#      point parameter. Retirement is already varied in the PSA
#      (psa_workforce_gap_inputs: retirement_source); this registry states the
#      review's judgement about which drivers matter and why.
#
#   3. international_migration_assumption()  -- a registered, documented
#      assumption that NET international provider in/out-migration (and re-entry
#      from abroad) is treated as ~0 by default. Distinct from CONUS geographic
#      migration (R/20 / urps_migration) and temporary-exit re-entry (R/16), both
#      of which ARE modeled. Makes an omission an explicit, adjustable assumption
#      instead of a silent zero.

# ---- 1. Entrant match -> board -> active staged pipeline --------------------

#' Project active clinical entrants from NRMP matches through a staged pipeline
#'
#' `active_entrants[y] = matched[y - cert_lag - active_lag] * p_complete_cert *
#' p_active_practice`. Each stage is an explicit, labeled conversion; a fellowship
#' MATCH is not an active US clinical entrant.
#'
#' @param matches Data frame with integer `year` and non-negative `matched`
#'   (NRMP fellowship matches per year).
#' @param p_complete_cert Fraction of matched fellows who complete fellowship and
#'   achieve board certification. In `[0, 1]`. Default 0.95 (labeled assumption).
#' @param p_active_practice Fraction of newly certified who enter active US
#'   clinical practice. In `[0, 1]`. Default 0.90 (labeled assumption).
#' @param cert_lag Years from match to certification. Non-negative integer.
#' @param active_lag Years from certification to active practice. Non-negative
#'   integer. Default 0.
#' @param status Calibration status stamped on the output. Default
#'   "assumed_illustrative" -- replace the stage fractions with cited values.
#' @return A tibble: `year`, `matched`, `certified`, `active_entrants`, plus the
#'   stage fractions and `calibration_status`. Years whose source match year is
#'   absent yield `NA` (leading edge of the lag).
#' @export
entrant_pipeline_transition <- function(matches,
                                        p_complete_cert = 0.95,
                                        p_active_practice = 0.90,
                                        cert_lag = 1L,
                                        active_lag = 0L,
                                        status = "assumed_illustrative") {
  if (!is.data.frame(matches) || !all(c("year", "matched") %in% names(matches)))
    stop("entrant_pipeline_transition(): `matches` needs columns `year` and `matched`.",
         call. = FALSE)
  stopifnot(
    is.numeric(matches$matched), all(is.finite(matches$matched)), all(matches$matched >= 0),
    length(p_complete_cert) == 1L, p_complete_cert >= 0, p_complete_cert <= 1,
    length(p_active_practice) == 1L, p_active_practice >= 0, p_active_practice <= 1,
    cert_lag >= 0, active_lag >= 0, cert_lag == as.integer(cert_lag),
    active_lag == as.integer(active_lag)
  )
  m <- matches[order(matches$year), ]
  total_lag <- as.integer(cert_lag) + as.integer(active_lag)
  src_year <- m$year - total_lag
  matched_src <- m$matched[match(src_year, m$year)]   # NA where the source year is absent
  certified <- matched_src * p_complete_cert
  tibble::tibble(
    year               = m$year,
    matched            = m$matched,
    certified          = round(certified, 3),
    active_entrants    = round(certified * p_active_practice, 3),
    p_complete_cert    = p_complete_cert,
    p_active_practice  = p_active_practice,
    cert_lag           = as.integer(cert_lag),
    active_lag         = as.integer(active_lag),
    calibration_status = status
  )
}

# ---- 2. Uncertainty-driver registry -----------------------------------------

#' Supply-side uncertainty drivers, labeled by observability and priority
#'
#' A first-class statement of which supply parameters drive projection
#' uncertainty and how well each is observed -- so retirement (weakly observed)
#' is a prominently labeled driver, not a routine point parameter. The `psa_knob`
#' column points to the input each driver is varied through in the PSA
#' (see `psa_workforce_gap_inputs()`).
#'
#' @return A tibble: `driver`, `observability`, `priority`, `psa_knob`, `rationale`.
#' @export
supply_uncertainty_drivers <- function() {
  tibble::tribble(
    ~driver,                 ~observability,     ~priority, ~psa_knob,           ~rationale,
    "retirement_hazard",     "weakly_observed",  "high",    "retirement_source", "Retirement is inferred, not reported; keep it an explicit, widely-varied driver rather than a fixed parameter.",
    "career_change_effort",  "weakly_observed",  "high",    "p_active/hours",    "Career change and clinical-effort shifts may contribute more spread than retirement or mortality alone.",
    "entrant_conversion",    "partially_observed","medium", "entrants/conversion","NRMP match is a leading indicator but match != active entrant; the staged conversion is uncertain (see entrant_pipeline_transition()).",
    "demand_population",     "well_observed",    "medium",  "demand_population", "Census-projection driven; comparatively well observed.",
    "international_migration","unquantified",    "low",     "(none)",            "Net international in/out-migration is probably small but unquantified; treated as an explicit assumption (see international_migration_assumption())."
  )
}

# ---- 3. International-migration assumption (explicit, not silent) ------------

#' Registered net-international-migration assumption
#'
#' Records that NET international provider in/out-migration and re-entry from
#' abroad are treated as `net_annual` providers per year (default 0), as an
#' explicit, adjustable assumption. This is distinct from CONUS geographic
#' migration (modeled, R/20 / urps_migration) and temporary-exit re-entry
#' (modeled, R/16 reentry_probability).
#'
#' @param net_annual Net international providers added to the active workforce
#'   per year (can be negative for net emigration). Default 0.
#' @param status Calibration status. Default "assumed_zero_unquantified".
#' @return A one-row tibble describing the assumption.
#' @export
international_migration_assumption <- function(net_annual = 0,
                                               status = "assumed_zero_unquantified") {
  if (!is.numeric(net_annual) || length(net_annual) != 1L || !is.finite(net_annual))
    stop("international_migration_assumption(): net_annual must be a finite scalar.",
         call. = FALSE)
  tibble::tibble(
    quantity             = "net_international_provider_migration_and_reentry",
    net_annual_providers = net_annual,
    calibration_status   = status,
    distinct_from        = "CONUS geographic migration (R/20); temporary-exit re-entry (R/16)",
    rationale            = paste("Net international in/out-migration and re-entry from abroad are",
                                 "probably small for this subspecialty but are not empirically",
                                 "quantified; treated as zero by default. Set net_annual to test",
                                 "sensitivity.")
  )
}
