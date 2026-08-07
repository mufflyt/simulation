# Supply-model scientific-review follow-ups ----
#
# Three additions from the supply-side scientific review. Each is a small,
# isolated, labeled artifact; none re-implements an existing engine.
#
#   1. entrant_pipeline_transition()  -- an explicit NRMP match -> board cert ->
#      active-practice staged pipeline (with lags), replacing a single scalar
#      match-to-active conversion. Fellowship matches are NOT eventual active US
#      clinical entrants; this makes each stage's conversion an explicit, labeled
#      parameter. (Complements the certification-regime model in R/supply-entrant_regime, which
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
#      migration (R/geography-provider_geography / urps_migration) and temporary-exit re-entry (R/supply-provider_lifecycle), both
#      of which ARE modeled. Makes an omission an explicit, adjustable assumption
#      instead of a silent zero.

# ---- 1. Entrant match -> board -> active staged pipeline --------------------

#' Observed NRMP-match to board-certification conversion
#'
#' The `p_complete_cert` stage of [entrant_pipeline_transition()], estimated from
#' the two observed series rather than assumed. Matches in appointment year
#' `y - cert_lag` are divided into certifications in year `y`, and the mean of
#' those annual ratios is returned.
#'
#' Two things keep this from being a number picked with the answer in hand:
#'
#' * `through_year` bounds BOTH series, so a back-test arm can estimate the
#'   conversion without seeing its own validation window.
#' * Certification years the regime screen flags as disrupted are dropped, using
#'   [classify_certification_regimes()] rather than a hand-listed year. A
#'   cancelled examination drives the observed ratio toward zero for reasons that
#'   say nothing about how many matched fellows eventually certify -- 2020 alone
#'   pulls the pre-cutoff mean from 0.75 to 0.60.
#'
#' @param through_year Latest year either series may be read to.
#' @param cert_lag Years from appointment to certification.
#' @param exclude_disrupted Drop disrupted certification years.
#' @return List with `ratio`, `years`, `n_years`, `excluded`, and `annual`.
#' @family review followups
#' @concept supply
#' @export
nrmp_match_to_cert_ratio <- function(through_year = BACKTEST_CUTOFF_YEAR,
                                     cert_lag = URPS_FELLOWSHIP_YEARS,
                                     exclude_disrupted = TRUE) {
  nrmp <- nrmp_entrant_series(available_by = through_year)
  certs <- urps_entrant_series(through_year)

  excluded <- integer(0)
  if (isTRUE(exclude_disrupted)) {
    reg <- classify_certification_regimes(
      as.data.frame(certs[, c("year", "count")]), verbose = FALSE)
    # Backlog years are excluded too: they certified an already-practising pool
    # that never passed through the match, so their ratio is meaningless.
    excluded <- reg$year[reg$regime %in% c("disrupted", "backlog")]
    certs <- certs[!certs$year %in% excluded, , drop = FALSE]
  }

  src <- certs$year - as.integer(cert_lag)
  matched <- nrmp$positions_filled[match(src, nrmp$appointment_year)]
  keep <- is.finite(matched) & matched > 0
  if (!any(keep)) {
    stop("nrmp_match_to_cert_ratio(): no certification year has a matching ",
         "appointment year at lag ", cert_lag, " within the cutoff.", call. = FALSE)
  }
  annual <- stats::setNames(certs$count[keep] / matched[keep], certs$year[keep])

  list(
    ratio = unname(mean(annual)),
    years = certs$year[keep],
    n_years = sum(keep),
    excluded = excluded,
    cert_lag = as.integer(cert_lag),
    annual = annual
  )
}

#' Project active clinical entrants from NRMP matches through a staged pipeline
#'
#' `active_entrants[y] = matched[y - cert_lag - active_lag] * p_complete_cert *
#' p_active_practice`. Each stage is an explicit, labeled conversion; a fellowship
#' MATCH is not an active US clinical entrant.
#'
#' @param matches Data frame with integer `year` and non-negative `matched`
#'   (NRMP fellowship matches per year).
#' @param p_complete_cert Fraction of matched fellows who complete fellowship and
#'   achieve board certification. Either one value in 0 to 1, one value per row of
#'   `matches`, or a data frame of `year` and `p_complete_cert` keyed on the
#'   CERTIFICATION year (years it omits fall back to its median). Defaults to
#'   0.86, the pooled entry-to-certification conversion from
#'   [entrant_to_cert_ratio()] against the ACGME fellow counts. Two earlier
#'   defaults were worse: 0.95 was an assumption that over-predicted every
#'   observed year by 8 to 18 certifications, and 0.75 was estimated on a window
#'   containing the COVID trough but not its release, which biases it low.
#' @param p_active_practice Fraction of newly certified who enter active US
#'   clinical practice. In 0 to 1. Default 0.90 (labeled assumption).
#' @param defer_shortfall When `p_complete_cert` varies by year, carry each
#'   year's shortfall against the undisrupted conversion into the next year
#'   instead of discarding it. A cancelled examination defers fellows; it does
#'   not remove them from the pipeline.
#' @param cert_lag Years from match to certification. Non-negative integer.
#'   Defaults to the three-year fellowship, `URPS_FELLOWSHIP_YEARS`. The former
#'   default of 1 contradicted the fellowship length this package documents, and
#'   scored worse against every observed year.
#' @param active_lag Years from certification to active practice. Non-negative
#'   integer. Default 0.
#' @param status Calibration status stamped on the output. Default
#'   "assumed_illustrative" -- replace the stage fractions with cited values.
#' @return A tibble: `year`, `matched`, `certified`, `active_entrants`,
#'   `deferred_in` (fellows carried in from a suppressed earlier year), plus the
#'   stage fractions and `calibration_status`. Years whose source match year is
#'   absent yield `NA` (leading edge of the lag).
#' @family review followups
#' @concept supply
#' @export
entrant_pipeline_transition <- function(matches,
                                        p_complete_cert = 0.86,
                                        p_active_practice = 0.90,
                                        cert_lag = URPS_FELLOWSHIP_YEARS,
                                        active_lag = 0L,
                                        defer_shortfall = TRUE,
                                        status = "assumed_illustrative") {
  if (!is.data.frame(matches) || !all(c("year", "matched") %in% names(matches)))
    stop("entrant_pipeline_transition(): `matches` needs columns `year` and `matched`.",
         call. = FALSE)
  stopifnot(
    is.numeric(matches$matched), all(is.finite(matches$matched)), all(matches$matched >= 0),
    length(p_active_practice) == 1L, p_active_practice >= 0, p_active_practice <= 1,
    cert_lag >= 0, active_lag >= 0, cert_lag == as.integer(cert_lag),
    active_lag == as.integer(active_lag)
  )
  m <- matches[order(matches$year), ]
  total_lag <- as.integer(cert_lag) + as.integer(active_lag)
  src_year <- m$year - total_lag
  matched_src <- m$matched[match(src_year, m$year)]   # NA where the source year is absent

  # The conversion may be a scalar or a PER-CERTIFICATION-YEAR schedule. A
  # constant cannot express the event this pipeline most needs to survive: the
  # 2020 examination was cancelled, so that year's conversion was near zero
  # while the fellows themselves were unaffected. Held constant at 0.95 the
  # pipeline predicts 56 certifications for 2020 against 10 observed.
  p_vec <- .resolve_cert_conversion(p_complete_cert, m$year)
  p_ref <- if (length(unique(p_vec)) == 1L) p_vec[1] else max(p_vec, na.rm = TRUE)

  certified <- matched_src * p_vec
  deferred_in <- rep(0, nrow(m))
  if (isTRUE(defer_shortfall) && length(unique(p_vec)) > 1L) {
    # A suppressed conversion year DEFERS its fellows, it does not destroy them:
    # candidates who miss a cancelled examination sit the next one. The shortfall
    # against the undisrupted conversion is carried into the following year --
    # the same mechanism R/49 estimates from the certification series directly.
    carry <- 0
    for (i in seq_len(nrow(m))) {
      if (!is.finite(certified[i])) next
      certified[i] <- certified[i] + carry
      deferred_in[i] <- carry
      shortfall <- matched_src[i] * (p_ref - p_vec[i])
      carry <- if (is.finite(shortfall) && shortfall > 0) shortfall else 0
    }
  }

  tibble::tibble(
    year               = m$year,
    matched            = m$matched,
    certified          = round(certified, 3),
    active_entrants    = round(certified * p_active_practice, 3),
    p_complete_cert    = p_vec,
    deferred_in        = round(deferred_in, 3),
    p_active_practice  = p_active_practice,
    cert_lag           = as.integer(cert_lag),
    active_lag         = as.integer(active_lag),
    calibration_status = status
  )
}

# Accept a scalar conversion or a per-year schedule keyed on the CERTIFICATION
# year, and return one value per row of the pipeline. Years absent from a
# supplied schedule fall back to its own median rather than to NA, so a partial
# schedule (the usual case -- one disrupted year named explicitly) does not
# silently blank every other year. Internal.
.resolve_cert_conversion <- function(p, years) {
  if (is.data.frame(p)) {
    if (!all(c("year", "p_complete_cert") %in% names(p))) {
      stop("entrant_pipeline_transition(): a `p_complete_cert` data frame needs ",
           "columns `year` and `p_complete_cert`.", call. = FALSE)
    }
    v <- p$p_complete_cert[match(years, p$year)]
    v[!is.finite(v)] <- stats::median(p$p_complete_cert, na.rm = TRUE)
  } else if (length(p) == 1L) {
    v <- rep(as.numeric(p), length(years))
  } else if (length(p) == length(years)) {
    v <- as.numeric(p)
  } else {
    stop(sprintf(paste(
      "entrant_pipeline_transition(): `p_complete_cert` has length %d; supply a",
      "single value, one per year (%d), or a data frame of `year` and",
      "`p_complete_cert`."), length(p), length(years)), call. = FALSE)
  }
  if (any(!is.finite(v)) || any(v < 0) || any(v > 1)) {
    stop("entrant_pipeline_transition(): every `p_complete_cert` must be finite ",
         "and within [0, 1].", call. = FALSE)
  }
  v
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
#' @examples
#' # Every driver carries an observability label, so a parameter that is merely
#' # assumed cannot be read as one that was measured.
#' supply_uncertainty_drivers()
#' @family review followups
#' @concept supply
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
#' migration (modeled, R/geography-provider_geography / urps_migration) and temporary-exit re-entry
#' (modeled, R/supply-provider_lifecycle reentry_probability).
#'
#' @param net_annual Net international providers added to the active workforce
#'   per year (can be negative for net emigration). Default 0.
#' @param status Calibration status. Default "assumed_zero_unquantified".
#' @return A one-row tibble describing the assumption.
#' @family review followups
#' @concept supply
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
    distinct_from        = "CONUS geographic migration (R/geography-provider_geography); temporary-exit re-entry (R/supply-provider_lifecycle)",
    rationale            = paste("Net international in/out-migration and re-entry from abroad are",
                                 "probably small for this subspecialty but are not empirically",
                                 "quantified; treated as zero by default. Set net_annual to test",
                                 "sensitivity.")
  )
}
