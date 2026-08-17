# Annual first-entry rate into URPS care ----
#
# THE ESTIMAND, settled 2026-08-17 (docs/PATHWAY_STATE_TRANSITION_AUDIT.md §8):
#
#   annual_first_urps_entry_rate(c,a,t)
#     = unique women with FIRST qualifying URPS entry in year t
#       ---------------------------------------------------------
#       ALL eligible prevalent women in that condition/age/year
#
# It is a POPULATION-LEVEL RATE, not a conditional hazard. Previously-entered
# women REMAIN in the denominator and cannot appear in the numerator, so
# historical depletion is embedded empirically in the measured rate. Do NOT
# subtract prior entrants: that double-counts history already inside it.
#
# It replaces per_entering = 1.00 at the conservative stage, which converted a
# prevalence STOCK into an annual FLOW, and it SUBSUMES recognition, p_seek and
# p_referral -- claims identify their product, never the components.

#' Annual first-entry rate into URPS care
#'
#' Combines a claims-derived numerator with an externally-derived prevalent
#' denominator and returns **both, separately**, alongside the rate.
#'
#' @details
#' NUMERATOR AND DENOMINATOR ARE RETAINED, NOT COLLAPSED. A rate alone hides
#' denominator-transport problems: an MA-APCD numerator over a national
#' prevalence denominator is a category error that a single number makes
#' invisible. Keeping `entrants_n` and `eligible_prevalent_n` beside the rate
#' forces the mismatch to be seen.
#'
#' THE DENOMINATOR IS A CURRENT-YEAR STOCK. `eligible_prevalent_n` counts women
#' who meet the modelled prevalent-disease and eligibility definition **in that
#' year, regardless of prior care history**. It is NOT a lifetime
#' ever-diagnosed cohort and NOT a never-treated cohort. It therefore mixes
#' never-treated women, previously-treated-but-still-symptomatic women, and
#' women with recurrent disease. If the external prevalence source removes
#' successfully treated asymptomatic women, that is correct and expected --
#' they are no longer in the current eligible prevalent stock.
#'
#' NO DEFAULTS. Both inputs must be supplied. A default numerator or
#' denominator would let this function return a number when no data exists,
#' which is the failure mode the whole estimand contract exists to prevent.
#'
#' @param entrants Data frame with `condition`, `age_band`, `year`, `n` --
#'   UNIQUE WOMEN with a first qualifying URPS entry, not visits.
#' @param eligible_prevalent Data frame with `condition`, `age_band`, `year`,
#'   `n` -- the current-year eligible prevalent stock.
#' @param conf_level Confidence level for the Wilson interval.
#' @param numerator_source,denominator_source Provenance strings. Required:
#'   an estimate whose sources are unrecorded cannot be audited later.
#' @return A tibble with one row per condition/age band/year carrying
#'   `entrants_n`, `eligible_prevalent_n`, `rate`, `rate_lo`, `rate_hi`, and
#'   both provenance columns.
#' @family first entry
#' @concept demand
#' @export
annual_first_urps_entry_rate <- function(entrants,
                                         eligible_prevalent,
                                         conf_level = 0.95,
                                         numerator_source = NULL,
                                         denominator_source = NULL) {
  if (is.null(numerator_source) || is.null(denominator_source)) {
    stop("numerator_source and denominator_source are required. An entry rate ",
         "whose numerator and denominator provenance is unrecorded cannot be ",
         "audited, and the two commonly come from different populations ",
         "(a state APCD numerator over a national denominator, for instance).",
         call. = FALSE)
  }
  key <- c("condition", "age_band", "year")
  .require_entry_columns(entrants, c(key, "n"), "entrants")
  .require_entry_columns(eligible_prevalent, c(key, "n"), "eligible_prevalent")

  num <- dplyr::rename(entrants, entrants_n = "n")
  den <- dplyr::rename(eligible_prevalent, eligible_prevalent_n = "n")

  # INNER join, deliberately. A left join would silently emit NA rates for
  # strata the denominator does not cover; an inner join makes the coverage
  # gap countable and it is reported below rather than absorbed.
  out <- dplyr::inner_join(num, den, by = key)
  dropped <- nrow(num) - nrow(out)
  if (dropped > 0L) {
    .msg_warn(sprintf(
      paste0("%d numerator stratum/strata have no matching denominator and were ",
             "dropped. A first-entry rate cannot be formed without both, and ",
             "carrying them as NA would understate coverage silently."), dropped))
  }
  if (nrow(out) == 0L) {
    stop("No condition/age/year stratum has both a numerator and a denominator.",
         call. = FALSE)
  }

  bad <- out$entrants_n > out$eligible_prevalent_n
  if (any(bad, na.rm = TRUE)) {
    stop(sprintf(
      paste0("%d stratum/strata have MORE first-time entrants than eligible ",
             "prevalent women (e.g. %s, age %s, %s: %s > %s). That is the ",
             "stock-as-flow signature this estimand exists to eliminate -- most ",
             "often a numerator counting VISITS rather than unique women, or a ",
             "denominator transported from a different population."),
      sum(bad, na.rm = TRUE), out$condition[which(bad)[1]],
      out$age_band[which(bad)[1]], out$year[which(bad)[1]],
      format(out$entrants_n[which(bad)[1]], big.mark = ","),
      format(out$eligible_prevalent_n[which(bad)[1]], big.mark = ",")),
      call. = FALSE)
  }

  ci <- wilson_ci(out$entrants_n, out$eligible_prevalent_n, conf_level = conf_level)
  out$rate <- out$entrants_n / out$eligible_prevalent_n
  out$rate_lo <- ci$lo
  out$rate_hi <- ci$hi
  out$numerator_source <- numerator_source
  out$denominator_source <- denominator_source
  out$estimand <- "annual_first_urps_entry_rate"
  out$denominator_definition <-
    "all eligible prevalent women in year, regardless of prior care history"

  tibble::as_tibble(out[, c(key, "entrants_n", "eligible_prevalent_n",
                            "rate", "rate_lo", "rate_hi",
                            "estimand", "denominator_definition",
                            "numerator_source", "denominator_source")])
}

.require_entry_columns <- function(x, cols, what) {
  if (!is.data.frame(x)) {
    stop(what, " must be a data frame with columns: ",
         paste(cols, collapse = ", "), call. = FALSE)
  }
  missing <- setdiff(cols, names(x))
  if (length(missing) > 0L) {
    stop(what, " is missing required column(s): ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

#' Status of the first-entry rate
#'
#' Reports whether a sourced estimate exists. Returns `"unresolved"` until one
#' does; the canonical pathway refuses in that state, which is deliberate.
#'
#' @return A length-one character calibration tier.
#' @family first entry
#' @concept demand
#' @export
first_entry_rate_status <- function() {
  # There is no shipped estimate, and this must not invent one. The gate in
  # .github/scripts/assert-canonical-science.R stays red until an APCD-derived
  # estimate is committed with its provenance.
  "unresolved_requires_source"
}
