################################################################################
# R/geography-chia_inpatient_flows.R
# Empirical surgical travel from Massachusetts CHIA inpatient discharges
#
# Calibration tier: observed_regional (Massachusetts only, not national)
#
# WHY THIS EXISTS
# ---------------
# The E2SFCA layer in mufflyt/twostep weights supply by generic Luo/Qi distance
# decay (`E2SFCA_DEFAULT_WEIGHTS`: 30 = 1.00, 60 = 0.68, 120 = 0.22, 180 = 0.09).
# Those are a defensible general-accessibility default. They are not a
# measurement of women travelling for major pelvic reconstructive surgery.
# CHIA is: 1,639,630 admitted operations on adult women, FY2007-2018, each
# carrying a patient residential ZIP and a facility ZIP.
#
# WHAT IS MEASURED AND WHAT IS ASSUMED
# ------------------------------------
# DISTANCE is measured. Great-circle miles between ZCTA centroids, 99.0% of
# cases geocoded. No tuning constants. This is the primary result.
#
# DRIVE TIME is NOT measured. There is no routing engine here. The conversion
# is `miles * 1.3 circuity / 40 mph`, and both constants are choices. They
# dominate the answer: the <=30-minute share ranges from 0.646 at 30 mph to
# 0.790 at 50 mph, a 14-point swing from the speed constant alone -- wider than
# most effects this kernel would be used to detect. Drive-time bands are
# therefore provided for comparability with Luo/Qi and MUST NOT be treated as
# observations. Real drive times require the HERE isochrone pipeline in
# mufflyt/isochrones; see travel_drivetime_speed_sensitivity.csv.
#
# WHAT THIS DOES NOT LICENCE
# --------------------------
# Swapping these numbers in for E2SFCA_DEFAULT_WEIGHTS. Two reasons.
# (1) The drive-time bands are assumption-driven, above.
# (2) Even the distance shares are supply-constrained: 95.4% of these women had
#     a hospital within 30 minutes, so a high near-band share measures where
#     hospitals are at least as much as willingness to travel. Read as raw
#     shares the decay looks ~3x steeper than Luo/Qi; read as observed-versus-
#     available the 61-120 band is used ~9x MORE than nearest-hospital
#     assignment predicts. Marginal shares cannot adjudicate between those.
# A substitute kernel needs a choice model over each patient's full option set.
# Until then this module reports and checks; it does not replace.
#
# INPATIENT ONLY
# --------------
# CHIA Case Mix is inpatient, outpatient-ED and outpatient-observation. There is
# no ambulatory-surgery file, and 957 CMR 8.00 binds acute care hospitals, so
# freestanding ASCs never submit. Every figure here is conditional on admission,
# and most urogynaecologic surgery is now ambulatory. See
# docs/CHIA_TECHNICAL_APPENDIX.md.
################################################################################

# ---- Measured: straight-line distance, no tuning constants -------------------

CHIA_INPATIENT_SURGERY_DISTANCE <- c(
  "5"   = 0.4064,   # 0-5 miles
  "10"  = 0.2028,   # 5-10
  "25"  = 0.2354,   # 10-25
  "50"  = 0.0964,   # 25-50
  "100" = 0.0384,   # 50-100
  "999" = 0.0205    # >100 (largely out-of-state residents)
)

CHIA_INPATIENT_SURGERY_DISTANCE_QUANTILES <- c(
  p25 = 3.3, p50 = 7.2, p75 = 17.2, p90 = 36.2, p95 = 58.3, p99 = 192.6
)

# ---- Assumption-driven: drive-time bands at the 40 mph central case ----------
# Provided ONLY for comparability with the Luo/Qi band structure. See the
# sensitivity table before using any of these figures.

CHIA_INPATIENT_SURGERY_TRAVEL_40MPH <- c(
  "30" = 0.7309, "60" = 0.1510, "120" = 0.0741, "180" = 0.0217, "999" = 0.0223
)

# Share whose NEAREST hospital falls in each band -- the availability
# denominator that makes the shares above interpretable.
CHIA_INPATIENT_SURGERY_AVAILABLE <- c(
  "30" = 0.9536, "60" = 0.0210, "120" = 0.0079, "180" = 0.0037, "999" = 0.0138
)

# Fraction travelling more than 15 minutes past their nearest hospital.
CHIA_INPATIENT_SURGERY_BYPASS_RATE <- 0.337

#' Observed inpatient surgical travel distribution
#'
#' Massachusetts all-payer, female, 18+, operative principal procedure, newborn
#' stays excluded. FY2007-2018, n = 1,639,630 admitted operations, 99.0%
#' geocoded.
#'
#' @param what One of:
#'   \describe{
#'     \item{"distance"}{measured share by straight-line mile band (default)}
#'     \item{"quantiles"}{measured distance quantiles in miles}
#'     \item{"drivetime"}{ASSUMED drive-time band shares at 40 mph -- see the
#'       speed sensitivity before use}
#'     \item{"available"}{share whose nearest hospital falls in each time band}
#'     \item{"ratio"}{drivetime / available; use this rather than raw shares
#'       when comparing against a distance-decay function}
#'   }
#' @return Named numeric vector.
#' @examples
#' chia_travel_kernel()                # measured distance -- prefer this
#' chia_travel_kernel("ratio")         # 61-120 min used ~9x nearest-assignment
#' @export
chia_travel_kernel <- function(what = c("distance", "quantiles", "drivetime",
                                        "available", "ratio")) {
  what <- match.arg(what)
  switch(what,
    distance  = CHIA_INPATIENT_SURGERY_DISTANCE,
    quantiles = CHIA_INPATIENT_SURGERY_DISTANCE_QUANTILES,
    drivetime = {
      warning("drive-time bands assume 1.3 circuity and 40 mph; the <=30 share ",
              "ranges 0.646-0.790 across 30-50 mph. Use 'distance' unless you ",
              "need Luo/Qi band comparability.", call. = FALSE)
      CHIA_INPATIENT_SURGERY_TRAVEL_40MPH
    },
    available = CHIA_INPATIENT_SURGERY_AVAILABLE,
    ratio     = CHIA_INPATIENT_SURGERY_TRAVEL_40MPH / CHIA_INPATIENT_SURGERY_AVAILABLE
  )
}

#' Compare the modelled E2SFCA weights against observed CHIA travel
#'
#' A regional external check, not a calibration step. Returns the generic
#' weights beside the observed distribution and the availability denominator, so
#' the supply-constraint caveat travels with the numbers.
#'
#' @param weights Named numeric distance-decay weights, defaulting to the
#'   twostep production values.
#' @return A data.frame with one row per band.
#' @export
compare_e2sfca_to_chia <- function(weights = c("30" = 1.00, "60" = 0.68,
                                               "120" = 0.22, "180" = 0.09)) {
  bands <- names(weights)
  obs   <- CHIA_INPATIENT_SURGERY_TRAVEL_40MPH[bands]
  avail <- CHIA_INPATIENT_SURGERY_AVAILABLE[bands]
  data.frame(
    band_max_minutes        = as.integer(bands),
    e2sfca_weight           = as.numeric(weights),
    chia_observed_40mph     = as.numeric(obs),
    chia_available          = as.numeric(avail),
    observed_rel_30         = as.numeric(obs / obs[1]),
    observed_over_available = as.numeric(obs / avail),
    row.names = NULL
  )
}

# ---- Urogynaecology-specific: the supply set is NOT all hospitals ------------
#
# Only 18-30 of ~76 Massachusetts acute hospitals host any URPS operation in a
# given year, and only 4-16 reach 10 cases. Measuring urogynaecologic access
# against all hospitals overstates availability by roughly 3x in the tail:
# median distance to the nearest ANY hospital is 2.9 miles, to the nearest
# urogyn-capable hospital 5.3 miles, and at p90 the gap is 8.2 vs 22.4 miles.
#
# "Urogynaecologic surgery" is defined by the OPERATOR -- an operation on the
# female-adult cohort by a board-certified URPS surgeon (NPI matched to
# urps.provider_snapshot). A procedure-code definition awaits
# config/chia_urps_inpatient_codes.yml. n = 9,081 operations at 38 sites,
# FY2007-2018, 100% geocoded.
#
# A site counts as urogyn-capable in a year at >= 10 URPS operations. The
# threshold is not delicate: median nearest-capable distance is 4.4 / 5.3 / 6.1
# miles at thresholds of 1 / 10 / 25.

CHIA_UROGYN_TRAVEL_ACTUAL <- c(
  "5" = 0.3389, "10" = 0.2170, "25" = 0.2895,
  "50" = 0.1035, "100" = 0.0385, "999" = 0.0124
)

# Nearest hospital that actually performs URPS surgery -- the correct
# availability denominator for a urogynaecology access surface.
CHIA_UROGYN_NEAREST_CAPABLE <- c(
  "5" = 0.4781, "10" = 0.2488, "25" = 0.1830,
  "50" = 0.0677, "100" = 0.0122, "999" = 0.0101
)

# Nearest hospital of ANY kind -- shown to make the overstatement explicit.
CHIA_UROGYN_NEAREST_ANY <- c(
  "5" = 0.7392, "10" = 0.1858, "25" = 0.0549,
  "50" = 0.0069, "100" = 0.0035, "999" = 0.0096
)

CHIA_UROGYN_TRAVEL_QUANTILES <- data.frame(
  quantile                   = c("p25","p50","p75","p90","p95","p99"),
  actual_miles               = c(3.7, 8.4, 17.8, 31.8, 50.5, 140.7),
  nearest_urps_capable_miles = c(2.7, 5.3, 10.8, 22.4, 34.1, 102.6),
  nearest_any_hospital_miles = c(0.0, 2.9,  5.1,  8.2, 12.1,  86.3)
)

# Share travelling more than 10 miles past their nearest urogyn-capable site.
CHIA_UROGYN_BYPASS_RATE <- 0.202

#' Urogynaecology-specific travel and availability
#'
#' Restricted to operations performed by board-certified URPS surgeons, because
#' not every hospital offers urogynaecology. Use `"capable"` -- not `"any"` --
#' as the availability denominator for any urogynaecologic access surface.
#'
#' @param what One of "actual" (where patients went), "capable" (nearest
#'   urogyn-capable hospital), "any" (nearest hospital of any kind), or
#'   "quantiles" (all three, in miles).
#' @return Named numeric vector indexed by upper mile bound, or a data.frame
#'   for "quantiles".
#' @examples
#' chia_urogyn_travel("capable")   # the right denominator
#' chia_urogyn_travel("any")       # what using all hospitals would assume
#' @export
chia_urogyn_travel <- function(what = c("actual", "capable", "any", "quantiles")) {
  what <- match.arg(what)
  switch(what,
    actual    = CHIA_UROGYN_TRAVEL_ACTUAL,
    capable   = CHIA_UROGYN_NEAREST_CAPABLE,
    any       = CHIA_UROGYN_NEAREST_ANY,
    quantiles = CHIA_UROGYN_TRAVEL_QUANTILES
  )
}

#' Provenance for the CHIA travel kernel
#' @export
chia_travel_kernel_provenance <- function() {
  list(
    source             = "MA CHIA Case Mix Hospital Inpatient Discharge Database",
    years              = "FY2007-2018",
    cohort             = "female, 18+, operative principal procedure, newborn excluded",
    n_operations       = 1639630L,
    geocoded_pct       = 99.0,
    distance_measured  = TRUE,
    drivetime_measured = FALSE,
    drivetime_note     = "miles * 1.3 circuity / 40 mph; <=30 share 0.646-0.790 over 30-50 mph",
    setting            = "hospital inpatient only -- no ambulatory surgery",
    calibration_tier   = "observed_regional",
    substitutes_e2sfca = FALSE,
    builder            = "scripts/chia/build_chia_surgical_travel_kernel.R",
    urogyn_builder     = "scripts/chia/build_chia_urogyn_travel_kernel.R",
    urogyn_n           = 9081L,
    urogyn_sites       = 38L,
    urogyn_definition  = "operator-based: board-certified URPS surgeon",
    appendix           = "docs/CHIA_TECHNICAL_APPENDIX.md"
  )
}
