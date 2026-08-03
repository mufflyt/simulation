# Supply-Side Calibration Loaders (real, vetted inputs) ----
#
# Loaders for the empirical supply-side calibration data carried over from the
# `cliff` repository and registered in config/canonical_sources.yml. These let
# the microsimulation run on OBSERVED URPS numbers instead of the illustrative
# defaults in R/12 / R/16:
#
#   * empirical age-band DEPARTURE hazards (cliff's 6-source signal, validated
#     vs state medical boards at 94.4% agreement),
#   * the age-PRODUCTIVITY curve (for effective-FTE validation),
#   * NRMP filled fellowship positions (real annual entrants).
#
# Everything is ADDITIVE and opt-in: the HWSM/FutureDocs literature schedules in
# R/16 remain the default (their survival anchors are test-pinned), and
# build_hazard_table()/MICROSIM_REFERENCE_HAZARD in R/12 are untouched. Callers
# choose the empirical inputs explicitly.
#
# Key data caveat, handled here so no caller re-discovers it: the empirical 70+
# cell is 0 events / 16 person-years across every subspecialty -- a sparse
# artifact, NOT evidence that no one retires after 70. Band-level reads floor it;
# the single-year schedule uses the HWSM tail past 70, where cliff has no data.

# Which hazard column of hazard_by_band_pooled_vs_unpooled.csv each pooling
# choice reads. "urps" = URPS-only observed; "pooled" = GO+URPS partial pool;
# "pooled_migs" = partial pool including MIGS; "go" = GO-only observed.
.HAZARD_POOLING_COLUMN <- c(
  urps = "urps_hazard_unpooled",
  pooled = "pooled_hazard",
  pooled_migs = "pooled_incl_migs_hazard",
  go = "go_hazard_unpooled"
)

#' Empirical age-band URPS departure hazards (with the sparse-70+ guard)
#'
#' Reads the canonical pooled-hazard file and returns annual departure hazards
#' over the model's age bands (`MICROSIM_AGE_BAND_LABELS`). Because the observed
#' 70+ cell is 0/16, hazards are made monotone non-decreasing from the `60-64`
#' band onward, so `70+` inherits the `65-69` hazard instead of collapsing to 0.
#'
#' @param pooling One of "urps" (default), "pooled", "pooled_migs", "go".
#' @param floor_sparse If TRUE (default), apply the monotone 70+ guard.
#' @param mode Reproducibility mode (SHA-256 drift handling in the resolver).
#' @return Named numeric vector of annual hazards over `MICROSIM_AGE_BAND_LABELS`.
#' @export
urps_empirical_hazard_by_ageband <- function(pooling = c("urps", "pooled", "pooled_migs", "go"),
                                             floor_sparse = TRUE,
                                             mode = resolve_reproducibility_mode()) {
  pooling <- match.arg(pooling)
  col <- .HAZARD_POOLING_COLUMN[[pooling]]
  path <- resolve_canonical("urps_hazard_pooled", mode = mode)
  df <- utils::read.csv(path, stringsAsFactors = FALSE)

  if (!all(c("band", col) %in% names(df))) {
    stop(sprintf("Pooled-hazard file missing column(s): %s",
                 paste(setdiff(c("band", col), names(df)), collapse = ", ")), call. = FALSE)
  }

  h <- stats::setNames(df[[col]], df$band)[MICROSIM_AGE_BAND_LABELS]
  if (anyNA(h)) {
    stop("Pooled-hazard file does not cover every model age band", call. = FALSE)
  }

  if (floor_sparse) {
    # Monotone non-decreasing from the retirement region so the 0/16 70+ cell
    # cannot read as "nobody retires after 70".
    from <- match("60-64", MICROSIM_AGE_BAND_LABELS)
    idx <- from:length(h)
    h[idx] <- cummax(h[idx])
  }
  h
}

#' Empirical single-year URPS retirement schedule (observed 50-69 + HWSM tail)
#'
#' Builds a single-year hazard schedule keyed by age (50..terminal-1) for use as
#' the `retirement_schedule` argument to the supply engine. Ages 50-69 use the
#' empirical URPS band hazards; ages 70+ use the HWSM physician tail
#' (`RETIREMENT_HAZARD_PHYSICIAN`) because cliff has no observed departures past
#' 70 (the sparse cell). This is a principled hybrid: observed where observed,
#' literature where the data run out.
#'
#' @param pooling Hazard pooling choice (see [urps_empirical_hazard_by_ageband()]).
#' @param tail_schedule Single-year schedule supplying the 70+ tail.
#' @param mode Reproducibility mode.
#' @return Named numeric vector of single-year hazards (names = ages as strings).
#' @export
urps_empirical_retirement_schedule <- function(pooling = "urps",
                                               tail_schedule = RETIREMENT_HAZARD_PHYSICIAN,
                                               mode = resolve_reproducibility_mode()) {
  h <- urps_empirical_hazard_by_ageband(pooling = pooling, floor_sparse = TRUE, mode = mode)

  band_for_age <- list("50-54" = 50:54, "55-59" = 55:59, "60-64" = 60:64, "65-69" = 65:69)
  observed <- unlist(lapply(names(band_for_age), function(b) {
    stats::setNames(rep(h[[b]], length(band_for_age[[b]])), band_for_age[[b]])
  }))

  # Tail (70+): take every age in the literature schedule at/above 70.
  tail_ages <- as.integer(names(tail_schedule))
  tail <- tail_schedule[tail_ages >= 70]

  sched <- c(observed, tail)
  sched[order(as.integer(names(sched)))]
}

#' Observed URPS age-band person-years and events (for CIs / weighting)
#'
#' @param mode Reproducibility mode.
#' @return Tibble `age_band`, `person_years`, `events`, `annual_hazard`.
#' @export
urps_hazard_exposure <- function(mode = resolve_reproducibility_mode()) {
  path <- resolve_canonical("urps_retirement_hazard_ageband", mode = mode)
  df <- utils::read.csv(path, stringsAsFactors = FALSE)
  tibble::as_tibble(df)
}

#' Real annual fellowship entrants from the NRMP match
#'
#' @param subspecialty Subspecialty label; "FPMRS" is treated as "URPS".
#' @param mode Reproducibility mode.
#' @return Integer annual filled positions for the subspecialty.
#' @export
nrmp_entrants <- function(subspecialty = "URPS", mode = resolve_reproducibility_mode()) {
  abbrev <- if (toupper(subspecialty) == "FPMRS") "URPS" else toupper(subspecialty)
  path <- resolve_canonical("urps_nrmp_entrants", mode = mode)
  df <- utils::read.csv(path, stringsAsFactors = FALSE)
  row <- df[df$subspecialty_abbrev == abbrev, , drop = FALSE]
  if (nrow(row) == 0) {
    stop(sprintf("NRMP entrants: no row for subspecialty '%s'", abbrev), call. = FALSE)
  }
  as.integer(row$annual_entrants[1])
}

#' Age-productivity curve for effective-FTE validation
#'
#' The current model defines FTE as an hours threshold (R/16), not a productivity
#' step function, so this is a VALIDATION reference (and an override for the
#' deprecated `legacy_weight` FTE method) rather than the default.
#'
#' @param mode Reproducibility mode.
#' @return Tibble `age`, `rel_to_peak`, `work_usd`.
#' @export
urps_age_productivity_curve <- function(mode = resolve_reproducibility_mode()) {
  path <- resolve_canonical("urps_age_productivity", mode = mode)
  df <- utils::read.csv(path, stringsAsFactors = FALSE)
  tibble::as_tibble(df)
}
