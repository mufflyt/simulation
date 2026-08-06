# Obstetric-exposure demand estimand (D4) ----
#
# WHY THIS EXISTS
# R/13-demand_urps.R's own design note states the limitation directly: the
# age-only "women 65+" (and age-band) demand denominators are "a convenient
# proxy for urogynaecology and understate demand from parous women in their
# 40s-60s." And HDMM_IMPROVEMENT_PLAN.md proposes rate models that carry
# `VaginalParity` as a covariate but has no principled source for a vaginal-parity
# distribution by birth cohort.
#
# This module supplies both: a cited birth-cohort vaginal-delivery exposure
# series, and a D4 demand estimand that weights each age band's PFD burden by the
# cohort's cumulative vaginal-delivery exposure RELATIVE to the Nygaard-era
# reference cohort. It is ADDITIVE -- D1/D2/D3 and compute_demand_denominators()
# are untouched -- and D4 diverges from D1 over time by construction (later high-
# age-band cohorts had fewer vaginal deliveries: falling parity + rising cesarean),
# so it is informative, not a rescaling.
#
# PORTED FROM cliff/demand_lifecourse (spec: cliff/DEMAND_LIFECOURSE_MODEL_SPEC.md;
# evidence & citations: cliff/demand_lifecourse/PARAMETERS_EVIDENCE.md).
#
# DERIVATION & ASSUMPTIONS (documented; refine with NSFG microdata)
#   No off-the-shelf US table gives the per-woman joint distribution of vaginal
#   vs cesarean deliveries by cohort, so mean vaginal exposure is DERIVED:
#     mean_vaginal_deliveries(cohort)
#         = mean_completed_parity(cohort) * (1 - cohort_cesarean_fraction(cohort))
#   where cohort_cesarean_fraction is the mean US total-cesarean rate over the
#   cohort's peak childbearing window (ages 20-35). Assumes the period cesarean
#   rate applies uniformly per birth (ignores repeat-cesarean correlation) and
#   even spacing of births -- both bias the vaginal/cesarean split, not total
#   parity, and are transparent levers for a "changing mode of delivery" scenario.
#
# Data (cited): inst/extdata/obstetric/{us_cesarean_rate_by_year,
#   us_completed_parity_by_cohort,parity_disease_dose_response}.csv

OBSTETRIC_CHILDBEAR_AGE_LO <- 20L
OBSTETRIC_CHILDBEAR_AGE_HI <- 35L

# Dose-response used to convert a difference in mean vaginal deliveries into a
# relative PFD-burden multiplier. Central per-additional-vaginal-birth odds ratio
# for pelvic organ prolapse. CITED, PROVISIONAL (verify full text):
#   Hendrix SL et al. AJOG 2002 (WHI): per additional birth OR ~1.10 (uterine
#   prolapse) to ~1.21 (cystocele/rectocele). A central 1.20 is used and is
#   configurable via the `or_per_vaginal_delivery` argument.
OBSTETRIC_POP_OR_PER_VAGINAL_DELIVERY <- 1.20

# Age spans for the demand bands (top band capped at 95 for a finite mean).
.obstetric_band_ages <- function(band) {
  switch(band,
    "20-39" = 20:39, "40-59" = 40:59, "60-64" = 60:64,
    "65-79" = 65:79, "80+" = 80:95,
    stop(sprintf("obstetric exposure: unknown age band '%s'", band), call. = FALSE))
}

.obstetric_extdata <- function(file) {
  p <- system.file("extdata", "obstetric", file, package = "urpssim")
  if (!nzchar(p)) p <- file.path("inst/extdata/obstetric", file)   # dev (load_all)
  if (!file.exists(p))
    stop(sprintf("obstetric exposure data not found: %s", file), call. = FALSE)
  utils::read.csv(p, stringsAsFactors = FALSE)
}

#' US total-cesarean rate, interpolated across cited anchor years
#' @param years Integer calendar years.
#' @return Numeric cesarean fraction per year (clamped at the anchor range ends).
#' @export
cesarean_rate_for_year <- function(years) {
  ces <- .obstetric_extdata("us_cesarean_rate_by_year.csv")
  ces <- ces[order(ces$year), ]
  stats::approx(ces$year, ces$cesarean_rate, xout = years, rule = 2)$y
}

#' Mean completed parity by birth cohort, interpolated across cited anchors
#' @param cohorts Integer birth-cohort years.
#' @return Numeric mean completed parity (clamped outside the anchor range).
#' @keywords internal
completed_parity_for_cohort <- function(cohorts) {
  par <- .obstetric_extdata("us_completed_parity_by_cohort.csv")
  par <- par[order(par$birth_cohort), ]
  stats::approx(par$birth_cohort, par$mean_completed_parity, xout = cohorts, rule = 2)$y
}

#' Cohort vaginal-delivery exposure
#'
#' Derives mean vaginal and cesarean deliveries per woman for each birth cohort
#' (see module header for the derivation and its assumptions).
#'
#' @param cohorts Integer birth cohorts.
#' @return Tibble: `birth_cohort`, `mean_total_parity`, `cohort_cesarean_fraction`,
#'   `mean_vaginal_deliveries`, `mean_cesarean_deliveries`.
#' @export
cohort_vaginal_exposure <- function(cohorts) {
  cohorts <- as.integer(cohorts)
  ces_frac <- vapply(cohorts, function(c0) {
    yrs <- (c0 + OBSTETRIC_CHILDBEAR_AGE_LO):(c0 + OBSTETRIC_CHILDBEAR_AGE_HI)
    mean(cesarean_rate_for_year(yrs))
  }, numeric(1))
  parity <- completed_parity_for_cohort(cohorts)
  tibble::tibble(
    birth_cohort             = cohorts,
    mean_total_parity        = round(parity, 3),
    cohort_cesarean_fraction = round(ces_frac, 4),
    mean_vaginal_deliveries  = round(parity * (1 - ces_frac), 3),
    mean_cesarean_deliveries = round(parity * ces_frac, 3)
  )
}

# Mean vaginal deliveries for the cohorts occupying `band` in calendar `year`.
.band_mean_vaginal <- function(year, band) {
  ages <- .obstetric_band_ages(band)
  mean(cohort_vaginal_exposure(year - ages)$mean_vaginal_deliveries)
}

#' Obstetric-exposure multiplier by year and age band
#'
#' For each (year, band), the relative PFD-burden multiplier implied by how the
#' band's mean cumulative vaginal-delivery exposure differs from a reference
#' cohort (default: the 65-79 band in the demand base year, i.e. the high-exposure
#' cohort underlying the Nygaard 2008 prevalence). Multiplier = 1 at the
#' reference; < 1 for later, lower-exposure cohorts.
#'
#' @param years Integer years.
#' @param bands Character age bands (subset of `DEMAND_AGE_BANDS`).
#' @param or_per_vaginal_delivery Dose-response OR per additional vaginal
#'   delivery. Default `OBSTETRIC_POP_OR_PER_VAGINAL_DELIVERY` (1.20, cited).
#' @param ref_year,ref_band Reference cohort. Default base year x "65-79".
#' @return Long tibble: `year`, `age_band`, `mean_vaginal_deliveries`,
#'   `exposure_multiplier`.
#' @export
obstetric_exposure_multiplier <- function(years, bands = DEMAND_AGE_BANDS,
                                          or_per_vaginal_delivery =
                                            OBSTETRIC_POP_OR_PER_VAGINAL_DELIVERY,
                                          ref_year = DEMAND_INDEX_BASE_YEAR,
                                          ref_band = "65-79") {
  assertthat::assert_that(or_per_vaginal_delivery > 0)
  ref_vag <- .band_mean_vaginal(ref_year, ref_band)
  grid <- expand.grid(year = years, age_band = bands, stringsAsFactors = FALSE)
  grid$mean_vaginal_deliveries <- mapply(.band_mean_vaginal, grid$year, grid$age_band)
  grid$exposure_multiplier <-
    or_per_vaginal_delivery^(grid$mean_vaginal_deliveries - ref_vag)
  tibble::as_tibble(grid[order(grid$age_band, grid$year), ])
}

#' Demand estimands D1/D2/D3 plus the obstetric-exposure estimand D4
#'
#' Wraps [compute_demand_denominators()] (D1/D2/D3 unchanged) and adds D4:
#' prevalent PFD cases weighted by each cohort's cumulative vaginal-delivery
#' exposure relative to the Nygaard-era reference. D4 answers R/13's documented
#' limitation that age-only denominators understate parous-women demand and that
#' future high-age-band cohorts carry less vaginal-delivery exposure.
#'
#' @param pop_by_band Tibble `year`, `age_band` (in `DEMAND_AGE_BANDS`),
#'   `female_pop`.
#' @param pfd_prevalence Named age-band prevalence for D1/D4.
#' @param or_per_vaginal_delivery Dose-response OR for the D4 exposure weight.
#' @param ... Passed to [compute_demand_denominators()].
#' @return Long tibble `year`, `estimand` (D1-D4), `label`, `demand_cases`.
#' @export
compute_demand_denominators_lifecourse <- function(pop_by_band,
                                                   pfd_prevalence = pfd_prevalence_by_band(),
                                                   or_per_vaginal_delivery =
                                                     OBSTETRIC_POP_OR_PER_VAGINAL_DELIVERY,
                                                   ...) {
  base <- compute_demand_denominators(pop_by_band, pfd_prevalence = pfd_prevalence, ...)

  mult <- obstetric_exposure_multiplier(
    years = sort(unique(pop_by_band$year)),
    bands = DEMAND_AGE_BANDS,
    or_per_vaginal_delivery = or_per_vaginal_delivery)

  d4 <- pop_by_band %>%
    dplyr::mutate(prev = unname(pfd_prevalence[.data$age_band])) %>%
    dplyr::left_join(mult, by = c("year", "age_band")) %>%
    dplyr::group_by(.data$year) %>%
    dplyr::summarise(
      demand_cases = sum(.data$female_pop * .data$prev * .data$exposure_multiplier,
                         na.rm = TRUE),
      .groups = "drop") %>%
    dplyr::mutate(
      estimand = "D4",
      label = "Obstetric-exposure-weighted PFD cases (birth-cohort vaginal parity)") %>%
    dplyr::select("year", "estimand", "label", "demand_cases")

  dplyr::bind_rows(base, d4) %>%
    dplyr::arrange(.data$estimand, .data$year)
}
