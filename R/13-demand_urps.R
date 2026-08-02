# URPS/FPMRS Demand Denominators & Sensitivity ----
#
# Ported from cliff (R/demand_denominator.R + scripts/urps_demand_denominators_sensitivity.R).
# The existing workforce.R demand model uses a SINGLE denominator
# (women 65+ x visit rate). A single denominator invites the objection that it
# drives the conclusion. cliff's contribution: carry THREE independent demand
# estimands through the same supply comparison and report their CONCORDANCE,
# never a blended number:
#
#   D1  prevalent PFD cases          (Nygaard 2008 JAMA / Wu 2009) -- burden
#   D2  new specialty consultations  (Kirby 2013)                  -- clinic load
#   D3  SUI + POP surgical volume    (Wu 2011)                     -- OR load
#
# Demand is driven by the aging population (women 65+, where pelvic floor
# disorders concentrate). All series are rebased to the projection base year and
# compared to supply via coverage = 100 * supply / demand; robustness is the
# agreement (Spearman rho) across the three coverage curves.

# ---- Demand constants (SSOT, guarded) -------------------------------------

DEMAND_AGE_MIN <- 65L                 # cliff DEMAND_AGE_MIN
DEMAND_INDEX_BASE_YEAR <- 2025L       # cliff DEMAND_INDEX_BASE_YEAR (= supply WC_YEAR0)

# D1: prevalence of >=1 pelvic floor disorder among women 65+ (Nygaard 2008
# reports 23.7% across all adult women, rising steeply with age; ~0.40 for 65+).
PFD_PREVALENCE_65PLUS <- 0.40

# D2: new urogynecology consultations per woman 65+ per year (Kirby 2013 scale).
CONSULT_RATE_PER_WOMAN_65PLUS <- 0.045

# D3: age-specific SUI+POP surgery rate per 1,000 women (Wu 2011, four bands).
WU2011_SURGERY_RATE_PER_1000 <- c(
  "20-39" = 1.5,
  "40-59" = 4.6,
  "60-79" = 6.3,
  "80+"   = 3.2
)

stopifnot(
  PFD_PREVALENCE_65PLUS > 0 && PFD_PREVALENCE_65PLUS <= 1,
  CONSULT_RATE_PER_WOMAN_65PLUS > 0,
  all(WU2011_SURGERY_RATE_PER_1000 >= 0)
)

# ---- Index helpers --------------------------------------------------------

#' Build a constant-geometric-growth index between two published anchors
#'
#' Port of cliff::anchor_index. Given two anchor totals (y0,v0) and (y1,v1),
#' fits a constant annual growth rate and returns the index over `years`,
#' normalised so the value at `base` equals 1.
#'
#' @param years Integer vector of years to index.
#' @param y0,v0 First anchor (year, value).
#' @param y1,v1 Second anchor (year, value).
#' @param base Base year at which the index equals 1.
#' @return Numeric index vector aligned to `years`.
#' @export
anchor_index <- function(years, y0, v0, y1, v1, base = DEMAND_INDEX_BASE_YEAR) {
  assertthat::assert_that(y1 != y0, v0 > 0, v1 > 0)
  g <- (v1 / v0)^(1 / (y1 - y0))        # constant annual growth factor
  raw <- v0 * g^(years - y0)
  base_val <- v0 * g^(base - y0)
  raw / base_val
}

#' Rebase a numeric series so its value in `base_year` equals 1
#' @param year Integer years.
#' @param value Numeric values.
#' @param base_year Year to rebase on.
#' @return Numeric rebased series.
#' @export
rebase_to_year <- function(year, value, base_year = DEMAND_INDEX_BASE_YEAR) {
  base_val <- value[year == base_year]
  if (length(base_val) != 1 || is.na(base_val) || base_val == 0) {
    logger::log_warn("rebase_to_year: base year {base_year} not found / zero; using first value")
    base_val <- value[which(!is.na(value))[1]]
  }
  value / base_val
}

# ---- Three demand estimands ------------------------------------------------

#' Compute D1/D2/D3 demand estimands from a women-65+ population series
#'
#' @param population_65plus Tibble with `year` and `population_65_plus` columns.
#' @param pfd_prevalence D1 prevalence among women 65+.
#' @param consult_rate D2 consultations per woman 65+ per year.
#' @param surgery_rate_per_1000 D3 crude surgery rate per 1,000 women 65+
#'   (used when age-band detail is unavailable; see
#'   [apply_age_specific_surgery_demand()] for the Wu 2011 age-specific path).
#' @return Long tibble: `year`, `estimand` (D1/D2/D3), `demand_cases`, `label`.
#' @export
compute_demand_denominators <- function(population_65plus,
                                         pfd_prevalence = PFD_PREVALENCE_65PLUS,
                                         consult_rate = CONSULT_RATE_PER_WOMAN_65PLUS,
                                         surgery_rate_per_1000 = 5.0) {
  assertthat::assert_that(is.data.frame(population_65plus))
  assertthat::assert_that(all(c("year", "population_65_plus") %in% names(population_65plus)))

  pop <- population_65plus$population_65_plus
  yr <- population_65plus$year

  dplyr::bind_rows(
    tibble::tibble(year = yr, estimand = "D1", label = "Prevalent PFD cases (Nygaard 2008)",
                   demand_cases = pop * pfd_prevalence),
    tibble::tibble(year = yr, estimand = "D2", label = "New consultations (Kirby 2013)",
                   demand_cases = pop * consult_rate),
    tibble::tibble(year = yr, estimand = "D3", label = "SUI+POP surgical volume (Wu 2011)",
                   demand_cases = pop * surgery_rate_per_1000 / 1000)
  )
}

#' Age-specific surgical demand (Wu 2011)
#'
#' Port of cliff::apply_age_specific_surgery_demand. Applies the four-band
#' Wu 2011 surgery rates to an age-banded female population, yielding an
#' age-resolved D3 that does not assume a single crude rate.
#'
#' @param pop_by_band Tibble with `year`, `age_band` (matching the names of
#'   `rates`), and `female_pop`.
#' @param rates Named per-1,000 surgery rates by age band.
#' @return Tibble `year`, `surgical_cases` (summed over bands).
#' @export
apply_age_specific_surgery_demand <- function(pop_by_band,
                                              rates = WU2011_SURGERY_RATE_PER_1000) {
  assertthat::assert_that(all(c("year", "age_band", "female_pop") %in% names(pop_by_band)))
  pop_by_band %>%
    dplyr::mutate(rate = unname(rates[.data$age_band])) %>%
    dplyr::filter(!is.na(.data$rate)) %>%
    dplyr::group_by(.data$year) %>%
    dplyr::summarise(surgical_cases = sum(.data$female_pop * .data$rate / 1000), .groups = "drop")
}

# ---- Coverage, adequacy, concordance --------------------------------------

#' Compute supply-vs-demand coverage for each demand estimand
#'
#' coverage = 100 * supply / demand, with each series expressed in comparable
#' provider-equivalent units by rebasing both supply and demand to the base year
#' and reporting the ratio (cliff adequacy). Also returns the raw coverage %.
#'
#' @param supply Tibble with `year` and a supply column.
#' @param demand_long Long demand tibble ([compute_demand_denominators()]).
#' @param supply_col Name of the supply column to use.
#' @param base_year Rebase year.
#' @return Long tibble: `year`, `estimand`, `coverage_pct`, `adequacy`.
#' @export
compute_demand_coverage <- function(supply, demand_long,
                                    supply_col = "effective_fte_median",
                                    base_year = DEMAND_INDEX_BASE_YEAR) {
  assertthat::assert_that(supply_col %in% names(supply))

  supply2 <- supply %>%
    dplyr::transmute(year = .data$year, supply = .data[[supply_col]]) %>%
    dplyr::mutate(supply_index = rebase_to_year(.data$year, .data$supply, base_year))

  demand_long %>%
    dplyr::group_by(.data$estimand) %>%
    dplyr::mutate(demand_index = rebase_to_year(.data$year, .data$demand_cases, base_year)) %>%
    dplyr::ungroup() %>%
    safe_left_join(supply2, by = "year") %>%
    dplyr::mutate(
      coverage_pct = 100 * .data$supply / .data$demand_cases,
      adequacy = .data$supply_index / .data$demand_index
    ) %>%
    dplyr::select("year", "estimand", "label", "coverage_pct", "adequacy",
                  "supply_index", "demand_index")
}

#' Assess concordance of the adequacy conclusion across demand estimands
#'
#' Port of cliff's concordance verdict: rank-correlate the coverage curves
#' (Spearman rho) and check whether the adequacy conclusion is invariant to the
#' choice of demand denominator (a reviewer-proofing robustness statement).
#'
#' @param coverage Long coverage tibble ([compute_demand_coverage()]).
#' @return List: `spearman` (pairwise rho matrix), `min_adequacy_by_estimand`,
#'   `robust` (logical: adequacy >= 1 in the final year under ALL estimands),
#'   `trough_year` (year of minimum adequacy, worst estimand).
#' @export
assess_demand_concordance <- function(coverage) {
  wide <- coverage %>%
    dplyr::select("year", "estimand", "coverage_pct") %>%
    tidyr::pivot_wider(names_from = "estimand", values_from = "coverage_pct") %>%
    dplyr::arrange(.data$year)

  estimand_cols <- setdiff(names(wide), "year")
  rho <- suppressWarnings(stats::cor(wide[estimand_cols], method = "spearman",
                                     use = "pairwise.complete.obs"))

  final_year <- max(coverage$year)
  final_adeq <- coverage %>%
    dplyr::filter(.data$year == final_year) %>%
    dplyr::select("estimand", "adequacy")

  min_adeq <- coverage %>%
    dplyr::group_by(.data$estimand) %>%
    dplyr::summarise(min_adequacy = min(.data$adequacy, na.rm = TRUE),
                     trough_year = .data$year[which.min(.data$adequacy)],
                     .groups = "drop")

  list(
    spearman = rho,
    min_adequacy_by_estimand = min_adeq,
    final_year_adequacy = final_adeq,
    robust = all(final_adeq$adequacy >= 1, na.rm = TRUE),
    trough_year = min_adeq$trough_year[which.min(min_adeq$min_adequacy)]
  )
}

# ---- Backward-compatible visit-based demand -------------------------------

#' Visit-based demand (legacy denominator, retained for continuity)
#'
#' Reproduces the original workforce.R demand model
#' (women 65+ x visit rate / provider hours) so results remain comparable.
#'
#' @param population_65plus Tibble with `year`, `population_65_plus`.
#' @param visits_per_woman_annually Annual visits per woman 65+.
#' @param hours_per_provider_yearly Provider clinical hours per year.
#' @param minutes_per_visit Average minutes per visit.
#' @return Tibble with `year`, `required_fte`.
#' @export
calculate_visit_based_demand <- function(population_65plus,
                                         visits_per_woman_annually = 1.5,
                                         hours_per_provider_yearly = 36 * 48,
                                         minutes_per_visit = 30) {
  hours_per_visit <- minutes_per_visit / 60
  population_65plus %>%
    dplyr::mutate(
      total_visits = .data$population_65_plus * visits_per_woman_annually,
      required_fte = .data$total_visits * hours_per_visit / hours_per_provider_yearly
    ) %>%
    dplyr::select("year", "required_fte")
}
