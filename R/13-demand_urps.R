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

# CRITICAL DESIGN NOTE -------------------------------------------------------
# The original implementation computed all three estimands as
#   demand = population_65plus * constant
# which makes D1, D2 and D3 exact proportional rescalings of ONE series. After
# rebasing, their adequacy curves are bit-identical and the Spearman rho across
# them is exactly 1.000 -- reported as a robustness result when it is pure
# arithmetic. Fraher & Knapton (2017) show what real concordance looks like: two
# models with different demand definitions (ESRD patients per nephrologist vs
# unmet visits by clinical service area), different data, and different
# geographies, agreeing on the qualitative conclusion.
#
# Each estimand now carries its OWN AGE PROFILE, so the three series diverge as
# the age structure of the female population shifts. `assert_estimands_independent()`
# (R/21) is wired into the orchestrator to stop the tautology returning.
#
# The deeper caution also comes from the nephrology comparison: the ASN report
# used Medicare ESRD patients as the measure of need while acknowledging
# "nephrologists provide many services to patients with kidney diseases and
# injury beyond ESRD care". "Women 65+" is the same kind of convenient proxy for
# urogynaecology and understates demand from parous women in their 40s-60s.

# BAND STRUCTURE ------------------------------------------------------------
# The bands were restructured to align EXACTLY with the boundary used by
# `mufflyaccess::pfd_prevalence()`, which owns prevalence for 65-79 and 80+.
# The previous 60-79 band could not take the contract's 65-79 value without a
# silent wrong-grain error, so 60-64 is now split out. Wu 2011 publishes surgery
# rates on a 60-79 band; that single rate is applied to both 60-64 and 65-79,
# which is stated here rather than hidden.
DEMAND_AGE_BANDS <- c("20-39", "40-59", "60-64", "65-79", "80+")

# D1: prevalence of >=1 pelvic floor disorder by age band.
#
# 65-79 and 80+ come from the mufflyaccess contract at load time via
# `pfd_prevalence_by_band()`. The younger bands are NOT in the contract and
# remain Nygaard-derived literals local to this package; `owner` in the returned
# table says which is which so a local literal cannot be mistaken for a
# contract value.
PFD_PREVALENCE_LOCAL_UNDER_65 <- c(
  "20-39" = 0.098,
  "40-59" = 0.269,
  "60-64" = 0.330
)

# D2: new urogynecology consultations per woman per year by age band (Kirby 2013
# scale). No contract equivalent; local.
CONSULT_RATE_BY_AGE <- c(
  "20-39" = 0.004,
  "40-59" = 0.021,
  "60-64" = 0.044,
  "65-79" = 0.048,
  "80+"   = 0.031
)

# D3: age-specific SUI+POP surgery rate per 1,000 women (Wu 2011, four bands).
# The published 60-79 rate of 6.3 is applied to BOTH 60-64 and 65-79. Note the
# DIFFERENT shape from prevalence: surgery peaks at 60-79 and halves at 80+,
# while prevalence keeps rising. That difference is what makes the three
# estimands informative rather than redundant.
WU2011_SURGERY_RATE_PER_1000 <- c(
  "20-39" = 1.5,
  "40-59" = 4.6,
  "60-64" = 6.3,
  "65-79" = 6.3,
  "80+"   = 3.2
)

#' PFD prevalence by demand age band
#'
#' 65-79 and 80+ are read from `mufflyaccess::pfd_prevalence()`; the younger
#' bands are local literals. Falls back entirely to local values when the
#' contract is unavailable, and says so.
#'
#' @param condition Condition passed to the contract.
#' @return Named numeric vector over `DEMAND_AGE_BANDS`.
#' @export
pfd_prevalence_by_band <- function(condition = "any_PFD") {
  local_65plus <- c("65-79" = 0.368, "80+" = 0.497)
  if (!has_mufflyaccess()) {
    .msg_warn("mufflyaccess unavailable: using local PFD prevalence for 65+ ",
              "instead of the contract value.")
    return(c(PFD_PREVALENCE_LOCAL_UNDER_65, local_65plus)[DEMAND_AGE_BANDS])
  }
  ssot <- ssot_pfd_prevalence(condition)
  from_ssot <- stats::setNames(ssot$prevalence, ssot$age_band)
  c(PFD_PREVALENCE_LOCAL_UNDER_65, from_ssot)[DEMAND_AGE_BANDS]
}

#' Which age bands are owned by the contract
#' @return Tibble of `age_band` and `owner`.
#' @export
pfd_prevalence_ownership <- function() {
  tibble::tibble(
    age_band = DEMAND_AGE_BANDS,
    owner = ifelse(DEMAND_AGE_BANDS %in% c("65-79", "80+"),
                   "mufflyaccess::pfd_prevalence()", "local (Nygaard-derived)")
  )
}

# Retained for backward compatibility with the crude single-rate path.
PFD_PREVALENCE_65PLUS <- 0.40
CONSULT_RATE_PER_WOMAN_65PLUS <- 0.045

stopifnot(
  all(CONSULT_RATE_BY_AGE > 0),
  all(WU2011_SURGERY_RATE_PER_1000 >= 0),
  all(PFD_PREVALENCE_LOCAL_UNDER_65 > 0 & PFD_PREVALENCE_LOCAL_UNDER_65 <= 1),
  identical(names(CONSULT_RATE_BY_AGE), DEMAND_AGE_BANDS),
  identical(names(WU2011_SURGERY_RATE_PER_1000), DEMAND_AGE_BANDS)
)

# ---- Census NPP population loader (real demand driver) --------------------
#
# The example population (example_female_population_by_band) is a placeholder.
# The real demand driver is the Census 2023 National Population Projections
# single-year-of-age file (np2023_d1_<series>.csv), resolved through the
# canonical-source registry so the path + SHA-256 live in exactly one place
# (config/canonical_sources.yml, keys census_npp_female_{mid,low,hi}).
#
# Age-band aggregation maps DEMAND_AGE_BANDS onto the file's POP_<age> columns.
# The female-population filter (SEX==2 & ORIGIN==0 & RACE==0) is cliff's SSOT
# (R/demand_denominator.R::npp_total_female): a wrong ORIGIN/RACE code silently
# narrows the denominator to a subgroup, so it is guarded, not inlined ad hoc.

# Inclusive single-year age bounds for each DEMAND_AGE_BAND (top bucket = 100+).
NPP_AGE_BAND_BOUNDS <- list(
  "20-39" = c(20, 39), "40-59" = c(40, 59), "60-64" = c(60, 64),
  "65-79" = c(65, 79), "80+"   = c(80, 100)
)
NPP_MAX_AGE <- 100L
stopifnot(identical(names(NPP_AGE_BAND_BOUNDS), DEMAND_AGE_BANDS))

#' Select total US female rows from a Census NPP table (cliff SSOT filter)
#'
#' @param df Data frame read from an NPP CSV (integer SEX/ORIGIN/RACE columns).
#' @return The all-origins, all-races female rows (one per projected year).
#' @keywords internal
npp_total_female <- function(df) {
  need <- c("SEX", "ORIGIN", "RACE", "YEAR")
  missing <- setdiff(need, names(df))
  if (length(missing) > 0) {
    stop(sprintf("NPP file missing column(s): %s", paste(missing, collapse = ", ")),
         call. = FALSE)
  }
  df[df$SEX == 2 & df$ORIGIN == 0 & df$RACE == 0, , drop = FALSE]
}

#' Load the Census-NPP female population by DEMAND_AGE_BAND
#'
#' Reads the canonical NPP series, applies the SSOT female filter, and sums the
#' single-year POP_<age> columns into the model's demand age bands.
#'
#' @param series One of "mid" (primary), "low", or "hi" (the NPP low/mid/high
#'   variants that drive demand uncertainty).
#' @param years Optional integer year filter (default: all years in the file).
#' @param mode Reproducibility mode (governs SHA-256 drift handling in the
#'   resolver).
#' @return Tibble `year`, `age_band` (a `DEMAND_AGE_BANDS` factor order), `female_pop`.
#' @export
npp_female_by_band <- function(series = c("mid", "low", "hi"),
                               years = NULL,
                               mode = resolve_reproducibility_mode()) {
  series <- match.arg(series)
  path <- resolve_canonical(sprintf("census_npp_female_%s", series), mode = mode)
  df <- npp_total_female(utils::read.csv(path))

  band_totals <- lapply(DEMAND_AGE_BANDS, function(b) {
    bounds <- NPP_AGE_BAND_BOUNDS[[b]]
    cols <- sprintf("POP_%d", bounds[1]:bounds[2])
    missing <- setdiff(cols, names(df))
    if (length(missing) > 0) {
      stop(sprintf("NPP file missing age column(s) for band %s: %s",
                   b, paste(missing, collapse = ", ")), call. = FALSE)
    }
    tibble::tibble(year = df$YEAR, age_band = b,
                   female_pop = rowSums(df[, cols, drop = FALSE]))
  })

  out <- dplyr::bind_rows(band_totals)
  if (!is.null(years)) out <- dplyr::filter(out, .data$year %in% years)
  out$age_band <- factor(out$age_band, levels = DEMAND_AGE_BANDS)
  dplyr::arrange(out, .data$year, .data$age_band) %>%
    dplyr::mutate(age_band = as.character(.data$age_band))
}

#' Load Census-NPP women 65+ (crude single-denominator path)
#'
#' @inheritParams npp_female_by_band
#' @return Tibble `year`, `population_65_plus`.
#' @export
npp_women_65plus <- function(series = c("mid", "low", "hi"),
                             years = NULL,
                             mode = resolve_reproducibility_mode()) {
  npp_female_by_band(series, years, mode) %>%
    dplyr::filter(.data$age_band %in% c("65-79", "80+")) %>%
    dplyr::group_by(.data$year) %>%
    dplyr::summarise(population_65_plus = sum(.data$female_pop), .groups = "drop")
}

#' Resolve the demand population, preferring real NPP data with example fallback
#'
#' Returns the canonical Census-NPP age-banded female population when the file is
#' registered and present; otherwise falls back to
#' [example_female_population_by_band()] (and, in `strict` mode, re-raises so an
#' uncalibrated run cannot masquerade as a result).
#'
#' @param years Integer years to return.
#' @param series NPP series (default "mid").
#' @param mode Reproducibility mode.
#' @return List: `pop_by_band` (tibble) and `source` ("census_npp_<series>" or
#'   "example").
#' @export
resolve_demand_population <- function(years = 2025:2050,
                                      series = "mid",
                                      mode = resolve_reproducibility_mode()) {
  attempt <- tryCatch(
    npp_female_by_band(series, years = years, mode = mode),
    error = function(e) e
  )
  if (inherits(attempt, "error")) {
    if (identical(mode, "strict")) {
      stop(sprintf("resolve_demand_population(strict): NPP series '%s' unavailable: %s",
                   series, conditionMessage(attempt)), call. = FALSE)
    }
    .msg_warn(sprintf("Census NPP '%s' unavailable (%s); using EXAMPLE population",
                      series, conditionMessage(attempt)))
    return(list(pop_by_band = example_female_population_by_band(years),
                source = "example"))
  }
  list(pop_by_band = attempt, source = sprintf("census_npp_%s", series))
}

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
    .msg_warn(sprintf("rebase_to_year: base year %s not found / zero; using first value",
                      format(base_year)))
    base_val <- value[which(!is.na(value) & value != 0)[1]]
  }
  if (!is.finite(base_val) || base_val == 0) {
    stop("rebase_to_year: no non-zero base value is available, so the series ",
         "cannot be rebased. Dividing by zero here would produce an index of ",
         "Inf that reads as a real number downstream.", call. = FALSE)
  }
  value / base_val
}

# ---- Three demand estimands ------------------------------------------------

#' Compute D1/D2/D3 demand estimands from an AGE-BANDED female population
#'
#' Each estimand applies its own age-specific rate schedule, so the three series
#' respond differently to a shifting age structure. This is the sanctioned entry
#' point; [compute_demand_denominators_crude()] reproduces the old single-rate
#' behaviour and is retained only for backward comparison.
#'
#' @param pop_by_band Tibble with `year`, `age_band` (one of `DEMAND_AGE_BANDS`)
#'   and `female_pop`.
#' @param pfd_prevalence Named age-band prevalence for D1.
#' @param consult_rate Named age-band consultation rate for D2.
#' @param surgery_rate_per_1000 Named age-band surgery rate per 1,000 for D3.
#' @return Long tibble: `year`, `estimand` (D1/D2/D3), `label`, `demand_cases`.
#' @export
compute_demand_denominators <- function(pop_by_band,
                                        pfd_prevalence = pfd_prevalence_by_band(),
                                        consult_rate = CONSULT_RATE_BY_AGE,
                                        surgery_rate_per_1000 = WU2011_SURGERY_RATE_PER_1000) {
  assertthat::assert_that(is.data.frame(pop_by_band))
  assertthat::assert_that(all(c("year", "age_band", "female_pop") %in% names(pop_by_band)))

  unknown <- setdiff(unique(pop_by_band$age_band), DEMAND_AGE_BANDS)
  if (length(unknown) > 0) {
    stop(sprintf("compute_demand_denominators: unknown age band(s): %s. Expected: %s",
                 paste(unknown, collapse = ", "), paste(DEMAND_AGE_BANDS, collapse = ", ")),
         call. = FALSE)
  }

  band_sum <- function(rates, scale = 1) {
    pop_by_band %>%
      dplyr::mutate(rate = unname(rates[.data$age_band])) %>%
      dplyr::group_by(.data$year) %>%
      dplyr::summarise(demand_cases = sum(.data$female_pop * .data$rate * scale,
                                          na.rm = TRUE),
                       .groups = "drop")
  }

  dplyr::bind_rows(
    dplyr::mutate(band_sum(pfd_prevalence), estimand = "D1",
                  label = "Prevalent PFD cases (Nygaard 2008, age-specific)"),
    dplyr::mutate(band_sum(consult_rate), estimand = "D2",
                  label = "New consultations (Kirby 2013 scale, age-specific)"),
    dplyr::mutate(band_sum(surgery_rate_per_1000, scale = 1 / 1000), estimand = "D3",
                  label = "SUI+POP surgical volume (Wu 2011, age-specific)")
  ) %>%
    dplyr::select("year", "estimand", "label", "demand_cases") %>%
    dplyr::arrange(.data$estimand, .data$year)
}

#' Crude single-rate demand estimands (deprecated)
#'
#' Reproduces the previous behaviour, in which every estimand was
#' `population_65plus * constant`. Retained only so the change can be quantified;
#' the three series it produces are proportional rescalings of one another and
#' any concordance computed across them is a tautology.
#'
#' @param population_65plus Tibble with `year` and `population_65_plus`.
#' @param pfd_prevalence,consult_rate,surgery_rate_per_1000 Crude rates.
#' @return Long tibble in the same shape as [compute_demand_denominators()].
#' @export
compute_demand_denominators_crude <- function(population_65plus,
                                              pfd_prevalence = PFD_PREVALENCE_65PLUS,
                                              consult_rate = CONSULT_RATE_PER_WOMAN_65PLUS,
                                              surgery_rate_per_1000 = 5.0) {
  assertthat::assert_that(all(c("year", "population_65_plus") %in% names(population_65plus)))
  .msg_warn("compute_demand_denominators_crude(): all three estimands are ",
            "proportional to one population series; concordance across them is arithmetic.")
  pop <- population_65plus$population_65_plus
  yr <- population_65plus$year
  dplyr::bind_rows(
    tibble::tibble(year = yr, estimand = "D1", label = "Prevalent PFD cases (crude)",
                   demand_cases = pop * pfd_prevalence),
    tibble::tibble(year = yr, estimand = "D2", label = "New consultations (crude)",
                   demand_cases = pop * consult_rate),
    tibble::tibble(year = yr, estimand = "D3", label = "SUI+POP surgical volume (crude)",
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

#' Relative growth adequacy of supply against each demand estimand
#'
#' RENAMED AND NARROWED. The previous version also returned
#' `coverage_pct = 100 * supply_FTE / demand_cases`, dividing provider FTE by a
#' count of prevalent cases, consultations or procedures. Those are not FTE
#' units and the ratio is dimensionally meaningless (D1 "coverage" printed as
#' 0.0108%). It has been removed; use [convert_workload_to_fte()] and
#' [compute_fte_gap()] for an absolute comparison with FTE on both sides.
#'
#' What remains is legitimate but LIMITED: both series are rebased to the base
#' year, so `growth_adequacy` is 1.0 in the base year BY CONSTRUCTION and can
#' only say whether supply grows faster than demand thereafter. It cannot
#' estimate a shortage. See R/18-baseline_gap.R for the absolute level.
#'
#' @param supply Tibble with `year` and a supplied-FTE column.
#' @param demand_long Long demand tibble ([compute_demand_denominators()]).
#' @param supply_col Name of the supplied-FTE column.
#' @param base_year Rebase year.
#' @return Long tibble: `year`, `estimand`, `label`, `supply_index`,
#'   `demand_index`, `growth_adequacy`.
#' @export
compute_growth_adequacy <- function(supply, demand_long,
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
    dplyr::mutate(growth_adequacy = .data$supply_index / .data$demand_index) %>%
    dplyr::select("year", "estimand", "label", "supply_index", "demand_index",
                  "growth_adequacy")
}

#' Deprecated alias that refuses the dimensionally invalid coverage ratio
#'
#' @param ... Ignored.
#' @export
compute_demand_coverage <- function(...) {
  stop(paste(
    "compute_demand_coverage() has been removed. It divided provider FTE by a count of",
    "prevalent cases / consultations / procedures, which are not FTE units.",
    "Use compute_growth_adequacy() for the (base-year-normalised) growth comparison, or",
    "convert_workload_to_fte() + compute_fte_gap() for an absolute comparison with FTE",
    "on both sides."
  ), call. = FALSE)
}

#' Assess concordance of the growth-adequacy conclusion across demand estimands
#'
#' Rank-correlates the growth-adequacy curves and checks whether the conclusion
#' survives the choice of demand denominator. The verdict now carries an
#' `informative` flag: if the estimands are proportional rescalings of one series
#' the rho is 1.000 by construction and means nothing, so the flag is FALSE and
#' the result must not be reported as robustness evidence.
#'
#' @param adequacy Long tibble from [compute_growth_adequacy()].
#' @param demand_long The demand tibble the adequacy was built from, used for the
#'   proportionality check.
#' @return List: `spearman`, `min_adequacy_by_estimand`, `final_year_adequacy`,
#'   `robust`, `trough_year`, `informative`, `proportional_pairs`.
#' @export
assess_demand_concordance <- function(adequacy, demand_long = NULL) {
  measure <- if ("growth_adequacy" %in% names(adequacy)) "growth_adequacy" else "adequacy"

  wide <- adequacy %>%
    dplyr::select("year", "estimand", dplyr::all_of(measure)) %>%
    tidyr::pivot_wider(names_from = "estimand", values_from = dplyr::all_of(measure)) %>%
    dplyr::arrange(.data$year)

  estimand_cols <- setdiff(names(wide), "year")
  rho <- suppressWarnings(stats::cor(wide[estimand_cols], method = "spearman",
                                     use = "pairwise.complete.obs"))

  # Spearman rho computed ACROSS YEARS is 1 for any pair of series that are both
  # monotone in time, whether or not they are related. Nearly every demand
  # projection is monotone, so a rho of 1.000 here is a property of the time
  # index, not evidence of agreement. Flag it rather than reporting it as a
  # robustness result. The informative comparisons are the SPREAD of adequacy
  # across estimands and whether they place the conclusion on the same side of
  # 1.0; rank correlation belongs across GEOGRAPHIES (see two_method_agreement()).
  monotone <- vapply(estimand_cols, function(cl) {
    v <- wide[[cl]]
    v <- v[is.finite(v)]
    length(v) < 2 || all(diff(v) >= 0) || all(diff(v) <= 0)
  }, logical(1))
  rho_uninformative <- all(monotone)

  final_year <- max(adequacy$year)
  final_adeq <- adequacy %>%
    dplyr::filter(.data$year == final_year) %>%
    dplyr::select("estimand", adequacy = dplyr::all_of(measure))

  min_adeq <- adequacy %>%
    dplyr::group_by(.data$estimand) %>%
    dplyr::summarise(min_adequacy = min(.data[[measure]], na.rm = TRUE),
                     trough_year = .data$year[which.min(.data[[measure]])],
                     .groups = "drop")

  prop <- if (!is.null(demand_long)) {
    detect_proportional_estimands(demand_long, "demand_cases")
  } else {
    tibble::tibble(a = character(0), b = character(0))
  }
  distinct_estimands <- nrow(prop) == 0

  # The informative statistics: how far apart the estimands place adequacy in the
  # final year, and whether they agree on which side of 1.0 it falls.
  spread <- if (nrow(final_adeq)) {
    max(final_adeq$adequacy, na.rm = TRUE) - min(final_adeq$adequacy, na.rm = TRUE)
  } else NA_real_
  same_side <- if (nrow(final_adeq)) {
    length(unique(sign(final_adeq$adequacy - 1))) == 1L
  } else NA

  list(
    spearman = rho,
    rho_uninformative = rho_uninformative,
    min_adequacy_by_estimand = min_adeq,
    final_year_adequacy = final_adeq,
    final_year_spread = spread,
    conclusion_agrees = same_side,
    robust = isTRUE(same_side) && all(final_adeq$adequacy >= 1, na.rm = TRUE),
    trough_year = if (nrow(min_adeq)) min_adeq$trough_year[which.min(min_adeq$min_adequacy)] else NA,
    distinct_estimands = distinct_estimands,
    informative = distinct_estimands,
    proportional_pairs = prop,
    note = paste(
      if (distinct_estimands) {
        "Estimands carry distinct age profiles."
      } else {
        "Estimands are proportional rescalings of one series and carry identical information."
      },
      if (rho_uninformative) {
        "Spearman rho across years is 1 by construction for monotone series - read final_year_spread and conclusion_agrees instead."
      } else ""
    )
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
