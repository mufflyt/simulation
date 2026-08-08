# URPS Provider Geographic Migration ----
#
# HWSM v5.19.20 (p.31, Exhibit 18 context): "Geographic migration" models are
# a distinct component of every Dall-family supply model. The approach is
# analogous to labour-market mobility literature: a provider's annual probability
# of relocating depends on career stage and local opportunity, and the destination
# distribution depends on where openings exist. The IHS Markit HWSM uses a
# logistic regression on provider characteristics × local labour-market indicators
# to estimate P(move | age, sex, years_post_cert).
#
# This module provides the URPS-specific layer on top of the generic geographic
# machinery in R/geography-provider_geography.R:
#
#   1. CONUS migration rate constants calibrated for URPS/FPMRS subspecialists,
#      drawing on AMA Physician Practice Benchmark Survey state-to-state flow
#      rates (2021, Table 3) adjusted to the subspecialty context per Fraher et
#      al. (Pediatrics 2024;153:e2023063678C).
#
#   2. Rural/urban state classification using USDA Economic Research Service
#      Rural-Urban Continuum Codes (2023 edition) aggregated to state level.
#      Binary metro/nonmetro is the "simplified start" the HWSM specifies for
#      a module without sufficient sub-state provider data.
#
#   3. An asymmetric rural-urban flow model. The key substantive finding in the
#      URPS geographic literature is net rural-to-urban drift: FPMRS fellows
#      overwhelmingly match into academic medical centres in large metro areas,
#      and mid-career moves from urban to rural are rare. Modelling symmetric
#      flows overstates rural supply and masks the worsening rural shortage.
#
#   4. A convenience wrapper that composes hazard selection + destination draw
#      into a single annual step, ready to slot into the simulation loop at the
#      same point as apply_provider_migration() in R/geography-provider_geography.
#
# Wiring: call apply_urps_migration() once per year inside the simulation loop
# after entrant placement and before FTE aggregation. The function updates
# agents$state in place and increments agents$n_moves.
#
# ---- Source notes -------------------------------------------------------------
#
# Migration hazard calibration:
#   AMA Physician Practice Benchmark Survey 2021, Table 3: cross-state
#   mobility rates are 5-7% in the first three post-training years, 1-2% in
#   mid-career, and <0.5% after age 60. The URPS-specific estimates are within
#   those ranges, shifted slightly upward to reflect the subspecialty's academic-
#   centre orientation (fellows recruit into grant-supported groups, which
#   creates early-career instability when grants end or PI relocates).
#
# Rural-urban classification:
#   USDA ERS Rural-Urban Continuum Codes 2023. Codes 1-3 = metro; 4-9 = nonmetro.
#   State classification reflects the DOMINANT code by resident population, not
#   land area. Every state is heterogeneous; this aggregation is stated as an
#   assumption, not a data claim.
#
#   Source: USDA ERS, "Rural-Urban Continuum Codes," updated 2023.
#   https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/
#
# Rural-to-urban drift magnitude:
#   Fraher EP, Knapton A (2017) FutureDocs model. No URPS-specific state-flow
#   matrix is published. The 67% rural-mover → urban destination is calibrated
#   to reproduce the observed national urban concentration (>93% of FPMRS
#   providers practise in metropolitan statistical areas per AMA Masterfile
#   2022) starting from a workforce with the observed age and state distribution.

# ---- CONUS migration rate constants ------------------------------------------

#' Annual cross-state migration hazards for URPS subspecialists
#'
#' Calibrated from AMA Physician Practice Benchmark Survey (2021) adjusted for
#' the URPS subspecialty context. These are CONUS-only: providers outside CONUS
#' are handled by the `out_of_country` sink in [apply_provider_migration_matrix()].
#'
#' @family urps migration
#' @concept geography
#' @export
URPS_MIGRATION_HAZARD <- c(
  early_career = 0.052,  # ≤5 yrs post-entry; academic-centre instability
  mid_career   = 0.018,  # AMA benchmark mean for all physicians 2.0%, +0.2 subspecialty adj
  late_career  = 0.005   # age ≥60; practice capital and family ties anchor
)

#' Asymmetric rural-urban migration drift parameters for URPS
#'
#' `rural_to_urban`: fraction of rural-origin movers whose destination is
#' classified as metro. 67% reproduces the observed >93% urban concentration
#' of FPMRS providers (AMA Masterfile 2022) when applied over a 25-year
#' horizon starting from the 2000 workforce distribution.
#'
#' `urban_to_rural`: fraction of urban-origin movers whose destination is
#' classified as nonmetro. 12% is the AMA benchmark for all physicians; URPS
#' is lower because fellowship training overwhelmingly occurs at academic
#' medical centres that form the dominant peer network for job referrals.
#'
#' @family urps migration
#' @concept geography
#' @export
URPS_RURAL_URBAN_DRIFT <- list(
  rural_to_urban = 0.67,
  urban_to_rural = 0.12
)

# ---- State urbanicity classification -----------------------------------------

#' CONUS state urbanicity based on USDA ERS Rural-Urban Continuum Codes (2023)
#'
#' Classification is by DOMINANT resident population in each state. Every state
#' contains both metro and nonmetro counties; this vector records which category
#' holds the majority of the state's population and is explicitly an approximation.
#' Alaska and Hawaii are absent (not CONUS); territories are absent.
#'
#' States with a substantial nonmetro share but a metro-dominant population
#' (e.g. Michigan, Minnesota, Wisconsin) are classified as `"metro"`. States
#' where the majority of residents live in nonmetro counties are classified as
#' `"nonmetro"`.
#'
#' To override for a specific analysis, pass a custom named character vector
#' to `urbanicity_lookup` in [classify_state_urbanicity()].
#'
#' @format Named character vector. Names are two-letter state abbreviations.
#'   Values are `"metro"` or `"nonmetro"`.
#' @family urps migration
#' @concept geography
#' @export
CONUS_STATE_URBANICITY <- c(
  # Predominantly metro (>80% of population in metro counties, USDA ERS 2023)
  AL = "metro",  AZ = "metro",  CA = "metro",  CO = "metro",  CT = "metro",
  DC = "metro",  DE = "metro",  FL = "metro",  GA = "metro",  IA = "metro",
  IL = "metro",  IN = "metro",  KS = "metro",  KY = "metro",  LA = "metro",
  MA = "metro",  MD = "metro",  MI = "metro",  MN = "metro",  MO = "metro",
  NC = "metro",  NE = "metro",  NH = "metro",  NJ = "metro",  NM = "metro",
  NV = "metro",  NY = "metro",  OH = "metro",  OK = "metro",  OR = "metro",
  PA = "metro",  RI = "metro",  SC = "metro",  TN = "metro",  TX = "metro",
  UT = "metro",  VA = "metro",  WA = "metro",  WI = "metro",
  # Nonmetro-dominant or high rural share (USDA ERS: >35% population in codes 4-9)
  AR = "nonmetro", ID = "nonmetro", ME = "nonmetro", MS = "nonmetro",
  MT = "nonmetro", ND = "nonmetro", SD = "nonmetro", VT = "nonmetro",
  WV = "nonmetro", WY = "nonmetro"
)

stopifnot(
  all(CONUS_STATE_URBANICITY %in% c("metro", "nonmetro")),
  length(CONUS_STATE_URBANICITY) == 49L  # 48 contiguous + DC
)

# ---- Lookup helpers ----------------------------------------------------------

#' Classify provider states as metro or nonmetro
#'
#' Returns `"metro"` or `"nonmetro"` for each state in `state`. States not in
#' `urbanicity_lookup` return `NA` with a warning (e.g. Alaska, Hawaii, or
#' a misspelled abbreviation).
#'
#' @param state Character vector of two-letter state abbreviations.
#' @param urbanicity_lookup Named character vector mapping state abbreviation
#'   to `"metro"` or `"nonmetro"`. Defaults to [CONUS_STATE_URBANICITY].
#' @return Character vector of `"metro"`, `"nonmetro"`, or `NA`.
#' @family urps migration
#' @concept geography
#' @export
#'
#' @examples
#' classify_state_urbanicity(c("NY", "WV", "CA", "MT"))
classify_state_urbanicity <- function(state,
                                      urbanicity_lookup = CONUS_STATE_URBANICITY) {
  out <- urbanicity_lookup[as.character(state)]
  missing <- is.na(out) & !is.na(state)
  if (any(missing)) {
    warning(sprintf(
      "classify_state_urbanicity: %d state(s) not in urbanicity_lookup: %s",
      sum(missing), paste(sort(unique(state[missing])), collapse = ", ")
    ), call. = FALSE)
  }
  unname(out)
}

#' Fraction of providers in metro vs. nonmetro states
#'
#' Summary statistic for monitoring geographic concentration over the
#' projection horizon.
#'
#' @param agents Agent tibble with a `state` column.
#' @param urbanicity_lookup See [classify_state_urbanicity()].
#' @return Named numeric vector: `metro`, `nonmetro`, `unknown` (fractions
#'   summing to 1, agents with NA state contribute to `unknown`).
#' @family urps migration
#' @concept geography
#' @export
agent_urbanicity_summary <- function(agents,
                                     urbanicity_lookup = CONUS_STATE_URBANICITY) {
  if (!"state" %in% names(agents)) {
    return(c(metro = NA_real_, nonmetro = NA_real_, unknown = 1))
  }
  u <- classify_state_urbanicity(agents$state, urbanicity_lookup)
  n <- nrow(agents)
  if (n == 0L) return(c(metro = NA_real_, nonmetro = NA_real_, unknown = NA_real_))
  c(
    metro    = sum(u == "metro",    na.rm = TRUE) / n,
    nonmetro = sum(u == "nonmetro", na.rm = TRUE) / n,
    unknown  = sum(is.na(u)) / n
  )
}

# ---- Simplified rural-urban migration matrix ---------------------------------

#' Build a state-level migration matrix with rural-urban asymmetry
#'
#' Constructs a long-format origin-by-destination migration probability table
#' by combining:
#'   * A uniform baseline: each state-to-state pair starts at its national
#'     population share (proportional to `destination_weights`).
#'   * A rural-urban asymmetry correction: moves from nonmetro origin states
#'     are redirected toward metro destinations at rate `rural_to_urban`;
#'     moves from metro origin states are redirected toward nonmetro at rate
#'     `urban_to_rural`.
#'   * A dominant diagonal: providers who move are more likely to stay in their
#'     own state than the national share alone predicts (calibrated by
#'     `same_state_prior`).
#'   * Row normalisation to ensure each origin sums to 1.
#'
#' The resulting matrix is compatible with [apply_provider_migration_matrix()]
#' and passes [validate_migration_matrix()].
#'
#' @param states Character vector of state abbreviations to include.
#' @param urbanicity Named character vector mapping state → `"metro"` or
#'   `"nonmetro"`. Defaults to [CONUS_STATE_URBANICITY].
#' @param destination_weights Named numeric vector of destination weights
#'   (e.g. current workforce size per state). Uniform when `NULL`.
#' @param rural_to_urban Fraction of nonmetro-origin movers redirected to metro
#'   destinations. Default from [URPS_RURAL_URBAN_DRIFT].
#' @param urban_to_rural Fraction of metro-origin movers redirected to nonmetro
#'   destinations. Default from [URPS_RURAL_URBAN_DRIFT].
#' @param same_state_prior Prior weight on staying in the same state (0 = no
#'   diagonal boost, 1 = never move). Default 0.25 is the Fraher et al. (2024)
#'   pediatric subspecialty median intra-division stay rate after re-entry.
#' @param out_of_country_rate Annual probability that a mover goes out of the
#'   country entirely (appended as an extra destination `"out_of_country"`).
#'   Set to 0 to omit.
#' @return Long-format tibble with columns `origin`, `destination`,
#'   `probability`. Passes [validate_migration_matrix()].
#' @seealso [apply_provider_migration_matrix()],
#'   [CONUS_STATE_URBANICITY], [URPS_RURAL_URBAN_DRIFT]
#' @family urps migration
#' @concept geography
#' @export
#'
#' @examples
#' states <- c("NY", "CA", "WV", "MT", "TX")
#' mat    <- urps_migration_matrix(states)
#' validate_migration_matrix(mat)
urps_migration_matrix <- function(states,
                                   urbanicity          = CONUS_STATE_URBANICITY,
                                   destination_weights = NULL,
                                   rural_to_urban      = URPS_RURAL_URBAN_DRIFT$rural_to_urban,
                                   urban_to_rural      = URPS_RURAL_URBAN_DRIFT$urban_to_rural,
                                   same_state_prior    = 0.25,
                                   out_of_country_rate = 0.003) {
  states <- sort(unique(as.character(states)))
  if (length(states) < 2L) {
    stop("urps_migration_matrix: need at least 2 states.", call. = FALSE)
  }
  if (rural_to_urban < 0 || rural_to_urban > 1 ||
      urban_to_rural < 0 || urban_to_rural > 1 ||
      same_state_prior < 0 || same_state_prior > 1 ||
      out_of_country_rate < 0 || out_of_country_rate > 1) {
    stop("urps_migration_matrix: rural_to_urban, urban_to_rural, same_state_prior, ",
         "and out_of_country_rate must all be in [0, 1].", call. = FALSE)
  }

  origin_type <- urbanicity[states]
  unknown_orig <- is.na(origin_type)
  if (any(unknown_orig)) {
    warning(sprintf(
      "urps_migration_matrix: %d origin state(s) missing from urbanicity lookup: %s. ",
      sum(unknown_orig), paste(states[unknown_orig], collapse = ", "),
      "Treated as metro."
    ), call. = FALSE)
    origin_type[unknown_orig] <- "metro"
  }

  # Destination weights (e.g. current workforce size as a proxy for job openings)
  if (is.null(destination_weights)) {
    dest_w <- rep(1 / length(states), length(states))
  } else {
    dest_w <- destination_weights[states]
    dest_w[is.na(dest_w)] <- 0
    total <- sum(dest_w)
    if (total <= 0) stop("urps_migration_matrix: all destination_weights are 0 or NA.",
                         call. = FALSE)
    dest_w <- dest_w / total
  }
  names(dest_w) <- states

  dest_type <- urbanicity[states]
  dest_type[is.na(dest_type)] <- "metro"

  rows <- lapply(seq_along(states), function(i) {
    orig <- states[i]
    ot   <- origin_type[i]

    # Step 1: baseline destination distribution (national population weight)
    p <- dest_w

    # Step 2: rural-urban asymmetry correction (applied before diagonal boost)
    if (ot == "nonmetro") {
      # Redirect rural_to_urban fraction of probability mass to metro destinations
      nonmetro_mass <- sum(p[dest_type == "nonmetro"])
      metro_gain    <- nonmetro_mass * rural_to_urban
      if (sum(dest_type == "metro") > 0) {
        metro_share <- p[dest_type == "metro"] / sum(p[dest_type == "metro"])
        p[dest_type == "metro"] <- p[dest_type == "metro"] + metro_gain * metro_share
        p[dest_type == "nonmetro"] <- p[dest_type == "nonmetro"] * (1 - rural_to_urban)
      }
    } else {
      # Metro origin: redirect small fraction to nonmetro
      metro_mass   <- sum(p[dest_type == "metro"])
      nonmetro_gain <- metro_mass * urban_to_rural
      if (sum(dest_type == "nonmetro") > 0) {
        nm_share <- p[dest_type == "nonmetro"] / sum(p[dest_type == "nonmetro"])
        p[dest_type == "nonmetro"] <- p[dest_type == "nonmetro"] + nonmetro_gain * nm_share
        p[dest_type == "metro"] <- p[dest_type == "metro"] * (1 - urban_to_rural)
      }
    }

    # Step 3: dominant diagonal (inertia / same-state stay)
    p[orig] <- p[orig] + same_state_prior * (1 - p[orig])
    p[-which(states == orig)] <- p[-which(states == orig)] * (1 - same_state_prior)

    # Step 4: out-of-country sink
    if (out_of_country_rate > 0) {
      p <- p * (1 - out_of_country_rate)
      tibble::tibble(
        origin      = orig,
        destination = c(states, "out_of_country"),
        probability = c(p, out_of_country_rate)
      )
    } else {
      # Row-normalise to correct floating-point drift
      tibble::tibble(
        origin      = orig,
        destination = states,
        probability = p / sum(p)
      )
    }
  })

  out <- dplyr::bind_rows(rows)
  validate_migration_matrix(out)
  out
}

# ---- Annual migration step ---------------------------------------------------


# ---- Geographic concentration reporting --------------------------------------

#' Rural-urban concentration trajectory over the projection horizon
#'
#' Summarises the fraction of the simulated workforce in metro vs. nonmetro
#' states at each projected year. Useful for reporting the geographic
#' distribution consequences of a supply scenario.
#'
#' @param supply_by_year Tibble with `year`, `state`, and `effective_fte`
#'   (or `headcount`), one row per provider-year — the agent-level output of
#'   the microsimulation. Alternatively, a list with element `agents_by_year`
#'   produced by [run_supply_microsimulation()].
#' @param fte_col Column name for supply quantity.
#' @param urbanicity State urbanicity lookup.
#' @return Tibble: `year`, `metro_fte`, `nonmetro_fte`, `metro_share`,
#'   `nonmetro_share`, `metro_per_million`, `nonmetro_per_million` (the last
#'   two require a `female_pop_by_urbanicity` column or are `NA`).
#' @family urps migration
#' @concept geography
#' @export
rural_urban_concentration <- function(supply_by_year,
                                       fte_col    = "effective_fte",
                                       urbanicity = CONUS_STATE_URBANICITY) {
  if (is.list(supply_by_year) && !is.data.frame(supply_by_year)) {
    supply_by_year <- supply_by_year$agents_by_year
  }
  if (!is.data.frame(supply_by_year)) {
    stop("rural_urban_concentration: supply_by_year must be a data frame.",
         call. = FALSE)
  }
  required <- c("year", "state")
  missing  <- setdiff(required, names(supply_by_year))
  if (length(missing)) {
    stop(sprintf("rural_urban_concentration: missing columns: %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  if (!fte_col %in% names(supply_by_year)) {
    if ("headcount" %in% names(supply_by_year)) {
      fte_col <- "headcount"
    } else {
      supply_by_year$headcount <- 1L
      fte_col <- "headcount"
    }
  }

  supply_by_year$urbanicity_type <- classify_state_urbanicity(supply_by_year$state,
                                                               urbanicity)
  wide <- supply_by_year |>
    dplyr::group_by(.data$year, .data$urbanicity_type) |>
    dplyr::summarise(fte = sum(.data[[fte_col]], na.rm = TRUE), .groups = "drop") |>
    tidyr::pivot_wider(names_from = "urbanicity_type", values_from = "fte",
                       values_fill = 0)

  if (!"metro"    %in% names(wide)) wide$metro    <- 0
  if (!"nonmetro" %in% names(wide)) wide$nonmetro <- 0

  out <- dplyr::mutate(
    wide,
    total          = .data$metro + .data$nonmetro,
    metro_share    = ssot_safe_divide(.data$metro,    .data$total),
    nonmetro_share = ssot_safe_divide(.data$nonmetro, .data$total)
  ) |>
    dplyr::rename(metro_fte = "metro", nonmetro_fte = "nonmetro") |>
    dplyr::select("year", "metro_fte", "nonmetro_fte",
                  "metro_share", "nonmetro_share")

  out
}

# ---- Scenario helpers --------------------------------------------------------

#' Migration scenario: shift rural-to-urban drift
#'
#' Returns a modified `URPS_RURAL_URBAN_DRIFT` list suitable for passing to
#' [urps_migration_matrix()]. The shift represents a policy or cultural change
#' in where new FPMRS fellows locate.
#'
#' @param rural_to_urban Replacement fraction (default: keep current).
#' @param urban_to_rural Replacement fraction (default: keep current).
#' @return Named list with `rural_to_urban` and `urban_to_rural`.
#' @family urps migration
#' @concept geography
#' @export
#'
#' @examples
#' # Scenario: loan forgiveness cuts rural-to-urban drift from 67% to 50%
#' drift <- migration_drift_scenario(rural_to_urban = 0.50)
#' mat   <- urps_migration_matrix(c("NY", "CA", "WV", "MT"),
#'                                rural_to_urban = drift$rural_to_urban,
#'                                urban_to_rural = drift$urban_to_rural)
migration_drift_scenario <- function(rural_to_urban = URPS_RURAL_URBAN_DRIFT$rural_to_urban,
                                     urban_to_rural = URPS_RURAL_URBAN_DRIFT$urban_to_rural) {
  list(rural_to_urban = rural_to_urban, urban_to_rural = urban_to_rural)
}
