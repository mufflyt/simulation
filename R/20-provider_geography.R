# Geographic Distribution and Migration of Providers ----
#
# A sufficient national headcount does not solve geographic access. Every
# Dall-family study models where providers practise, and the HWMM documentation
# spells out the entrant-allocation algorithm precisely (HWSM, "Geographic
# migration"):
#
#   1. Projected growth in demand in each state over the forecast horizon;
#   2. Projected retirements in each state over the same horizon;
#   3. Add projected growth and projected retirements to estimate total new
#      workers required to meet future demand for services;
#   4. Sum total new requirements across states and estimate each state's share
#      of total requirements; and
#   5. Distribute new workers across states using this distribution as a proxy.
#
#   "Each new entrant to the workforce is assigned a state using this calculated
#    distribution under the assumption that new graduates will migrate to those
#    geographic locations where growth in demand or retirements creates
#    opportunities for employment... (but allowing current mal-distribution of
#    health professionals to persist)."
#
# The parenthesis is the important part: the opportunity-responsive rule does NOT
# correct existing maldistribution, it only tracks where new openings appear. The
# historical-share rule is the comparison case. Both are implemented, and running
# them side by side is the point.
#
# The UNC FutureDocs model (Fraher & Knapton 2017) takes the further step of
# computing a per-provider annual probability of moving, so mid-career migration
# is represented as well as entrant placement. That is supported via
# `apply_provider_migration()`.

GEO_ALLOCATION_RULES <- c("historical", "opportunity", "hybrid")

#' Historical share of providers by geography
#'
#' The comparison case: new entrants are distributed exactly as the current
#' workforce is distributed, so today's maldistribution is reproduced forever.
#'
#' @param roster Provider roster with a geography column.
#' @param geo_col Name of the geography column.
#' @return Tibble with `geo` and `share`.
#' @export
historical_placement_shares <- function(roster, geo_col = "state") {
  assertthat::assert_that(is.data.frame(roster), geo_col %in% names(roster))
  counts <- roster %>%
    dplyr::filter(!is.na(.data[[geo_col]])) %>%
    dplyr::count(geo = .data[[geo_col]], name = "n")
  total <- sum(counts$n)
  if (total <= 0) stop("historical_placement_shares: no geographic records", call. = FALSE)
  dplyr::mutate(counts, share = .data$n / total)
}

#' Opportunity-responsive placement shares (HWSM five-step algorithm)
#'
#' @param demand_growth Tibble with `geo` and `demand_growth_fte` over the
#'   horizon (step 1).
#' @param retirements Tibble with `geo` and `retirements_fte` over the same
#'   horizon (step 2).
#' @return Tibble with `geo`, `requirements_fte` (step 3) and `share` (step 4),
#'   ready to distribute entrants (step 5).
#' @export
opportunity_placement_shares <- function(demand_growth, retirements) {
  assertthat::assert_that(all(c("geo", "demand_growth_fte") %in% names(demand_growth)))
  assertthat::assert_that(all(c("geo", "retirements_fte") %in% names(retirements)))

  joined <- safe_left_join(demand_growth, retirements, by = "geo", min_match_rate = 1.0)
  out <- dplyr::mutate(
    joined,
    requirements_fte = pmax(.data$demand_growth_fte, 0) + pmax(.data$retirements_fte, 0)
  )
  total <- sum(out$requirements_fte, na.rm = TRUE)
  if (total <= 0) {
    stop("opportunity_placement_shares: total requirements are zero; cannot form shares",
         call. = FALSE)
  }
  dplyr::mutate(out, share = .data$requirements_fte / total) %>%
    dplyr::select("geo", "requirements_fte", "share")
}

#' Blend historical and opportunity-responsive placement
#'
#' @param historical Tibble from [historical_placement_shares()].
#' @param opportunity Tibble from [opportunity_placement_shares()].
#' @param weight Weight on the opportunity-responsive shares (0 = purely
#'   historical, 1 = purely opportunity-responsive).
#' @return Tibble with `geo` and blended `share`.
#' @export
blend_placement_shares <- function(historical, opportunity, weight = 0.5) {
  assertthat::assert_that(weight >= 0, weight <= 1)
  h <- dplyr::select(historical, "geo", historical_share = "share")
  o <- dplyr::select(opportunity, "geo", opportunity_share = "share")
  out <- dplyr::full_join(h, o, by = "geo")
  out$historical_share[is.na(out$historical_share)] <- 0
  out$opportunity_share[is.na(out$opportunity_share)] <- 0
  out <- dplyr::mutate(
    out,
    share = (1 - weight) * .data$historical_share + weight * .data$opportunity_share
  )
  dplyr::mutate(out, share = .data$share / sum(.data$share))
}

#' Assign entrants to geographies from a share distribution
#'
#' Multinomial draw, so entrant placement carries the same stochastic variation
#' as the rest of the microsimulation rather than being a deterministic split.
#'
#' @param n_entrants Number of entrants to place.
#' @param shares Tibble with `geo` and `share`.
#' @param stochastic Draw multinomially (TRUE) or allocate proportionally (FALSE).
#' @return Character vector of geographies, length `n_entrants`.
#' @export
assign_entrant_geography <- function(n_entrants, shares, stochastic = TRUE) {
  assertthat::assert_that(all(c("geo", "share") %in% names(shares)))
  n_entrants <- as.integer(n_entrants)
  if (n_entrants <= 0) return(character(0))
  p <- shares$share / sum(shares$share)

  if (isTRUE(stochastic)) {
    return(sample(shares$geo, size = n_entrants, replace = TRUE, prob = p))
  }
  counts <- floor(p * n_entrants)
  remainder <- n_entrants - sum(counts)
  if (remainder > 0) {
    top <- order(p * n_entrants - counts, decreasing = TRUE)[seq_len(remainder)]
    counts[top] <- counts[top] + 1L
  }
  rep(shares$geo, times = counts)
}

# ---- Mid-career migration (FutureDocs) ------------------------------------

# Annual probability that an active provider relocates across state lines.
# Physicians are in a national labour market but have high self-employment and
# practice-specific capital, so mid-career moves are uncommon and concentrated
# in the first years after training (HWSM: "providers in occupations with high
# rates of self-employment... are probably less likely to move mid-career").
PROVIDER_MIGRATION_HAZARD <- c(
  "early_career" = 0.045,   # within 5 years of entry
  "mid_career"   = 0.012,
  "late_career"  = 0.004
)

#' Annual cross-geography migration hazard for providers
#'
#' @param years_since_entry Years since entry to practice.
#' @param age Provider age (late career slows moves further).
#' @param hazards Named hazard vector.
#' @return Numeric annual migration probability.
#' @export
migration_hazard <- function(years_since_entry, age = NA_real_,
                             hazards = PROVIDER_MIGRATION_HAZARD) {
  y <- as.numeric(years_since_entry)
  out <- rep(hazards[["mid_career"]], length(y))
  out[!is.na(y) & y <= 5] <- hazards[["early_career"]]
  if (length(age) == length(y)) out[!is.na(age) & age >= 60] <- hazards[["late_career"]]
  out
}

#' Apply one year of stochastic provider migration
#'
#' @param agents Agent tibble with `state`, `entry_year`.
#' @param year Current calendar year.
#' @param shares Destination share distribution (`geo`, `share`).
#' @param hazards Migration hazards.
#' @return The agent tibble with updated `state` and an incremented `n_moves`.
#' @export
apply_provider_migration <- function(agents, year, shares,
                                     hazards = PROVIDER_MIGRATION_HAZARD) {
  if (!"state" %in% names(agents)) return(agents)
  if (!"n_moves" %in% names(agents)) agents$n_moves <- 0L

  yrs <- year - agents$entry_year
  h <- migration_hazard(yrs, agents$age, hazards)
  moves <- stats::runif(nrow(agents)) < h & !is.na(agents$state)

  if (any(moves)) {
    agents$state[moves] <- assign_entrant_geography(sum(moves), shares, stochastic = TRUE)
    agents$n_moves[moves] <- agents$n_moves[moves] + 1L
  }
  agents
}

# ---- Reporting ------------------------------------------------------------

#' Provider supply per capita by geography
#'
#' Reported as FTE per million population, the unit used in the physiatry tables
#' (national 27 per million; New Jersey 47, Mississippi 8).
#'
#' @param supply Tibble with `geo` and `fte`.
#' @param population Tibble with `geo` and `population`.
#' @param per Population denominator (default 1e6).
#' @return Tibble with `geo`, `fte`, `population`, `fte_per_capita`.
#' @export
supply_per_capita <- function(supply, population, per = 1e6) {
  out <- safe_left_join(supply, population, by = "geo", min_match_rate = 1.0)
  # A geography with zero recorded population yields no density, not an
  # infinite one.
  dplyr::mutate(out, fte_per_capita = ssot_safe_divide(.data$fte * per,
                                                       .data$population))
}

#' Providers required to raise every geography to a benchmark density
#'
#' The physiatry paper's alternative shortfall benchmark: 30 physiatrists per
#' million as a minimum adequate level implies 984 additional physiatrists
#' nationally, or 1,747 when applied state by state (because surpluses in
#' well-supplied states cannot offset deficits elsewhere). Reporting both numbers
#' makes the difference between national and geographic adequacy explicit.
#'
#' @param per_capita Tibble from [supply_per_capita()].
#' @param benchmark Target FTE per `per` population.
#' @param per Population denominator matching `per_capita`.
#' @return List with `national_additional`, `geographic_additional`, and detail.
#' @export
benchmark_density_shortfall <- function(per_capita, benchmark, per = 1e6) {
  detail <- dplyr::mutate(
    per_capita,
    target_fte = .data$population / per * benchmark,
    deficit_fte = pmax(.data$target_fte - .data$fte, 0),
    surplus_fte = pmax(.data$fte - .data$target_fte, 0)
  )
  total_pop <- sum(detail$population, na.rm = TRUE)
  total_fte <- sum(detail$fte, na.rm = TRUE)

  if (!is.finite(total_pop) || total_pop <= 0) {
    stop("benchmark_density_shortfall: total population is zero, so a density ",
         "benchmark is undefined.", call. = FALSE)
  }
  list(
    national_additional = max(total_pop / per * benchmark - total_fte, 0),
    geographic_additional = sum(detail$deficit_fte, na.rm = TRUE),
    n_geo_below_benchmark = sum(detail$deficit_fte > 0, na.rm = TRUE),
    detail = detail
  )
}
