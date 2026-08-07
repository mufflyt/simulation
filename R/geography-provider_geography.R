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
#' @family provider geography
#' @concept geography
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
#' @family provider geography
#' @concept geography
#' @export
opportunity_placement_shares <- function(demand_growth, retirements) {
  assertthat::assert_that(all(c("geo", "demand_growth_fte") %in% names(demand_growth)))
  assertthat::assert_that(all(c("geo", "retirements_fte") %in% names(retirements)))

  # Full join, not left: a geo present only in `retirements` (retirements but no
  # projected demand-growth row) is a real opportunity a left join would silently
  # drop from the placement distribution, understating that geo's entrant share.
  # Coalesce the unmatched side's fte to 0.
  joined <- dplyr::full_join(demand_growth, retirements, by = "geo")
  out <- dplyr::mutate(
    joined,
    requirements_fte = pmax(dplyr::coalesce(.data$demand_growth_fte, 0), 0) +
      pmax(dplyr::coalesce(.data$retirements_fte, 0), 0)
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
#' @family provider geography
#' @concept geography
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
#' @family provider geography
#' @concept geography
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
#' @family provider geography
#' @concept geography
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
#' @family provider geography
#' @concept geography
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
#' @examples
#' supply_per_capita(
#'   supply = tibble::tibble(geo = c("CO", "NY", "TX"), fte = c(60, 140, 120)),
#'   population = tibble::tibble(geo = c("CO", "NY", "TX"),
#'                               population = c(2.9e6, 1.0e7, 1.5e7))
#' )
#' @family provider geography
#' @concept geography
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
#' @family provider geography
#' @concept geography
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

# ---- Origin-dependent migration -------------------------------------------
#
# `apply_provider_migration()` above draws a mover and then assigns a
# destination from `shares`, a single national vector. That vector does not
# depend on where the provider currently practises, which makes the implied
# origin-by-destination matrix rank-1: every row identical. Under that
# structure a provider leaving Mississippi and one leaving New Jersey face the
# same destination distribution, and the expected inflow to a geography is the
# same no matter which geographies the movers came from.
#
# The ABP pediatric subspecialty model (Fraher, Knapton, McCartha & Leslie,
# Pediatrics 2024;153(suppl 2):e2023063678C) is the relevant counter-example.
# Their Table 3 is an explicit origin-by-destination matrix built by comparing
# each trainee's program location to their subsequent practice address, and the
# diagonal dominates: 51% to 68% of trainees stay in the division where they
# trained. Rank-1 shares cannot represent a dominant diagonal without forcing
# every origin to have the same stay probability.
#
# The distinction changed their published numbers. During model validation they
# found divisions whose forecasts diverged from historical trend, and traced it
# to geographies that depend on inflow from *specific* higher-supply
# geographies rather than on a national pool. They also found a small
# out-of-country return flow that mattered disproportionately for less-populous
# divisions. Neither effect is expressible with origin-independent shares.
#
# This section adds the matrix form as an opt-in alternative. Nothing above
# changes: `apply_provider_migration()` keeps its current behaviour and remains
# the default, so seeded runs reproduce exactly.

#' Validate an origin-by-destination migration matrix
#'
#' Checks that `matrix` is a long-format origin/destination/probability table
#' whose probabilities are in `[0, 1]` and sum to one within each origin.
#'
#' A row that sums to less than one silently destroys providers on every
#' simulated year. Because the loss compounds, a 5% shortfall is invisible in a
#' single-year check and removes roughly three quarters of that origin's
#' outflow over a 25-year horizon, so this is checked before any arithmetic
#' rather than after.
#'
#' @param matrix Tibble with `origin`, `destination`, `probability`.
#' @param tol Absolute tolerance on each origin's row sum.
#' @return `matrix`, invisibly, or an error naming the offending origins.
#' @family provider geography
#' @concept geography
#' @export
validate_migration_matrix <- function(matrix, tol = 1e-8) {
  assertthat::assert_that(is.data.frame(matrix))
  assertthat::assert_that(
    all(c("origin", "destination", "probability") %in% names(matrix))
  )
  if (nrow(matrix) == 0L) {
    stop("validate_migration_matrix: matrix has no rows", call. = FALSE)
  }
  if (any(!is.finite(matrix$probability))) {
    stop("validate_migration_matrix: probability contains NA/NaN/Inf",
         call. = FALSE)
  }
  # Checked before the row sums: -0.1 and 1.1 sum to exactly 1.0, so a row-sum
  # test alone accepts a matrix that is not a probability distribution.
  if (any(matrix$probability < 0) || any(matrix$probability > 1)) {
    stop("validate_migration_matrix: probability outside [0, 1]", call. = FALSE)
  }
  dup <- matrix %>%
    dplyr::count(.data$origin, .data$destination, name = "n_pairs") %>%
    dplyr::filter(.data$n_pairs > 1L)
  if (nrow(dup) > 0L) {
    stop(sprintf(
      "validate_migration_matrix: %d duplicated origin-destination pair(s)",
      nrow(dup)
    ), call. = FALSE)
  }
  sums <- matrix %>%
    dplyr::group_by(.data$origin) %>%
    dplyr::summarise(row_sum = sum(.data$probability), .groups = "drop") %>%
    dplyr::filter(abs(.data$row_sum - 1) > tol)
  if (nrow(sums) > 0L) {
    stop(sprintf(
      paste("validate_migration_matrix: probabilities do not sum to 1",
            "for origin(s): %s"),
      paste(sums$origin, collapse = ", ")
    ), call. = FALSE)
  }
  invisible(matrix)
}

#' Build an origin-by-destination matrix from observed moves
#'
#' Row-normalises a table of observed origin-to-destination counts. Origins
#' with sparse observation are shrunk toward `prior_shares` (an empirical-Bayes
#' step): with `k` observed moves the fitted row is weighted
#' `k / (k + shrinkage)` and the prior takes the remainder, so an origin with
#' two observed moves is not allowed to assert a 100% destination probability.
#'
#' This matters for URPS specifically. The subspecialty graduates on the order
#' of 150 fellows a year, so pooling a decade across a 50-state matrix leaves
#' most cells empty and many rows resting on a handful of moves. Fraher et al.
#' avoided the problem by using nine census divisions rather than states; the
#' shrinkage argument is the alternative when a finer geography is required.
#'
#' @param moves Tibble with `origin`, `destination`, and `n` observed moves.
#' @param prior_shares Tibble with `geo` and `share`, the national destination
#'   distribution to shrink toward. Defaults to the marginal of `moves`.
#' @param shrinkage Prior weight in pseudo-moves. `0` disables shrinkage.
#' @return Tibble with `origin`, `destination`, `probability`, `n_observed`.
#' @family provider geography
#' @concept geography
#' @export
migration_matrix_from_moves <- function(moves, prior_shares = NULL,
                                        shrinkage = 10) {
  assertthat::assert_that(is.data.frame(moves))
  assertthat::assert_that(
    all(c("origin", "destination", "n") %in% names(moves))
  )
  assertthat::assert_that(shrinkage >= 0)
  if (any(!is.finite(moves$n)) || any(moves$n < 0)) {
    stop("migration_matrix_from_moves: n must be finite and non-negative",
         call. = FALSE)
  }

  if (is.null(prior_shares)) {
    prior_shares <- moves %>%
      dplyr::group_by(geo = .data$destination) %>%
      dplyr::summarise(share = sum(.data$n), .groups = "drop") %>%
      dplyr::mutate(share = .data$share / sum(.data$share))
  }
  assertthat::assert_that(all(c("geo", "share") %in% names(prior_shares)))

  grid <- tidyr::expand_grid(
    origin = sort(unique(moves$origin)),
    destination = sort(unique(c(moves$destination, prior_shares$geo)))
  )

  out <- grid %>%
    dplyr::left_join(moves, by = c("origin", "destination")) %>%
    dplyr::mutate(n = dplyr::coalesce(.data$n, 0)) %>%
    dplyr::left_join(
      dplyr::select(prior_shares, destination = "geo", prior = "share"),
      by = "destination"
    ) %>%
    dplyr::mutate(prior = dplyr::coalesce(.data$prior, 0)) %>%
    dplyr::group_by(.data$origin) %>%
    dplyr::mutate(
      n_observed = sum(.data$n),
      empirical  = ssot_safe_divide(.data$n, .data$n_observed),
      weight     = ssot_safe_divide(.data$n_observed,
                                    .data$n_observed + shrinkage),
      weight     = dplyr::coalesce(.data$weight, 0),
      probability = .data$weight * dplyr::coalesce(.data$empirical, 0) +
        (1 - .data$weight) * .data$prior
    ) %>%
    # Renormalise while still grouped: coalescing a zero-observation row to the
    # prior can leave the row off 1 when the prior does not cover every column.
    dplyr::mutate(
      row_total = sum(.data$probability),
      probability = ssot_safe_divide(.data$probability, .data$row_total)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("origin", "destination", "probability", "n_observed")

  validate_migration_matrix(out, tol = 1e-6)
  out
}

#' Apply one year of origin-dependent provider migration
#'
#' The origin-dependent counterpart to [apply_provider_migration()]. A mover's
#' destination is drawn from the row of `matrix` matching their current
#' geography, so a dominant diagonal, an asymmetric corridor between two
#' geographies, and an absorbing out-of-country state are all representable.
#'
#' Providers whose destination is `out_of_country` have their geography set to
#' `NA` and are marked `left_country = TRUE`. They remain in the agent table
#' rather than being deleted, so headcount can be audited; downstream supply
#' counts should filter them out.
#'
#' @param agents Agent tibble with `state` and `entry_year`.
#' @param year Current calendar year.
#' @param matrix Origin-by-destination matrix; see
#'   [migration_matrix_from_moves()].
#' @param hazards Migration hazards, as in [apply_provider_migration()].
#' @param out_of_country Destination label treated as an absorbing sink, or
#'   `NULL` for none.
#' @return The agent tibble with updated `state`, `n_moves`, `left_country`.
#' @family provider geography
#' @concept geography
#' @export
apply_provider_migration_matrix <- function(agents, year, matrix,
                                            hazards = PROVIDER_MIGRATION_HAZARD,
                                            out_of_country = "out_of_country") {
  if (!"state" %in% names(agents)) return(agents)
  validate_migration_matrix(matrix)
  if (!"n_moves" %in% names(agents)) agents$n_moves <- 0L
  if (!"left_country" %in% names(agents)) agents$left_country <- FALSE

  eligible <- !is.na(agents$state) & !agents$left_country
  if (!any(eligible)) return(agents)

  orphan <- setdiff(agents$state[eligible], matrix$origin)
  if (length(orphan)) {
    stop(sprintf(
      "apply_provider_migration_matrix: no matrix row for origin(s): %s",
      paste(sort(unique(orphan)), collapse = ", ")
    ), call. = FALSE)
  }

  yrs <- year - agents$entry_year
  h <- migration_hazard(yrs, agents$age, hazards)
  moves <- eligible & (stats::runif(nrow(agents)) < h)
  if (!any(moves)) return(agents)

  # Draw per origin rather than per agent: one sample() call for each distinct
  # origin instead of one per mover.
  for (origin_geo in unique(agents$state[moves])) {
    idx <- which(moves & agents$state == origin_geo)
    row <- matrix[matrix$origin == origin_geo, ]
    agents$state[idx] <- sample(row$destination, length(idx),
                                replace = TRUE, prob = row$probability)
  }

  agents$n_moves[moves] <- agents$n_moves[moves] + 1L

  if (!is.null(out_of_country)) {
    gone <- moves & agents$state == out_of_country
    if (any(gone)) {
      agents$left_country[gone] <- TRUE
      agents$state[gone] <- NA_character_
    }
  }
  agents
}

#' Add returning providers to the roster
#'
#' The inflow counterpart to the out-of-country sink. Fraher et al. found that
#' under-forecast census divisions depended on providers returning from outside
#' the country, and that although the flow is small nationally it is material
#' for less-populous geographies.
#'
#' @param agents Agent tibble.
#' @param returners Tibble with `geo` and `n_returners`.
#' @param year Calendar year recorded as the returners' entry year.
#' @param age Age assigned to returners.
#' @return The agent tibble with returners appended.
#' @family provider geography
#' @concept geography
#' @export
add_returning_providers <- function(agents, returners, year, age = 45) {
  assertthat::assert_that(is.data.frame(returners))
  assertthat::assert_that(all(c("geo", "n_returners") %in% names(returners)))
  if (any(!is.finite(returners$n_returners)) ||
      any(returners$n_returners < 0)) {
    stop("add_returning_providers: n_returners must be finite, non-negative",
         call. = FALSE)
  }
  total <- sum(returners$n_returners)
  if (total <= 0) return(agents)

  new_agents <- tibble::tibble(
    state = rep(returners$geo,
                times = as.integer(round(returners$n_returners))),
    entry_year = year,
    age = age
  )
  for (col in setdiff(names(agents), names(new_agents))) {
    new_agents[[col]] <- if (is.numeric(agents[[col]])) 0 else
      if (is.logical(agents[[col]])) FALSE else NA
  }
  dplyr::bind_rows(agents, new_agents[names(agents)])
}
