# Regression guards for the ported workforce-microsimulation modules.
# Focused on the PURE, deterministic functions whose contracts are load-bearing.

test_that("E2SFCA incremental weights follow the diff(W^power) identity", {
  w <- c("30" = 1.00, "60" = 0.68, "120" = 0.22, "180" = 0.09)

  incr1 <- e2sfca_incremental_weights(w, step2_power = 1)
  expect_equal(unname(incr1), c(1.00 - 0.68, 0.68 - 0.22, 0.22 - 0.09, 0.09))

  # M2SFCA squares the CUMULATIVE weights THEN differences (not diff(W)^2).
  incr2 <- e2sfca_incremental_weights(w, step2_power = 2)
  expect_equal(unname(incr2), c(1.00^2 - 0.68^2, 0.68^2 - 0.22^2, 0.22^2 - 0.09^2, 0.09^2))
  expect_false(isTRUE(all.equal(unname(incr2), (unname(incr1))^2)))
})

test_that("active-in-year uses a STRICT > predicate (first-inactive convention)", {
  agents <- tibble::tibble(
    provider_id = c("a", "b", "c"),
    entry_year = c(2010, 2020, 2026),
    retirement_year = c(2030, NA, NA)
  )
  # a retires 2030 (first-inactive) => active THROUGH 2029, not in 2030.
  expect_equal(provider_active_in_year(agents, 2029), c(TRUE, TRUE, TRUE))
  expect_equal(provider_active_in_year(agents, 2030), c(FALSE, TRUE, TRUE))
  # c enters practice in 2026: not active in 2025, active from 2026.
  expect_equal(provider_active_in_year(agents, 2025), c(TRUE, TRUE, FALSE))
  expect_equal(provider_active_in_year(agents, 2026), c(TRUE, TRUE, TRUE))
})

test_that("workforce outlook classification matches cliff cutpoints", {
  expect_equal(classify_workforce_outlook(c(1.3, 1.0, 0.5, NA)),
               c("Adequate", "Marginal", "Insufficient", NA))
  expect_equal(classify_workforce_outlook(1.2), "Adequate")   # boundary inclusive
  expect_equal(classify_workforce_outlook(0.8), "Marginal")   # boundary inclusive
})

test_that("age banding matches cliff WC_BANDS", {
  expect_equal(microsim_age_band_of(c(34, 45, 49, 50, 64, 65, 70, 82)),
               c("<45", "45-49", "45-49", "50-54", "60-64", "65-69", "70+", "70+"))
})

test_that("hazard table scales the reference gradient to a subspecialty rate", {
  fpmrs <- build_hazard_table(0.044)
  expect_equal(unname(fpmrs), unname(MICROSIM_REFERENCE_HAZARD))
  go <- build_hazard_table(0.052)  # higher overall rate -> uniformly higher hazards
  expect_true(all(go >= fpmrs))
  expect_true(all(build_hazard_table(0.034) <= fpmrs))  # MIGS lower
})

test_that("anchor_index is 1 at the base year and grows geometrically", {
  idx <- anchor_index(2020:2030, y0 = 2020, v0 = 100, y1 = 2030, v1 = 200, base = 2025)
  expect_equal(idx[which(2020:2030 == 2025)], 1)
  # constant growth factor => ratio of successive terms is constant
  ratios <- idx[-1] / idx[-length(idx)]
  expect_equal(length(unique(round(ratios, 8))), 1)
})

test_that("haversine and CONUS gate behave", {
  expect_equal(haversine_km(0, 0, 0, 1), 111.19, tolerance = 0.1)
  expect_true(conus_ok(39.74, -104.99))    # Denver
  expect_false(conus_ok(61.2, -149.9))     # Anchorage, AK (out of scope)
  expect_false(conus_ok(21.3, -157.8))     # Honolulu, HI (out of scope)
})

test_that("point-to-isochrone match uses 5 km haversine, not exact coords", {
  iso <- tibble::tibble(coord_id = "denver", lat = 39.7400, lon = -104.9900)
  pts <- tibble::tibble(
    id = c("exact", "near_200m", "far"),
    lat = c(39.7400, 39.7418, 41.0000),   # ~0, ~200 m, ~140 km
    lon = c(-104.9900, -104.9900, -104.9900)
  )
  m <- match_points_to_isochrones(pts, iso, threshold_km = 5)
  expect_true(m$matched[m$id == "exact"])
  expect_true(m$matched[m$id == "near_200m"])   # same cluster -> must match
  expect_false(m$matched[m$id == "far"])
})

test_that("E2SFCA access reproduces a hand-computed example", {
  membership <- tibble::tibble(
    demand_id   = c("X", "Y", "Y"),
    provider_id = c("A", "A", "B"),
    band        = c(30, 60, 30)
  )
  supply <- tibble::tibble(provider_id = c("A", "B"), supply = c(10, 5))
  demand <- tibble::tibble(demand_id = c("X", "Y"), population = c(1000, 2000))

  res <- compute_e2sfca_access(membership, supply, demand,
                               weights = c("30" = 1.00, "60" = 0.68),
                               per_capita_scale = 1e5)

  # Step 1: R_A = 10 / (1000*0.32 + 2000*0.68); R_B = 5 / (2000*0.32)
  rA <- 10 / (1000 * 0.32 + 2000 * 0.68)
  rB <- 5 / (2000 * 0.32)
  # Step 2: A_X = 1.00*R_A ; A_Y = 0.68*R_A + 1.00*R_B
  ax <- 1.00 * rA
  ay <- 0.68 * rA + 1.00 * rB

  acc <- res$access
  expect_equal(acc$access_scaled[acc$demand_id == "X"], ax * 1e5, tolerance = 1e-6)
  expect_equal(acc$access_scaled[acc$demand_id == "Y"], ay * 1e5, tolerance = 1e-6)
  expect_equal(acc$n_providers[acc$demand_id == "Y"], 2L)
  expect_equal(res$audit$n_zero_demand_origins, 0L)
})

test_that("E2SFCA zero-demand provider yields ratio NA, not 0 capacity", {
  membership <- tibble::tibble(
    demand_id = c("Z"), provider_id = c("C"), band = c(30)
  )
  supply <- tibble::tibble(provider_id = "C", supply = 4)
  demand <- tibble::tibble(demand_id = "Z", population = 0)   # zero demand

  res <- compute_e2sfca_access(membership, supply, demand)
  pr <- res$provider_ratios
  expect_true(is.na(pr$ratio[pr$provider_id == "C"]))         # undefined, NOT 0
  expect_equal(pr$ratio_for_surface[pr$provider_id == "C"], 0)
  expect_equal(res$audit$n_zero_demand_origins, 1L)
})

test_that("the deterministic backbone tracks the stochastic engine", {
  # R/12 keeps project_supply_deterministic() alongside the microsimulation so the
  # stochastic engine can be validated against its analytic expectation. That
  # agreement was previously asserted only in a comment, and the two once diverged
  # by 37% at 2050 because conversion_floor was missing from one of them. Lock it.
  set.seed(20260801)
  agents <- initialize_provider_agents(600, "FPMRS", 2025)
  agents$sex <- rep(c("female", "male"), length.out = nrow(agents))
  ic <- calibrate_hours_intercept(agents$age, agents$sex)

  agree_within <- function(conversion, tol_pct) {
    det <- project_supply_deterministic(agents, 2025:2050, 40,
                                        conversion_floor = conversion,
                                        hours_intercept = ic)
    sto <- run_supply_microsimulation(agents, 2025:2050, 40, "FPMRS",
                                      n_iterations = 60, conversion_floor = conversion,
                                      hours_intercept = ic, verbose = FALSE)
    j <- dplyr::inner_join(det, sto$summary, by = "year")
    expect_lt(max(abs(100 * (j$headcount_median - j$headcount) / j$headcount)), tol_pct)
    expect_lt(max(abs(100 * (j$effective_fte_median - j$effective_fte) / j$effective_fte)),
              tol_pct)
  }

  agree_within(1.0, 3)
  # The conversion haircut must be applied by BOTH engines, or they diverge as the
  # entrant cohorts accumulate.
  agree_within(0.7, 3)
})

test_that("SPAR rescales access to a national mean of 1", {
  access <- tibble::tibble(
    demand_id = c("a", "b", "c", "d"),
    population = c(1000, 1000, 2000, 1000),
    access_scaled = c(0, 10, 20, 30)
  )
  s <- summarize_access(access)
  # Population-weighted mean: (0*1000 + 10*1000 + 20*2000 + 30*1000) / 5000
  expect_equal(s$mean_access, 16)
  expect_equal(s$zero_access_share, 0.2)          # one 1000-person unit at zero

  spar <- spatial_access_ratio(access)
  expect_equal(spar$relative_access, c(0, 10, 20, 30) / 16)
  # The SPAR of the population-weighted mean is 1.0 by construction.
  expect_equal(sum(spar$relative_access * spar$population) / sum(spar$population), 1)
})

test_that("access thresholds report population shares at or above each cut", {
  access <- tibble::tibble(
    demand_id = letters[1:4], population = c(100, 100, 100, 100),
    access_scaled = c(0, 1, 5, 50)
  )
  ts <- summarize_access(access, thresholds = c(0, 1, 5, 50))$threshold_shares
  expect_equal(ts$pop_share_at_or_above, c(1.00, 0.75, 0.50, 0.25))
})

test_that("access categories are a zero class plus QUARTILES of positive access", {
  # Documented contract: category 1 is zero-access, 2-5 are quartiles of the
  # POSITIVE distribution -- not quintiles of everything.
  cats <- assign_access_category(c(0, 0, 1, 2, 3, 4, 5, 6, 7, 8))
  expect_equal(cats[1:2], c(1L, 1L))
  expect_equal(sort(unique(cats)), 1:5)
  expect_true(all(diff(cats[-(1:2)]) >= 0))       # monotone in access
  # All-zero input collapses to the zero class rather than erroring.
  expect_equal(assign_access_category(c(0, 0, 0)), rep(1L, 3))
})

test_that("migration hazard is highest early-career and lowest late-career", {
  expect_equal(migration_hazard(2, 40), unname(PROVIDER_MIGRATION_HAZARD[["early_career"]]))
  expect_equal(migration_hazard(20, 45), unname(PROVIDER_MIGRATION_HAZARD[["mid_career"]]))
  expect_equal(migration_hazard(20, 65), unname(PROVIDER_MIGRATION_HAZARD[["late_career"]]))
  # Vectorised, and age must be honoured element-wise.
  h <- migration_hazard(c(1, 20, 20), c(35, 45, 65))
  expect_equal(h, unname(PROVIDER_MIGRATION_HAZARD[c("early_career", "mid_career",
                                                     "late_career")]))
})

test_that("provider migration moves only some providers and never invents states", {
  agents <- tibble::tibble(
    provider_id = sprintf("P%02d", 1:200),
    entry_year = 2023, age = 40,
    state = rep(c("CO", "NY"), 100)
  )
  shares <- tibble::tibble(geo = c("CO", "NY", "TX"), share = c(0.4, 0.4, 0.2))
  set.seed(11)
  moved <- apply_provider_migration(agents, 2025, shares)

  expect_equal(nrow(moved), nrow(agents))
  expect_true(all(moved$state %in% shares$geo))
  expect_true(all(moved$n_moves %in% c(0L, 1L)))   # one move per provider per year
  # Early-career hazard is 4.5%/yr, so most providers must stay put.
  expect_lt(sum(moved$n_moves), nrow(agents) * 0.2)
  expect_gt(sum(moved$n_moves), 0)
  # A roster with no state column is passed through untouched.
  expect_identical(apply_provider_migration(dplyr::select(agents, -"state"), 2025, shares),
                   dplyr::select(agents, -"state"))
})

test_that("safe_left_join blocks unintended fan-out", {
  x <- tibble::tibble(k = c(1, 2, 3), v = c("a", "b", "c"))
  y_dup <- tibble::tibble(k = c(1, 1, 2), w = c(10, 11, 12))
  expect_error(safe_left_join(x, y_dup, by = "k"), "fan out")
  y_ok <- tibble::tibble(k = c(1, 2), w = c(10, 12))
  expect_equal(nrow(safe_left_join(x, y_ok, by = "k")), 3L)
})

test_that("provenance round-trip verifies content SHA and rejects tampering", {
  tmp <- tempfile(fileext = ".rds")
  obj <- data.frame(a = 1:3, b = letters[1:3])
  write_artifact_with_provenance(obj, tmp, inputs = list(seed = 1), run_id = "test_run")
  back <- read_artifact_with_provenance(tmp, mode = "relaxed")
  expect_equal(back, obj)

  # Tamper with the payload but keep the sidecar -> must reject (return NULL in relaxed).
  saveRDS(data.frame(a = 9:11, b = letters[4:6]), tmp)
  expect_null(read_artifact_with_provenance(tmp, mode = "relaxed"))
})
