# Tests for the E2SFCA -> catchments -> Lizeth join glue
# (R/calibration-isochrone_access_response.R). Synthetic fixtures only: the real
# isochrone artifacts (mufflyt/isochrones) and Lizeth export (../lizeth) are not
# present in CI, so these pin the transformation arithmetic and the join
# contract, not an end-to-end run.

# A minimal compute_e2sfca_access()-shaped result: three providers, the third
# with zero competed-for demand (its ratio is undefined).
.fixture_access <- function() {
  list(
    provider_ratios = tibble::tibble(
      provider_id = c("A", "B", "C"),
      weighted_demand = c(100, 200, 0),
      supply = c(50, 50, 10),
      zero_demand = c(FALSE, FALSE, TRUE),
      ratio = c(0.5, 0.25, NA_real_),
      ratio_for_surface = c(0.5, 0.25, 0)
    )
  )
}

test_that("e2sfca_catchments_from_access builds clear_access-currency loads", {
  cat <- e2sfca_catchments_from_access(.fixture_access())
  # zero-demand provider C is dropped by default.
  expect_equal(nrow(cat), 2L)
  expect_setequal(cat$catchment, c("A", "B"))
  # demand_workload = weighted_demand * workload_per_capita (1); capacity = supply.
  a <- cat[cat$catchment == "A", ]
  b <- cat[cat$catchment == "B", ]
  expect_equal(a$demand_workload, 100)
  expect_equal(a$accessible_capacity, 50)
  expect_equal(b$demand_workload, 200)
  # adequacy_relative = capacity / workload (= ratio when workload_per_capita = 1).
  expect_equal(a$adequacy_relative, 0.5)
  expect_equal(b$adequacy_relative, 0.25)
  expect_equal(a$e2sfca_ratio, 0.5)
  # the columns fit_wait_scale()/forward_lizeth_adequacy() require are present...
  expect_true(all(c("demand_workload", "accessible_capacity") %in% names(cat)))
  # ...and finite-positive, so clear_access() yields a defined rho.
  expect_true(all(cat$demand_workload > 0))
  expect_true(all(cat$accessible_capacity > 0))
})

test_that("workload_per_capita rescales demand but not capacity or ordering", {
  base_cat <- e2sfca_catchments_from_access(.fixture_access(),
                                            workload_per_capita = 1)
  scaled <- e2sfca_catchments_from_access(.fixture_access(),
                                          workload_per_capita = 2)
  # demand doubles, capacity unchanged, adequacy halves.
  expect_equal(scaled$demand_workload, base_cat$demand_workload * 2)
  expect_equal(scaled$accessible_capacity, base_cat$accessible_capacity)
  expect_equal(scaled$adequacy_relative, base_cat$adequacy_relative / 2)
  # rank-preserving: the scale cannot reorder providers by adequacy.
  expect_equal(order(scaled$adequacy_relative), order(base_cat$adequacy_relative))
})

test_that("drop_zero_demand = FALSE keeps zero-demand providers with NA adequacy", {
  cat <- e2sfca_catchments_from_access(.fixture_access(), drop_zero_demand = FALSE)
  expect_equal(nrow(cat), 3L)
  c_row <- cat[cat$catchment == "C", ]
  expect_true(is.na(c_row$adequacy_relative))
  expect_true(is.na(c_row$e2sfca_ratio))
})

test_that("e2sfca_catchments_from_access accepts a provider_ratios data frame", {
  pr <- .fixture_access()$provider_ratios
  cat <- e2sfca_catchments_from_access(pr)
  expect_equal(nrow(cat), 2L)
  expect_equal(attr(cat, "capacity_anchor"), "medicare_procedure_volume_fem65")
})

test_that("e2sfca_catchments_from_access rejects malformed input", {
  expect_error(e2sfca_catchments_from_access(list(nope = 1)),
               "provider_ratios")
  expect_error(
    e2sfca_catchments_from_access(tibble::tibble(provider_id = "A", supply = 1)),
    "weighted_demand"
  )
  expect_error(e2sfca_catchments_from_access(.fixture_access(),
                                             workload_per_capita = -1),
               "positive")
})

test_that("join_lizeth_to_catchments attaches access by NPI and audits misses", {
  cat <- e2sfca_catchments_from_access(.fixture_access())
  lizeth <- tibble::tibble(
    npi = c("A", "B", "X"),
    wait_business_days = c(10, 20, 30)
  )
  joined <- join_lizeth_to_catchments(lizeth, cat)
  expect_equal(joined$matched, c(TRUE, TRUE, FALSE))
  expect_equal(attr(joined, "match_rate"), 2 / 3)
  # matched rows carry the catchment's access; the miss carries NA.
  expect_equal(joined$accessible_capacity[joined$npi == "A"], 50)
  expect_true(is.na(joined$accessible_capacity[joined$npi == "X"]))
  # the outcome column is preserved untouched.
  expect_equal(joined$wait_business_days, c(10, 20, 30))
})

test_that("join_lizeth_to_catchments honours a non-NPI crosswalk", {
  cat <- e2sfca_catchments_from_access(.fixture_access())
  lizeth <- tibble::tibble(npi = c("n1", "n2"), wait_business_days = c(5, 15))
  xwalk <- data.frame(npi = c("n1", "n2"), catchment = c("A", "B"),
                      stringsAsFactors = FALSE)
  joined <- join_lizeth_to_catchments(lizeth, cat, crosswalk = xwalk)
  expect_true(all(joined$matched))
  expect_equal(joined$catchment, c("A", "B"))
  expect_equal(joined$adequacy_relative, c(0.5, 0.25))
})

test_that("join_lizeth_to_catchments validates its inputs", {
  cat <- e2sfca_catchments_from_access(.fixture_access())
  expect_error(
    join_lizeth_to_catchments(tibble::tibble(x = 1), cat),
    "no `npi` column"
  )
  expect_error(
    join_lizeth_to_catchments(tibble::tibble(npi = "A"),
                              tibble::tibble(catchment = "A")),
    "accessible_capacity"
  )
  expect_error(
    join_lizeth_to_catchments(tibble::tibble(npi = "A"), cat,
                              crosswalk = data.frame(npi = "A")),
    "crosswalk"
  )
})

# --- Build #3: wait-response loss + decay-sigma fit --------------------------

test_that("lizeth_wait_response_loss recovers wait_scale closed-form", {
  # rho known -> x = rho/(1-rho); wait = k * x with k = 12 exactly.
  rho <- c(0.2, 0.4, 0.6, 0.8)
  x <- rho / (1 - rho)
  rt <- data.frame(
    wait_business_days = 12 * x,
    demand_workload = rho,          # capacity = 1 => rho = demand_workload
    accessible_capacity = rep(1, 4)
  )
  loss <- lizeth_wait_response_loss(rt)
  expect_equal(loss$wait_scale, 12, tolerance = 1e-8)
  expect_lt(loss$sse, 1e-12)
  expect_equal(loss$n_used, 4L)
  expect_equal(loss$n_censored, 0L)
})

test_that("lizeth_wait_response_loss excludes and counts saturated calls", {
  rt <- data.frame(
    wait_business_days = c(3, 6, 99, 99),
    demand_workload = c(0.25, 0.5, 1.0, 1.5),  # last two rho >= 1 (saturated)
    accessible_capacity = rep(1, 4)
  )
  loss <- lizeth_wait_response_loss(rt)
  expect_equal(loss$n_used, 2L)
  expect_equal(loss$n_censored, 2L)
})

test_that("lizeth_wait_response_loss validates inputs", {
  expect_error(
    lizeth_wait_response_loss(data.frame(wait_business_days = 1)),
    "demand_workload"
  )
  expect_error(
    lizeth_wait_response_loss(data.frame(
      wait_business_days = 5, demand_workload = 0.5, accessible_capacity = 1
    )),
    "at least two"
  )
})

# A base-R synthetic access model: each provider has band-level competing
# populations and a fixed supply; rho_j(sigma) = sum_b g(b;sigma) pop_jb / supply.
# Injected as catchments_for_sigma so the optimiser is exercised without the
# dplyr-backed E2SFCA recompute.
.decay_fixture <- function(true_sigma, true_wait_scale, n = 150, seed = 42) {
  set.seed(seed)
  bands <- c(30, 60, 120, 180)
  pop <- matrix(runif(n * 4, 20, 200), n, 4)
  supply <- runif(n, 300, 900)
  npi <- sprintf("np%03d", seq_len(n))
  gdecay <- function(b, s) exp(-(b^2) / (2 * s^2))
  cfs <- function(sigma) {
    wd <- as.numeric(pop %*% gdecay(bands, sigma))
    data.frame(catchment = npi, demand_workload = wd,
               accessible_capacity = supply, weight = wd,
               adequacy_relative = supply / wd, stringsAsFactors = FALSE)
  }
  cat0 <- cfs(true_sigma)
  rho <- cat0$demand_workload / cat0$accessible_capacity
  lizeth <- data.frame(npi = npi,
                       wait_business_days = true_wait_scale * rho / (1 - rho),
                       stringsAsFactors = FALSE)
  list(cfs = cfs, lizeth = lizeth, share_unsaturated = mean(rho < 1))
}

test_that("fit_decay_sigma recovers sigma and wait_scale on noiseless data", {
  fx <- .decay_fixture(true_sigma = 55, true_wait_scale = 22)
  expect_gt(fx$share_unsaturated, 0.9)   # fixture is well-posed
  fit <- fit_decay_sigma(fx$lizeth, fx$cfs, sigma_bounds = c(15, 240))
  expect_equal(fit$sigma, 55, tolerance = 3)
  expect_equal(fit$wait_scale, 22, tolerance = 0.5)
  expect_equal(fit$calibration_status, "fitted_to_lizeth_wait_response")
})

test_that("fit_decay_sigma is not anchored to one sigma (alternate truth)", {
  fit <- with(.decay_fixture(true_sigma = 90, true_wait_scale = 14),
              fit_decay_sigma(lizeth, cfs, sigma_bounds = c(15, 240)))
  expect_equal(fit$sigma, 90, tolerance = 4)
  expect_equal(fit$wait_scale, 14, tolerance = 1)
})

test_that("fit_decay_sigma validates its inputs", {
  fx <- .decay_fixture(true_sigma = 55, true_wait_scale = 22)
  expect_error(fit_decay_sigma(fx$lizeth, "not a function"), "must be a function")
  expect_error(
    fit_decay_sigma(fx$lizeth, fx$cfs, sigma_bounds = c(240, 15)),
    "increasing positive"
  )
  expect_error(
    fit_decay_sigma(fx$lizeth, fx$cfs, n_grid = 2),
    "at least 3"
  )
})

# --- Build #4: geographic holdout guard -------------------------------------

# Per-call rows across regions; wait = k_region * rho/(1-rho) + small noise.
.holdout_fixture <- function(k_by_region, n_per = 40, seed = 1) {
  set.seed(seed)
  do.call(rbind, lapply(names(k_by_region), function(rg) {
    rho <- runif(n_per, 0.1, 0.85)
    data.frame(
      wait_business_days = k_by_region[[rg]] * rho / (1 - rho) +
        rnorm(n_per, 0, 0.3),
      demand_workload = rho,
      accessible_capacity = rep(1, n_per),
      region = rg,
      stringsAsFactors = FALSE
    )
  }))
}

test_that("wait_response_region_holdout confirms a transportable response", {
  rt <- .holdout_fixture(setNames(rep(20, 6), paste0("R", 1:6)))
  h <- wait_response_region_holdout(rt)
  expect_equal(h$n_regions, 6L)
  expect_equal(h$metrics$calibration_slope, 1, tolerance = 0.15)
  expect_gt(h$metrics$r2_oos, 0.8)
})

test_that("wait_response_region_holdout flags a non-transportable response", {
  rt <- .holdout_fixture(setNames(c(5, 10, 15, 60, 80, 100), paste0("R", 1:6)))
  h <- wait_response_region_holdout(rt)
  # a response that differs sharply by region does not predict held-out regions.
  expect_lt(h$metrics$calibration_slope, 0.75)
})

test_that("wait_response_region_holdout validates inputs", {
  rt <- .holdout_fixture(setNames(rep(20, 6), paste0("R", 1:6)))
  expect_error(
    wait_response_region_holdout(rt[, c("wait_business_days", "region")]),
    "demand_workload"
  )
  few <- .holdout_fixture(setNames(rep(20, 2), paste0("R", 1:2)))
  expect_error(wait_response_region_holdout(few), "at least 4 distinct regions")
})

test_that("capacity_status_with_isochrone_response resolves only when transportable", {
  stub <- list(resolved = FALSE, source = "prior")
  fit <- list(sigma = 55, wait_scale = 20)

  ok <- wait_response_region_holdout(
    .holdout_fixture(setNames(rep(20, 6), paste0("R", 1:6)))
  )
  s_ok <- capacity_status_with_isochrone_response(fit, ok, base_status = stub)
  expect_true(s_ok$resolved)
  expect_equal(s_ok$calibration_status, "fitted_and_geographically_validated")
  expect_equal(s_ok$fitted_sigma, 55)

  bad <- wait_response_region_holdout(
    .holdout_fixture(setNames(c(5, 10, 15, 60, 80, 100), paste0("R", 1:6)))
  )
  s_bad <- capacity_status_with_isochrone_response(fit, bad, base_status = stub)
  expect_false(s_bad$resolved)
  expect_equal(s_bad$calibration_status, "fitted_but_not_transportable")
  expect_match(s_bad$why_unresolved, "did not transport")
})

test_that("capacity_status_with_isochrone_response validates inputs", {
  ok <- wait_response_region_holdout(
    .holdout_fixture(setNames(rep(20, 6), paste0("R", 1:6)))
  )
  expect_error(
    capacity_status_with_isochrone_response(list(sigma = 1), ok,
                                            base_status = list()),
    "fit_decay_sigma"
  )
  expect_error(
    capacity_status_with_isochrone_response(list(sigma = 1, wait_scale = 1),
                                            list(nope = TRUE),
                                            base_status = list()),
    "wait_response_region_holdout"
  )
})
