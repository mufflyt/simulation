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
