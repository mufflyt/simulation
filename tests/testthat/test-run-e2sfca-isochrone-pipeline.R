# Unit tests for end-to-end E2SFCA real isochrone pipeline (Steps 1 - 4)

.repo_path <- function(...) {
  p1 <- file.path(...)
  p2 <- file.path("..", "..", ...)
  if (file.exists(p1)) p1 else p2
}

test_that("run_e2sfca_isochrone_pipeline.R script exists and has valid syntax", {
  script_path <- .repo_path("scripts", "run_e2sfca_isochrone_pipeline.R")
  testthat::skip_if_not(file.exists(script_path), "source file not shipped under R CMD check")
  expect_true(file.exists(script_path))

  parsed <- tryCatch(parse(script_path), error = function(e) e)
  expect_false(inherits(parsed, "error"))
})

test_that("load_tract_demand loads real Census tract female demand", {
  demand <- load_tract_demand()
  expect_s3_class(demand, "data.frame")
  expect_gt(nrow(demand), 80000L) # 83,492 US Census tracts
  expect_true(all(c("demand_id", "population", "lon", "lat") %in% names(demand)))
  expect_gt(sum(demand$population), 30000000) # ~30.5M women 65+
})

test_that("compute_e2sfca_access produces valid SPAR scores and catchments", {
  demand <- load_tract_demand()[1:50, ]
  supply <- tibble::tibble(provider_id = c("P1", "P2"), supply = c(1.0, 1.0))
  membership <- tibble::tibble(
    demand_id = rep(demand$demand_id, 2),
    provider_id = rep(c("P1", "P2"), each = 50),
    band = 30L
  )

  access_res <- compute_e2sfca_access(
    membership = membership,
    supply = supply,
    demand = demand
  )
  expect_type(access_res, "list")
  expect_true(all(c("access", "provider_ratios") %in% names(access_res)))

  catchments <- e2sfca_catchments_from_access(access_res)
  expect_s3_class(catchments, "data.frame")
  expect_equal(nrow(catchments), 2L)
  expect_true(all(c("catchment", "demand_workload", "accessible_capacity", "adequacy_relative") %in% names(catchments)))
})
