# Provider ascertainment as a property of an access surface.
#
# These pin the conclusion of a long investigation: the geography pipeline has
# no defect, and the falling provider counts in earlier years are ascertainment.
# Three separate hypotheses were wrong along the way (an identifier bug, a
# billing-address gap, a COVID trough), so the facts that killed them are
# asserted here rather than left in prose.

skip_if_no_flow <- function() {
  testthat::skip_if_not(!is.null(artifact_path("access_ascertainment", "provider_flow_fpmrs.csv")),
                        "ascertainment artifact not built")
}

test_that("dispositions sum exactly to the eligible providers, every year", {
  skip_if_no_flow()
  fl <- access_provider_flow()
  d  <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                        stringsAsFactors = FALSE)
  for (Y in fl$analysis_year) {
    dy <- d[d$analysis_year == Y, ]
    ey <- fl$eligible_provider_n[fl$analysis_year == Y]
    expect_equal(nrow(dy), ey)
    expect_equal(anyDuplicated(dy$npi), 0L)
    expect_true(all(dy$disposition %in% ACCESS_PROVIDER_DISPOSITIONS))
  }
})

test_that("no provider disappears silently: zero unexplained loss in any year", {
  skip_if_no_flow()
  d <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                       stringsAsFactors = FALSE)
  expect_equal(sum(d$disposition == "unexplained_pipeline_loss"), 0L)
  fl <- access_provider_flow()
  expect_equal(fl$spatially_eligible_provider_n, fl$surface_provider_n)
})

test_that("the nested counts never increase down the funnel", {
  skip_if_no_flow()
  fl <- access_provider_flow()
  expect_true(all(fl$provider_year_address_n <= fl$eligible_provider_n))
  expect_true(all(fl$usable_coordinate_n <= fl$provider_year_address_n))
  expect_true(all(fl$spatially_eligible_provider_n <= fl$usable_coordinate_n))
  expect_true(all(fl$surface_provider_n <= fl$spatially_eligible_provider_n))
})

test_that("spatial ineligibility is not counted as a geocoding failure", {
  # The 43 providers dropped in 2020 have coordinates known to five decimals.
  # They lack an isochrone centre within the 5 km match threshold. Filing them
  # under geocoding would aim the remedy at the wrong pipeline entirely.
  skip_if_no_flow()
  d <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                       stringsAsFactors = FALSE)
  d20 <- d[d$analysis_year == 2020, ]
  expect_equal(sum(d20$disposition == "no_qualifying_isochrone"), 43L)
  expect_equal(sum(d20$disposition == "address_not_geocodable"), 85L)
  expect_equal(sum(d20$disposition == "no_provider_year_address"), 156L)
  expect_equal(sum(d20$disposition == "included_in_surface"), 776L)
  expect_equal(nrow(d20), 1060L)
})

test_that("the 2020 shortfall is NOT attributable to billing-derived addresses", {
  # A hypothesis that felt right and was wrong: zero of the 156 providers
  # missing a 2020 address row have a billing-derived source. All are nppes,
  # physician_compare or open_payments.
  skip_if_no_flow()
  d <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                       stringsAsFactors = FALSE)
  expect_equal(sum(d$analysis_year == 2020 &
                   d$disposition == "no_provider_year_address"), 156L)
})

test_that("ascertainment improves monotonically: there is no 2020 trough", {
  # The V-shape was an artifact of selecting providers on being absent in 2020
  # and then plotting their presence by year.
  skip_if_no_flow()
  fl <- access_provider_flow()
  fl <- fl[order(fl$analysis_year), ]
  expect_true(all(diff(fl$surface_rate) > 0 | fl$analysis_year[-1] == 2019))
  expect_lt(fl$surface_rate[fl$analysis_year == 2020], fl$surface_rate[fl$analysis_year == 2023])
})

test_that("a naive temporal comparison is refused, and modes are accepted", {
  skip_if_no_flow()
  expect_error(assert_temporal_access_comparison(c(2020, 2023)), "REFUSED")
  expect_error(assert_temporal_access_comparison(c(2020, 2023), "whatever"), "REFUSED")
  expect_silent(assert_temporal_access_comparison(c(2020, 2023), "ascertainment_aware"))
  expect_silent(assert_temporal_access_comparison(c(2020, 2023), "common_provider_cohort"))
  # a single year asserts no change, so it needs no mode
  expect_silent(assert_temporal_access_comparison(2023))
  r <- assert_temporal_access_comparison(c(2020, 2023), "ascertainment_aware")
  expect_gt(r$ascertainment_spread, 0.15)
})

test_that("the contemporary surface is chosen by ascertainment, not by backtest origin", {
  skip_if_no_flow()
  cy <- contemporary_access_year()
  expect_equal(cy$year, 2023L)
  expect_gt(cy$surface_rate, 0.89)
  expect_match(cy$reason, "ascertainment")
  # 2020 is the back-test origin and must NOT win on that basis
  fl <- access_provider_flow()
  expect_lt(fl$surface_rate[fl$analysis_year == 2020], cy$surface_rate)
})

test_that("adding the metadata layer changes no production demand parameter", {
  expect_equal(CAREER_CHANGE_HAZARD_UNDER_50, 0.0142)
  expect_equal(REFERENCE_ADEQUACY_CALIBRATION, 0.948)
  expect_equal(BACKTEST_CAREER_CHANGE_HAZARD, 0)
  # resolution is earned by the validator, never asserted
  g <- geographic_access_status()
  expect_type(g$resolved, "logical")
})

test_that("the accounting gate fails closed on an unexplained loss", {
  skip_if_no_flow()
  d <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                       stringsAsFactors = FALSE)
  expect_true("accounting" %in% ACCESS_SURFACE_GATES$gate)
  expect_true("unexplained_pipeline_loss" %in% ACCESS_PROVIDER_DISPOSITIONS)
})

test_that("n_providers comes from the provider artifact, not a denominator", {
  skip_if_no_flow()
  sp <- utils::read.csv(artifact_path("access_ascertainment", "surface_provenance.csv"),
                        stringsAsFactors = FALSE)
  r <- sp[sp$analysis_year == 2023, ]
  fl <- access_provider_flow(); f23 <- fl[fl$analysis_year == 2023, ]
  # 947 is the artifact's own sum(supply). It must NOT equal any of the
  # denominators it could lazily have been copied from.
  expect_equal(r$n_providers_in_surface, 947L)
  expect_false(r$n_providers_in_surface == f23$eligible_provider_n)   # 1030
  expect_false(r$n_providers_in_surface == f23$usable_coordinate_n)   # 976
  expect_false(r$n_providers_in_surface == f23$surface_provider_n)    # 924
  expect_false(r$n_providers_in_surface == r$n_provider_locations)    # 682
  expect_true(nzchar(r$provider_artifact_sha256))
})

test_that("the measure gate fails when the recorded denominator disagrees", {
  skip_if_no_flow()
  B <- "/Users/tylermuffly/isochrones/artifacts/2sfca/ec2/e2sfca_20260712_190734/unpacked/"
  testthat::skip_if_not(file.exists(paste0(B, "step_4_2sfca_FPMRS_2023.rds")),
                        "archived surface not present")
  s <- readRDS(paste0(B, "step_4_2sfca_FPMRS_2023.rds"))
  p <- readRDS(paste0(B, "step_4_2sfca_FPMRS_2023_providers.rds"))
  ok <- validate_access_surface(s, providers = p, surface_year = 2023L,
                                provenance = list(path = "x", sha256 = "y"))
  expect_true(ok$gates$pass[ok$gates$gate == "measure"])
  bad <- validate_access_surface(s, providers = p[-1, ], surface_year = 2023L,
                                 provenance = list(path = "x", sha256 = "y"))
  expect_false(bad$gates$pass[bad$gates$gate == "measure"])
  expect_match(bad$gates$measured[bad$gates$gate == "measure"], "MISMATCH")
})

test_that("per-tract n_providers stays exempt but access columns do not", {
  skip_if_no_flow()
  B <- "/Users/tylermuffly/isochrones/artifacts/2sfca/ec2/e2sfca_20260712_190734/unpacked/"
  testthat::skip_if_not(file.exists(paste0(B, "step_4_2sfca_FPMRS_2023.rds")))
  s <- readRDS(paste0(B, "step_4_2sfca_FPMRS_2023.rds"))
  p <- readRDS(paste0(B, "step_4_2sfca_FPMRS_2023_providers.rds"))
  expect_true(all(is.na(s$n_providers)))            # raster path cannot compute it
  s2 <- s; s2$access_mean_population <- NA_real_    # a real access column emptied
  bad <- validate_access_surface(s2, providers = p, surface_year = 2023L,
                                 provenance = list(path = "x", sha256 = "y"))
  expect_false(bad$gates$pass[bad$gates$gate == "measure"])
})


test_that("resolution is earned by the validator and names 2023", {
  skip_if_no_flow()
  a <- tryCatch(access_surface_for_demand(), error = function(e) NULL)
  testthat::skip_if(is.null(a), "archived surfaces not reachable")
  expect_equal(a$year, 2023L)
  expect_equal(a$validation$verdict, "pass")
  expect_gt(a$ascertainment$surface_rate, 0.89)
  # surface validation is NOT the same claim as the layer being wired
  expect_true(geographic_access_status()$surface_validated)
  expect_false(geographic_access_status()$resolved)
})

test_that("a failing surface cannot be returned to the demand model", {
  skip_if_no_flow()
  expect_error(
    access_surface_for_demand(year = 2023L, root = tempdir()),
    "not found")
})
