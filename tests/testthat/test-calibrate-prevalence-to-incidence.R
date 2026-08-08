# Guard for the DisMod-style prevalence-consistent onset calibrator that serves
# both UI and AI. It must (a) build observations with a usable SE from a reported
# CI, (b) integrate the modeled curve over each reported age interval with
# POPULATION weights (not a flat mean), (c) score the fit by a CI-informed
# likelihood so a wide-CI band constrains the curve less, (d) reproduce the
# observations within their uncertainty given an external remission, and
# (e) expose the onset<->remission joint identifiability so PSA samples the pair.

# NO source() OF PACKAGE CODE. This file used to do
#   source(here::here("R", "calibration-prevalence_to_incidence.R"))
# which works in a source checkout and fails under R CMD check, where the package
# is installed and no source R/ directory exists -- the connection cannot be
# opened and the whole file errors before a single expectation runs. It was the
# only test in the repository that sourced package code; every other one reaches
# the loaded namespace, which is what testthat::test_check() provides. Internal
# dot-prefixed helpers are visible that way too, because the test environment's
# parent IS the package namespace.
#
# Fifth instance of this shape in this repository (config/ twice,
# inst/legacy, and now R/): a test must never reach for a source-tree path.

# Wu 2014 clinical UI older-age bands, with plausible survey SEs expressed as CIs.
WU_UI <- do.call(rbind, list(
  prevalence_observation("60-69", 60, 69, 0.247, ci_lower = 0.210, ci_upper = 0.284),
  prevalence_observation("70-79", 70, 79, 0.297, ci_lower = 0.255, ci_upper = 0.339),
  prevalence_observation("80+",   80, 89, 0.382, ci_lower = 0.330, ci_upper = 0.434)))

test_that("prevalence_observation derives a usable SE and fails closed without one", {
  o <- prevalence_observation("b", 60, 69, 0.25, ci_lower = 0.20, ci_upper = 0.30)
  expect_equal(o$se, (0.30 - 0.20) / (2 * qnorm(0.975)), tolerance = 1e-8)
  # binomial SE when only n is given
  on <- prevalence_observation("b", 60, 69, 0.25, n = 400)
  expect_equal(on$se, sqrt(0.25 * 0.75 / 400), tolerance = 1e-8)
  # neither CI nor n nor se -> a "certain" observation would dominate: refuse
  expect_error(prevalence_observation("b", 60, 69, 0.25), "supply se, a CI, or n")
})

test_that("interval integration is POPULATION-weighted, not a flat mean", {
  ages <- 18:100
  p <- seq(0, 0.5, length.out = length(ages))          # rising prevalence
  flat_w <- rep(1, length(ages))
  young_w <- ifelse(ages == 60, 100, 1)                # concentrate weight at the youngest age
  flat <- .band_prevalence(p, ages, 60, 69, flat_w)
  wtd  <- .band_prevalence(p, ages, 60, 69, young_w)
  expect_equal(flat, mean(p[ages >= 60 & ages <= 69]), tolerance = 1e-8)  # uniform == flat mean
  expect_lt(wtd, flat)                                  # weighting the young pulls the band down
})

test_that("calibrator reproduces the Wu observations within their CI given a remission", {
  fit <- fit_prevalence_consistent_transitions(WU_UI, remission = 0.10)
  expect_true(fit$plausible)                    # onset a valid probability everywhere
  expect_true(fit$compatible)                   # every band within ~2 SE
  expect_lt(fit$worst_z, 2)
  expect_true(fit$fitted["80+"] > fit$fitted["60-69"])   # rises at >=80, as Wu does
})

test_that("a band with a WIDE CI is down-weighted in the fit", {
  wide <- WU_UI
  wide$se[wide$band == "80+"] <- wide$se[wide$band == "80+"] * 6   # loosen the 80+ anchor
  tight <- fit_prevalence_consistent_transitions(WU_UI, remission = 0.10)
  loose <- fit_prevalence_consistent_transitions(wide,  remission = 0.10)
  # loosening 80+ lets the curve pull away from that point more than the tight fit does
  expect_gt(abs(loose$fitted["80+"] - 0.382), abs(tight$fitted["80+"] - 0.382) - 1e-6)
})

test_that("recovers the incidence of a KNOWN onset+remission (round trip)", {
  ages <- 18:100
  true_inc <- .incidence_prob(as.numeric(.log_incidence_basis(ages) %*% c(-4, 0.4, 0.1, -0.05)))
  p <- prevalence_from_incidence(true_inc, remission = 0.08)
  obs <- do.call(rbind, lapply(list(c(60,69),c(70,79),c(80,89)), function(g) {
    pr <- mean(p[ages >= g[1] & ages <= g[2]])
    prevalence_observation(paste(g, collapse="-"), g[1], g[2], pr, se = 0.01)
  }))
  fit <- fit_prevalence_consistent_transitions(obs, remission = 0.08)
  expect_true(fit$compatible)                   # reproduces the generating prevalence
})

test_that("PSA returns correlated joint (remission, onset) draws with an identifiability read", {
  psa <- fit_prevalence_consistent_psa(WU_UI,
           remission_prior = function(n) stats::runif(n, 0.05, 0.15),
           n_draws = 40L)
  expect_equal(nrow(psa$draws), 40L)
  expect_true(all(psa$draws$plausible))                 # every draw a valid onset
  expect_true(all(psa$draws$compatible))                # every draw fits Wu within CI
  # onset and remission are NOT separately identified from prevalence: to hold the
  # same prevalence a higher remission must be met by a higher onset -> strong
  # POSITIVE correlation. PSA must sample the pair together, never independently.
  expect_gt(psa$identifiability, 0.5)
})
