# Guard for the general prevalence->incidence calibrator (DisMod/CISNET style) that
# serves both UI and AI. It must (a) reproduce age-band prevalence given an external
# remission, (b) keep onset in (0,1), and (c) EXPOSE the onset<->remission
# identifiability -- different remission values fit the same prevalence with
# different onset, so PSA must sample the joint pair, never the two independently.

suppressPackageStartupMessages(library(here))
source(here::here("R", "calibrate_prevalence_to_incidence.R"))

WU_UI <- c("60-69" = 0.247, "70-79" = 0.297, "80+" = 0.382)   # Wu 2014 clinical UI
BANDS <- list("60-69" = 60:69, "70-79" = 70:79, "80+" = 80:89)

test_that("calibrator reproduces the target age-band prevalence given a remission", {
  fit <- calibrate_onset_given_remission(WU_UI, BANDS, remission = 0.10)
  expect_lt(fit$worst, 0.05)                 # within 5% on every band...
  expect_true(fit$plausible)                 # ...with a valid onset hazard
  expect_true(fit$fitted["80+"] > fit$fitted["60-69"])   # rises at >=80 (as Wu does)
})

test_that("onset and remission are only jointly identified from prevalence", {
  lo <- calibrate_onset_given_remission(WU_UI, BANDS, remission = 0.05)
  hi <- calibrate_onset_given_remission(WU_UI, BANDS, remission = 0.15)
  # both fit the SAME prevalence well...
  expect_lt(lo$worst, 0.08); expect_lt(hi$worst, 0.08)
  # ...but require materially different onset -> not separately identifiable
  expect_gt(hi$onset["65"] - lo$onset["65"], 0.01)
})

test_that("recovers the prevalence of a KNOWN onset+remission (round trip)", {
  ages <- 18:100
  true_onset <- stats::plogis(as.numeric(.onset_age_basis(ages) %*% c(-4, 0.4, 0.1, -0.05)))
  p <- prevalence_from_onset(true_onset, remission = 0.08)
  tgt <- vapply(BANDS, function(g) mean(p[match(g, ages)]), numeric(1))
  fit <- calibrate_onset_given_remission(tgt, BANDS, remission = 0.08)
  expect_lt(fit$worst, 0.02)                 # reproduces the generating prevalence
})

test_that("PSA returns correlated joint (remission, onset) draws with an identifiability read", {
  psa <- calibrate_onset_psa(WU_UI, BANDS,
           remission_prior = function(n) stats::runif(n, 0.05, 0.15),
           n_draws = 40L)
  expect_equal(nrow(psa$draws), 40L)
  expect_true(all(psa$draws$worst < 0.10))          # every draw fits prevalence
  expect_true(all(psa$draws$plausible))
  # onset and remission are NOT separately identified from prevalence: to hold the
  # SAME prevalence a higher remission must be met by a higher onset, so the fitted
  # (remission, onset@65) pair is strongly POSITIVELY correlated across the prior.
  # PSA must therefore sample the joint pair, never the two independently.
  expect_gt(psa$identifiability, 0.5)
})
