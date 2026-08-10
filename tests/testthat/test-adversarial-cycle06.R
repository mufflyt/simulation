# Adversarial cycle 06 -- the systematic range-guard sweep, and the demand-side
# multipliers it found.
#
# Cycles 03, 04 and 05 each found the same class opportunistically, in a
# different module: negative provider counts, sum-to-one validators without a
# range check, probability arguments to the aging recurrence. Cycle 05 carried
# forward the instruction to stop tripping over it and enumerate instead.
#
# The sweep listed every exported argument documented as a probability, share,
# fraction, rate or hazard, then probed the high-consequence ones. Most were
# already guarded -- conservative_management_multipliers(), urps_migration_matrix(),
# telemedicine_reach(), clear_access_trajectory(), compute_namcs_demand_estimand()
# all refuse out-of-range input. Three did not, and all three are multipliers
# that scale the headline demand estimate directly.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

# Minimal cell table with the five columns project_urps_demand() reads.
cyc06_cells <- function() {
  data.frame(
    age_group = rep(URPS_POP_AGE_BANDS, each = 2),
    insurance = rep(c("Insured", "Uninsured"), times = length(URPS_POP_AGE_BANDS)),
    income_tier = rep(c("GT100k", "LT25k"), times = length(URPS_POP_AGE_BANDS)),
    pop_weight = rep(c(6e6, 2e6), times = length(URPS_POP_AGE_BANDS)),
    ui_prevalence = rep(c(0.12, 0.24, 0.35, 0.44, 0.51), each = 2),
    stringsAsFactors = FALSE
  )
}
cyc06_visits <- function(x) sum(x$n_urgy_visits)

cyc06_pop <- function(pop = 1e6) {
  data.frame(year = rep(2025L, length(DEMAND_AGE_BANDS)),
             age_band = DEMAND_AGE_BANDS,
             female_pop = rep(pop, length(DEMAND_AGE_BANDS)),
             stringsAsFactors = FALSE)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the care-seeking and referral fractions are closed on [0, 1]", {
  pop <- cyc06_pop()
  cells <- cyc06_cells()

  # Both endpoints are meaningful: 0 is "nobody seeks care", 1 is "everybody
  # does", and both are legitimate scenario bounds rather than errors.
  expect_silent(project_urps_demand(cells, care_seeking_rate = 0, referral_rate = 0,
                                    verbose = FALSE))
  expect_silent(project_urps_demand(cells, care_seeking_rate = 1, referral_rate = 1,
                                    verbose = FALSE))
  expect_error(project_urps_demand(cells, care_seeking_rate = 1 + 1e-9, verbose = FALSE),
               "care_seeking_rate")
  expect_error(project_urps_demand(cells, referral_rate = -1e-9, verbose = FALSE),
               "referral_rate")
  expect_error(project_urps_demand(cells, care_seeking_rate = NA_real_, verbose = FALSE),
               "finite")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a consultation rate is bounded below but not above; a per-1,000 rate is bounded by 1,000", {
  # These two are NOT the same shape and must not be guarded as if they were.
  # A woman can consult more than once a year, so consult_rate > 1 is legal.
  # A surgery rate per 1,000 above 1,000 would operate on more women than exist.
  pop <- cyc06_pop()
  cr <- CONSULT_RATE_BY_AGE; cr[] <- 1.8
  expect_silent(compute_demand_denominators(pop, consult_rate = cr))

  cr_neg <- CONSULT_RATE_BY_AGE; cr_neg[] <- -1e-9
  expect_error(compute_demand_denominators(pop, consult_rate = cr_neg), "consult_rate")

  sr <- WU2011_SURGERY_RATE_PER_1000; sr[] <- 1000
  expect_silent(compute_demand_denominators(pop, surgery_rate_per_1000 = sr))
  sr_hi <- WU2011_SURGERY_RATE_PER_1000; sr_hi[] <- 1000 + 1e-6
  expect_error(compute_demand_denominators(pop, surgery_rate_per_1000 = sr_hi),
               "surgery_rate_per_1000")
  sr_neg <- WU2011_SURGERY_RATE_PER_1000; sr_neg[] <- -1
  expect_error(compute_demand_denominators(pop, surgery_rate_per_1000 = sr_neg),
               "surgery_rate_per_1000")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the shared range helper is inclusive at both ends and names its caller", {
  expect_silent(.assert_in_range(c(0, 0.5, 1), "p"))
  expect_error(.assert_in_range(1 + 1e-12, "p"), "in \\[0, 1\\]")
  expect_error(.assert_in_range(-1e-12, "p"), "in \\[0, 1\\]")
  expect_error(.assert_in_range(NaN, "p"), "finite")
  expect_error(.assert_in_range(Inf, "p"), "finite")
  expect_error(.assert_in_range("0.5", "p"), "finite")
  # An open upper bound is expressible, for genuinely unbounded rates.
  expect_silent(.assert_in_range(c(0, 5, 1e6), "r", lo = 0, hi = Inf))
  # The message names the parameter and the offending value, so a caller can
  # find it without reading the source.
  expect_error(.assert_in_range(1.5, "care_seeking_rate", fn = "f"),
               "care_seeking_rate")
  expect_error(.assert_in_range(1.5, "p", fn = "f"), "1.5")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: demand scales exactly with the product of the two access fractions", {
  # care_seeking_rate and referral_rate enter as a pure product. If the
  # relationship is not exactly multiplicative, one of them is being applied
  # somewhere else as well and the scenario grid double-counts it.
  cells <- cyc06_cells()
  base <- project_urps_demand(cells, care_seeking_rate = 0.25, referral_rate = 0.50,
                              verbose = FALSE)
  half <- project_urps_demand(cells, care_seeking_rate = 0.125, referral_rate = 0.50,
                              verbose = FALSE)
  swap <- project_urps_demand(cells, care_seeking_rate = 0.50, referral_rate = 0.25,
                              verbose = FALSE)

  expect_equal(cyc06_visits(half), cyc06_visits(base) / 2)
  # Only the product matters: swapping the two fractions cannot move the answer.
  expect_equal(cyc06_visits(swap), cyc06_visits(base))

  # Zero on either side is zero demand, not a floor.
  zero <- project_urps_demand(cells, care_seeking_rate = 0, verbose = FALSE)
  expect_equal(cyc06_visits(zero), 0)
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the three demand denominators are distinct estimands, not rescalings", {
  # D1/D2/D3 come from different sources and answer different questions. If any
  # two are exact multiples of each other across populations, their agreement
  # is arithmetic and carries no corroboration -- the thing
  # assert_estimands_independent() exists to refuse.
  small <- compute_demand_denominators(cyc06_pop(1e6))
  large <- compute_demand_denominators(cyc06_pop(2e6))

  # Each is linear in population (they are all per-capita rates applied to it).
  expect_equal(large$demand_cases, small$demand_cases * 2)
  expect_setequal(small$estimand, c("D1", "D2", "D3"))

  # But they are ordered and separated: prevalent cases exceed consultations,
  # which exceed surgeries. An inversion would mean a rate table is misaligned
  # to its age bands.
  v <- setNames(small$demand_cases, small$estimand)
  expect_gt(v[["D1"]], v[["D2"]])
  expect_gt(v[["D2"]], v[["D3"]])
  # And no two share a ratio of exactly 1 -- they are not the same number
  # relabelled.
  expect_false(any(duplicated(round(v, 6))))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: an age band absent from the rate table stops instead of dropping demand", {
  # A band silently missing its rate contributes zero, and zero looks exactly
  # like "no demand in that band" -- the understatement is invisible in the
  # output. compute_demand_denominators() already refuses it; this pins that
  # the refusal covers the band, not just the join.
  bad <- cyc06_pop()
  bad$age_band[1] <- "not_a_band"
  expect_error(compute_demand_denominators(bad), "unknown age band")

  # Dropping a band entirely is legal (a population may not span every band)
  # and must reduce demand rather than error.
  fewer <- cyc06_pop()[-1, ]
  full <- compute_demand_denominators(cyc06_pop())
  part <- compute_demand_denominators(fewer)
  expect_true(all(part$demand_cases < full$demand_cases))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no access fraction can manufacture demand above prevalence", {
  # The referral cascade is a filter: prevalent -> seeking -> referred. Each
  # stage can only remove women. If any parameterisation lets the final count
  # exceed the prevalent pool, the cascade is multiplying where it should be
  # filtering.
  pop <- cyc06_pop()
  d <- compute_demand_denominators(pop)
  prevalent <- d$demand_cases[d$estimand == "D1"]

  cells <- cyc06_cells()
  for (cs in c(0, 0.25, 0.5, 1)) for (rr in c(0, 0.5, 1)) {
    got <- project_urps_demand(cells, care_seeking_rate = cs, referral_rate = rr,
                               verbose = FALSE)
    expect_true(cyc06_visits(got) >= 0,
                info = sprintf("care_seeking=%g referral=%g gave negative demand", cs, rr))
    # Every stage of the cascade can only REMOVE women, so referrals can never
    # exceed the prevalent pool. All the insurance and income multipliers are
    # <= 1, which is what makes that true; one above 1 would let the effective
    # care-seeking rate exceed the rate the caller asked for.
    expect_lte(cyc06_visits(got), sum(got$n_pfd) + 1e-6)
  }
  # The maximum over the whole admissible grid is attained at (1, 1), because
  # the cascade is monotone in both.
  most <- project_urps_demand(cells, care_seeking_rate = 1, referral_rate = 1,
                              verbose = FALSE)
  mid <- project_urps_demand(cells, care_seeking_rate = 0.6, referral_rate = 0.9,
                             verbose = FALSE)
  expect_gt(cyc06_visits(most), cyc06_visits(mid))
  expect_gt(prevalent, 0)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the guards that already existed cannot be lost", {
  # The sweep's real finding is that MOST of this surface is guarded. Those
  # guards are load-bearing and undocumented as a set, so they are pinned here
  # as one group: a future refactor that drops any of them fails in one place.
  expect_error(conservative_management_multipliers(ui_uptake = 1.5), "\\[0, 1\\]")
  expect_error(conservative_management_multipliers(ui_uptake = -0.5), "\\[0, 1\\]")
  expect_error(conservative_management_multipliers(surgical_reduction = 1.5), "\\[0, 1\\]")

  cat_df <- data.frame(demand_workload = c(100, 200), accessible_capacity = c(80, 250),
                       metro = c("Metro", "NonMetro"), stringsAsFactors = FALSE)
  expect_error(telemedicine_reach(cat_df, nonmetro_uplift = -0.1))
  panel <- data.frame(year = c(2025L, 2026L), demand_workload = c(100, 100),
                      accessible_capacity = c(80, 80))
  expect_error(clear_access_trajectory(panel, backlog_fraction = 1.5))
  expect_error(clear_access_trajectory(panel, backlog_fraction = -0.1))

  expect_error(urps_migration_matrix(c("CO", "NY"), rural_to_urban = 1.5))
  expect_error(urps_migration_matrix(c("CO", "NY"), out_of_country_rate = -0.2))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: an out-of-range multiplier is refused, never clamped", {
  # Clamping is the tempting fix and the wrong one: a referral rate of 1.4
  # silently becoming 1.0 produces a plausible number from an impossible
  # assumption, and the run reports no problem. Every guard in this family must
  # stop rather than saturate.
  cells <- cyc06_cells()
  at_one <- project_urps_demand(cells, referral_rate = 1, verbose = FALSE)
  expect_error(project_urps_demand(cells, referral_rate = 1.4, verbose = FALSE))
  # If it had clamped, the two calls would agree; the error is what proves it did not.
  expect_gt(cyc06_visits(at_one), 0)

  pop <- cyc06_pop()
  sr_hi <- WU2011_SURGERY_RATE_PER_1000; sr_hi[] <- 5000
  expect_error(compute_demand_denominators(pop, surgery_rate_per_1000 = sr_hi))
  cr_neg <- CONSULT_RATE_BY_AGE; cr_neg[] <- -0.3
  expect_error(compute_demand_denominators(pop, consult_rate = cr_neg))
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: a negative rate can no longer produce a negative case count", {
  # The measured defect. Before the guard, consult_rate = -0.3 over a 5,000,000
  # woman population returned D2 = -1,500,000 demand cases: a negative number
  # of consultations, carried into every downstream total that sums the
  # estimands without checking their sign.
  pop <- cyc06_pop()
  ok <- compute_demand_denominators(pop)
  expect_true(all(ok$demand_cases >= 0))

  for (v in c(-1e-9, -0.3, -100)) {
    cr <- CONSULT_RATE_BY_AGE; cr[] <- v
    expect_error(compute_demand_denominators(pop, consult_rate = cr),
                 "consult_rate",
                 info = sprintf("consult_rate = %g was accepted", v))
  }
  # And the same for the fraction pair on the other estimand.
  cells <- cyc06_cells()
  expect_error(project_urps_demand(cells, care_seeking_rate = -0.3, verbose = FALSE))
  expect_true(cyc06_visits(project_urps_demand(cells, care_seeking_rate = 0.25,
                                               verbose = FALSE)) > 0)
})

# ---- AUDIT REMEDIATION (added after the cycle-22 mutation audit) -------------
#
# The audit reverted each of the 31 fixes in an isolated worktree and checked
# that its pinning test failed. Thirty were killed. D12 SURVIVED: the range
# guard on compute_brfss_demand_estimand() could be deleted and not one test in
# tests/testthat failed -- the function is exercised (return shape, monotonicity)
# but its guard never was.
#
# The fix was real and present the whole time. Nothing would have caught its
# removal, which makes it a fix with no evidence behind it. This is that
# evidence.

test_that("AUDIT: both access fractions are guarded on the BRFSS estimand too", {
  # Same pair, same reasoning as project_urps_demand() above -- and the reason
  # this test exists separately is that testing one of the two call sites is
  # not testing the guard.
  # Fixture built inline rather than borrowed from another test file: a helper
  # that is not in scope turns this into a skip, and a skip is what the audit
  # was correcting in the first place.
  cells <- data.frame(
    age_group = rep(URPS_POP_AGE_BANDS, each = 2),
    pop_weight = rep(c(6e6, 2e6), times = length(URPS_POP_AGE_BANDS)),
    ui_prevalence = rep(c(0.12, 0.24, 0.35, 0.44, 0.51), each = 2),
    pop_prevalence = 0.10, fi_prevalence = 0.05, stringsAsFactors = FALSE)
  pop <- data.frame(year = rep(2025:2027, each = length(DEMAND_AGE_BANDS)),
                    age_band = rep(DEMAND_AGE_BANDS, times = 3),
                    female_pop = 1e6, stringsAsFactors = FALSE)

  expect_error(compute_brfss_demand_estimand(pop, cells, care_seeking_rate = 1.4),
               "care_seeking_rate")
  expect_error(compute_brfss_demand_estimand(pop, cells, care_seeking_rate = -0.1),
               "care_seeking_rate")
  expect_error(compute_brfss_demand_estimand(pop, cells, referral_rate = 1.4),
               "referral_rate")
  expect_error(compute_brfss_demand_estimand(pop, cells, referral_rate = -0.1),
               "referral_rate")
  expect_error(compute_brfss_demand_estimand(pop, cells, care_seeking_rate = NA_real_),
               "finite")

  # The endpoints are legal, and the estimand is the exact product of the two.
  expect_silent(compute_brfss_demand_estimand(pop, cells, care_seeking_rate = 0,
                                              referral_rate = 0))
  full <- compute_brfss_demand_estimand(pop, cells, care_seeking_rate = 1,
                                        referral_rate = 1)
  quarter <- compute_brfss_demand_estimand(pop, cells, care_seeking_rate = 0.5,
                                           referral_rate = 0.5)
  expect_equal(quarter$demand_cases, full$demand_cases * 0.25)
  expect_true(all(full$demand_cases > 0))
})
