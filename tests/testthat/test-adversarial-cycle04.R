# Adversarial cycle 04 -- FTE vs headcount semantics, the workload denominator,
# entrant/departure accounting.
#
# Cycle 03 left a bug class open: thresholds written as strict inequalities
# where the boundary case is the dangerous one. Tests 1-4 walk the cutpoints of
# every classification and range guard on the FTE path.
#
# The sweep found a different family in the same neighbourhood: validators that
# check a distribution SUMS to 1 without checking its parts are non-negative.
# validate_migration_matrix() already records why that is not enough --
# "-0.1 and 1.1 sum to exactly 1.0" -- and three siblings had not adopted it.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc04_agents <- function(n = 60, seed = 401) {
  set.seed(seed)
  data.frame(
    provider_id = sprintf("P%03d", seq_len(n)),
    subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(36, 68, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "baseline", stringsAsFactors = FALSE
  )
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the workforce-outlook cutpoints are closed from below", {
  # cliff's published cutpoints: Adequate >= 1.2, Marginal 0.8-1.2,
  # Insufficient < 0.8. A ratio sitting exactly on a cutpoint is the case a
  # published table reports, so which side it falls on is not a detail.
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_ADEQUATE_MIN), "Adequate")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_ADEQUATE_MIN - 1e-9), "Marginal")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_MARGINAL_MIN), "Marginal")
  expect_equal(classify_workforce_outlook(WORKFORCE_OUTLOOK_MARGINAL_MIN - 1e-9), "Insufficient")

  # A replacement ratio of exactly 1 -- one entrant per departure, the steady
  # state -- is Marginal, not Adequate. Reading it as Adequate would call a
  # workforce that is merely holding still a growing one.
  expect_equal(classify_workforce_outlook(1), "Marginal")
  expect_equal(classify_workforce_outlook(0), "Insufficient")
  expect_true(is.na(classify_workforce_outlook(NA_real_)))
  # Vectorised, with the boundaries preserved elementwise.
  expect_equal(classify_workforce_outlook(c(1.2, 0.8, 0.79, NA)),
               c("Adequate", "Marginal", "Insufficient", NA))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the productivity benchmark range is closed at both ends", {
  # The solved denominator absorbs every error in the service volumes, so this
  # check is the only thing standing between a wrong volume and a silently
  # suppressed demand projection. Both bounds must be inclusive or a value
  # exactly at the published benchmark is rejected as implausible.
  lo <- WRVU_PER_FTE_BENCHMARK[["low"]]; hi <- WRVU_PER_FTE_BENCHMARK[["high"]]
  expect_true(suppressMessages(check_productivity_plausible(lo, mode = "strict")))
  expect_true(suppressMessages(check_productivity_plausible(hi, mode = "strict")))
  expect_error(suppressMessages(check_productivity_plausible(lo - 1e-6, mode = "strict")),
               "outside the")
  expect_error(suppressMessages(check_productivity_plausible(hi + 1e-6, mode = "strict")),
               "outside the")

  # Relaxed mode must still SAY so and must report FALSE, or a caller branching
  # on the return value treats an implausible denominator as verified.
  expect_message(ok <- check_productivity_plausible(hi * 3, mode = "relaxed"),
                 "VOLUMES are wrong")
  expect_false(ok)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: indirect time is closed at 0 and open at 1", {
  # gross_up = 1 / (1 - indirect_share). At 1 that is division by zero, and an
  # infinite denominator would report an infinite required workforce.
  expect_equal(suppressMessages(calibrate_wrvu_per_fte(1e7, 1000, indirect_share = 0)), 1e4)
  expect_gt(suppressMessages(calibrate_wrvu_per_fte(1e7, 1000, indirect_share = 0.5)),
            suppressMessages(calibrate_wrvu_per_fte(1e7, 1000, indirect_share = 0)))
  expect_error(calibrate_wrvu_per_fte(1e7, 1000, indirect_share = 1))
  expect_error(calibrate_wrvu_per_fte(1e7, 1000, indirect_share = -1e-9))

  # A zero or negative anchor makes the solved denominator meaningless rather
  # than large, so it is refused rather than divided by.
  expect_error(calibrate_wrvu_per_fte(1e7, 0))
  expect_error(calibrate_wrvu_per_fte(0, 1000))
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: setting time shares are checked on the unit interval and at the sum tolerance", {
  expect_equal(nrow(allocate_fte_by_setting(100, c(a = 0.5, b = 0.5))), 2L)
  # Degenerate but legal: one setting takes everything, another takes nothing.
  edge <- allocate_fte_by_setting(100, c(a = 1, b = 0))
  expect_equal(edge$required_fte, c(100, 0))

  # The sum tolerance is 1e-8 and closed.
  expect_silent(allocate_fte_by_setting(100, c(a = 0.5, b = 0.5 + 1e-8)))
  expect_error(allocate_fte_by_setting(100, c(a = 0.5, b = 0.5 + 1.1e-8)), "not 1")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: a distribution that sums to 1 with a negative part is not a distribution", {
  # THE DEFECT, and it is a family. validate_migration_matrix() already carries
  # the note that "-0.1 and 1.1 sum to exactly 1.0, so a row-sum test alone
  # accepts a matrix that is not a probability distribution". Three siblings
  # had not adopted it. allocate_fte_by_setting() was the live one: shares of
  # 1.5 / -0.5 passed the sum check and emitted required_fte = -50 for a
  # setting -- negative clinical FTE, which subtracts from any total it enters.
  expect_error(allocate_fte_by_setting(100, c(a = 1.5, b = -0.5)), "must be in \\[0, 1\\]")
  expect_error(psa_discrete("x", values = c(1, 2), probs = c(1.5, -0.5)))
  bad_basket <- data.frame(service = c("s", "s"),
                           hcpcs = utils::head(CMS_WORK_RVU$hcpcs, 2),
                           mix = c(1.5, -0.5), stringsAsFactors = FALSE)
  expect_error(validate_cpt_basket(bad_basket), "in \\[0, 1\\]")

  # The three that already carried the guard must keep it.
  expect_error(validate_migration_matrix(
    tibble::tibble(origin = c("A", "A"), destination = c("A", "B"),
                   probability = c(1.1, -0.1))), "outside \\[0, 1\\]")
  bad_part <- data.frame(age = 40, sex = "female",
                         p_full = 1.5, p_part = -0.5, p_none = 0)
  expect_error(validate_participation_table(bad_part), "negative probability")

  # And a genuine distribution still passes everywhere.
  expect_silent(allocate_fte_by_setting(100, c(a = 0.6, b = 0.4)))
  expect_silent(psa_discrete("x", values = c(1, 2), probs = c(0.6, 0.4)))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: required FTE is linear in volume and grossed up by exactly 1/(1 - indirect)", {
  # Required FTE is a workload divided by a productivity denominator. If it is
  # not linear in the workload, some step is clipping or saturating and the
  # projection's response to a demand scenario is not interpretable.
  vol <- tibble::tibble(year = 2025L,
                        service = utils::head(urps_service_workload()$service, 3),
                        volume = c(10000, 5000, 2000))
  base <- convert_workload_to_fte(vol, wrvu_per_fte = 7500, indirect_share = 0)
  dbl <- convert_workload_to_fte(dplyr::mutate(vol, volume = .data$volume * 2),
                                 wrvu_per_fte = 7500, indirect_share = 0)
  expect_equal(dbl$required_fte, base$required_fte * 2)

  # Halving the productivity denominator doubles the required workforce.
  half_prod <- convert_workload_to_fte(vol, wrvu_per_fte = 3750, indirect_share = 0)
  expect_equal(half_prod$required_fte, base$required_fte * 2)

  # Indirect time is a pure gross-up, not a separate model.
  with_indirect <- convert_workload_to_fte(vol, wrvu_per_fte = 7500, indirect_share = 0.25)
  expect_equal(with_indirect$required_fte, base$required_fte / (1 - 0.25))
  expect_gt(with_indirect$required_fte, base$required_fte)
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the FTE gap is an identity, and a year with no demand is refused", {
  supply <- tibble::tibble(year = 2025:2027, effective_fte_median = c(1300, 1310, 1320))
  required <- tibble::tibble(year = 2025:2027, required_fte = c(1400, 1300, 1320))
  g <- compute_fte_gap(supply, required)

  expect_equal(g$gap_fte, supply$effective_fte_median - required$required_fte)
  # Sign convention: negative is a shortfall. The percentage must agree in sign
  # with the level, or a table can report a shortfall and a positive percentage.
  expect_equal(sign(g$gap_pct), sign(g$gap_fte))
  expect_equal(g$pct_supply_to_demand, 100 * supply$effective_fte_median / required$required_fte)
  expect_equal(g$gap_fte[3], 0)          # exact balance is 0, not a small residual

  # Zero required FTE is degenerate, not infinite.
  z <- compute_fte_gap(tibble::tibble(year = 2025L, effective_fte_median = 100),
                       tibble::tibble(year = 2025L, required_fte = 0))
  expect_true(is.na(z$gap_pct))
  expect_false(any(is.infinite(z$gap_pct)))

  # A supply year with no demand row is a missing comparison, not a zero gap.
  expect_message(compute_fte_gap(supply, required[1:2, ]), "match rate")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: the solved productivity denominator round-trips to its own anchor", {
  # calibrate_wrvu_per_fte() SOLVES the denominator so that the base year
  # reproduces the demand anchor. If converting the same base-year volumes back
  # through that denominator does not return the anchor, the calibration is not
  # self-consistent and every projected year inherits the discrepancy.
  vol <- tibble::tibble(service = utils::head(urps_service_workload()$service, 4),
                        volume = c(120000, 60000, 25000, 9000))
  anchor <- 1450
  base_wrvu <- service_volume_to_wrvu(vol)$work_rvu
  denom <- suppressMessages(calibrate_wrvu_per_fte(base_wrvu, anchor))
  back <- convert_workload_to_fte(vol, wrvu_per_fte = denom)
  expect_equal(back$required_fte, anchor, tolerance = 1e-8)

  # And the round trip must hold at any indirect-time share, because the
  # gross-up appears on both sides and must cancel exactly.
  denom_25 <- suppressMessages(calibrate_wrvu_per_fte(base_wrvu, anchor, indirect_share = 0.25))
  back_25 <- convert_workload_to_fte(vol, wrvu_per_fte = denom_25, indirect_share = 0.25)
  expect_equal(back_25$required_fte, anchor, tolerance = 1e-8)
  # A different indirect share must move the DENOMINATOR, or the parameter is inert.
  expect_false(isTRUE(all.equal(denom, denom_25)))
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: headcount moves by exactly entrants minus departures", {
  # The conservation identity. Aggregate headcount can be right on average while
  # entrants and departures are individually wrong in offsetting ways; only the
  # per-year identity catches that.
  ag <- cyc04_agents()
  yrs <- 2025:2032
  ic <- calibrate_hours_intercept(ag$age, ag$sex)
  set.seed(77)
  sim <- simulate_provider_career_once(ag, yrs, entrants_per_year = 6,
                                       hours_intercept = ic,
                                       track_career_states = TRUE)
  p <- sim$panel
  expect_true(all(c("n_retired", "n_early_career") %in% names(p)))

  # Departures are cumulative in n_retired, so the per-year count is its diff.
  departures <- diff(p$n_retired)
  delta <- diff(p$headcount)
  expect_equal(delta, 6L - departures,
               info = "headcount did not move by entrants minus departures")

  # Nobody leaves the retired state: departures are absorbing, so the cumulative
  # count is non-decreasing.
  expect_true(all(departures >= 0),
              info = "the retired count fell, so a provider un-retired")
  # Active-state counts plus retired must equal every provider ever present.
  active <- p$n_early_career + p$n_mid_career + p$n_late_career
  expect_equal(active, p$headcount)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: allocating FTE across settings neither creates nor destroys it", {
  # A partition must partition. If the setting split can change the total, then
  # reporting demand by setting and reporting it nationally give two different
  # workforces from one model run.
  total <- 1487.3
  alloc <- allocate_fte_by_setting(total)
  expect_equal(sum(alloc$required_fte), total, tolerance = 1e-9)
  expect_true(all(alloc$required_fte >= 0))
  expect_equal(nrow(alloc), 3L)

  # Any legal share vector conserves the total, including degenerate ones.
  for (ts in list(c(a = 1, b = 0), c(a = 0.5, b = 0.5), c(a = 0.1, b = 0.2, c = 0.7))) {
    got <- allocate_fte_by_setting(total, ts)
    expect_equal(sum(got$required_fte), total, tolerance = 1e-9)
    expect_true(all(got$required_fte >= 0))
  }

  # Zero total allocates zero everywhere rather than dividing by it.
  expect_true(all(allocate_fte_by_setting(0)$required_fte == 0))
})
