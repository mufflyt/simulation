# Adversarial cycle 03 -- denominators, joins, and uncertainty propagation.
#
# Cycle 02 left a bug class open: guards that only WARN in relaxed mode while
# the caller carries no fallback branch. The gap-projection contract turned out
# to be the sharpest instance -- its demand join accepts a 50% year match, and
# at exactly 50% it does not even warn -- so tests 8 and 9 discharge it here.
#
# The concentration family is a port of cliff/R/workforce_concentration_metrics.R.
# Comparing against the canonical source rather than reasoning from scratch is
# what found the missing denominator guard: cliff has it, this port dropped it.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the unit universe may equal the occupied units but never fall below", {
  # n_units_total is the DENOMINATOR of pct_units_zero and the padding length
  # for Gini. Below n_occupied it produced a negative share of empty units while
  # computing Gini over more units than it reported.
  five <- c(10, 8, 6, 4, 2)
  expect_equal(provider_concentration(five, n_units_total = 5L)$pct_units_zero, 0)
  expect_equal(provider_concentration(five, n_units_total = 6L)$pct_units_zero, 16.7)
  expect_error(provider_concentration(five, n_units_total = 4L), "cannot be smaller")

  # A unit that reports zero providers is occupied by nobody, so a universe
  # equal to the count of NON-ZERO units is still legal.
  with_zeros <- c(10, 8, 0, 0, 0)
  expect_equal(provider_concentration(with_zeros, n_units_total = 2L)$n_occupied, 2L)
  expect_error(provider_concentration(with_zeros, n_units_total = 1L), "cannot be smaller")

  # Empty geography: no units, no denominator, no invented percentage.
  expect_true(is.na(provider_concentration(numeric(0), n_units_total = 0L)$pct_units_zero))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: top-k share is closed at k = 0 and saturates at k >= n", {
  x <- c(50, 30, 15, 5)
  expect_equal(workforce_top_k_share(x, k = 0L), 0)
  expect_equal(workforce_top_k_share(x, k = 1L), 0.5)
  expect_equal(workforce_top_k_share(x, k = 4L), 1)
  expect_equal(workforce_top_k_share(x, k = 5L), 1)     # k > n cannot exceed 1
  expect_error(workforce_top_k_share(x, k = -1L), "non-negative")
  # The share is monotone non-decreasing in k, which is what makes top5 <= top10
  # meaningful in the summary row.
  shares <- vapply(0:6, function(k) workforce_top_k_share(x, k = k), numeric(1))
  expect_false(is.unsorted(shares))
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: Monte Carlo standard error falls as 1/sqrt(n) and is undefined below 2 draws", {
  set.seed(303)
  draws <- stats::rnorm(1600, mean = 1000, sd = 50)
  se100 <- monte_carlo_se(draws[1:100])
  se400 <- monte_carlo_se(draws[1:400])
  # Quadrupling n halves the MCSE, up to the sampling variation in sd itself.
  expect_equal(se400$mcse_mean / se100$mcse_mean, 0.5, tolerance = 0.15)

  # One draw has no spread to estimate. Reporting 0 would read as perfect
  # precision, which is the opposite of the truth.
  expect_true(is.na(monte_carlo_se(1000)$mcse_mean))
  expect_equal(monte_carlo_se(1000)$n, 1L)
  expect_false(is.na(monte_carlo_se(c(1000, 1001))$mcse_mean))
  expect_true(is.na(series_mean_se(50)))
  expect_false(is.na(series_mean_se(c(50, 57))))

  # The median's MCSE is the wider of the two by sqrt(pi/2); a median band is
  # never tighter than the mean's at the same n.
  expect_gt(se400$mcse_median, se400$mcse_mean)
  expect_equal(se400$mcse_median / se400$mcse_mean, sqrt(pi / 2), tolerance = 1e-9)
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: no concentration metric accepts a negative count", {
  # workforce_gini() refused it and the other three did not, so the same data
  # produced an error from one function and a confident number from its sibling:
  # a top-k share of 1.2 and a Lorenz curve running to -1, both outside their
  # own documented [0, 1] ranges.
  bad <- c(-5, 10, 20)
  expect_error(workforce_gini(bad), "negative")
  expect_error(workforce_hhi(bad), "negative counts are not allowed")
  expect_error(workforce_lorenz(bad), "negative counts are not allowed")
  expect_error(workforce_top_k_share(bad, k = 2), "negative counts are not allowed")
  expect_error(provider_concentration(bad), "negative counts are not allowed")

  # Zero is not negative: an empty unit is the case the whole module exists to
  # count, and it must still pass everywhere.
  ok <- c(0, 10, 20, 0)
  expect_silent(workforce_hhi(ok))
  expect_silent(workforce_lorenz(ok))
  expect_equal(workforce_top_k_share(ok, k = 1L), 2 / 3)
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: per-capita density is linear in the denominator and undefined at zero population", {
  supply <- tibble::tibble(geo = c("CO", "NY", "TX"), fte = c(60, 140, 120))
  pop <- tibble::tibble(geo = c("CO", "NY", "TX"), population = c(2.9e6, 1.0e7, 1.5e7))

  per_m <- supply_per_capita(supply, pop, per = 1e6)
  per_100k <- supply_per_capita(supply, pop, per = 1e5)
  # Changing the reporting base rescales every rate by the same factor and
  # cannot reorder the geographies.
  expect_equal(per_100k$fte_per_capita, per_m$fte_per_capita / 10)
  expect_equal(order(per_100k$fte_per_capita), order(per_m$fte_per_capita))
  expect_equal(per_m$fte_per_capita[1], 60 * 1e6 / 2.9e6)

  # A geography with no recorded population has no density. Inf would dominate
  # every downstream ranking and read as infinitely well supplied.
  zero_pop <- supply_per_capita(supply, dplyr::mutate(pop, population = c(2.9e6, 0, 1.5e7)))
  expect_true(is.na(zero_pop$fte_per_capita[2]))
  expect_false(any(is.infinite(zero_pop$fte_per_capita)))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the concentration metrics agree on which distribution is more concentrated", {
  # Four metrics measuring one construct must at least order two distributions
  # the same way, or the summary row is internally inconsistent.
  even <- c(25, 25, 25, 25)
  skewed <- c(70, 20, 8, 2)
  expect_lt(workforce_gini(even), workforce_gini(skewed))
  expect_lt(workforce_hhi(even), workforce_hhi(skewed))
  expect_lt(workforce_top_k_share(even, 1L), workforce_top_k_share(skewed, 1L))

  # Padding empty units in can only INCREASE measured concentration: the
  # providers have not moved, but the geography they fail to cover is larger.
  occupied_only <- provider_concentration(skewed)
  full_universe <- provider_concentration(skewed, n_units_total = 50L)
  expect_gt(full_universe$gini, occupied_only$gini)
  expect_gt(full_universe$pct_units_zero, occupied_only$pct_units_zero)
  # HHI is a share-of-total measure, so zero units cannot move it at all.
  expect_equal(full_universe$hhi, occupied_only$hhi)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a gap projection with a half-covered demand series is refused", {
  # THE DEFECT. The demand join runs at min_match_rate = 0.5, and a match rate
  # of exactly 0.5 is not BELOW 0.5, so a demand series covering half the
  # horizon joined without any warning at all. The other half exported NA
  # demand and NA gap, and validate_urps_gap_projection() passed it in STRICT
  # mode: the arithmetic guard uses na.rm = TRUE, so NA - NA held vacuously.
  supply <- data.frame(year = 2025:2030, scenario = "baseline",
                       headcount_median = seq(1000, 1050, by = 10),
                       effective_fte_median = seq(900, 945, by = 9))
  half <- data.frame(year = 2025:2027, required_fte = c(1200, 1210, 1220))

  expect_error(
    as_urps_gap_projection(supply, half, cohort_basis = "certification_cohorts",
                           mode = "strict"),
    "non-finite values")
  # Relaxed still produces the frame -- but says so.
  expect_message(
    p <- as_urps_gap_projection(supply, half, cohort_basis = "certification_cohorts",
                                mode = "relaxed"),
    "non-finite values")
  expect_equal(sum(is.na(p$gap_fte)), 3L)

  # Full coverage is silent, so the guard is not merely always-on.
  full <- data.frame(year = 2025:2030, required_fte = seq(1200, 1250, by = 10))
  expect_silent(suppressMessages(
    as_urps_gap_projection(supply, full, cohort_basis = "certification_cohorts",
                           mode = "strict")))
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a missing gap is never treated as a gap of zero", {
  # The failure mode that motivates the guard: an NA gap silently summed as 0
  # turns "we do not know" into "there is no shortage", and the aggregate looks
  # more balanced the more data are missing.
  ok <- data.frame(
    year = 2025:2027, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = c(1000, 1010, 1020), supply_clinical_fte = c(900, 909, 918),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = c(1300, 1310, 1320), demand_clinical_fte = c(1200, 1210, 1220),
    gap_fte = c(-300, -301, -302), gap_headcount = c(-300, -300, -300),
    stringsAsFactors = FALSE)
  expect_silent(suppressMessages(validate_urps_gap_projection(ok, mode = "strict")))

  holed <- ok
  holed$demand_clinical_fte[2] <- NA_real_
  holed$gap_fte[2] <- NA_real_
  expect_error(suppressMessages(validate_urps_gap_projection(holed, mode = "strict")),
               "not a gap of zero")

  # An Inf gap is equally unexportable and equally invisible to na.rm.
  infinite <- ok
  infinite$gap_fte[3] <- Inf
  expect_error(suppressMessages(validate_urps_gap_projection(infinite, mode = "strict")),
               "non-finite values")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: a duplicated denominator row is refused rather than fanned out", {
  # A population table with one geography listed twice would silently duplicate
  # that state's supply row, double-counting its FTE in every national total
  # while every per-capita rate still looked individually correct.
  supply <- tibble::tibble(geo = c("CO", "NY"), fte = c(60, 140))
  dup_pop <- tibble::tibble(geo = c("CO", "CO", "NY"),
                            population = c(2.9e6, 3.1e6, 1.0e7))
  expect_error(supply_per_capita(supply, dup_pop), "would fan out")

  # A geography with no population row is a missing denominator, not a zero
  # one, and the join must at minimum say so. Note the guard emits a MESSAGE,
  # not an R warning: it does not reach warnings(), so a caller cannot promote
  # it with options(warn = 2). Pinned as-is because the whole package routes
  # diagnostics through .msg_warn(), not because a message is the stronger form.
  expect_message(out <- supply_per_capita(supply, tibble::tibble(geo = "CO",
                                                                 population = 2.9e6)),
                 "match rate")
  expect_true(is.na(out$fte_per_capita[2]))

  # And the fan-out guard is not vacuous: opting in genuinely changes the shape.
  wide <- safe_left_join(supply, dup_pop, by = "geo", allow_fanout = TRUE)
  expect_equal(nrow(wide), 3L)
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: a degenerate Monte Carlo band is not reported as precision", {
  # Every draw identical means the simulator moved nothing, not that the answer
  # is known exactly. half_width 0 would divide into Inf noise (reads as
  # catastrophe) or, unguarded the other way, 0 noise (reads as certainty).
  d <- monte_carlo_diagnostics(rep(1000, 200))
  expect_equal(d$half_width, 0)
  expect_true(is.na(d$noise_share))
  expect_false(is.infinite(d$noise_share))

  # A band that is mostly simulation noise must report a share near or above 1,
  # and the iteration-count flag must be independent of the spread.
  set.seed(304)
  few <- monte_carlo_diagnostics(stats::rnorm(8, 1000, 50))
  expect_false(few$bounds_are_quantiles)     # 8 draws cannot support a 95% band
  expect_gt(few$noise_share, 0.2)
  many <- monte_carlo_diagnostics(stats::rnorm(2000, 1000, 50))
  expect_true(many$bounds_are_quantiles)
  # More iterations must reduce the share of the band that is simulation noise.
  expect_lt(many$noise_share, few$noise_share)

  # The threshold is a property of the interval width, not of the draws.
  expect_equal(many$min_iterations_for_ci, mc_min_iterations(0.95))
  expect_gt(mc_min_iterations(0.99), mc_min_iterations(0.95))
})
