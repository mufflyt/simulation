# Entrant trajectories (R/supply-entrant_trajectory).
#
# Supply growth is the whole of the surplus story, and it rested on one number
# held flat for 25 years. These tests pin the two things that make a trajectory
# honest: the growth rate is DERIVED from the NRMP series rather than chosen,
# and the naive extrapolation is labelled as such rather than offered as a peer.

test_that("compound_growth_rate is the plain CAGR, and refuses nonsense", {
  expect_equal(compound_growth_rate(100, 121, 2), 0.1, tolerance = 1e-9)
  expect_equal(compound_growth_rate(70, 70, 8), 0)
  expect_true(is.na(compound_growth_rate(0, 10, 5)))
  expect_true(is.na(compound_growth_rate(10, 20, 0)))
})

test_that("the sustainable rate is the POSITIONS rate, not the filled rate", {
  r <- nrmp_growth_rates()
  # THE SUBSTANTIVE CLAIM. Filled positions grew nearly twice as fast as offered
  # positions, and the whole difference is fill-rate catch-up from 92% to 100%.
  # A fill rate cannot exceed 1, so that source of growth is spent; extrapolating
  # the filled rate double-counts a one-off.
  expect_gt(r$filled, r$offered)
  expect_equal(r$sustainable, r$offered)
  expect_true(r$headroom_exhausted)
  expect_gte(r$fill_rate_last, 0.995)
  expect_lt(r$fill_rate_first, r$fill_rate_last)
})

test_that("a trajectory grows, is non-negative, and honours a cap", {
  y <- 2025:2050
  flat <- entrant_trajectory(70, y, 0)
  expect_length(flat, length(y))
  expect_true(all(flat == 70))

  up <- entrant_trajectory(70, y, 0.02)
  expect_equal(up[1], 70)
  expect_true(all(diff(up) > 0))

  down <- entrant_trajectory(70, y, -0.02)
  expect_true(all(down > 0))
  expect_lt(down[length(down)], 70)

  capped <- entrant_trajectory(70, y, 0.05, cap = 80)
  expect_true(all(capped <= 80))
  expect_equal(max(capped), 80)
})

test_that("the naive arm is present and carries its warning label", {
  sc <- entrant_trajectory_scenarios(years = 2025:2050)
  expect_setequal(names(sc), names(urpssim:::ENTRANT_TRAJECTORY_LABELS))
  # It must be OFFERED (so its consequence is visible) and LABELLED (so it is
  # never mistaken for an equally supported scenario).
  expect_true("filled_naive" %in% names(sc))
  expect_match(urpssim:::ENTRANT_TRAJECTORY_LABELS[["filled_naive"]], "NAIVE")
  expect_match(urpssim:::ENTRANT_TRAJECTORY_LABELS[["filled_naive"]], "double-counts")
  # It must be the most aggressive arm, which is why it needs the label.
  totals <- vapply(sc, sum, numeric(1))
  expect_equal(names(which.max(totals)), "filled_naive")
  expect_equal(names(which.min(totals)), "contraction")
})

test_that("expansion and contraction are symmetric about flat", {
  sc <- entrant_trajectory_scenarios(base = 70, years = 2025:2050)
  r <- nrmp_growth_rates()
  # Both use the same observed rate, signed. Choosing a different magnitude for
  # the downside would be an editorial judgement dressed as evidence.
  expect_equal(sc$expansion_sustainable[26], 70 * (1 + r$sustainable)^25)
  expect_equal(sc$contraction[26], 70 * (1 - r$sustainable)^25)
  expect_true(all(sc$flat == 70))
})

test_that("the engine accepts a trajectory and the path differs from its mean", {
  a <- tibble::tibble(provider_id = sprintf("P%03d", 1:60), subspecialty = "FPMRS",
                      sex = "female", age = seq(40, 64, length.out = 60),
                      entry_year = 2015L, retirement_year = NA_real_,
                      origin_cohort = "roster", clinical_fte = 1)
  rising <- c(10, 20, 30, 40, 50)
  set.seed(3); traj <- simulate_provider_career_once(a, 2025:2029, rising, fte_method = "hours")
  set.seed(3); flat <- simulate_provider_career_once(a, 2025:2029, mean(rising), fte_method = "hours")
  # Same mean intake, different path: the trajectory must lag early and catch up.
  expect_lt(traj$panel$headcount[2], flat$panel$headcount[2])
  expect_equal(length(traj$panel$headcount), 5L)

  expect_error(simulate_provider_career_once(a, 2025:2029, c(1, 2)), "has length 2")
  expect_error(simulate_provider_career_once(a, 2025:2029, c(1, 2, 3, -4, 5)),
               "non-negative")
})

test_that("a param_spec sets the LEVEL without flattening the trajectory", {
  a <- tibble::tibble(provider_id = sprintf("P%03d", 1:60), subspecialty = "FPMRS",
                      sex = "female", age = seq(40, 64, length.out = 60),
                      entry_year = 2015L, retirement_year = NA_real_,
                      origin_cohort = "roster", clinical_fte = 1)
  tr <- entrant_trajectory(70, 2025:2030, growth = 0.05)
  sp <- supply_parameter_spec(entrant_series = c(59, 59, 58, 56), entrant_mean = 40)
  r <- run_supply_microsimulation(a, 2025:2030, entrants_per_year = tr, subspecialty = "FPMRS",
                                  n_iterations = 5, param_spec = sp, seed = 2L, verbose = FALSE)
  used <- r$scenario$entrants_per_year
  # THE CONTRACT. entrant_mean would previously have replaced the whole vector
  # with a single number, silently discarding the path. It must rescale instead.
  expect_length(used, length(tr))
  expect_equal(mean(used), 40)
  expect_equal(used / tr, rep(40 / mean(tr), length(tr)))
})
