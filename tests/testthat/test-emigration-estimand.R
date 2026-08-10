# Emigration: two national quantities, not one.
#
# `apply_provider_migration_matrix()` marks an out-of-country mover with
# left_country = TRUE and sets their state to NA. Nothing read that column, so
# an emigrant left every STATE total while remaining in the NATIONAL one, and
# state FTE no longer summed to national FTE.
#
# The ledger recorded that as a modelling decision rather than a bug, because
# which total is right depends on which question is being asked -- and the
# answer turned out to be "both, and they are different numbers":
#
#   CERTIFICATION count  what the observed contract series measures. Its own
#     documentation records that n_active == n_ever_certified in every row, so
#     it counts people who HOLD certification whether or not they practise here.
#     An emigrant is still certified; removing them would break the definition
#     match the back-test depends on.
#
#   US-PRACTISING count  the numerator of a gap against US demand. An emigrant
#     cannot see a US patient, so they must leave it -- and they have already
#     left every state total.
#
# The two differ by exactly the emigrant stock. The engine now reports both and
# the gap uses the second.

emig_agents <- function(n = 20, n_left = 0) {
  a <- data.frame(
    provider_id = sprintf("P%02d", seq_len(n)), subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(40, 60, length.out = n), entry_year = 2015L,
    retirement_year = NA_real_, origin_cohort = "baseline",
    state = rep(c("CO", "NY"), length.out = n), stringsAsFactors = FALSE)
  if (n_left > 0) {
    a$left_country <- c(rep(TRUE, n_left), rep(FALSE, n - n_left))
    a$state[seq_len(n_left)] <- NA_character_        # as the migration matrix leaves them
  }
  a
}
emig_panel <- function(a, years = 2025:2027) {
  simulate_provider_career_once(
    a, years, entrants_per_year = 0,
    hours_intercept = calibrate_hours_intercept(a$age, a$sex),
    retirement_schedule = setNames(rep(0, 120), 1:120),
    career_change_hazard = 0)$panel
}

test_that("with no emigration the two national series are identical", {
  # Every run that does not apply the migration matrix. If the two ever differ
  # here, the US-practising series is subtracting something other than emigrants.
  p <- emig_panel(emig_agents(20))
  expect_identical(p$headcount, p$headcount_us_practising)
  expect_identical(p$effective_fte, p$effective_fte_us_practising)
  expect_true(all(p$headcount == 20L))
})

test_that("the two series differ by exactly the emigrant stock", {
  # The defining property. Not "roughly", not "in the base year" -- exactly, in
  # every year, because emigration removes whole providers.
  p <- emig_panel(emig_agents(20, n_left = 3))
  expect_equal(p$headcount - p$headcount_us_practising, rep(3L, nrow(p)))
  expect_true(all(p$effective_fte > p$effective_fte_us_practising))
  # And the certification series is untouched by emigration, which is what makes
  # it still comparable to the observed contract series.
  p0 <- emig_panel(emig_agents(20))
  expect_identical(p$headcount, p0$headcount)
  expect_identical(p$effective_fte, p0$effective_fte)
})

test_that("state totals sum to the US-practising national total, not the certification one", {
  # THE IDENTITY. Cycle 14 pinned this as a DISCREPANCY, because there was only
  # one national number and it was the wrong one to compare against. With two,
  # it is an identity -- which is the test that the decomposition is right.
  a <- emig_agents(20, n_left = 3)
  p <- emig_panel(a)
  by_state <- sum(!is.na(a$state))
  expect_equal(by_state, p$headcount_us_practising[1])
  expect_equal(p$headcount[1] - by_state, sum(a$left_country))
})

test_that("the engine never creates an emigrant, it only carries one", {
  # left_country is set by apply_provider_migration_matrix() and by nothing else.
  # An engine that could set it would be modelling emigration twice.
  a <- emig_agents(20)
  sim <- simulate_provider_career_once(
    a, 2025:2030, entrants_per_year = 5,
    hours_intercept = calibrate_hours_intercept(a$age, a$sex))
  expect_false("left_country" %in% names(sim$agents))
  expect_identical(sim$panel$headcount, sim$panel$headcount_us_practising)

  # Carried in, it survives to the returned cohort.
  b <- emig_agents(20, n_left = 2)
  sim2 <- simulate_provider_career_once(
    b, 2025:2030, entrants_per_year = 5,
    hours_intercept = calibrate_hours_intercept(b$age, b$sex))
  expect_true("left_country" %in% names(sim2$agents))
  expect_equal(sum(sim2$agents$left_country), 2L)
  # Entrants arrive in the country.
  expect_false(any(sim2$agents$left_country[sim2$agents$origin_cohort == "entrant"]))
})

test_that("the predicate answers the supply question, not the certification one", {
  a <- emig_agents(20, n_left = 3)
  expect_equal(sum(provider_active_in_year(a, 2026L)), 20L)
  expect_equal(sum(provider_us_practising_in_year(a, 2026L)), 17L)

  # Absent the column, the two coincide -- an agent table that has never been
  # through migration has nobody out of the country.
  b <- emig_agents(20)
  expect_identical(provider_active_in_year(b, 2026L),
                   provider_us_practising_in_year(b, 2026L))

  # NA is not emigration: a missing flag means unknown, and unknown providers
  # are not silently deported.
  c3 <- emig_agents(20, n_left = 2); c3$left_country[5] <- NA
  expect_equal(sum(provider_us_practising_in_year(c3, 2026L)), 18L)
})

test_that("an emigrant is still certified and still counted by the back-test quantity", {
  # The reason the certification series must NOT drop them. The observed
  # contract series is cumulative certifications with no attrition; a model that
  # removed emigrants would diverge from the only series it is scored against,
  # and would look like an improvement while breaking the definition match.
  a <- emig_agents(20, n_left = 5)
  expect_equal(sum(provider_active_in_year(a, 2030L)), 20L)
  p <- emig_panel(a, 2025:2035)
  expect_true(all(p$headcount == 20L))          # certification count, flat
  expect_true(all(p$headcount_us_practising == 15L))
})

test_that("the gap is computed on the US-practising supply", {
  # The consequence that motivated the split: comparing a certification count
  # against US demand inflates supply by every provider who left.
  supply <- tibble::tibble(year = 2025:2027,
                           effective_fte_median = c(1000, 1010, 1020),
                           effective_fte_us_practising_median = c(950, 959, 969))
  required <- tibble::tibble(year = 2025:2027, required_fte = c(1200, 1210, 1220))

  cert_gap <- compute_fte_gap(supply, required, supply_col = "effective_fte_median")
  us_gap <- compute_fte_gap(supply, required,
                            supply_col = "effective_fte_us_practising_median")
  # The US-practising gap is the LARGER shortfall, by exactly the emigrant FTE.
  expect_true(all(us_gap$gap_fte < cert_gap$gap_fte))
  expect_equal(cert_gap$gap_fte - us_gap$gap_fte,
               supply$effective_fte_median - supply$effective_fte_us_practising_median)
})

test_that("a run with no emigration reports the identical gap it always did", {
  # The compatibility requirement. This change must move no published number in
  # any run that does not apply the migration matrix -- which is every run today.
  supply <- tibble::tibble(year = 2025:2027,
                           effective_fte_median = c(1000, 1010, 1020),
                           effective_fte_us_practising_median = c(1000, 1010, 1020))
  required <- tibble::tibble(year = 2025:2027, required_fte = c(1200, 1210, 1220))
  a <- compute_fte_gap(supply, required, supply_col = "effective_fte_median")
  b <- compute_fte_gap(supply, required,
                       supply_col = "effective_fte_us_practising_median")
  expect_equal(a$gap_fte, b$gap_fte)
  expect_equal(a$gap_pct, b$gap_pct)
})

test_that("the Monte Carlo summary carries both series", {
  skip_if_not_installed("mufflyaccess")
  a <- emig_agents(30, n_left = 4)
  res <- suppressMessages(run_supply_microsimulation(
    initial_workforce = a, years = 2025:2028, entrants_per_year = 0,
    n_iterations = 3, hours_intercept = calibrate_hours_intercept(a$age, a$sex),
    retirement_schedule = setNames(rep(0, 120), 1:120), career_change_hazard = 0,
    allow_fixed_parameters = TRUE, verbose = FALSE))
  s <- res$summary
  expect_true(all(c("headcount_median", "headcount_us_practising_median",
                    "effective_fte_median", "effective_fte_us_practising_median")
                  %in% names(s)))
  expect_equal(s$headcount_median - s$headcount_us_practising_median, rep(4, nrow(s)))
  expect_true(all(s$effective_fte_us_practising_median < s$effective_fte_median))
})

test_that("neither series can exceed the other in the wrong direction", {
  # An invariant rather than a value: US-practising is a SUBSET of certified, so
  # it can never be larger, under any emigrant count including none and all.
  for (k in c(0, 1, 10, 19, 20)) {
    p <- emig_panel(emig_agents(20, n_left = k))
    expect_true(all(p$headcount_us_practising <= p$headcount), info = paste("k =", k))
    expect_true(all(p$effective_fte_us_practising <= p$effective_fte + 1e-9),
                info = paste("k =", k))
    expect_true(all(p$headcount_us_practising >= 0))
  }
})

test_that("the engine's flat-vector form and the exported predicate agree", {
  # They are the same rule in two shapes: the predicate takes an agent tibble
  # and is the definition of record; the engine works on preallocated vectors
  # and cannot call it without materialising one per year. Cycle 13 established
  # that logic written twice drifts, so this asserts they do not.
  for (k in c(0, 1, 7, 20)) {
    a <- emig_agents(20, n_left = k)
    p <- emig_panel(a, 2025L)                      # base year only, no dynamics
    expect_equal(p$headcount_us_practising[1],
                 sum(provider_us_practising_in_year(a, 2025L)),
                 info = paste("k =", k))
    expect_equal(p$headcount[1], sum(provider_active_in_year(a, 2025L)),
                 info = paste("k =", k))
  }
})
