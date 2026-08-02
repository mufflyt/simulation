# Guards for the base-year provider cohort.
#
# The cohort was a normal draw, rnorm(n, 52, 9). The contract cannot replace it
# with a real roster -- it ships aggregate counts only -- but it can do far
# better than a single draw, and these tests lock in both the improvement and an
# honest account of what is still assumed.

test_that("the certification series decomposes into cohorts that sum to the count", {
  skip_if_not_installed("mufflyaccess")
  coh <- urps_certification_cohorts()
  expect_true(all(c("cert_year", "n_certified", "basis") %in% names(coh)))
  # Cohort sizes must reconstruct the published active count exactly.
  total_2023 <- sum(coh$n_certified[coh$cert_year <= 2023])
  expect_equal(total_2023,
               mufflyaccess::urps_count(2023, geography = "national",
                                        include_urology = TRUE))
  expect_true(all(coh$n_certified >= 0))
})

test_that("the 2013 cohort is flagged as backlog, not a graduate cohort", {
  skip_if_not_installed("mufflyaccess")
  coh <- urps_certification_cohorts()
  expect_match(coh$basis[coh$cert_year == 2013], "backlog")
  expect_true(all(grepl("fellowship", coh$basis[coh$cert_year > 2013])))
  # It is also about half the workforce, so its age assumption matters.
  expect_gt(coh$n_certified[coh$cert_year == 2013] / sum(coh$n_certified), 0.4)
})

test_that("the cohort separates observed ages from assumed ones", {
  skip_if_not_installed("mufflyaccess")
  set.seed(7)
  a <- agents_from_certification_cohorts(baseline_year = 2023L)
  comp <- cohort_composition(a)

  expect_setequal(comp$cohort_source, c("observed", "assumed"))
  # About half the 2023 workforce has a known certification year.
  expect_equal(comp$share[comp$cohort_source == "observed"], 0.5, tolerance = 0.05)

  # The two populations have genuinely different age structures, which is why
  # one normal draw could not represent them: fellowship graduates are young and
  # tightly grouped, the backlog cohort is older and widely spread.
  obs <- comp[comp$cohort_source == "observed", ]
  asm <- comp[comp$cohort_source == "assumed", ]
  expect_lt(obs$mean_age, asm$mean_age - 10)
  expect_lt(obs$max_age - obs$min_age, asm$max_age - asm$min_age)
})

test_that("the cohort reproduces the contract headcount", {
  skip_if_not_installed("mufflyaccess")
  set.seed(3)
  a <- agents_from_certification_cohorts(baseline_year = 2023L)
  expect_equal(nrow(a), mufflyaccess::urps_count(2023, geography = "national",
                                                 include_urology = TRUE))
  expect_true(all(a$age >= MICROSIM_ENTRY_AGE))
  expect_true(all(a$age < MICROSIM_TERMINAL_AGE))
})

test_that("cohort provenance never calls a derived cohort a roster", {
  skip_if_not_installed("mufflyaccess")
  set.seed(3)
  derived <- cohort_provenance(agents_from_certification_cohorts(baseline_year = 2023L))
  expect_equal(derived$source, "certification_cohorts")
  expect_false(derived$is_production)
  expect_match(derived$note, "not a roster")

  synth <- cohort_provenance(initialize_provider_agents(100, "URPS", 2025))
  expect_equal(synth$source, "synthetic")
  expect_false(synth$is_production)

  roster <- tibble::tibble(
    provider_id = c("a", "b"), pathway = "ABOG", age = c(45, 60),
    sex = c("female", "male"), state = c("CO", "TX"),
    certification_year = c(2010, 2015), last_confirmed_active_year = 2023
  )
  prod <- cohort_provenance(agents_from_roster(roster, 2025))
  expect_equal(prod$source, "roster")
  expect_true(prod$is_production)
})

test_that("the entrant rate excludes the certification ramp-up", {
  skip_if_not_installed("mufflyaccess")
  # Net growth averaged ~86/yr while the backlog cleared (2014-2017) against
  # ~53/yr from 2018. Fitting on the ramp-up would badly over-project.
  coh <- urps_certification_cohorts()
  ramp_years <- coh$n_certified[coh$cert_year >= 2014 & coh$cert_year <= 2017]
  steady <- observed_entrant_rate(from_year = 2018L)
  expect_gt(mean(ramp_years), steady$mean_net_growth * 1.4)
  expect_equal(steady$window[1], 2018)
  # The default window must start after the ramp, not at the series start.
  expect_gt(observed_entrant_rate()$window[1], 2013)
})

test_that("the hardcoded entrant assumption is reconciled against the series", {
  skip_if_not_installed("mufflyaccess")
  set.seed(7)
  a <- agents_from_certification_cohorts(baseline_year = 2023L)
  # gross entrants = observed net growth + modelled departures
  e <- suppressMessages(implied_gross_entrants(a, assumed = 55))
  expect_equal(e$gross_entrants, e$net_growth + e$departures)
  expect_gt(e$gross_entrants, 70)

  # A materially wrong assumption must warn rather than pass silently.
  expect_message(implied_gross_entrants(a, assumed = 55), "differ from the observed-series")
  expect_silent(implied_gross_entrants(a, assumed = e$gross_entrants))
})
