# URPS roster: production cohort and workload evidence (R/supply-roster_capacity).

rc_have <- function() {
  any(file.exists(c("data-raw/urps_roster/urps_roster_2026-07-22.csv",
                    file.path("..", "..", "data-raw", "urps_roster",
                              "urps_roster_2026-07-22.csv"))))
}
rc_path <- function() {
  p <- "data-raw/urps_roster/urps_roster_2026-07-22.csv"
  if (file.exists(p)) p else file.path("..", "..", p)
}

test_that("the roster carries no physician names", {
  skip_if_not(rc_have())
  r <- load_urps_roster(rc_path())
  # The model needs age, sex, state, certification year and workload. It never
  # needs to identify an individual, so the extract does not carry the means to.
  expect_false(any(grepl("name", names(r), ignore.case = TRUE)))
  expect_true(all(c("npi", "state", "cert_year", "age_proxy_from_cert") %in% names(r)))
})

test_that("the roster satisfies the production-cohort contract", {
  skip_if_not(rc_have())
  pr <- suppressMessages(urps_provider_roster(load_urps_roster(rc_path())))
  expect_silent(validate_provider_roster(pr))
  expect_true(all(pr$age >= 18 & pr$age <= 100))
  expect_true(all(pr$sex %in% c("female", "male")))
  # Medicare billing is the activity attestation; providers without it are
  # UNCONFIRMED rather than assumed inactive or assumed active.
  expect_true(any(is.na(pr$last_confirmed_active_year)))
  expect_true(all(stats::na.omit(pr$last_confirmed_active_year) == 2024L))
})

test_that("a roster cohort is a production cohort, unlike the certification cohort", {
  skip_if_not(rc_have())
  pr <- suppressMessages(urps_provider_roster(load_urps_roster(rc_path())))
  a <- agents_from_roster(pr, baseline_year = 2025L)
  # THE GATE THIS CLEARS. origin_cohort == "roster" is what makes example_only
  # FALSE; agents_from_certification_cohorts() cannot produce it.
  expect_equal(unique(a$origin_cohort), "roster")
  expect_gt(nrow(a), 1000)
  expect_gt(length(unique(a$state)), 40)
})

test_that("a missing sex is filled as a declared assumption, with a warning", {
  r <- tibble::tibble(npi = c("1", "2"), gender = c("F", ""), state = c("CO", "TX"),
                      cert_year = c(2010, 2012), age_proxy_from_cert = c(45, 43),
                      has_medicare_2024 = c(TRUE, FALSE))
  # Silently defaulting produces NA clinical FTE and an unreadable engine error
  # ("missing value where TRUE/FALSE needed") far from the cause.
  expect_message(pr <- urps_provider_roster(r), "no sex")
  expect_equal(pr$sex, c("female", "female"))
  expect_equal(pr$last_confirmed_active_year, c(2024L, NA_integer_))
})

test_that("workload concentration is computed and carries its caveats", {
  r <- tibble::tibble(urogyn_services_2024 = c(rep(0, 50), rep(10, 30), rep(1000, 20)))
  c1 <- roster_workload_concentration(r)
  expect_equal(c1$n_providers, 100)
  expect_equal(c1$n_zero, 50)
  expect_equal(c1$share_zero, 0.5)
  expect_equal(c1$total_volume, 20300)
  expect_gt(c1$share_from_top_quartile, 0.9)

  cav <- attr(c1, "caveats")
  # The suppression floor and the workload-is-not-capacity point must travel
  # with the number: without them the zero share reads as measured inactivity,
  # and the volumes read as an adequacy anchor they cannot support.
  expect_true(any(grepl("suppress", cav)))
  expect_true(any(grepl("UPPER bounds", cav)))
  expect_true(any(grepl("Workload is not capacity", cav)))
})

test_that("an all-zero workload column yields NA shares rather than dividing by zero", {
  c1 <- roster_workload_concentration(tibble::tibble(urogyn_services_2024 = rep(0, 10)))
  expect_equal(c1$share_zero, 1)
  expect_true(is.na(c1$share_from_top_decile))
  expect_true(is.na(c1$n_for_90pct))
})

test_that("URPS work is concentrated in a minority of the certified roster", {
  skip_if_not(rc_have())
  c1 <- roster_workload_concentration(load_urps_roster(rc_path()))
  # The substantive supply-side finding: board certification is not the same as
  # delivering urogynaecologic care. Directional, not exact -- suppression makes
  # these upper bounds.
  expect_gt(c1$share_zero, 0.15)
  expect_gt(c1$share_from_top_quartile, 0.8)
  expect_lt(c1$share_of_roster_for_90pct, 0.4)
})
