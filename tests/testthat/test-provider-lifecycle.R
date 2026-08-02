# Regression guards for the provider lifecycle: retirement, hours, FTE, roster.
#
# These lock the model to the published survival and hours specifications, so a
# schedule cannot drift away from the literature without failing the suite.

test_that("physician retirement survival reproduces the published anchors", {
  # HWSM Exhibit 17 (Florida physician survey): of 100 physicians active at 50,
  # ~80 are still active at 60 and ~30 at 70. FutureDocs: all retired before 80.
  s <- retirement_survival(50, c(55, 60, 65, 70, 75, 80))
  expect_equal(unname(s[["60"]]), 0.797, tolerance = 0.02)
  expect_equal(unname(s[["65"]]), 0.548, tolerance = 0.02)
  expect_equal(unname(s[["70"]]), 0.301, tolerance = 0.02)
  expect_lt(unname(s[["80"]]), 0.06)
})

test_that("the physician curve is materially longer than the allied-health curve", {
  # Using a physical-therapy retirement curve for physicians would overstate
  # attrition badly: PT is >90% retired by 70, physicians only ~70%.
  phys <- retirement_survival(50, 70)
  allied <- retirement_survival(50, 70, retirement_schedule = RETIREMENT_HAZARD_ALLIED,
                                terminal_age = MICROSIM_TERMINAL_AGE_ALLIED)
  expect_gt(unname(phys[["70"]]), 2 * unname(allied[["70"]]))
})

test_that("terminal age forces departure and defaults to the physician value", {
  expect_equal(MICROSIM_TERMINAL_AGE, 90L)   # HWSM: 90 for physicians and dentists
  expect_equal(departure_hazard(90), 1)
  expect_equal(departure_hazard(95), 1)
  expect_equal(departure_hazard(75, terminal_age = 75L), 1)
  expect_lt(departure_hazard(74, terminal_age = 75L), 1)
})

test_that("retirement and career change are separate processes below age 50", {
  # A 38-year-old must not receive a retirement-shaped hazard.
  expect_equal(departure_hazard(38), CAREER_CHANGE_HAZARD_UNDER_50)
  expect_equal(departure_hazard(49), CAREER_CHANGE_HAZARD_UNDER_50)
  expect_gt(departure_hazard(50), departure_hazard(49))
})

test_that("female physicians retire slightly earlier", {
  expect_gt(departure_hazard(62, "female"), departure_hazard(62, "male"))
  # ...but only slightly: the two published curves nearly overlap.
  ratio <- departure_hazard(62, "female") / departure_hazard(62, "male")
  expect_lt(ratio, 1.15)
})

test_that("retirement scenarios shift the age axis and preserve curve shape", {
  later <- shift_retirement_schedule(2)
  earlier <- shift_retirement_schedule(-2)
  # Shifting later means a 65-year-old faces the hazard a 63-year-old faced.
  expect_equal(unname(later[["65"]]), unname(RETIREMENT_HAZARD_BY_AGE[["63"]]))
  expect_equal(unname(earlier[["65"]]), unname(RETIREMENT_HAZARD_BY_AGE[["67"]]))
  # Monotonicity of the curve survives the shift.
  expect_false(is.unsorted(later))
  expect_false(is.unsorted(earlier))
  # Retiring later leaves more of the cohort active.
  expect_gt(unname(retirement_survival(50, 70, retirement_schedule = later)[["70"]]),
            unname(retirement_survival(50, 70)[["70"]]))
})

test_that("the crude departure rate is an output of age structure, not an input", {
  young <- rep(40, 100)
  old <- rep(66, 100)
  expect_lt(implied_annual_departure_rate(young), implied_annual_departure_rate(old))
  # Calibrating to a target rate solves against the ACTUAL age distribution, so
  # the target is reproduced rather than assumed.
  ages <- c(rep(45, 50), rep(58, 30), rep(67, 20))
  sched <- calibrate_retirement_schedule(ages, target_rate = 0.05)
  expect_equal(implied_annual_departure_rate(ages, retirement_schedule = sched),
               0.05, tolerance = 1e-6)
})

test_that("hours worked are flat until the late 50s and fall only from 60", {
  # HWSM Exhibit 14: age 35-44 +0.52, 45-54 +0.30, 55-59 +0.17, 60-64 -2.04.
  m <- hwsm_reference_hours(c(34, 40, 50, 57, 62, 67, 78), "male")
  expect_gt(m[2], m[1])            # 35-44 above the <35 reference
  expect_gt(m[4], m[5])            # 55-59 above 60-64: the decline starts at 60
  expect_true(all(diff(m[4:7]) < 0))
})

test_that("the sex gap in hours varies with age (interaction terms present)", {
  gap_40 <- hwsm_reference_hours(40, "male") - hwsm_reference_hours(40, "female")
  gap_57 <- hwsm_reference_hours(57, "male") - hwsm_reference_hours(57, "female")
  expect_false(isTRUE(all.equal(gap_40, gap_57)))
  expect_equal(gap_57, -HWSM_HOURS_FEMALE_MAIN)   # no interaction in the 55-59 band
})

test_that("FTE is an hours threshold, and the intercept calibration is consistent", {
  set.seed(11)
  age <- round(stats::rnorm(500, 52, 9))
  sex <- ifelse(stats::runif(500) < 0.55, "female", "male")
  ic <- calibrate_hours_intercept(age, sex, fte_hours = URPS_FTE_CLINICAL_HOURS_PER_WEEK)
  # The cohort mean hours must equal the FTE threshold, so base-year FTE tracks
  # headcount rather than exceeding it.
  expect_equal(mean(hwsm_reference_hours(age, sex, intercept = ic)),
               URPS_FTE_CLINICAL_HOURS_PER_WEEK, tolerance = 1e-8)
  fte <- provider_clinical_fte(age, sex, method = "hours", hours_intercept = ic)
  expect_equal(mean(fte), 1, tolerance = 1e-8)
})

test_that("the intercept calibration is not defeated by the non-negativity floor", {
  # hwsm_reference_hours() truncates at 0; solving the intercept through it would
  # silently mis-calibrate. .hwsm_hours_offset() must be untruncated.
  expect_true(any(.hwsm_hours_offset(78, "female") < 0))
})

test_that("FTE definitions from different studies are not interchangeable", {
  d <- fte_definition()
  expect_equal(d$hours_per_week, 37.2)
  # 6,533 nephrology FTE at the FutureDocs 70 hr/wk definition is a much larger
  # workforce when restated on a 37.2 hr/wk definition.
  expect_gt(restate_fte(6533, from_hours = 70), 6533 * 1.8)
  expect_equal(restate_fte(100, from_hours = 37.2), 100)
})

test_that("categorical participation reproduces the FutureDocs qualitative finding", {
  validate_participation_table()

  # Male expected FTE is nearly FLAT across the age range: for men the shift is
  # full-time to part-time, not out of patient care.
  m <- participation_fte(c(30, 50, 65, 80), "male")
  expect_lt(max(m) - min(m), 0.06)

  # Female expected FTE PEAKS around 50 and then collapses.
  f <- participation_fte(c(30, 50, 65, 80), "female")
  expect_gt(f[2], f[1])
  expect_lt(f[4], f[2] / 4)

  # The mechanism is P(no patient care), which rises steeply for women only.
  pn_f <- participation_p_no_patient_care(c(50, 80), "female")
  pn_m <- participation_p_no_patient_care(c(50, 80), "male")
  expect_lt(pn_f[1], 0.06)
  expect_gt(pn_f[2], 0.70)
  expect_lt(max(pn_m) - min(pn_m), 0.12)

  # Ages outside the tabulated range clamp rather than returning NA.
  expect_false(is.na(participation_fte(25, "female")))
  expect_false(is.na(participation_fte(95, "male")))
})

test_that("the FutureDocs FTE basis reconciles with the Dall hours estimates", {
  # Mean expected FTE ~0.6 on the FutureDocs 70 hr/wk definition is ~42 clinical
  # hrs/wk, agreeing with the 42.3 patient-care hrs/wk used in Dall 2013.
  mean_fte <- mean(participation_fte(35:70, rep(c("male", "female"), 18)))
  expect_equal(mean_fte * 70, 42, tolerance = 5)
})

test_that("provider roster validation is fail-closed", {
  good <- tibble::tibble(
    provider_id = c("a", "b"), pathway = "ABOG", age = c(45, 60), sex = c("female", "male"),
    state = c("CO", "TX"), certification_year = c(2010, 1995),
    last_confirmed_active_year = c(2023, 2023)
  )
  expect_silent(validate_provider_roster(good))
  expect_error(validate_provider_roster(good[, -1]), "missing required field")
  expect_error(validate_provider_roster(good[0, ]), "zero rows")
  bad_age <- good; bad_age$age[1] <- 130
  expect_error(validate_provider_roster(bad_age), "implausible age")
})

test_that("multistate licences collapse to unique providers with an audit trail", {
  # Zarek 2025: 344,920 licences held by 246,440 residents -- a naive licence
  # count overstates the workforce by 29%.
  roster <- tibble::tibble(
    provider_id = c("n1", "n1", "n1", "n2", "n3"),
    pathway = "ABOG", age = c(50, 50, 50, 61, 44), sex = "female",
    state = c("CO", "TX", "NM", "NY", "CA"),
    certification_year = 2005,
    last_confirmed_active_year = c(2021, 2023, 2019, 2023, 2022)
  )
  d <- deduplicate_provider_roster(roster)
  expect_equal(d$audit$n_records, 5L)
  expect_equal(d$audit$n_unique_providers, 3L)
  expect_equal(d$audit$inflation_factor, 5 / 3)
  # The surviving row is the most recently confirmed active one.
  expect_equal(d$roster$state[d$roster$provider_id == "n1"], "TX")
})

test_that("roster agents drop providers at or beyond the terminal age", {
  roster <- tibble::tibble(
    provider_id = c("a", "b"), pathway = "ABOG", age = c(50, 92), sex = "female",
    state = "CO", certification_year = 2000, last_confirmed_active_year = 2023
  )
  agents <- agents_from_roster(roster, 2025)
  expect_equal(nrow(agents), 1L)
  expect_true(all(c("sex", "state", "clinical_fte") %in% names(agents)))
})

test_that("base-year supply comes from the versioned contract, not a literal", {
  skip_if_not_installed("mufflyaccess")
  s <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
  expect_true(is.numeric(s$national) && s$national > 0)
  expect_true(is.numeric(s$conus) && s$conus > 0)
  # CONUS excludes AK/HI/PR, so it can never exceed the national count. The two
  # must stay distinguishable: CONUS for geographic access, national for supply.
  expect_lte(s$conus, s$national)
  expect_true(nzchar(s$contract_version))
})
