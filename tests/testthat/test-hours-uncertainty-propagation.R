# Regression guards for the hours-uncertainty propagation defect.
#
# `draw_supply_parameters()` drew `hours_coef` from the fitted hours model's
# coef/vcov, and `run_supply_microsimulation()` then discarded it: the run
# metadata reported "hours" as a quantified parameter while the intervals
# contained none of it. That is the exact failure mode R/calibration-parameter_uncertainty exists to prevent --
# an interval that looks rigorous and is too narrow -- so it is locked here.
#
# `hours_coef` is the COEFFICIENT VECTOR of lm(clinical_hours ~ ns(age) + sex),
# not a scalar multiplier. The no-uncertainty case is hours_coef == coef(model).
#
# The invariant that matters: headcount is the size of the active set and can
# never move with hours; only `effective_fte` may.

hu_survey <- function(n_per = 40) {
  ages <- rep(c(34, 42, 50, 58, 66, 74), each = n_per)
  data.frame(
    age = ages,
    sex = rep(c("male", "female"), length.out = length(ages)),
    clinical_hours = pmax(5, 44 - 0.35 * pmax(ages - 55, 0) +
                            stats::rnorm(length(ages), 0, 4))
  )
}

hu_model <- function(seed = 101) {
  set.seed(seed)
  fit_clinical_hours_model(hu_survey(), df = 2L)
}

hu_cohort <- function() {
  tibble::tibble(
    provider_id = sprintf("P%03d", 1:60),
    subspecialty = "FPMRS",
    sex = rep(c("female", "male"), 30),
    age = rep(seq(36, 70, by = 2), length.out = 60),
    entry_year = 2020,
    retirement_year = NA_real_,
    origin_cohort = "baseline"
  )
}

hu_bump <- function(model, by) {
  b <- stats::coef(model)
  b[["(Intercept)"]] <- b[["(Intercept)"]] + by
  urpssim:::.hours_model_with_coef(model, b)
}

# Attrition and entry switched off, so the ONLY thing that can move effective FTE
# across replicates is the hours draw. Without this isolation the hours component
# (SD ~0.4 FTE on this cohort) is swamped by retirement stochasticity (SD ~10),
# and a variance comparison would be measuring noise rather than the defect.
hu_frozen <- function() stats::setNames(rep(0, 40), 50:89)

test_that("a drawn hours coefficient reaches the microsimulation", {
  m <- hu_model()
  agents <- dplyr::filter(hu_cohort(), .data$age >= 50)   # no career-change hazard
  sd_at <- function(r) {
    v <- dplyr::filter(r$iterations, .data$year == 2032)
    stats::sd(v$effective_fte)
  }

  # Hours FIXED at the point estimate: with no attrition and no entrants the run
  # is fully deterministic, so every replicate is identical.
  fixed <- run_supply_microsimulation(agents, 2025:2032, 0, "FPMRS",
                                      n_iterations = 40, hours_model = m,
                                      retirement_schedule = hu_frozen(),
                                      verbose = FALSE)
  expect_equal(sd_at(fixed), 0)

  # Hours DRAWN: the coefficient uncertainty of the fit is now the only source of
  # variation, so a non-zero spread proves the draw arrives. Before the fix this
  # was exactly 0 -- run_supply_microsimulation() computed hours_coef and
  # discarded it while reporting "hours" as a quantified parameter.
  spec <- supply_parameter_spec(entrant_mean = 0, hours_model = m)
  drawn <- run_supply_microsimulation(agents, 2025:2032, 0, "FPMRS",
                                      n_iterations = 40, param_spec = spec,
                                      retirement_schedule = hu_frozen(),
                                      verbose = FALSE)
  expect_gt(sd_at(drawn), 0)

  s <- dplyr::filter(drawn$summary, .data$year == 2032)
  expect_gt(s$effective_fte_hi - s$effective_fte_lo, 0)
  # Headcount is untouched by any of this.
  expect_equal(dplyr::filter(drawn$summary, .data$year == 2032)$headcount_median,
               dplyr::filter(fixed$summary, .data$year == 2032)$headcount_median)
})

test_that("the propagated spread matches the coefficient draw, so it is not doubled", {
  m <- hu_model()
  agents <- dplyr::filter(hu_cohort(), .data$age >= 50)
  spec <- supply_parameter_spec(entrant_mean = 0, hours_model = m)

  # Spread implied by substituting the drawn coefficients directly.
  set.seed(1)
  analytic <- stats::sd(replicate(300, {
    mm <- urpssim:::.hours_model_with_coef(m, draw_supply_parameters(spec)$hours_coef)
    sum(predict_clinical_hours(agents$age, agents$sex, mm)) /
      urpssim:::URPS_FTE_CLINICAL_HOURS_PER_WEEK
  }))
  expect_gt(analytic, 0)

  # Spread the engine actually reports for the same (frozen) cohort.
  drawn <- run_supply_microsimulation(agents, 2025:2026, 0, "FPMRS",
                                      n_iterations = 300, param_spec = spec,
                                      retirement_schedule = hu_frozen(),
                                      verbose = FALSE)
  engine <- stats::sd(dplyr::filter(drawn$iterations, .data$year == 2025)$effective_fte)

  # A coefficient applied twice would roughly double this ratio.
  expect_equal(engine / analytic, 1, tolerance = 0.2)
})

test_that("changing only the hours coefficients moves FTE and leaves headcount alone", {
  m <- hu_model()
  agents <- hu_cohort()

  run_with <- function(model) {
    set.seed(4242)
    simulate_provider_career_once(agents, 2025:2032, 20, hours_model = model)$panel
  }
  base <- run_with(m)
  more <- run_with(hu_bump(m, 6))     # +6 weekly clinical hours for everyone

  # Headcount is the size of the active set; hours cannot touch it.
  expect_identical(base$headcount, more$headcount)
  expect_identical(base$mean_age, more$mean_age)
  # Effective FTE must move, and upward.
  expect_true(all(more$effective_fte > base$effective_fte))
})

test_that("adjusted supply is monotone in the hours intercept", {
  m <- hu_model()
  agents <- hu_cohort()
  fte_at <- function(by) {
    set.seed(99)
    simulate_provider_career_once(agents, 2025:2030, 20,
                                  hours_model = hu_bump(m, by))$panel$effective_fte[1]
  }
  vals <- vapply(c(-8, -4, 0, 4, 8), fte_at, numeric(1))
  expect_equal(vals, sort(vals))
  expect_true(all(diff(vals) > 0))
})

test_that("hours_coef equal to the fitted coefficients reproduces the fixed-model run", {
  m <- hu_model()
  agents <- hu_cohort()
  identity_model <- urpssim:::.hours_model_with_coef(m, stats::coef(m))

  set.seed(7)
  a <- simulate_provider_career_once(agents, 2025:2032, 20, hours_model = m)$panel
  set.seed(7)
  b <- simulate_provider_career_once(agents, 2025:2032, 20, hours_model = identity_model)$panel
  expect_equal(a, b)
})

test_that("the drawn coefficients are applied exactly once", {
  m <- hu_model()
  agents <- hu_cohort()
  drawn <- hu_bump(m, 3)

  set.seed(5)
  panel <- simulate_provider_career_once(agents, 2025:2030, 20, hours_model = drawn)$panel

  # Year one is evaluated on the base cohort at its starting ages, so the
  # expected FTE is closed-form. Applying the coefficients twice (or scaling the
  # FTE as well as the hours) would not reproduce this.
  expected <- sum(predict_clinical_hours(agents$age, agents$sex, drawn)) /
    urpssim:::URPS_FTE_CLINICAL_HOURS_PER_WEEK
  expect_equal(panel$effective_fte[1], expected, tolerance = 1e-10)
})

test_that("a non-finite or unusable hours draw fails closed", {
  m <- hu_model()
  # Coefficients that share no names with the fit cannot be substituted.
  expect_error(
    urpssim:::.hours_model_with_coef(m, c(nonsense = 1)),
    "share no names"
  )
  # A spec with no model draws nothing, and must not invent a coefficient.
  spec <- supply_parameter_spec(entrant_series = c(30, 35, 33), entrant_mean = 40)
  expect_false(spec$quantified[["hours"]])
  expect_null(draw_supply_parameters(spec)$hours_coef)
})

test_that("quantifying hours under an FTE method that ignores them is refused", {
  m <- hu_model()
  agents <- hu_cohort()
  spec <- supply_parameter_spec(entrant_mean = 20, hours_model = m)

  # participation/legacy_weight never consult an hours model, so the interval
  # would omit a component the metadata claims.
  expect_message(
    run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                               fte_method = "participation", param_spec = spec,
                               verbose = FALSE),
    "never consults an hours model"
  )

  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE")
          else Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)
  Sys.setenv(REPRODUCIBILITY_MODE = "strict")
  expect_error(
    run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                               fte_method = "legacy_weight", param_spec = spec,
                               verbose = FALSE),
    "never consults an hours model"
  )
})

test_that("the spec's hours model takes precedence over a conflicting argument", {
  m <- hu_model()
  other <- hu_bump(m, 10)
  agents <- hu_cohort()
  spec <- supply_parameter_spec(entrant_mean = 20, hours_model = m)

  expect_message(
    run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                               hours_model = other, param_spec = spec,
                               verbose = FALSE),
    "takes precedence"
  )
})

# ---- The uncertainty guard runs unconditionally ----------------------------

hu_with_mode <- function(mode, code) {
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE")
          else Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)
  Sys.setenv(REPRODUCIBILITY_MODE = mode)
  force(code)
}

test_that("strict mode refuses a run that holds every parameter fixed", {
  m <- hu_model(); agents <- hu_cohort()
  # The guard used to be skipped when param_spec was NULL -- exactly the case it
  # exists to catch -- so a strict run could still emit a sampling-noise band.
  hu_with_mode("strict", {
    expect_error(
      run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                 hours_model = m, verbose = FALSE),
      "individual stochasticity ONLY")
  })
  # Relaxed mode still only warns.
  hu_with_mode("relaxed", {
    expect_message(
      run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                 hours_model = m, verbose = FALSE),
      "individual stochasticity ONLY")
  })
})

test_that("an exploratory run must be declared, and says what its band means", {
  m <- hu_model(); agents <- hu_cohort()
  hu_with_mode("strict", {
    expect_message(
      r <- run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                      hours_model = m, allow_fixed_parameters = TRUE,
                                      verbose = FALSE),
      "must not be reported as a forecast interval")
    expect_s3_class(r$summary, "data.frame")
    # The metadata must keep saying the band is noise, not forecast uncertainty.
    expect_match(r$scenario$parameter_uncertainty, "sampling noise")
  })
  # A spec that genuinely draws something needs no override and gets no warning.
  spec <- supply_parameter_spec(entrant_series = c(30, 35, 33), entrant_mean = 20)
  hu_with_mode("strict", {
    expect_no_error(
      run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                 hours_model = m, param_spec = spec, verbose = FALSE))
  })
})

test_that("strict mode refuses an hours intercept that yields more FTE than people", {
  agents <- hu_cohort()
  # allow_fixed_parameters clears the uncertainty guard so this reaches the
  # FTE-consistency guard, which is the one under test.
  hu_with_mode("strict", {
    expect_error(
      run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                 allow_fixed_parameters = TRUE, verbose = FALSE),
      "FTE supply will exceed headcount")
  })
  # A calibrated intercept passes the same guard.
  ic <- calibrate_hours_intercept(agents$age, agents$sex)
  hu_with_mode("strict", {
    expect_no_error(
      run_supply_microsimulation(agents, 2025:2027, 20, "FPMRS", n_iterations = 3,
                                 hours_intercept = ic, allow_fixed_parameters = TRUE,
                                 verbose = FALSE))
  })
})
