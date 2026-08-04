# The gates that keep uncalibrated or non-production inputs out of published
# numbers (R/29 assert_calibrated_transitions, R/15 cohort provenance).
#
# Each of these previously produced a warning that a caller could ignore, or in
# the analogy case suppressed a strict-mode stop outright. The point of the tests
# is that the DEFAULT is refusal and the override has to be typed.

with_mode <- function(mode, code) {
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE")
          else Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)
  Sys.setenv(REPRODUCIBILITY_MODE = mode)
  force(code)
}

mk_co <- function(n = 300) {
  data.frame(age = seq_len(n) %% 40 + 45, cumulative_vaginal_deliveries = 2L,
             years_since_last_vaginal_birth = 20, bmi = 28, hysterectomy = 0,
             menopause_status = 1L, comorbidity = 0)
}

# ---- assert_calibrated_transitions ----------------------------------------

test_that("fitted and calibrated transitions pass silently", {
  tr <- dmdm_default_transitions()
  for (s in c("fitted", "calibrated")) {
    tr$status <- s
    expect_silent(assert_calibrated_transitions(tr))
    expect_true(assert_calibrated_transitions(tr))
  }
})

test_that("strict mode refuses placeholder transitions", {
  with_mode("strict", {
    expect_error(assert_calibrated_transitions(dmdm_default_transitions()),
                 "placeholder_uncalibrated")
    # The message must name the fix, not just the problem.
    expect_error(assert_calibrated_transitions(dmdm_default_transitions()),
                 "allow_uncalibrated")
  })
})

test_that("relaxed mode warns rather than stopping", {
  with_mode("relaxed", {
    expect_message(assert_calibrated_transitions(dmdm_default_transitions()),
                   "placeholder_uncalibrated")
    expect_false(suppressMessages(
      assert_calibrated_transitions(dmdm_default_transitions())))
  })
})

test_that("the override is honoured in strict mode but announces itself", {
  with_mode("strict", {
    expect_message(
      assert_calibrated_transitions(dmdm_default_transitions(),
                                    allow_uncalibrated = TRUE),
      "EXPLORATORY")
    expect_false(suppressMessages(assert_calibrated_transitions(
      dmdm_default_transitions(), allow_uncalibrated = TRUE)))
  })
})

test_that("an object with no status is refused, not waved through", {
  # A bare list of coefficients carries no provenance at all. Treating that as
  # acceptable would let the gate be bypassed by dropping the status field.
  with_mode("strict", {
    expect_error(assert_calibrated_transitions(list(onset = list(), remission = c())),
                 "unknown")
  })
})

test_that("derived_by_analogy is not an estimated tier", {
  with_mode("strict", {
    expect_error(
      assert_calibrated_transitions(dmdm_transitions_with_pop_literature()),
      "derived_by_analogy")
  })
})

# ---- the engines refuse by default ----------------------------------------

test_that("simulate_dmdm fails closed in strict mode", {
  with_mode("strict", {
    expect_error(simulate_dmdm(mk_co(), 2025, 2027), "placeholder_uncalibrated")
    expect_s3_class(
      suppressMessages(simulate_dmdm(mk_co(), 2025, 2027, allow_uncalibrated = TRUE)),
      "data.frame")
  })
})

test_that("dmdm_prevalence_trajectory fails closed before building a cohort", {
  pop <- tibble::tibble(age = 40:85, population = 1e5)
  with_mode("strict", {
    expect_error(dmdm_prevalence_trajectory(pop, 2025, 2027, n = 500),
                 "placeholder_uncalibrated")
  })
})

test_that("simulate_dmdm_open fails closed in strict mode", {
  init <- data.frame(
    age = 40:84, cumulative_vaginal_deliveries = 2L,
    years_since_last_vaginal_birth = 20, bmi = 28, hysterectomy = 0,
    menopause_status = as.integer((40:84) >= 51), comorbidity = 0,
    weight = 1e5, p_ui = .05, p_pop = .025, p_ai = .025)
  with_mode("strict", {
    expect_error(simulate_dmdm_open(init, NULL, 2025, 2027),
                 "placeholder_uncalibrated")
  })
})

# ---- cohort provenance gate (R/15) ----------------------------------------

test_that("a non-production cohort is refused in strict mode", {
  # cohort_provenance() marks only agents_from_roster() output as production;
  # the certification series is a cumulative certification count and the
  # synthetic draw is a placeholder. Neither may seed a strict run.
  synth <- data.frame(age = 45:60, sex = "female",
                      origin_cohort = "baseline", stringsAsFactors = FALSE)
  prov <- cohort_provenance(synth)
  expect_identical(prov$source, "synthetic")
  expect_false(prov$is_production)
})

test_that("a roster cohort is the only production source", {
  ros <- data.frame(age = 45:60, sex = "female",
                    origin_cohort = "roster", stringsAsFactors = FALSE)
  expect_true(cohort_provenance(ros)$is_production)
})
