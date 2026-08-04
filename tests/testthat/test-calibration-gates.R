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

# ---- contract exporters (R/export_demand_contract.R) -----------------------
#
# The engines refuse uncalibrated transitions, but the exporters take a bare
# data frame. Without their own gate, a hand-assembled trajectory still reached
# cliff/twostep/isochrones as a demand contract with real-looking numbers in it.

mk_dmdm_traj <- function() {
  data.frame(year = 2025:2030, population = seq(45e6, 48e6, length.out = 6),
             prev_ui = seq(.20, .26, length.out = 6),
             prev_pop = seq(.08, .14, length.out = 6),
             prev_ai = seq(.05, .07, length.out = 6))
}
mk_hdmm_traj <- function() {
  data.frame(year = 2025:2030,
             care_seeking_national  = seq(4.0e6, 4.6e6, length.out = 6),
             service_units_national = seq(9.0e6, 10.8e6, length.out = 6))
}

test_that("the DMDM exporter refuses placeholder tiers in strict mode", {
  with_mode("strict", {
    expect_error(
      export_dmdm_demand_contract(mk_dmdm_traj(), output_directory = tempfile("g_"),
                                  verbose = FALSE),
      "placeholder_uncalibrated")
  })
})

test_that("a refused export writes nothing at all", {
  # The gate runs before dir.create(), so a refusal must not leave an artifact
  # directory behind for a later run to find and mistake for a real export.
  d <- tempfile("norun_")
  with_mode("strict", {
    expect_error(export_dmdm_demand_contract(mk_dmdm_traj(), output_directory = d,
                                             verbose = FALSE))
  })
  expect_false(dir.exists(d))
})

test_that("the override lets the DMDM export through and still stamps status", {
  out <- suppressMessages(export_dmdm_demand_contract(
    mk_dmdm_traj(), output_directory = tempfile("ok_"), verbose = FALSE,
    allow_uncalibrated = TRUE))
  expect_true(file.exists(out$csv_path))
  expect_true(all(out$data$tier_calibration_status == "placeholder_uncalibrated"))
})

test_that("the gate reads the WEAKEST tier, not the object-level status", {
  # The literature POP object is "derived_by_analogy" at the object level, but
  # its UI and AI tiers are still placeholders. Gating on the object status
  # alone would let a placeholder tier be written under an analogy label.
  with_mode("strict", {
    expect_error(
      export_dmdm_demand_contract(
        mk_dmdm_traj(), output_directory = tempfile("w_"), verbose = FALSE,
        transitions = dmdm_transitions_with_pop_literature()),
      "placeholder_uncalibrated")
  })
})

test_that("fitted transitions export with no override needed", {
  fitted <- dmdm_transitions_with_pop_literature()
  fitted$status <- "fitted"
  fitted$calibration_status <- "fitted"
  fitted$provenance <- list(ui = "fitted", pop = "fitted", ai = "fitted")
  out <- expect_silent(export_dmdm_demand_contract(
    mk_dmdm_traj(), output_directory = tempfile("fit_"), verbose = FALSE,
    transitions = fitted))
  expect_true(all(out$data$tier_calibration_status == "fitted"))
})

test_that("the HDMM exporter is gated the same way", {
  with_mode("strict", {
    expect_error(
      export_hdmm_demand_contract(mk_hdmm_traj(), output_directory = tempfile("h_"),
                                  verbose = FALSE),
      "placeholder_uncalibrated")
  })
  out <- suppressMessages(export_hdmm_demand_contract(
    mk_hdmm_traj(), output_directory = tempfile("h_"), verbose = FALSE,
    allow_uncalibrated = TRUE))
  expect_true(file.exists(out$csv_path))
})

test_that("the shape check still runs before the calibration gate", {
  # A malformed trajectory must report the column problem, not the calibration
  # one: the caller has a different bug and the message has to say which.
  bad <- data.frame(year = 2025:2026, care_seeking_national = c(1, 2))
  with_mode("strict", {
    expect_error(export_hdmm_demand_contract(bad, output_directory = tempfile("b_")),
                 "trajectory needs columns")
  })
})
