# Retirement OOS calibration gate and uncertainty propagation ---------------
#
# Hermetic: constructed metric tables and a trivial access function.

.roos_metrics <- function(coverage = 0.96,
                          n_origins = 5L,
                          future_leakage_pass = TRUE,
                          horizons = c(1L, 3L, 5L)) {
  tibble::tibble(
    horizon_years = horizons,
    coverage = base::rep_len(coverage, base::length(horizons)),
    bias = 0.01,
    mae = 12.0,
    rmse = 18.0,
    n_origins = base::rep_len(n_origins, base::length(horizons)),
    future_leakage_pass = base::rep_len(
      future_leakage_pass, base::length(horizons)
    )
  )
}

testthat::test_that("a well-calibrated back-test passes the gate", {
  testthat::expect_true(
    suppressMessages(validate_retirement_oos(.roos_metrics()))
  )
})

testthat::test_that("THE MODEL AS IT STANDS FAILS THE GATE", {
  # This is the point of the gate, not an incidental case. Measured coverage
  # of a nominally 95% interval has been near 0.80, so the published
  # uncertainty is far too narrow. Encoding that as an executable refusal
  # turns a known limitation into a condition somebody has to clear before a
  # retirement model can be promoted.
  #
  # WHEN THIS TEST STARTS FAILING, the under-dispersion has been fixed and the
  # fixture should be updated -- deliberately, with the new coverage recorded.
  observed <- .roos_metrics(coverage = 0.80)

  testthat::expect_error(
    suppressMessages(validate_retirement_oos(observed)),
    "under-calibrated"
  )
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(observed)),
    "80%", fixed = TRUE
  )
})

testthat::test_that("future leakage fails before coverage is even considered", {
  # A back-test that saw the future produces a coverage number that is not
  # evidence. Checking coverage first would launder it into a pass.
  leaked <- .roos_metrics(coverage = 0.99, future_leakage_pass = FALSE)
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(leaked)),
    "future-leakage"
  )

  # NA is a failure, not an unknown that propagates away.
  unknown_leakage <- .roos_metrics(coverage = 0.99)
  unknown_leakage$future_leakage_pass <- NA
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(unknown_leakage)),
    "future-leakage"
  )
})

testthat::test_that("too few origin years is not a calibration measurement", {
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(.roos_metrics(n_origins = 2L))),
    "origin years"
  )
})

testthat::test_that("an empty metric table is not a passing gate", {
  # A gate that goes green when nothing was measured is the failure shape
  # docs/HALL_OF_SHAME.md records repeatedly.
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(.roos_metrics()[0L, ])),
    "empty"
  )
})

testthat::test_that("missing coverage is refused rather than skipped", {
  missing_coverage <- .roos_metrics()
  missing_coverage$coverage[[2]] <- NA_real_
  testthat::expect_error(
    suppressMessages(validate_retirement_oos(missing_coverage)),
    "under-calibrated"
  )
})

# ---- uncertainty propagation ----------------------------------------------

.roos_providers <- function(active_probability = 0.5, n = 40L) {
  tibble::tibble(
    provider_id = base::sprintf("P%03d", base::seq_len(n)),
    geography_id = base::rep(c("A", "B"), length.out = n),
    clinical_fte = 1.0,
    active_probability = base::rep_len(active_probability, n)
  )
}

.roos_access <- function(sampled_tbl) {
  sampled_tbl |>
    dplyr::group_by(.data$geography_id) |>
    dplyr::summarise(
      access_value = base::sum(.data$active_fte_draw), .groups = "drop"
    )
}

testthat::test_that("access uncertainty is reproducible under a fixed seed", {
  first <- suppressMessages(propagate_retirement_uncertainty_to_access(
    .roos_providers(), .roos_access, n_draws = 50L, seed = 42L
  ))
  second <- suppressMessages(propagate_retirement_uncertainty_to_access(
    .roos_providers(), .roos_access, n_draws = 50L, seed = 42L
  ))
  testthat::expect_equal(first$summary, second$summary)
})

testthat::test_that("a probabilistic workforce produces a non-degenerate interval", {
  # The reason this function exists: treating the retirement panel as known
  # collapses the interval and reports shortage estimates as more certain than
  # they are.
  uncertain <- suppressMessages(propagate_retirement_uncertainty_to_access(
    .roos_providers(active_probability = 0.5), .roos_access,
    n_draws = 200L, seed = 7L
  ))
  testthat::expect_true(base::all(uncertain$summary$sd > 0))
  testthat::expect_true(
    base::all(uncertain$summary$upper_95 > uncertain$summary$lower_95)
  )

  # A workforce known with certainty has no spread -- the contrast that shows
  # the interval is driven by retirement uncertainty and not by noise.
  certain <- suppressMessages(propagate_retirement_uncertainty_to_access(
    .roos_providers(active_probability = 1), .roos_access,
    n_draws = 50L, seed = 7L
  ))
  testthat::expect_true(base::all(certain$summary$sd == 0))
  testthat::expect_true(
    base::all(uncertain$summary$sd > certain$summary$sd)
  )
})

testthat::test_that("a missing active probability is refused, not read as active", {
  missing_probability <- .roos_providers()
  missing_probability$active_probability[[3]] <- NA_real_
  testthat::expect_error(
    suppressMessages(propagate_retirement_uncertainty_to_access(
      missing_probability, .roos_access, n_draws = 5L
    )),
    "non-missing"
  )
})

testthat::test_that("an access function that drops the contract columns is caught", {
  testthat::expect_error(
    suppressMessages(propagate_retirement_uncertainty_to_access(
      .roos_providers(),
      function(sampled_tbl) tibble::tibble(geography_id = "A"),
      n_draws = 2L
    )),
    "access_value"
  )
})
