# Guards for the historical back-test.
#
# The point of a back-test is destroyed by leakage, so these tests are about the
# integrity of the procedure rather than the accuracy of the result. A back-test
# that quietly saw the validation window would look excellent and mean nothing.

# ---- Leakage ---------------------------------------------------------------

test_that("no post-cutoff record enters model fitting", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  seed_microsimulation(1)

  cohort <- backtest_cohort_at(2020L)
  est <- backtest_entrant_estimate(2020L, agents = cohort)

  # Every audited read stopped at the cutoff.
  expect_silent(assert_no_leakage(2020L))

  # The cohort itself contains no post-cutoff certification year.
  expect_lte(max(cohort$cert_year), 2020L)
  expect_lte(max(cohort$entry_year), 2020L)

  # The entrant estimate used only years inside the pre-cutoff window.
  expect_lte(max(as.integer(names(est$yearly))), 2020L)
  expect_equal(unname(est$window), c(2018L, 2020L))
})

test_that("the leakage assertion actually fires when the cutoff is exceeded", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  invisible(.series_through(2023L, what = "deliberate leak"))
  expect_error(assert_no_leakage(2020L), "LEAKAGE")
})

test_that("an unaudited run cannot pass the leakage check", {
  # Silence must not read as success: with no audited reads the assertion fails.
  reset_leakage_audit()
  expect_error(assert_no_leakage(2020L), "no audited contract reads")
})

test_that("the entrant estimate cannot be computed from the leaking window", {
  skip_if_not_installed("mufflyaccess")
  reset_leakage_audit()
  seed_microsimulation(1)
  cohort <- backtest_cohort_at(2020L)

  pre <- backtest_entrant_estimate(2020L, agents = cohort)
  # The main model's estimator uses 2018-2023 and would leak the whole
  # validation window; it must give a materially different answer.
  leaky <- observed_entrant_rate(from_year = 2018L)
  expect_gt(leaky$mean_net_growth, pre$net_growth * 1.4)
})

# ---- Target contract -------------------------------------------------------

test_that("the validated target is 1306 with every dimension recorded", {
  skip_if_not_installed("mufflyaccess")
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_equal(t$value, 1306L)
  expect_equal(t$geography, "national")
  expect_equal(t$board_pathway, "ABOG_PLUS_ABU")
  expect_equal(t$measure, "board_certified_active")
  expect_equal(t$contract_version, "3.0.0")
  expect_match(t$basis, "subspecialty")
  expect_true(nzchar(t$rationale))
})

test_that("the retired 1332/1329 values are identified and rejected", {
  skip_if_not_installed("mufflyaccess")
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_setequal(t$retired_values_rejected, c(1332, 1329))
  expect_false(t$value %in% t$retired_values_rejected)

  cand <- backtest_target_candidates()
  # Every candidate in the project is accounted for, with the dimension that
  # distinguishes it from the chosen target.
  expect_true(all(c(1306L, 1303L, 1332L, 1329L, 1027L, 1339L) %in% cand$value))
  expect_equal(cand$basis[cand$value == 1332L], "primary board cert year")
  expect_equal(cand$status[cand$value == 1332L], "retired")
})

test_that("a mismatched target is an ERROR, not a warning", {
  skip_if_not_installed("mufflyaccess")
  # Wrong pathway: ABOG-only excludes urology and returns 1,027. That count is
  # internally consistent, which is exactly why it is dangerous -- stating the
  # expected target turns a silent substitution into an error.
  expect_error(
    validate_backtest_target(board_pathway = "ABOG", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH"
  )
  # Wrong geography: CONUS returns 1,303.
  expect_error(
    validate_backtest_target(geography = "conus", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH"
  )
  # Wrong measure: roster_snapshot is a headcount, not an active count.
  expect_error(
    validate_backtest_target(measure = "roster_snapshot", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    "CONTRACT MISMATCH|roster_snapshot|not available|measure"
  )
  # The correct combination passes, and states what it matched.
  ok <- validate_backtest_target(acknowledge_no_attrition = TRUE, expected_value = 1306L)
  expect_equal(ok$value, 1306L)

  # None of these may warn-and-continue.
  expect_no_warning(try(
    validate_backtest_target(board_pathway = "ABOG", acknowledge_no_attrition = TRUE,
                             expected_value = 1306L),
    silent = TRUE))
})

test_that("the attrition definition mismatch fails closed by default", {
  skip_if_not_installed("mufflyaccess")
  # The observed series applies no attrition; the model does. Proceeding
  # requires an explicit acknowledgement.
  expect_error(validate_backtest_target(), "NO ATTRITION")
  expect_silent(validate_backtest_target(acknowledge_no_attrition = TRUE))
  t <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_false(t$observed_series_applies_attrition)
})

# ---- Reproducibility -------------------------------------------------------

test_that("fixed seeds reproduce identical summaries", {
  skip_if_not_installed("mufflyaccess")
  a <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 99L)
  b <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 99L)
  expect_identical(a$iterations, b$iterations)

  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  expect_identical(score_backtest_arm(a, obs, "x"), score_backtest_arm(b, obs, "x"))

  # A different seed must give a different draw, or the seeding is inert.
  d <- run_backtest_arm("derived", 60, n_iterations = 25L, seed = 100L)
  expect_false(identical(a$iterations$headcount, d$iterations$headcount))
})

# ---- Metric correctness ----------------------------------------------------

test_that("interval calculations match the empirical quantiles", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 200L, seed = 7L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  pred <- arm$iterations$headcount[arm$iterations$year == 2023]
  expect_equal(s$predicted_median, stats::median(pred))
  expect_equal(s$predicted_mean, mean(pred))
  expect_equal(s$pi95_lower, unname(stats::quantile(pred, 0.025)))
  expect_equal(s$pi95_upper, unname(stats::quantile(pred, 0.975)))
  expect_equal(s$pi80_lower, unname(stats::quantile(pred, 0.10)))
  expect_equal(s$pi80_upper, unname(stats::quantile(pred, 0.90)))

  # The 80% interval must sit inside the 95% interval.
  expect_gte(s$pi80_lower, s$pi95_lower)
  expect_lte(s$pi80_upper, s$pi95_upper)

  # Coverage flags must agree with the intervals they report.
  expect_equal(s$within_80, s$observed >= s$pi80_lower && s$observed <= s$pi80_upper)
  expect_equal(s$within_95, s$observed >= s$pi95_lower && s$observed <= s$pi95_upper)

  # Monte Carlo standard error is sd / sqrt(n).
  expect_equal(s$mc_standard_error, stats::sd(pred) / sqrt(length(pred)))
})

test_that("errors and annual changes are computed consistently", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 100L, seed = 11L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  expect_equal(s$absolute_error, s$predicted_median - s$observed)
  expect_equal(s$percent_error, 100 * s$absolute_error / s$observed)
  expect_equal(s$observed_annual_change, (1306 - 1099) / 3)
  expect_equal(s$predicted_annual_change, (s$predicted_median - 1099) / 3)
})

# ---- Units -----------------------------------------------------------------

test_that("observed and projected quantities are the same unit", {
  skip_if_not_installed("mufflyaccess")
  arm <- run_backtest_arm("derived", 60, n_iterations = 50L, seed = 5L)
  obs <- c("2020" = 1099, "2021" = 1180, "2022" = 1234, "2023" = 1306)
  s <- score_backtest_arm(arm, obs, "x")

  # Both sides are HEADCOUNT, never clinical FTE. Scoring a headcount series
  # against an FTE projection would be a unit error of roughly 10%.
  expect_true("headcount" %in% names(arm$iterations))
  expect_false(any(grepl("fte", names(arm$iterations), ignore.case = TRUE)))
  expect_equal(s$observed, 1306)
  expect_gt(s$predicted_median, 500)
  expect_lt(s$predicted_median, 3000)

  # The baseline the projection starts from is the observed baseline count.
  expect_equal(s$baseline_count, 1099)
  start <- arm$iterations$headcount[arm$iterations$year == 2020]
  expect_equal(unique(start), 1099)
})

test_that("the no-attrition arm removes departures entirely", {
  skip_if_not_installed("mufflyaccess")
  with_att <- run_backtest_arm("derived", 60, n_iterations = 40L, seed = 3L,
                               apply_attrition = TRUE)
  no_att <- run_backtest_arm("derived", 60, n_iterations = 40L, seed = 3L,
                             apply_attrition = FALSE)
  m <- function(x) stats::median(x$iterations$headcount[x$iterations$year == 2023])
  expect_gt(m(no_att), m(with_att))

  # Without attrition the trajectory is exactly baseline + entrants per year.
  expect_equal(m(no_att), 1099 + 3 * 60, tolerance = 2)
})

# ---- Provenance ------------------------------------------------------------

test_that("every scored row records the artifact it was scored against", {
  skip_if_not_installed("mufflyaccess")
  bt <- run_backtest(n_iterations = 15L)
  need <- c("contract_version", "artifact_version", "artifact_source",
            "snapshot_date", "source_sha256", "canonical_release",
            "target_basis", "observed_applies_attrition")
  expect_true(all(need %in% names(bt$summary)))
  # A frozen artifact with no contract identity is untraceable: if mufflyaccess
  # ships a new snapshot where 2023 reads differently, the stale CSV must be
  # detectable rather than silent.
  expect_equal(unique(bt$summary$contract_version), "3.0.0")
  expect_match(unique(bt$summary$target_basis), "subspecialty")
  expect_true(all(nzchar(bt$summary$artifact_version)))
  # canonical_release is FALSE for the bundled bootstrap and must be recorded,
  # not assumed.
  expect_false(unique(bt$summary$canonical_release))
})

test_that("the written summary carries its provenance to disk", {
  # Self-contained: .repo_root() lives in another test file and is not visible
  # under R CMD check, and artifacts/ is .Rbuildignore'd so it is absent from a
  # built package. Skip cleanly in both cases.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  path <- file.path(root[1], "artifacts", "backtest_2020_to_2023_summary.csv")
  skip_if_not(file.exists(path))
  s <- utils::read.csv(path)
  expect_true(all(c("contract_version", "source_sha256", "target_basis") %in% names(s)))
  expect_equal(unique(as.character(s$contract_version)), "3.0.0")

  man <- file.path(root[1], "artifacts", "backtest_2020_to_2023_manifest.json")
  expect_true(file.exists(man))
  m <- jsonlite::read_json(man)
  expect_equal(m$target_value, 1306)
  expect_setequal(unlist(m$retired_values_rejected), c(1332, 1329))
  expect_false(m$observed_series_applies_attrition)
})

# ---- Interval construction -------------------------------------------------

test_that("a fixed-parameter arm produces a degenerate interval", {
  skip_if_not_installed("mufflyaccess")
  # THE DEFECT THIS PINS. Without a param_spec the entrant rate is identical in
  # every replicate, so with attrition switched off there is nothing left to
  # vary and the "95% interval" collapses to a point. An interval like this
  # cannot fail coverage informatively -- which is why run_backtest() scoring
  # 0/8 against it told us nothing about the forecast.
  fixed <- run_backtest_arm("derived", entrants_per_year = 55, n_iterations = 40,
                            apply_attrition = FALSE, param_spec = NULL)
  final <- fixed$iterations$headcount[fixed$iterations$year == BACKTEST_TARGET_YEAR]
  expect_equal(diff(range(final)), 0)
  expect_false(fixed$settings$parameter_uncertainty)
})

test_that("drawing the entrant rate widens the interval without moving the centre", {
  skip_if_not_installed("mufflyaccess")
  spec <- supply_parameter_spec(entrant_series = c(40, 48, 10), entrant_mean = 55)
  drawn <- run_backtest_arm("derived", entrants_per_year = 55, n_iterations = 400,
                            apply_attrition = FALSE, param_spec = spec, seed = 11L)
  fixed <- run_backtest_arm("derived", entrants_per_year = 55, n_iterations = 400,
                            apply_attrition = FALSE, param_spec = NULL, seed = 11L)
  d <- drawn$iterations$headcount[drawn$iterations$year == BACKTEST_TARGET_YEAR]
  f <- fixed$iterations$headcount[fixed$iterations$year == BACKTEST_TARGET_YEAR]

  expect_gt(stats::sd(d), 20)
  expect_true(drawn$settings$parameter_uncertainty)
  # The draw is centred on entrant_mean, so it adds spread and NOT bias. A
  # centre that moved would mean the reported median no longer matched the
  # point estimate the model claims to be reporting.
  expect_lt(abs(stats::median(d) - stats::median(f)), 3 * stats::sd(d) / sqrt(length(d)))
})

test_that("run_backtest gives every arm uncertainty but keeps its own centre", {
  skip_if_not_installed("mufflyaccess")
  skip_on_cran()
  bt <- run_backtest(n_iterations = 40L)

  # Every arm must carry a spec: passing none was the original defect.
  widths <- bt$summary$pi95_upper - bt$summary$pi95_lower
  expect_true(all(widths > 0))

  # ...and the prespecified contrast must survive it. A single shared spec would
  # overwrite entrants_per_year on every iteration, silently collapsing the
  # assumed-entrant arms into the estimated-entrant arms.
  #
  # Compare arms BY NAME, not by rate: arm 5 draws on NRMP and sits at 58/yr,
  # above the shipped assumption of 55, so an "everything that isn't 55 is the
  # low arm" test would silently invert once that arm exists.
  expect_gte(length(unique(bt$summary$entrants_per_year)), 3)
  matched <- !bt$summary$apply_attrition
  assumed <- bt$summary$predicted_median[matched & grepl("entrants = 55", bt$summary$arm)]
  estimated <- bt$summary$predicted_median[matched & grepl("pre-2021 data", bt$summary$arm)]
  expect_length(assumed, 2); expect_length(estimated, 2)
  expect_gt(min(assumed), max(estimated))

  # The NRMP arm sits above both, because 58 > 55 > 32.7.
  nrmp <- bt$summary$predicted_median[matched & grepl("NRMP", bt$summary$arm)]
  expect_length(nrmp, 1)
  expect_gt(nrmp, max(assumed))
})

# ---- NRMP pre-cutoff entrant series ----------------------------------------

test_that("the NRMP series filters on PUBLICATION year, not appointment year", {
  # THE LEAKAGE GUARD. Appointment year and publication year happen to coincide
  # for these reports, but the filter must be on availability: a report issued
  # after the cutoff cannot enter a back-test arm however tempting its value.
  s <- nrmp_entrant_series(available_by = 2020)
  expect_true(all(s$available_by_year <= 2020))
  expect_false(any(s$appointment_year == 2025))
  expect_equal(nrow(s), 4L)
  expect_equal(s$positions_filled, c(59L, 59L, 58L, 56L))

  # The 2025 value must be reachable when no cutoff is imposed, and must equal
  # the value nrmp_entrants() reports.
  full <- nrmp_entrant_series()
  expect_equal(full$positions_filled[full$appointment_year == 2025], 70L)
  skip_if_not_installed("mufflyaccess")
  expect_equal(nrmp_entrants("URPS"), 70L)
})

test_that("filled never exceeds offered, and the frozen series matches data-raw", {
  s <- nrmp_entrant_series()
  expect_true(all(s$positions_filled <= s$positions_offered))

  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  path <- file.path(root[1], "data-raw", "calibration", "nrmp_urps_entrants_series.csv")
  skip_if_not(file.exists(path))
  csv <- utils::read.csv(path, stringsAsFactors = FALSE)
  # If these drift, the constant compiled into the package no longer matches the
  # artifact the fetcher produces, and the back-test is scoring a stale series.
  expect_equal(sort(csv$appointment_year), sort(s$appointment_year))
  m <- merge(csv, s, by = "appointment_year", suffixes = c("_csv", "_pkg"))
  expect_equal(m$positions_filled_csv, m$positions_filled_pkg)
  expect_equal(m$positions_offered_csv, m$positions_offered_pkg)
})

test_that("the NRMP arm is scored and is the most accurate one", {
  skip_if_not_installed("mufflyaccess")
  # Arm 5 exists to test whether pre-cutoff information that DID exist would
  # have helped. It should be the best point estimate; if it ever stops being so,
  # something upstream of the entrant rate has changed.
  expect_true(any(grepl("NRMP", BACKTEST_ARMS$label)))
  rec <- BACKTEST_RECORD_2020_2023
  best <- rec$arm[which.min(abs(rec$percent_error))]
  expect_match(best, "NRMP")
  # ...and it still fails coverage, because its interval is sharp rather than
  # wide. Accuracy and coverage are different things and this pins that.
  expect_false(rec$within_95[rec$arm == best])
})
