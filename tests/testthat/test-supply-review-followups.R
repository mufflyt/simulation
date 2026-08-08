# Guards for R/supply-review_followups.R: entrant staged pipeline,
# uncertainty-driver registry, international-migration assumption.

matches_df <- function() tibble::tibble(year = 2020:2025,
                                        matched = c(55, 58, 60, 60, 62, 64))

# ---- Entrant match -> board -> active pipeline ------------------------------

test_that("staged pipeline applies both conversions and the total lag", {
  p <- entrant_pipeline_transition(matches_df(), p_complete_cert = 0.95,
                                   p_active_practice = 0.90, cert_lag = 2)
  expect_equal(p$active_entrants[p$year == 2022], 55 * 0.95 * 0.90, tolerance = 1e-6)
  # source years before the series start yield NA (leading edge of the lag)
  expect_true(all(is.na(p$active_entrants[p$year %in% c(2020, 2021)])))
})

test_that("attrition is monotone: active <= certified <= source matches", {
  p <- entrant_pipeline_transition(matches_df(), p_complete_cert = 0.9,
                                   p_active_practice = 0.8, cert_lag = 1)
  ok <- !is.na(p$active_entrants)
  expect_true(all(p$active_entrants[ok] <= p$certified[ok] + 1e-9))
  expect_true(all(p$certified[ok] <= p$matched[match(p$year[ok] - 1, p$year)] + 1e-9))
})

test_that("doubling matches doubles active entrants (linearity)", {
  base <- entrant_pipeline_transition(matches_df(), cert_lag = 1)
  dbl  <- entrant_pipeline_transition(
    dplyr::mutate(matches_df(), matched = matched * 2), cert_lag = 1)
  ok <- !is.na(base$active_entrants)
  expect_equal(dbl$active_entrants[ok], 2 * base$active_entrants[ok], tolerance = 1e-6)
})

test_that("a longer lag shifts the active series later", {
  p1 <- entrant_pipeline_transition(matches_df(), cert_lag = 1, active_lag = 0)
  p2 <- entrant_pipeline_transition(matches_df(), cert_lag = 1, active_lag = 2)
  # p2 needs an extra 2 years of source history, so it has more leading NAs
  expect_gt(sum(is.na(p2$active_entrants)), sum(is.na(p1$active_entrants)))
})

test_that("pipeline rejects bad fractions, negative matches, and negative/non-integer lags", {
  expect_error(entrant_pipeline_transition(matches_df(), p_complete_cert = 1.2))
  expect_error(entrant_pipeline_transition(matches_df(), p_active_practice = -0.1))
  expect_error(entrant_pipeline_transition(tibble::tibble(year = 2020, matched = -5)))
  expect_error(entrant_pipeline_transition(matches_df(), cert_lag = -1))
  expect_error(entrant_pipeline_transition(matches_df(), cert_lag = 1.5))
  expect_error(entrant_pipeline_transition(tibble::tibble(y = 1, m = 2)))  # wrong columns
})

# ---- Uncertainty-driver registry --------------------------------------------

test_that("retirement is registered as a weakly-observed, high-priority driver", {
  d <- supply_uncertainty_drivers()
  expect_true(all(c("driver", "observability", "priority", "psa_knob", "rationale") %in% names(d)))
  ret <- d[d$driver == "retirement_hazard", ]
  expect_equal(ret$observability, "weakly_observed")
  expect_equal(ret$priority, "high")
  # every listed driver carries a rationale and an observability label
  expect_true(all(nzchar(d$rationale)))
  expect_true(all(d$observability %in% c("weakly_observed", "partially_observed",
                                         "well_observed", "unquantified")))
})

# ---- International-migration assumption --------------------------------------

test_that("the international-migration assumption defaults to an explicit zero and is adjustable", {
  a0 <- international_migration_assumption()
  expect_equal(a0$net_annual_providers, 0)
  expect_match(a0$calibration_status, "zero")
  a1 <- international_migration_assumption(net_annual = -25)
  expect_equal(a1$net_annual_providers, -25)          # net emigration is representable
  expect_error(international_migration_assumption("x"))
  expect_error(international_migration_assumption(c(1, 2)))
  expect_error(international_migration_assumption(Inf))
})

# ---- Entrant pipeline: lag, calibrated conversion, and disruption -----------
#
# Four defects found by scoring the pipeline against the observed certification
# series rather than only checking its internal arithmetic.

test_that("the NRMP series is contiguous, so the pipeline can span the validation window", {
  s <- nrmp_entrant_series()
  # 2021-2024 were missing until 2026-08-05; the series jumped 2020 -> 2025 and
  # could produce nothing for the back-test validation window.
  expect_equal(setdiff(2010:2025, s$appointment_year), integer(0))
  expect_true(all(diff(sort(s$appointment_year)) == 1))
  expect_equal(s$positions_filled[s$appointment_year == 2021], 62L)
  expect_equal(s$positions_filled[s$appointment_year == 2024], 65L)
  # Filled can never exceed offered, in any year.
  expect_true(all(s$positions_filled <= s$positions_offered))
})

test_that("the certification lag defaults to the documented fellowship length", {
  # A 1-year default contradicted the three-year fellowship this package
  # documents, and scored worse against every observed certification year.
  #
  # Evaluate the default IN THE NAMESPACE. URPS_FELLOWSHIP_YEARS is internal, so
  # a bare eval() resolves it in the test environment, where it does not exist
  # under R CMD check export semantics -- the test passed only because
  # load_all(export_all = TRUE) had put it within reach.
  expect_equal(eval(formals(entrant_pipeline_transition)$cert_lag,
                    envir = asNamespace("urpssim")),
               urpssim:::URPS_FELLOWSHIP_YEARS)
  expect_equal(urpssim:::URPS_FELLOWSHIP_YEARS, 3L)
})

test_that("the match-to-cert conversion is estimated, and excludes uninformative years", {
  skip_if_not_installed("mufflyaccess")
  r <- nrmp_match_to_cert_ratio(2020L)
  expect_equal(r$cert_lag, 3L)
  # Backlog years certified an already-practising pool that never passed through
  # the match, and 2020's examination was cancelled. Including either makes the
  # ratio meaningless -- with them the estimate is above 4.0.
  expect_true(all(c(2013L, 2014L, 2015L, 2020L) %in% r$excluded))
  expect_gt(r$ratio, 0.5)
  expect_lt(r$ratio, 1.0)
  expect_gt(nrmp_match_to_cert_ratio(2020L, exclude_disrupted = FALSE)$ratio, 2)

  # This estimator is retained for the back-test, which must not see past its
  # cutoff -- but it is NO LONGER the pipeline default. Its window holds the
  # cancelled-exam trough without the release that repays it, so it reads low.
  # The default now comes from entrant_to_cert_ratio(), pooled over a window
  # spanning both; see "pooling, not the source, is what corrected the
  # conversion" below.
  expect_lt(r$ratio, eval(formals(entrant_pipeline_transition)$p_complete_cert))
})

test_that("a per-year conversion represents a cancelled examination", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  sched <- data.frame(year = 2013:2025, p_complete_cert = 0.8)
  sched$p_complete_cert[sched$year == 2020] <- 0.1

  flat <- entrant_pipeline_transition(m, p_complete_cert = 0.8)
  vary <- entrant_pipeline_transition(m, p_complete_cert = sched)

  # A constant conversion cannot express the event at all.
  expect_equal(length(unique(flat$p_complete_cert)), 1L)
  expect_lt(vary$certified[vary$year == 2020], flat$certified[flat$year == 2020])
})

test_that("a suppressed year defers its fellows rather than destroying them", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  sched <- data.frame(year = 2013:2025, p_complete_cert = 0.8)
  sched$p_complete_cert[sched$year == 2020] <- 0.1

  kept <- entrant_pipeline_transition(m, p_complete_cert = sched, defer_shortfall = TRUE)
  lost <- entrant_pipeline_transition(m, p_complete_cert = sched, defer_shortfall = FALSE)

  # The deficit reappears in the following year, and only there.
  expect_gt(kept$deferred_in[kept$year == 2021], 0)
  expect_equal(kept$deferred_in[kept$year == 2022], 0)
  expect_gt(kept$certified[kept$year == 2021], lost$certified[lost$year == 2021])

  # Deferral conserves fellows across the disruption; discarding them does not.
  win <- 2019:2022
  expect_equal(sum(kept$certified[kept$year %in% win]),
               sum(lost$certified[lost$year %in% win]) +
                 50 * (0.8 - 0.1), tolerance = 1e-6)
})

test_that("a scalar conversion is unchanged by the per-year machinery", {
  m <- data.frame(year = 2013:2025, matched = seq(40, 64, length.out = 13))
  a <- entrant_pipeline_transition(m, p_complete_cert = 0.75)
  b <- entrant_pipeline_transition(m, p_complete_cert = rep(0.75, 13))
  expect_identical(a$certified, b$certified)
  expect_true(all(a$deferred_in == 0))   # nothing to defer when nothing varies
})

test_that("malformed conversion schedules are rejected", {
  m <- data.frame(year = 2013:2025, matched = rep(50, 13))
  expect_error(entrant_pipeline_transition(m, p_complete_cert = c(0.5, 0.6)),
               "supply a")
  expect_error(entrant_pipeline_transition(m, p_complete_cert = 1.5), "\\[0, 1\\]")
  expect_error(entrant_pipeline_transition(m, p_complete_cert = data.frame(year = 2020)),
               "needs")
  # A partial schedule falls back to its own median rather than blanking years.
  part <- entrant_pipeline_transition(
    m, p_complete_cert = data.frame(year = 2020, p_complete_cert = 0.1))
  expect_true(all(is.finite(part$p_complete_cert)))
})

# ---- ACGME fellow counts, and what recalibrating against them did ----------

test_that("the ACGME series carries both parent pathways and respects publication lag", {
  x <- acgme_urps_fellows()
  expect_setequal(unique(x$parent), c("obgyn", "urology"))
  # Each Data Resource Book appears the autumn AFTER its academic year closes,
  # so a back-test at cutoff 2020 may not see the 2020-2021 book.
  expect_true(all(x$available_by_year == x$entry_year + 1L))
  expect_false(2020L %in% acgme_urps_fellows(available_by = 2020L)$entry_year)
  expect_true(2019L %in% acgme_urps_fellows(available_by = 2020L)$entry_year)

  # year_1..3 must sum to the printed total -- the arithmetic that identified
  # the row in the first place, re-checked in-package.
  expect_equal(x$active_total, x$year_1 + x$year_2 + x$year_3)
})

test_that("ACGME sees entrants NRMP does not, and the gap widens", {
  a <- entrant_source_series("acgme")
  n <- entrant_source_series("nrmp")
  m <- merge(a, n, by = "entry_year", suffixes = c("_a", "_n"))
  gap <- m$entrants_a - m$entrants_n

  # ACGME counts fellows on duty, so it includes off-match entry.
  expect_true(mean(gap) > 0)
  # And the undercount grows: the recent gap is far larger than the early one.
  early <- gap[m$entry_year <= 2018]
  late  <- gap[m$entry_year >= 2022]
  expect_gt(mean(late), mean(early))
  expect_gt(max(gap), 10)
})

test_that("active_total is a stock and is never used as the entry flow", {
  # ~215 fellows on duty against ~74 entering: confusing them would treat the
  # whole three-year pipeline as one year's entry.
  x <- acgme_urps_fellows()
  latest <- x[x$entry_year == max(x$entry_year), ]
  expect_gt(sum(latest$active_total), 2 * sum(latest$year_1))
  expect_equal(sum(acgme_entering_cohort()$entering_cohort[
    acgme_entering_cohort()$entry_year == max(x$entry_year)]),
    sum(latest$year_1))
})

test_that("pooling, not the source, is what corrected the conversion", {
  old   <- nrmp_match_to_cert_ratio(2020L)$ratio              # 0.754
  nrmp_pooled  <- entrant_to_cert_ratio("nrmp",  pooled = TRUE)$ratio
  acgme_pooled <- entrant_to_cert_ratio("acgme", pooled = TRUE)$ratio

  # The window/pooling change moved the estimate an order of magnitude more than
  # swapping the entry source did. Recorded so the ACGME fetch is not
  # mis-remembered as having fixed the conversion.
  expect_gt(abs(nrmp_pooled - old), 10 * abs(acgme_pooled - nrmp_pooled))
  expect_equal(acgme_pooled, 0.86, tolerance = 0.02)

  # The pipeline default must be the ACGME pooled conversion.
  expect_equal(eval(formals(entrant_pipeline_transition)$p_complete_cert),
               round(acgme_pooled, 2), tolerance = 0.02)
})

test_that("a window holding a disruption but not its release is biased low", {
  # This is why the old 0.75 was wrong: certifications through 2020 include the
  # cancelled-exam trough, while the 2021 release that repays it sits outside.
  short <- entrant_to_cert_ratio("acgme", through_year = 2020L, pooled = TRUE)$ratio
  full  <- entrant_to_cert_ratio("acgme", pooled = TRUE)$ratio
  expect_lt(short, full)
  expect_lt(short, 0.7)
})

test_that("an unknown entry source is rejected rather than silently defaulted", {
  expect_error(entrant_source_series("scopus"), "should be one of")
  expect_error(entrant_to_cert_ratio("scopus"), "should be one of")
})

# ---- NRMP track split (OB/GYN vs urology) ----------------------------------

test_that("the shipped track split reconciles with NRMP's own aggregate", {
  s <- nrmp_track_split()
  tot <- stats::aggregate(cbind(positions_offered, positions_filled) ~ appointment_year,
                          s, sum)
  ref <- nrmp_entrant_series()
  m <- merge(tot, ref, by.x = "appointment_year", by.y = "appointment_year")

  # The gate that decides what ships: reconstruction within 2 of Table 1 on both
  # counts. Nothing outside that tolerance may be present.
  expect_true(all(abs(m$positions_offered.x - m$positions_offered.y) <= 2))
  expect_true(all(abs(m$positions_filled.x - m$positions_filled.y) <= 2))

  # Each row states its own residual, and the stated residual must be the real
  # one -- otherwise the caveat is decorative.
  for (y in unique(s$appointment_year)) {
    r <- s[s$appointment_year == y, ]
    expect_equal(unique(r$residual_offered),
                 sum(r$positions_offered) - m$positions_offered.y[m$appointment_year == y])
    expect_equal(unique(r$residual_filled),
                 sum(r$positions_filled) - m$positions_filled.y[m$appointment_year == y])
  }
})

test_that("residuals only ever under-recover, never double-count", {
  # A missed program is a whole entry lost to a line wrap; there is no mechanism
  # that invents one. A positive residual would mean the extractor is matching
  # something it should not, which the sign check would catch.
  s <- nrmp_track_split()
  expect_true(all(s$residual_offered <= 0))
  expect_true(all(s$residual_filled <= 0))
})

test_that("reconciles_exactly identifies the slack-free subset", {
  s <- nrmp_track_split()
  e <- nrmp_track_split(exact_only = TRUE)
  expect_true(all(e$residual_offered == 0 & e$residual_filled == 0))
  expect_setequal(unique(e$appointment_year), c(2015L, 2018L, 2021L, 2023L))
  expect_lt(nrow(e), nrow(s))
  expect_equal(s$reconciles_exactly, s$residual_offered == 0 & s$residual_filled == 0)
})

test_that("both tracks are present every year, and urology is the minority", {
  s <- nrmp_track_split()
  per_year <- table(s$appointment_year)
  expect_true(all(per_year == 2L))   # a year with one track would understate entry
  u <- s[s$track == "urology", ]
  expect_true(all(u$urology_share > 0.15 & u$urology_share < 0.35))
  expect_true(all(is.na(s$urology_share[s$track == "obgyn"])))
})

test_that("the NRMP and ACGME pathway mixes disagree, and both are retained", {
  # NRMP puts the urology share of MATCHED POSITIONS higher than ACGME puts the
  # urology share of ENTERING FELLOWS. Neither is corrected to the other: they
  # count different things, and the gap is a finding rather than an error.
  nu <- nrmp_track_split(track = "urology")
  ac <- acgme_urps_fellows()
  yrs <- intersect(nu$appointment_year, ac$entry_year)
  skip_if(length(yrs) < 3, "fewer than three overlapping years in the observed series")
  a_share <- vapply(yrs, function(y) {
    r <- ac[ac$entry_year == y, ]
    r$year_1[r$parent == "urology"] / sum(r$year_1)
  }, numeric(1))
  n_share <- nu$urology_share[match(yrs, nu$appointment_year)]
  expect_gt(mean(n_share), mean(a_share))
})

# ---- Pathway-specific lag, completion, and the locked-in pipeline -----------

test_that("fellowship length is pathway-specific", {
  # OB/GYN-based URPS is three years, urology-based is two. Treating them alike
  # misaligns half the series.
  expect_equal(URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["obgyn"]], 3L)
  expect_equal(URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["urology"]], 2L)
})

test_that("each pathway's conversion is possible once aligned on its own lag", {
  skip_if_not_installed("mufflyaccess")
  r <- entrant_to_cert_ratio_by_pathway()
  expect_setequal(r$parent, c("obgyn", "urology"))
  expect_equal(r$cert_lag[r$parent == "urology"], 2L)

  # A conversion above 1.0 means more certified than ever entered, which is the
  # signature of a wrong alignment rather than a real rate. Urology at lag 3
  # returned 1.050; at its own lag it must be a possible number.
  expect_true(all(r$ratio > 0 & r$ratio < 1))
})

test_that("OB/GYN fellowship attrition is measured, and is near zero", {
  skip_if_not_installed("mufflyaccess")
  r <- fellowship_completion_rate("obgyn")
  expect_gt(r$rate, 0.95)
  expect_gte(r$n_cohorts, 5)
  # This is the point of measuring it: the entry-to-certification conversion is
  # NOT mostly fellows leaving training, so it must not be described as such.
  ratio <- entrant_to_cert_ratio_by_pathway()
  expect_lt(ratio$ratio[ratio$parent == "obgyn"], r$rate)
})

test_that("an impossible completion rate is refused, not reported", {
  skip_if_not_installed("mufflyaccess")
  # Urology year_2 exceeds year_1 in most cohorts, so the columns are not
  # following a cohort and the mean (1.349) is not a completion rate. Returning
  # it would put an impossible number into a field named for a probability.
  expect_error(fellowship_completion_rate("urology"), "impossible")
  tr <- acgme_cohort_tracking("urology")
  expect_gt(mean(tr$retention > 1), 0.5)
})

test_that("the locked-in pipeline forecasts from fellows already in training", {
  skip_if_not_installed("mufflyaccess")
  l <- locked_in_certifications()
  # Every fellow counted must be in a real program year for their pathway.
  for (p in unique(l$parent)) {
    expect_true(all(l$program_year[l$parent == p] <=
                      URPS_FELLOWSHIP_YEARS_BY_PATHWAY[[p]]))
  }
  # Each pathway is converted at its OWN rate, not a pooled one.
  expect_equal(length(unique(l$certification_rate)), 2L)
  expect_true(all(l$expected_certifications <= l$fellows_in_training))
  # The horizon is bounded by the longest programme: nothing beyond it is known.
  expect_lte(max(l$certifying_year) - min(l$certifying_year),
             max(URPS_FELLOWSHIP_YEARS_BY_PATHWAY) - 1L)
})
