# Adversarial cycle 20 -- the relation between a pair of columns.
#
# Cycle 19 carried forward "the guarded half of a pair": `py` was validated and
# `ev` was not, and the same asymmetry is available wherever two columns carry a
# joint constraint. The richest instance in this package is an INTERVAL. Seventeen
# files in R/ produce a lower/upper pair, and grepping every ordering comparison
# in R/ found exactly one -- and it was a read, not a check.
#
# So an interval could be inverted end to end. Measured:
#
#   validate_urps_gap_projection(lower_95 = 1400, upper_95 = 1000, mode="strict")
#     -> PASSED CLEAN
#   forecast_scorecard() on that interval
#     -> coverage 0, mean_width -400
#
# Each column is individually finite and individually plausible. The relation
# between them is the entire content of an interval, and nothing looked at it.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc20_projection <- function(...) {
  base <- data.frame(
    year = 2025:2026, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = c(1000, 1010), supply_clinical_fte = c(900, 909),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = c(1300, 1310), demand_clinical_fte = c(1200, 1210),
    gap_fte = c(-300, -301), gap_headcount = c(-300, -300),
    stringsAsFactors = FALSE)
  extra <- list(...)
  for (nm in names(extra)) base[[nm]] <- extra[[nm]]
  base
}
cyc20_validates <- function(p, mode = "strict") {
  tryCatch({ suppressMessages(validate_urps_gap_projection(p, mode = mode)); TRUE },
           error = function(e) conditionMessage(e))
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: a degenerate interval is legal and an inverted one is not", {
  # lo == hi is a point interval -- legitimate for a deterministic run and for a
  # quantity with no spread. One unit the other way is not a narrow interval, it
  # is a swapped one.
  eq <- cyc20_projection(lower_95 = c(1000, 1010), upper_95 = c(1000, 1010))
  expect_true(isTRUE(cyc20_validates(eq)))

  inv <- cyc20_projection(lower_95 = c(1000, 1010), upper_95 = c(1000 - 1e-9, 1010))
  expect_match(cyc20_validates(inv), "inverted")

  wide <- cyc20_projection(lower_95 = c(900, 910), upper_95 = c(1100, 1110))
  expect_true(isTRUE(cyc20_validates(wide)))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a non-finite bound is a malformed interval, not a wide one", {
  # NA on one side used to pass because the required-column completeness guard
  # (cycle 03) only covers the six REQUIRED numeric columns; every bound column
  # is OPTIONAL and was checked by nothing.
  # All three are refused. WHICH guard owns each is itself a contract, added in
  # cycle 21: NA means "undefined, and we said so" and only the interval guard
  # rejects it (an interval with an unknown end is not an interval); Inf and NaN
  # mean arithmetic escaped, and the Inf/NaN guard names that cause first.
  for (bad in list(c(NA_real_, 1010), c(Inf, 1010), c(NaN, 1010))) {
    p <- cyc20_projection(lower_95 = bad, upper_95 = c(1100, 1110))
    err <- cyc20_validates(p)
    expect_type(err, "character")          # refused in strict mode, whichever guard
    expect_match(err, if (is.na(bad[1]) && !is.nan(bad[1])) "non-finite bound" else "Inf/NaN",
                 info = paste("lower_95 =", format(bad[1])))
  }
  # And a fully finite pair passes, so the guard is about the bound and not
  # about the column existing.
  ok <- cyc20_projection(lower_95 = c(900, 910), upper_95 = c(1100, 1110))
  expect_true(isTRUE(cyc20_validates(ok)))
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a projection carrying no interval at all is still valid", {
  # Every bound column is OPTIONAL. A guard that required them would break the
  # deterministic path, which reports a point estimate and no band.
  bare <- cyc20_projection()
  expect_true(isTRUE(cyc20_validates(bare)))
  expect_equal(length(.interval_pairs_in(names(bare))), 0L)

  # One half of a pair present without the other is also not an interval, and
  # must not be treated as one.
  half <- cyc20_projection(lower_95 = c(900, 910))
  expect_true(isTRUE(cyc20_validates(half)))
  expect_equal(length(.interval_pairs_in(names(half))), 0L)
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: every bound pair in the contract is discovered, not a hardcoded list", {
  # OPTIONAL_COLS carries ten bound columns. A guard naming them explicitly would
  # miss the eleventh -- and the eleventh is exactly the one a future change adds
  # without remembering the validator.
  nm <- c("gap_fte_lo", "gap_fte_hi", "gap_pct_lo", "gap_pct_hi",
          "demand_headcount_lo", "demand_headcount_hi",
          "demand_clinical_fte_lo", "demand_clinical_fte_hi",
          "gap_headcount_lo", "gap_headcount_hi", "lower_95", "upper_95",
          "something_new_lo", "something_new_hi")
  found <- .interval_pairs_in(nm)
  expect_equal(length(found), 7L)
  flat <- vapply(found, paste, character(1), collapse = "/")
  expect_true("lower_95/upper_95" %in% flat)
  expect_true("something_new_lo/something_new_hi" %in% flat)
  expect_true("gap_pct_lo/gap_pct_hi" %in% flat)
  # A lone suffix is not a pair.
  expect_equal(length(.interval_pairs_in(c("gap_fte_lo", "unrelated"))), 0L)
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: an inverted interval is refused wherever it appears, not only on lower_95", {
  # Ten pairs, one rule. A guard applied to the headline band only would leave
  # the nine columns a reader actually plots.
  for (pair in list(c("gap_fte_lo", "gap_fte_hi"),
                    c("gap_pct_lo", "gap_pct_hi"),
                    c("demand_clinical_fte_lo", "demand_clinical_fte_hi"),
                    c("demand_headcount_lo", "demand_headcount_hi"))) {
    args <- list(c(500, 500), c(100, 100))
    names(args) <- pair
    p <- do.call(cyc20_projection, args)
    expect_match(cyc20_validates(p), "inverted", info = paste(pair, collapse = "/"))
  }
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: strict refuses and relaxed says so, matching the file's other guards", {
  # Every other check in this validator is strict-errors / relaxed-warns. A new
  # guard that stopped in both modes would break exploratory runs; one that only
  # warned in both would not gate a publication run.
  p <- cyc20_projection(lower_95 = c(1400, 1410), upper_95 = c(1000, 1010))
  expect_match(cyc20_validates(p, mode = "strict"), "inverted")
  expect_message(validate_urps_gap_projection(p, mode = "relaxed"), "inverted")
  # Relaxed still returns the frame, so a caller can inspect what it built.
  out <- suppressMessages(validate_urps_gap_projection(p, mode = "relaxed"))
  expect_equal(nrow(out), 2L)
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the scorecard refuses to score an interval it cannot interpret", {
  # THE DOWNSTREAM CONSEQUENCE. Unguarded, forecast_scorecard() reported
  # coverage 0 and mean_width -400 for a swapped interval. A negative width is
  # not a diagnostic, and the zero coverage reads as "the model never covers the
  # truth" when the bounds are simply the wrong way round.
  d <- data.frame(y = c(1300, 1310, 1320), yhat = c(1290, 1300, 1330),
                  lo = c(1400, 1410, 1420), hi = c(1000, 1010, 1020))
  expect_error(forecast_scorecard(d, observed = "y", point = "yhat",
                                  lower = "lo", upper = "hi"),
               "inverted interval|lo > hi")

  # Correctly ordered, it scores, and the width is positive.
  ok <- d; ok$lo <- c(1200, 1210, 1220); ok$hi <- c(1400, 1410, 1420)
  s <- forecast_scorecard(ok, observed = "y", point = "yhat", lower = "lo", upper = "hi")
  expect_gt(s$mean_width, 0)
  expect_gte(s$coverage, 0)
  expect_lte(s$coverage, 1)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a single inverted row among good ones is caught", {
  # A guard checking the first row, or an aggregate like mean(hi - lo) > 0,
  # passes a frame whose damage is one row deep -- and one bad projection year
  # is exactly what a partial join or an off-by-one produces.
  p <- data.frame(
    year = 2025:2030, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = seq(1000, 1050, by = 10),
    supply_clinical_fte = seq(900, 945, by = 9),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = seq(1300, 1350, by = 10),
    demand_clinical_fte = seq(1200, 1250, by = 10),
    gap_fte = seq(900, 945, by = 9) - seq(1200, 1250, by = 10),
    gap_headcount = seq(1000, 1050, by = 10) - seq(1300, 1350, by = 10),
    lower_95 = c(900, 910, 920, 1500, 940, 950),      # row 4 only
    upper_95 = c(1100, 1110, 1120, 1000, 1140, 1150),
    stringsAsFactors = FALSE)
  err <- cyc20_validates(p)
  expect_match(err, "inverted")
  expect_match(err, "1 row")
  # The mean width is POSITIVE across this frame, so an aggregate check would
  # have passed it -- which is why the guard is elementwise.
  expect_gt(mean(p$upper_95 - p$lower_95), 0)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the guard cannot be satisfied by a plausible-looking swap", {
  # The realistic failure is not a garbage value: it is two correct numbers in
  # the wrong columns. Both are finite, both are in range, both are plausible
  # workforce counts, and the only thing wrong is which is which.
  good <- cyc20_projection(lower_95 = c(950, 960), upper_95 = c(1050, 1060))
  expect_true(isTRUE(cyc20_validates(good)))

  swapped <- cyc20_projection(lower_95 = c(1050, 1060), upper_95 = c(950, 960))
  expect_match(cyc20_validates(swapped), "inverted")

  # Both frames carry the same six numbers, so nothing about the VALUES
  # distinguishes them -- only the relation does.
  expect_setequal(c(good$lower_95, good$upper_95),
                  c(swapped$lower_95, swapped$upper_95))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: a well-formed projection is unaffected by the new guard", {
  # A guard that fires on ordinary output gets switched off. Every legitimate
  # shape must stay silent: no bands, one band, all ten bands, and degenerate
  # bands from a deterministic run.
  expect_true(isTRUE(cyc20_validates(cyc20_projection())))
  expect_true(isTRUE(cyc20_validates(
    cyc20_projection(lower_95 = c(900, 910), upper_95 = c(1100, 1110)))))

  all_bands <- cyc20_projection(
    lower_95 = c(900, 910), upper_95 = c(1100, 1110),
    gap_fte_lo = c(-400, -401), gap_fte_hi = c(-200, -201),
    gap_pct_lo = c(-30, -30), gap_pct_hi = c(-20, -20),
    demand_headcount_lo = c(1200, 1210), demand_headcount_hi = c(1400, 1410),
    demand_clinical_fte_lo = c(1100, 1110), demand_clinical_fte_hi = c(1300, 1310),
    gap_headcount_lo = c(-400, -400), gap_headcount_hi = c(-200, -200))
  expect_true(isTRUE(cyc20_validates(all_bands)))
  expect_equal(length(.interval_pairs_in(names(all_bands))), 6L)

  # A deterministic run collapses every band to its point estimate.
  degenerate <- cyc20_projection(
    lower_95 = c(1000, 1010), upper_95 = c(1000, 1010),
    gap_fte_lo = c(-300, -301), gap_fte_hi = c(-300, -301))
  expect_true(isTRUE(cyc20_validates(degenerate)))
})
