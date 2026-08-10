# Adversarial cycle 21 -- what a hardcoded column list cannot see.
#
# Cycle 20 carried forward the shape, and it pointed at my own earlier work: the
# completeness guard added in cycle 03 opens with
#
#   na_cols <- intersect(c("supply_headcount", "supply_clinical_fte",
#                          "demand_headcount", "demand_clinical_fte",
#                          "gap_fte", "gap_headcount"), names(x))
#
# Six literal names. Cycle 20 found the ten OPTIONAL bound columns it could not
# see. This cycle asks what is left after that, and the answer is the columns
# that are neither required nor half of a pair.
#
# supply_observed_share is a SHARE and was checked by nothing. At 1.7 it
# validated clean in strict mode and print.urps_gap_projection() rendered
#
#   "170.0% of the base cohort has an observed certification year"
#
# -- the provenance caveat the whole supply_cohort_basis machinery exists to
# carry, stating something the data cannot support.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

cyc21_projection <- function(...) {
  b <- data.frame(
    year = 2025:2026, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = c(1000, 1010), supply_clinical_fte = c(900, 909),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = c(1300, 1310), demand_clinical_fte = c(1200, 1210),
    gap_fte = c(-300, -301), gap_headcount = c(-300, -300),
    stringsAsFactors = FALSE)
  e <- list(...); for (n in names(e)) b[[n]] <- e[[n]]; b
}
cyc21_validates <- function(p, mode = "strict") {
  tryCatch({ suppressMessages(validate_urps_gap_projection(p, mode = mode)); TRUE },
           error = function(e) conditionMessage(e))
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the observed share is closed at 0 and at 1", {
  # A share of the base cohort with an observed certification year. Both
  # endpoints are meaningful -- 0 is a fully reconstructed cohort, 1 is a fully
  # observed one -- and both are the states the caveat exists to distinguish.
  expect_true(isTRUE(cyc21_validates(cyc21_projection(supply_observed_share = c(0, 0)))))
  expect_true(isTRUE(cyc21_validates(cyc21_projection(supply_observed_share = c(1, 1)))))
  expect_match(cyc21_validates(cyc21_projection(supply_observed_share = c(1 + 1e-9, 1))),
               "supply_observed_share")
  expect_match(cyc21_validates(cyc21_projection(supply_observed_share = c(-1e-9, 0))),
               "supply_observed_share")
  # The real cohort sits near 0.5 and must pass untouched.
  expect_true(isTRUE(cyc21_validates(cyc21_projection(supply_observed_share = c(0.498, 0.498)))))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: NA is allowed and Inf/NaN are not, in every numeric column", {
  # The distinction the new guard rests on. Cycle 18 made NA the honest answer
  # for gap_pct when demand is zero -- "undefined, and we said so". Inf and NaN
  # mean a division or a product escaped, and they export as numbers.
  expect_true(isTRUE(cyc21_validates(cyc21_projection(gap_pct = c(NA_real_, NA_real_)))))
  expect_match(cyc21_validates(cyc21_projection(gap_pct = c(Inf, -25))), "Inf/NaN")
  expect_match(cyc21_validates(cyc21_projection(gap_pct = c(NaN, -25))), "Inf/NaN")
  expect_true(isTRUE(cyc21_validates(cyc21_projection(gap_pct = c(-25, -24.9)))))

  # And a REQUIRED column is still refused for NA, by the older guard, because
  # a missing gap is not a gap of zero.
  expect_match(cyc21_validates(cyc21_projection(gap_fte = c(NA_real_, -301))),
               "non-finite values")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a projection carrying none of the optional columns is still valid", {
  # Every column this cycle touches is optional. A guard that required them
  # would break the plain deterministic export.
  bare <- cyc21_projection()
  expect_true(isTRUE(cyc21_validates(bare)))
  expect_false("supply_observed_share" %in% names(bare))
  expect_false("gap_pct" %in% names(bare))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the range table is the scope, so a bounded column is declared not remembered", {
  # The fix for a hardcoded list is not a longer hardcoded list. Ranges live in
  # GAP_PROJECTION_COLUMN_RANGES, so adding a bounded column means adding a row
  # rather than remembering to extend a condition inside the validator.
  expect_true(is.list(GAP_PROJECTION_COLUMN_RANGES))
  expect_true("supply_observed_share" %in% names(GAP_PROJECTION_COLUMN_RANGES))
  expect_equal(GAP_PROJECTION_COLUMN_RANGES$supply_observed_share, c(0, 1))
  # Every declared range is a well-formed interval, which is the cycle-20 rule
  # applied to the table that now declares ranges.
  for (nm in names(GAP_PROJECTION_COLUMN_RANGES)) {
    r <- GAP_PROJECTION_COLUMN_RANGES[[nm]]
    expect_length(r, 2L)
    expect_lt(r[1], r[2])
    expect_true(all(is.finite(r)))
  }
  # And every declared column is actually part of the contract.
  expect_true(all(names(GAP_PROJECTION_COLUMN_RANGES) %in% c(REQUIRED_COLS, OPTIONAL_COLS)))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the non-finite scope is derived from the frame, not from a list", {
  # The property that distinguishes this guard from the one it supplements: a
  # numeric column the contract has never heard of is still covered, because
  # the scope is "every numeric column present".
  p <- cyc21_projection()
  p$some_future_metric <- c(1, Inf)
  expect_match(cyc21_validates(p), "some_future_metric")

  # A non-numeric column is not swept up by it.
  q <- cyc21_projection()
  q$scenario_label <- c("Status quo", "Status quo")
  expect_true(isTRUE(cyc21_validates(q)))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: strict refuses and relaxed reports, for both new guards", {
  # Matching every other guard in this validator. A new check that stopped in
  # both modes would break exploratory work; one that warned in both would not
  # gate a publication run.
  share <- cyc21_projection(supply_observed_share = c(1.7, 1.7))
  expect_match(cyc21_validates(share, mode = "strict"), "supply_observed_share")
  expect_message(validate_urps_gap_projection(share, mode = "relaxed"),
                 "supply_observed_share")

  esc <- cyc21_projection(gap_pct = c(Inf, -25))
  expect_match(cyc21_validates(esc, mode = "strict"), "Inf/NaN")
  expect_message(validate_urps_gap_projection(esc, mode = "relaxed"), "Inf/NaN")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: an impossible share can no longer be printed as a provenance claim", {
  # THE DEFECT, at the point a reader meets it. print.urps_gap_projection()
  # renders 100 * supply_observed_share as the caveat that says how much of the
  # cohort is reconstructed rather than observed. At 1.7 it printed
  # "170.0% of the base cohort has an observed certification year".
  bad <- cyc21_projection(supply_observed_share = c(1.7, 1.7))
  expect_match(cyc21_validates(bad, mode = "strict"), "a claim the data")

  # A share that IS in range still prints, and prints the number it was given.
  ok <- cyc21_projection(supply_observed_share = c(0.498, 0.498))
  expect_true(isTRUE(cyc21_validates(ok, mode = "strict")))
  class(ok) <- c("urps_gap_projection", class(ok))
  out <- capture.output(print(ok))
  line <- grep("observed certification", out, value = TRUE)
  expect_length(line, 1L)
  expect_match(line, "49\\.8%")
  expect_false(any(grepl("1[0-9][0-9]\\.[0-9]%", out)))
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: one bad row among good ones is caught, in every new guard", {
  # An aggregate check -- mean share in range, any() over the frame -- passes a
  # frame whose damage is one row deep, and one bad projection year is what a
  # partial join produces.
  p <- cyc21_projection(supply_observed_share = c(0.498, 1.4))
  err <- cyc21_validates(p)
  expect_match(err, "1 row")
  expect_match(err, "1\\.4")
  # The MEAN share is in range, so an aggregate guard would have passed it.
  expect_lt(mean(c(0.498, 1.4)), 1.0)

  q <- cyc21_projection(gap_pct = c(-25, NaN))
  expect_match(cyc21_validates(q), "gap_pct \\(1\\)")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the three guards cover disjoint failures and none masks another", {
  # Completeness (cycle 03, required columns), intervals (cycle 20, pairs),
  # ranges and Inf/NaN (this cycle). A frame can fail exactly one of them, and
  # the message must name which -- otherwise a reader fixes the wrong thing.
  na_required <- cyc21_projection(gap_fte = c(NA_real_, -301))
  expect_match(cyc21_validates(na_required), "non-finite values")

  inverted <- cyc21_projection(lower_95 = c(1400, 1410), upper_95 = c(1000, 1010))
  expect_match(cyc21_validates(inverted), "inverted")

  out_of_range <- cyc21_projection(supply_observed_share = c(1.7, 1.7))
  expect_match(cyc21_validates(out_of_range), "supply_observed_share")

  escaped <- cyc21_projection(gap_pct = c(Inf, -25))
  expect_match(cyc21_validates(escaped), "Inf/NaN")

  # And a clean frame trips none of them.
  expect_true(isTRUE(cyc21_validates(cyc21_projection(
    supply_observed_share = c(0.498, 0.498), gap_pct = c(-25, -24.9),
    lower_95 = c(900, 910), upper_95 = c(1100, 1110)))))
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: a value that is in range but wrong for its row is still caught by arithmetic", {
  # The limit of a range guard, stated so it is not mistaken for more than it
  # is. supply_observed_share = 0.2 is perfectly in range and may still be the
  # wrong number; nothing here can tell. What the contract CAN check is the
  # arithmetic between columns, and that guard is separate and still live.
  plausible_but_unverifiable <- cyc21_projection(supply_observed_share = c(0.2, 0.9))
  expect_true(isTRUE(cyc21_validates(plausible_but_unverifiable)))

  # Whereas a gap that does not equal supply minus demand is caught, because
  # that relation IS checkable.
  broken <- cyc21_projection(supply_observed_share = c(0.2, 0.9))
  broken$gap_fte[2] <- broken$gap_fte[2] + 5
  expect_match(cyc21_validates(broken), "does not equal")
})
