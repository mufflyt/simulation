# Adversarial cycle 22 -- guard ordering, and the diagnosis a reader is given.
#
# Cycle 21 carried forward: which of several sequential checks speaks first
# decides what a reader is told to fix. Six guards now run in sequence on the
# gap-projection contract, five of them added by this ledger, and nothing had
# ever asked whether an earlier one can swallow a later, more specific one.
#
# It could, and the case was mine. `!is.finite()` is TRUE for NA, NaN AND Inf,
# so the cycle-03 completeness guard answered for all three -- while its message
# makes a causal claim that is right for only one:
#
#   "A MISSING gap is not a gap of zero ... the usual cause is a demand series
#    that does not cover every projection year."
#
# For gap_fte = Inf that is simply false. The demand series covers every year;
# a division escaped. Cycle 21's Inf/NaN guard carries the correct diagnosis and
# could never be reached for a required column.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc22_projection <- function(...) {
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
cyc22_msg <- function(p, mode = "strict") {
  tryCatch({ suppressMessages(validate_urps_gap_projection(p, mode = mode)); NA_character_ },
           error = function(e) conditionMessage(e))
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: NA, NaN and Inf in a required column each get their own diagnosis", {
  # THE DEFECT. All three used to produce the identical "missing gap ... the
  # usual cause is a demand series that does not cover every projection year"
  # message, which is a wrong causal claim for two of them.
  na_msg <- cyc22_msg(cyc22_projection(gap_fte = c(NA_real_, -301)))
  inf_msg <- cyc22_msg(cyc22_projection(gap_fte = c(Inf, -301)))
  nan_msg <- cyc22_msg(cyc22_projection(gap_fte = c(NaN, -301)))

  expect_match(na_msg, "missing gap")
  expect_match(na_msg, "does not cover every projection year")
  expect_match(inf_msg, "Inf/NaN")
  expect_match(inf_msg, "escaped")
  expect_match(nan_msg, "Inf/NaN")

  # And the two diagnoses are genuinely different text, not the same message
  # with a different count.
  expect_false(identical(na_msg, inf_msg))
  expect_false(grepl("does not cover every projection year", inf_msg))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: NaN is not NA for the purposes of the missing-value guard", {
  # is.na(NaN) is TRUE in R, so `is.na(x) & !is.nan(x)` is the only spelling
  # that separates them. Getting this wrong in either direction re-merges the
  # two diagnoses.
  expect_true(is.na(NaN))
  expect_true(is.nan(NaN))
  expect_false(is.nan(NA_real_))
  expect_true(is.na(NA_real_) && !is.nan(NA_real_))
  expect_false(is.na(NaN) && !is.nan(NaN))

  # Which is exactly what the validator now distinguishes.
  expect_match(cyc22_msg(cyc22_projection(gap_fte = c(NA_real_, -301))), "missing gap")
  expect_match(cyc22_msg(cyc22_projection(gap_fte = c(NaN, -301))), "Inf/NaN")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a clean frame reaches the end of every guard", {
  # The other side of ordering: with nothing wrong, no guard fires. If any did,
  # the ordering tests below would be measuring a permanently-failing frame.
  expect_true(is.na(cyc22_msg(cyc22_projection())))
  expect_true(is.na(cyc22_msg(cyc22_projection(
    supply_observed_share = c(0.498, 0.498), gap_pct = c(-25, -24.9),
    lower_95 = c(900, 910), upper_95 = c(1100, 1110)))))
  # Including the boundary values each guard is closed at.
  expect_true(is.na(cyc22_msg(cyc22_projection(
    supply_observed_share = c(0, 1), lower_95 = c(1000, 1010),
    upper_95 = c(1000, 1010)))))
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: an optional column with NA is allowed where a required one is not", {
  # The asymmetry the two guards encode. NA in gap_pct means "undefined, and we
  # said so" (cycle 18). NA in gap_fte means the projection has a hole.
  expect_true(is.na(cyc22_msg(cyc22_projection(gap_pct = c(NA_real_, -25)))))
  expect_match(cyc22_msg(cyc22_projection(gap_fte = c(NA_real_, -301))), "missing gap")
  # But Inf is refused in BOTH, because arithmetic escaping is never allowed.
  expect_match(cyc22_msg(cyc22_projection(gap_pct = c(Inf, -25))), "Inf/NaN")
  expect_match(cyc22_msg(cyc22_projection(gap_fte = c(Inf, -301))), "Inf/NaN")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: every guard is reachable by some input", {
  # A guard no input can reach is dead code that reads as protection. Each of
  # the six is reached here by a frame that trips exactly it.
  reach <- list(
    schema      = list(p = cyc22_projection()[, setdiff(names(cyc22_projection()), "gap_fte")],
                       pat = "missing required column"),
    provenance  = list(p = cyc22_projection(supply_cohort_basis = "undeclared"),
                       pat = "undeclared"),
    range       = list(p = cyc22_projection(supply_observed_share = c(1.7, 1.7)),
                       pat = "supply_observed_share"),
    escape      = list(p = cyc22_projection(gap_pct = c(Inf, -25)),
                       pat = "Inf/NaN"),
    interval    = list(p = cyc22_projection(lower_95 = c(1400, 1410),
                                            upper_95 = c(1000, 1010)),
                       pat = "inverted"),
    arithmetic  = list(p = {q <- cyc22_projection(); q$gap_fte[2] <- q$gap_fte[2] + 5; q},
                       pat = "does not equal")
  )
  for (nm in names(reach)) {
    expect_match(cyc22_msg(reach[[nm]]$p), reach[[nm]]$pat,
                 info = paste("guard:", nm))
  }
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: a structural problem is reported before a derived one", {
  # Ordering is a judgement, and this is the judgement: fix the schema before
  # the arithmetic, because arithmetic computed on a frame missing a column is
  # not a second finding, it is the first one again.
  broken_both <- cyc22_projection()
  broken_both$gap_fte[2] <- broken_both$gap_fte[2] + 5      # arithmetic wrong
  broken_both$supply_cohort_basis <- "undeclared"           # provenance missing
  msg <- cyc22_msg(broken_both)
  expect_match(msg, "undeclared")
  expect_false(grepl("does not equal", msg))

  # And with provenance restored, the arithmetic problem surfaces -- so the
  # second finding was real and merely deferred, not lost.
  broken_both$supply_cohort_basis <- "certification_cohorts"
  expect_match(cyc22_msg(broken_both), "does not equal")
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: relaxed mode reports every problem, not just the first", {
  # Strict stops at the first guard, which is what a gate should do. Relaxed
  # continues, and its value is that a reader sees the whole list in one run
  # instead of fixing one problem per iteration.
  p <- cyc22_projection(supply_observed_share = c(1.7, 1.7),
                        lower_95 = c(1400, 1410), upper_95 = c(1000, 1010))
  p$gap_fte[2] <- p$gap_fte[2] + 5
  msgs <- capture_messages(validate_urps_gap_projection(p, mode = "relaxed"))
  joined <- paste(msgs, collapse = " ")
  expect_match(joined, "supply_observed_share")
  expect_match(joined, "inverted")
  expect_match(joined, "does not equal")
  expect_gte(length(msgs), 3L)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no guard's message makes a causal claim it cannot support", {
  # The property that failed. A message may say WHAT is wrong unconditionally;
  # it may only say WHY when the guard has established the why. The missing-gap
  # guard names a cause ("a demand series that does not cover every projection
  # year"), so it must fire only for the case that cause explains.
  na_msg <- cyc22_msg(cyc22_projection(gap_fte = c(NA_real_, -301)))
  expect_match(na_msg, "demand series")

  # Every other way of producing a non-finite required value must NOT claim it.
  for (v in c(Inf, -Inf, NaN)) {
    m <- cyc22_msg(cyc22_projection(gap_fte = c(v, -301)))
    expect_false(grepl("demand series", m),
                 info = sprintf("gap_fte = %s claimed a demand-coverage cause", format(v)))
    expect_match(m, "escaped", info = format(v))
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: an earlier guard cannot be made to swallow a later one by row order", {
  # A frame whose first row trips the broad guard and whose second trips the
  # narrow one. If the broad guard short-circuits on the frame rather than per
  # cause, the narrow finding disappears with the row that carries it.
  p <- cyc22_projection(gap_pct = c(NA_real_, Inf))
  msg <- cyc22_msg(p)
  expect_match(msg, "Inf/NaN")          # the NA in row 1 does not hide the Inf in row 2

  # Reversed, the same holds.
  q <- cyc22_projection(gap_pct = c(Inf, NA_real_))
  expect_match(cyc22_msg(q), "Inf/NaN")

  # And a required column mixing both causes reports the structural one first,
  # deterministically, regardless of which row carries which.
  r1 <- cyc22_projection(gap_fte = c(NA_real_, Inf))
  r2 <- cyc22_projection(gap_fte = c(Inf, NA_real_))
  expect_match(cyc22_msg(r1), "missing gap")
  expect_match(cyc22_msg(r2), "missing gap")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the ordering is stable, so a reader fixing one problem meets the next", {
  # The iterative property. Fix what you are told, re-run, and you must make
  # progress -- never be told the same thing twice, and never see an earlier
  # problem reappear.
  p <- cyc22_projection(supply_cohort_basis = "undeclared",
                        supply_observed_share = c(1.7, 1.7),
                        lower_95 = c(1400, 1410), upper_95 = c(1000, 1010))
  p$gap_fte[2] <- p$gap_fte[2] + 5

  seen <- character(0)
  for (step in 1:4) {
    m <- cyc22_msg(p)
    expect_false(is.na(m), info = paste("step", step))
    expect_false(m %in% seen, info = paste("repeated diagnosis at step", step))
    seen <- c(seen, m)
    if (grepl("undeclared", m)) p$supply_cohort_basis <- "certification_cohorts"
    else if (grepl("supply_observed_share", m)) p$supply_observed_share <- c(0.5, 0.5)
    else if (grepl("inverted", m)) p$upper_95 <- c(1500, 1510)
    else if (grepl("does not equal", m)) p$gap_fte[2] <- p$gap_fte[2] - 5
  }
  expect_equal(length(seen), 4L)
  expect_true(is.na(cyc22_msg(p)))       # four fixes, and the frame is clean
})
