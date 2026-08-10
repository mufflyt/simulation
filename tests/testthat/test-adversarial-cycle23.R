# Adversarial cycle 23 -- guards nobody was checking.
#
# The post-cycle-22 audit reverted all 31 of this ledger's fixes and found one
# unpinned (D12). That method is more useful than the finding, so this cycle
# turned it on guards the ledger did NOT write: sixteen pre-existing stop()
# guards, each reverted in an isolated worktree with its test file run.
#
#   422 stop() guards in R/, across 243 functions
#   167 (40%) interpolate the offending value into the message
#   14 of 16 mutations killed; 2 SURVIVED
#
# The two survivors:
#
#   weighted_interval_score()'s length guard -- which cycle 02 cited as THE
#     in-repo precedent for the whole silent-recycling rule. The precedent was
#     itself untested.
#   validate_participation_table()'s sum-to-1 guard -- and reverting it was not
#     even necessary, because it was already passing vacuously.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc23_q <- function(n = 3) {
  matrix(c(1200, 1300, 1400, 1250, 1350, 1450, 1100, 1200, 1300),
         nrow = 3, byrow = TRUE)[seq_len(n), , drop = FALSE]
}
cyc23_lv <- c(0.25, 0.5, 0.75)
cyc23_part <- function(...) {
  b <- data.frame(age = c(40, 41), sex = c("female", "female"),
                  p_full = c(0.6, 0.6), p_part = c(0.3, 0.3), p_none = c(0.1, 0.1),
                  stringsAsFactors = FALSE)
  e <- list(...); for (n in names(e)) b[[n]] <- e[[n]]; b
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the interval score accepts exactly length 1 or nrow, and nothing between", {
  # The guard cycle 02 cited as the codebase's own standard for refusing partial
  # recycling -- and which the audit found no test exercised. Pinned at both
  # admissible lengths and on each side of them.
  q <- cyc23_q(3)
  expect_length(weighted_interval_score(1300, q, cyc23_lv), 3L)
  expect_length(weighted_interval_score(c(1300, 1310, 1320), q, cyc23_lv), 3L)
  expect_error(weighted_interval_score(c(1300, 1310), q, cyc23_lv), "must be 1 or")
  expect_error(weighted_interval_score(rep(1300, 4), q, cyc23_lv), "must be 1 or")
  expect_error(weighted_interval_score(numeric(0), q, cyc23_lv), "must be 1 or")
  # The message names the arithmetic it is preventing, not just the lengths.
  expect_error(weighted_interval_score(c(1300, 1310), q, cyc23_lv), "partial-recycle")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a single-row quantile matrix is the degenerate case", {
  # nrow == 1 is where "length 1" and "length nrow" coincide, so a guard written
  # with the wrong comparison passes here and fails nowhere else.
  q1 <- cyc23_q(1)
  expect_length(weighted_interval_score(1300, q1, cyc23_lv), 1L)
  expect_error(weighted_interval_score(c(1300, 1310), q1, cyc23_lv), "must be 1 or")
  # A bare vector is promoted to one row rather than scored elementwise.
  expect_length(weighted_interval_score(1300, c(1200, 1300, 1400), cyc23_lv), 1L)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the participation table is refused when empty and accepted at one row", {
  # Zero rows is the other way every check in that function reduces to
  # any(logical(0)) -- true of an empty frame as surely as of a missing column.
  expect_error(validate_participation_table(cyc23_part()[0, ]), "no rows")
  expect_silent(validate_participation_table(cyc23_part()[1, ]))
  expect_silent(validate_participation_table(cyc23_part()))
  # And the shipped table still validates, which is what makes the guard usable.
  expect_silent(validate_participation_table())
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: a participation table missing a column is refused, not silently valid", {
  # THE DEFECT. `table$p_full + table$p_part + table$p_none` with any one column
  # absent is `x + y + NULL`, which is numeric(0). any(logical(0)) is FALSE, so
  # BOTH the sum-to-1 check and the non-negativity check passed vacuously and
  # the function returned a table missing a required column as VALID.
  for (drop in c("p_full", "p_part", "p_none")) {
    t <- cyc23_part(); t[[drop]] <- NULL
    err <- tryCatch(validate_participation_table(t), error = function(e) conditionMessage(e))
    expect_type(err, "character")
    expect_match(err, drop, info = paste("dropped", drop))
    expect_match(err, "any\\(logical\\(0\\)\\)")   # the message names WHY, not just what
  }
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: with every column present, the real checks still fire", {
  # The fix must not have replaced a vacuous check with an unreachable one: the
  # sum and non-negativity guards have to still be doing their job.
  expect_error(validate_participation_table(cyc23_part(p_none = c(0.5, 0.1))),
               "sum to 1")
  expect_error(validate_participation_table(cyc23_part(p_part = c(-0.3, 0.3),
                                                       p_none = c(0.7, 0.1))),
               "negative probability")
  # A row summing to 1 within tolerance passes; outside it does not.
  expect_silent(validate_participation_table(cyc23_part(p_none = c(0.1 + 5e-7, 0.1))))
  expect_error(validate_participation_table(cyc23_part(p_none = c(0.1 + 5e-6, 0.1))),
               "sum to 1")
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the four sibling validators already refuse a missing column", {
  # The sweep's result, pinned. validate_participation_table() was the ONLY one
  # of five that failed vacuously; the others check presence before arithmetic.
  # Recorded so a later "simplification" cannot quietly bring them into line
  # with the broken one.
  expect_error(validate_cpt_basket(data.frame(service = "s", hcpcs = "x")))
  expect_error(validate_setting_mix(data.frame(service = "s", setting = "ambulatory")))
  expect_error(validate_delegation_matrix(data.frame(service = "s")),
               "no share columns")
  expect_error(validate_migration_matrix(data.frame(origin = "A", destination = "B")))
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the interval score is a proper score with the properties it claims", {
  # Pinning the guard is not the same as pinning the quantity. A score that
  # accepts the right shapes and computes the wrong number is no better.
  q <- cyc23_q(3)
  perfect <- weighted_interval_score(c(1300, 1350, 1200), q, cyc23_lv)
  poor <- weighted_interval_score(c(2000, 2000, 2000), q, cyc23_lv)
  expect_true(all(poor > perfect))            # worse forecasts score worse
  expect_true(all(perfect >= 0))              # a score is non-negative
  expect_true(all(is.finite(perfect)))

  # An observation at the median scores better than one outside the interval.
  at_median <- weighted_interval_score(1300, cyc23_q(1), cyc23_lv)
  outside <- weighted_interval_score(1600, cyc23_q(1), cyc23_lv)
  expect_lt(at_median, outside)

  # The median is required, because without it the score is not this score.
  expect_error(weighted_interval_score(1300, cyc23_q(1), c(0.25, 0.4, 0.75)),
               "must include the median")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no participation-table check can be satisfied by absence", {
  # The general form of the defect: every check in that function reduces to
  # any(...) over a vector, and a vector can be emptied two ways -- a missing
  # column, or no rows. Both must be refused before any check runs.
  empties <- list(
    "no rows"          = cyc23_part()[0, ],
    "no p_none"        = { t <- cyc23_part(); t$p_none <- NULL; t },
    "no p_full"        = { t <- cyc23_part(); t$p_full <- NULL; t },
    "no rows, no col"  = { t <- cyc23_part()[0, ]; t$p_none <- NULL; t }
  )
  for (nm in names(empties)) {
    expect_error(validate_participation_table(empties[[nm]]), info = nm)
  }
  # And the shipped table -- the one thing that must keep working -- does.
  expect_silent(validate_participation_table(FUTUREDOCS_PARTICIPATION))
  expect_gt(nrow(FUTUREDOCS_PARTICIPATION), 0L)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: participation_fte reads a table the validator would accept", {
  # The consumer's view. participation_fte() uses p_full and p_part and never
  # touches p_none, so a table missing p_none 'worked' -- which is exactly why
  # the vacuous validator went unnoticed. The validator's claim must match what
  # the table is actually required to contain.
  expect_silent(validate_participation_table(FUTUREDOCS_PARTICIPATION))
  v <- participation_fte(c(35, 50, 70), "female")
  expect_true(all(is.finite(v)))
  expect_true(all(v >= 0 & v <= 1))

  # A table the validator refuses must not be silently usable either: the point
  # is that the two agree about what a valid table is.
  broken <- FUTUREDOCS_PARTICIPATION
  broken$p_none <- NULL
  expect_error(validate_participation_table(broken), "p_none")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the recycling precedent refuses what the rule it established refuses", {
  # weighted_interval_score() is the function cycle 02 cited when generalising
  # the no-partial-recycling rule into .recycle_aligned(). The two must agree,
  # or the precedent no longer supports the rule drawn from it.
  q <- cyc23_q(3)
  expect_error(weighted_interval_score(c(1300, 1310), q, cyc23_lv), "must be 1 or")
  expect_error(.recycle_aligned(c(1300, 1310), 3, "y"), "length 2 but must be length 1 or 3")

  # Both accept the same two admissible shapes.
  expect_length(weighted_interval_score(1300, q, cyc23_lv), 3L)
  expect_length(.recycle_aligned(1300, 3, "y"), 3L)
  expect_length(weighted_interval_score(c(1, 2, 3) * 1000, q, cyc23_lv), 3L)
  expect_length(.recycle_aligned(c(1, 2, 3), 3, "y"), 3L)
})
