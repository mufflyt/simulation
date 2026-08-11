# Diagnostic specificity: guards that reject correctly but do not locate the
# failure.
#
# Cycle 22 fixed the opposite failure -- a guard whose message asserted a CAUSE
# it could not support. This is the mirror: a guard that is right about the
# rejection and leaves the reader without the offending value.
#
# THE SWEEP. 425 stop() guards across 84 files, with each message resolved back
# through its `msg <- sprintf(...)` assembly rather than read off the stop() line:
#
#   413 / 425  (97%)  name the offending value
#   153 / 425  (36%)  offer a concrete correction
#    46 / 425  (11%)  distinguish NA / NaN / Inf
#     8         fail BOTH value-naming and correction
#
# Six of the eight are correct as they stand -- delegated messages, policy
# statements about a missing package, and a guard whose whole diagnosis IS that
# a total is zero. Two were not, and both are here.
#
# The 11% on NA/NaN/Inf is not a deficiency to fix. Most guards do not KNOW
# which of the three they were handed, and cycle 22's rule forbids claiming.

ds_projection <- function(n = 6, ...) {
  b <- data.frame(
    year = seq(2025, length.out = n), scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = seq(1000, by = 10, length.out = n),
    supply_clinical_fte = seq(900, by = 9, length.out = n),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = seq(1300, by = 10, length.out = n),
    demand_clinical_fte = seq(1200, by = 10, length.out = n),
    stringsAsFactors = FALSE)
  b$gap_fte <- b$supply_clinical_fte - b$demand_clinical_fte
  b$gap_headcount <- b$supply_headcount - b$demand_headcount
  e <- list(...); for (nm in names(e)) b[[nm]] <- e[[nm]]
  b
}
ds_err <- function(p, mode = "strict") {
  tryCatch({ suppressMessages(validate_urps_gap_projection(p, mode = mode)); NA_character_ },
           error = function(e) conditionMessage(e))
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the message locates a single breaching row in a long frame", {
  # THE DEFECT. The guard computes `residual` and reported none of it, so a
  # 26-year multi-scenario frame produced "gap_fte does not equal
  # supply_clinical_fte - demand_clinical_fte (tolerance 0.01 FTE)." and nothing
  # locating it.
  p <- ds_projection(26)
  p$gap_fte[19] <- p$gap_fte[19] + 3.7
  err <- ds_err(p)
  expect_match(err, "gap_fte does not equal")     # (1) what failed
  expect_match(err, "2043")                       # (2) where -- year 2025 + 18
  expect_match(err, "3\\.7")                      # (2) by how much
  expect_match(err, "1 of 26 row")                # (2) how widespread
  expect_match(err, "0\\.01")                     # the tolerance it breached
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the worst row is reported, not the first", {
  # With several breaches the reader needs the largest, because that is the one
  # that decides whether this is rounding or a wrong column.
  p <- ds_projection(10)
  p$gap_fte[2] <- p$gap_fte[2] + 0.5
  p$gap_fte[7] <- p$gap_fte[7] + 9.25          # the worst, and not the first
  err <- ds_err(p)
  expect_match(err, "9\\.25")
  expect_match(err, "2031")                    # 2025 + 6
  expect_match(err, "2 of 10 row")
  expect_false(grepl("2026", err))             # the smaller breach is not the headline
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a breach exactly at the tolerance is not reported, one above is", {
  # The message must not fire on the boundary the guard itself admits.
  at <- ds_projection(4); at$gap_fte[2] <- at$gap_fte[2] + GAP_IDENTITY_TOLERANCE_FTE
  expect_true(is.na(ds_err(at)))
  over <- ds_projection(4); over$gap_fte[2] <- over$gap_fte[2] + GAP_IDENTITY_TOLERANCE_FTE * 1.5
  expect_match(ds_err(over), "1 of 4 row")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the headcount guard is as specific as the FTE one", {
  # One rule, two columns. A guard that located the failure for FTE and not for
  # headcount would send a reader to the wrong half of the frame.
  p <- ds_projection(8)
  p$gap_headcount[5] <- p$gap_headcount[5] - 4
  err <- ds_err(p)
  expect_match(err, "gap_headcount does not equal")
  expect_match(err, "2029")
  expect_match(err, "4\\.0000")
  expect_match(err, "1 of 8 row")
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the message states WHAT and does not speculate about WHY", {
  # Cycle 22's rule, applied to a message this cycle made longer. A residual can
  # come from rounding, a partial join, or a column assembled from the wrong
  # scenario, and this guard cannot tell which. Adding a plausible cause would
  # repeat exactly the defect cycle 22 fixed.
  p <- ds_projection(6); p$gap_fte[3] <- p$gap_fte[3] + 2
  err <- ds_err(p)
  expect_match(err, "does not equal")
  for (speculation in c("usual cause", "demand series", "rounding", "because",
                        "probably", "likely")) {
    expect_false(grepl(speculation, err, fixed = TRUE),
                 info = paste("message speculates:", speculation))
  }
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: both implementations of the identity are diagnostically equal", {
  # validation_report() checks the same identity and has always reported
  # "max residual %.4f FTE". The projection validator did not. One rule with two
  # implementations of differing quality is cycle 13's class, in the message
  # rather than the tolerance.
  p <- ds_projection(6); p$gap_fte[3] <- p$gap_fte[3] + 2.5
  supply <- tibble::tibble(year = p$year, effective_fte_median = p$supply_clinical_fte)
  rep <- suppressMessages(validation_report(supply, gap_projection = p))
  detail <- rep$detail[rep$check == "gap_projection_arithmetic"]
  expect_match(detail, "2\\.5")                    # names the magnitude
  expect_match(ds_err(p), "2\\.5")                 # and so does the validator
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: a frame with no year or scenario still gets located", {
  # The locator degrades to a row index rather than dropping the location. A
  # message that silently omits WHERE on an unusual frame is the failure this
  # cycle is fixing, one layer down.
  p <- ds_projection(5)
  p$year <- NULL; p$scenario_id <- NULL
  p$gap_fte[4] <- p$gap_fte[4] + 6
  err <- tryCatch(suppressMessages(validate_urps_gap_projection(p, mode = "relaxed")),
                  error = function(e) conditionMessage(e))
  msgs <- capture_messages(validate_urps_gap_projection(p, mode = "relaxed"))
  joined <- paste(msgs, collapse = " ")
  expect_match(joined, "row 4")
  expect_match(joined, "6\\.0000")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: every guard is still reachable by an input that trips it alone", {
  # Cycle 22's property, re-asserted because this cycle edited one of the six.
  # A message change that reordered or merged guards would show up here.
  reach <- list(
    schema     = list(p = ds_projection()[, setdiff(names(ds_projection()), "gap_fte")],
                      pat = "missing required column"),
    provenance = list(p = ds_projection(supply_cohort_basis = "undeclared"),
                      pat = "undeclared"),
    range      = list(p = ds_projection(supply_observed_share = 1.7), pat = "supply_observed_share"),
    escape     = list(p = ds_projection(gap_pct = Inf), pat = "Inf/NaN"),
    interval   = list(p = ds_projection(lower_95 = 1400, upper_95 = 1000), pat = "inverted"),
    arithmetic = list(p = {q <- ds_projection(); q$gap_fte[2] <- q$gap_fte[2] + 5; q},
                      pat = "does not equal")
  )
  for (nm in names(reach)) {
    expect_match(ds_err(reach[[nm]]$p), reach[[nm]]$pat, info = paste("guard:", nm))
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: iterative correction still makes forward progress", {
  # Cycle 22's other property. A more informative message must not become a
  # repeating one: fix what you are told, re-run, and meet the next thing.
  p <- ds_projection(6, supply_cohort_basis = "undeclared", supply_observed_share = 1.7,
                     lower_95 = 1400, upper_95 = 1000)
  p$gap_fte[3] <- p$gap_fte[3] + 5

  seen <- character(0)
  for (step in 1:4) {
    m <- ds_err(p)
    expect_false(is.na(m), info = paste("step", step))
    expect_false(m %in% seen, info = paste("repeated diagnosis at step", step))
    seen <- c(seen, m)
    if (grepl("undeclared", m)) p$supply_cohort_basis <- "certification_cohorts"
    else if (grepl("supply_observed_share", m)) p$supply_observed_share <- 0.5
    else if (grepl("inverted", m)) p$upper_95 <- 1500
    else if (grepl("does not equal", m)) p$gap_fte[3] <- p$gap_fte[3] - 5
  }
  expect_equal(length(seen), 4L)
  expect_true(is.na(ds_err(p)))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the located value is the residual, not the gap itself", {
  # The number a reader needs is how far the identity is off, not what the
  # column happens to contain. Reporting the gap would look informative and
  # send them to the wrong quantity.
  p <- ds_projection(4)
  p$gap_fte[2] <- p$gap_fte[2] + 1.25
  err <- ds_err(p)
  expect_match(err, "1\\.2500")                       # the residual
  expect_false(grepl(format(p$gap_fte[2]), err, fixed = TRUE))   # not the gap value
  expect_false(grepl("-29", err))                      # nor the underlying columns
})
