# Adversarial cycle 19 -- the numerator nobody validated.
#
# Cycle 18 carried forward: denominators that arrive as inputs to one module but
# are computed by another -- the boundary where "the caller validated it" and
# "the callee validated it" can both be false. Cycle 18 named two suspects.
#
# One of them was WRONG, and correcting it is where this cycle started. `py`
# (person-years) in the pooled-hazard module is guarded on BOTH sides:
# hazard_pooled_long() filters py > 0 and fit_partial_pooled_hazards() stops on
# a non-positive cell with a message naming the quantity. Cycle 18's ledger note
# is corrected in place.
#
# Looking at the guarded denominator showed what it could not see. `ev` -- the
# event count, the NUMERATOR of every hazard in the module -- was validated by
# neither side. A hazard is a probability, so 0 <= ev <= py, and both bounds
# were unenforced.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc19_wide <- function(ev = c(2, 3), py = c(100, 80), bands = c("<45", "50-54")) {
  data.frame(band = bands, urps_events = ev, urps_py = py, stringsAsFactors = FALSE)
}
cyc19_long <- function(ev = 2, py = 100, band = "<45", sub = "URPS") {
  data.frame(subspecialty = sub, band = band, py = py, ev = ev, stringsAsFactors = FALSE)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: an event count is closed at 0 and at py", {
  # A hazard is a probability. ev == 0 is the observed 70+ cell (0 events over
  # 16 person-years) and must pass; ev == py is total departure in the period
  # and is also legal. Everything outside is not a probability.
  expect_silent(hazard_pooled_long(cyc19_wide(ev = c(0, 3), py = c(16, 80)), "urps"))
  expect_silent(hazard_pooled_long(cyc19_wide(ev = c(80, 3), py = c(80, 80)), "urps"))
  expect_error(hazard_pooled_long(cyc19_wide(ev = c(-1e-9, 3), py = c(16, 80)), "urps"),
               "in \\[0, py\\]")
  expect_error(hazard_pooled_long(cyc19_wide(ev = c(80 + 1e-9, 3), py = c(80, 80)), "urps"),
               "in \\[0, py\\]")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a non-finite event count is refused rather than propagated", {
  for (bad in c(NA_real_, NaN, Inf, -Inf)) {
    expect_error(hazard_pooled_long(cyc19_wide(ev = c(bad, 3)), "urps"),
                 "finite", info = paste("ev =", bad))
  }
  # And the denominator guard that already existed still fires, on its own terms.
  expect_error(fit_partial_pooled_hazards(cyc19_long(ev = 2, py = 0)),
               "person-years")
  expect_error(fit_partial_pooled_hazards(cyc19_long(ev = 2, py = -5)),
               "person-years")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the zero-event cell the module was built for still passes", {
  # The module header records the motivating case: "the 70+ cell is 0 events /
  # 16 person-years". A guard that refused ev = 0 would break the exact data
  # this module exists to pool.
  w <- cyc19_wide(ev = c(0, 5), py = c(16, 120), bands = c("70+", "60-64"))
  out <- hazard_pooled_long(w, "urps")
  expect_equal(nrow(out), 2L)
  expect_equal(out$ev[out$band == "70+"], 0)
  expect_equal(out$ev[out$band == "70+"] / out$py[out$band == "70+"], 0)
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: a zero-person-year cell is dropped, not divided by", {
  # py == 0 is the one case where the ev bound is vacuous (ev must be in [0, 0]),
  # so the ordering matters: the ev check must not reject a cell the py filter
  # was going to drop anyway.
  w <- cyc19_wide(ev = c(0, 3), py = c(0, 80), bands = c("70+", "50-54"))
  out <- hazard_pooled_long(w, "urps")
  expect_equal(nrow(out), 1L)
  expect_equal(out$band, "50-54")
  # A zero-py cell carrying events is contradictory and must NOT slip through
  # on the strength of being dropped.
  expect_error(hazard_pooled_long(cyc19_wide(ev = c(4, 3), py = c(0, 80)), "urps"),
               "in \\[0, py\\]")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the unpooled hazard is a probability for every admissible input", {
  # THE PROPERTY THE NUMERATOR GUARD ENFORCES. ev/py is written into the
  # returned table before any model is fitted, so it is a reported quantity in
  # its own right. Measured before the fix: ev = 25 over py = 10 gave 2.5.
  set.seed(19)
  for (i in 1:40) {
    py <- runif(1, 1, 500); ev <- runif(1, 0, py)
    out <- hazard_pooled_long(cyc19_wide(ev = c(ev, 3), py = c(py, 80)), "urps")
    h <- out$ev[1] / out$py[1]
    expect_true(h >= 0 && h <= 1,
                info = sprintf("ev=%.3f py=%.3f gave hazard %.4f", ev, py, h))
  }
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the failure names this package's quantity, not a third party's internals", {
  # The reason to guard at the boundary rather than let it fail downstream.
  # Unguarded, ev = 25 over py = 10 reached blme as cbind(25, -15) and died with
  # "Value 2.31818 out of range (0, 1)" -- naming neither the column, the band,
  # the subspecialty, nor this package. A reader cannot act on that.
  err <- tryCatch(hazard_pooled_long(cyc19_wide(ev = c(25, 3), py = c(10, 80)), "urps"),
                  error = function(e) conditionMessage(e))
  expect_match(err, "hazard_pooled_long")     # which function
  expect_match(err, "ev")                     # which column
  expect_match(err, "URPS")                   # which subspecialty
  expect_match(err, "<45")                    # which band
  expect_match(err, "ev=25")                  # the offending value
  expect_match(err, "py=10")                  # and what it was measured against
  expect_false(grepl("out of range \\(0, 1\\)", err))   # not blme's message
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: both sides of the boundary enforce the same rule", {
  # One guard in a shared helper, called from the reshape and from the fit --
  # cycle 13 established that two copies is how one starts accepting what the
  # other rejects. Same input, same verdict, whichever door it arrives at.
  bad <- cyc19_long(ev = 25, py = 10)
  expect_error(fit_partial_pooled_hazards(bad), "in \\[0, py\\]")
  expect_error(hazard_pooled_long(cyc19_wide(ev = c(25, 3), py = c(10, 80)), "urps"),
               "in \\[0, py\\]")

  neg <- cyc19_long(ev = -3, py = 10)
  expect_error(fit_partial_pooled_hazards(neg), "in \\[0, py\\]")
  expect_error(hazard_pooled_long(cyc19_wide(ev = c(-3, 3), py = c(10, 80)), "urps"),
               "in \\[0, py\\]")

  # And a table with no py/ev columns at all is another module's problem, not
  # silently "valid" here.
  expect_silent(urpssim:::.assert_event_counts(data.frame(x = 1), "f"))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: the denominator guard cannot see a numerator problem", {
  # The cycle's thesis, stated as a test. py is guarded on both sides and always
  # was; that guard passes every one of these, because none of them is a
  # denominator problem.
  for (ev in c(-5, 25, Inf, NaN)) {
    d <- cyc19_long(ev = ev, py = 10)
    expect_true(all(is.finite(d$py)) && all(d$py > 0),
                info = sprintf("py is fine at ev = %s", format(ev)))
    expect_error(fit_partial_pooled_hazards(d),
                 info = sprintf("ev = %s was accepted", format(ev)))
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a whole table is judged, not just its first row", {
  # A guard that checks row 1 passes any file whose bad cell is anywhere else --
  # and real files are sorted by band, so the sparse cells are last.
  w <- data.frame(band = c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+"),
                  urps_events = c(1, 1, 2, 3, 5, 8, 40),      # only the LAST is bad
                  urps_py = c(200, 180, 160, 140, 120, 53, 16),
                  stringsAsFactors = FALSE)
  expect_error(hazard_pooled_long(w, "urps"), "70\\+")

  # Several bad cells are reported together, not one per run.
  w2 <- w; w2$urps_events[c(1, 7)] <- c(-2, 40)
  err <- tryCatch(hazard_pooled_long(w2, "urps"), error = function(e) conditionMessage(e))
  expect_match(err, "<45")
  expect_match(err, "70\\+")

  # The same table with the sparse cell corrected passes intact.
  w3 <- w; w3$urps_events[7] <- 0
  expect_equal(nrow(hazard_pooled_long(w3, "urps")), 7L)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: a bad cell in one subspecialty is not hidden by good ones", {
  # The reshape stacks several subspecialties. A per-column check that ran only
  # on the first prefix would pass a file whose damage is in the second, and the
  # pooled hazard sums ev and py ACROSS subspecialties -- so one bad cell moves
  # the pooled estimate for that band for everyone.
  w <- data.frame(band = c("<45", "50-54"),
                  go_events = c(2, 3), go_py = c(100, 80),
                  urps_events = c(2, 99), urps_py = c(100, 80),   # bad: 99 > 80
                  stringsAsFactors = FALSE)
  err <- tryCatch(hazard_pooled_long(w, c("go", "urps")), error = function(e) conditionMessage(e))
  expect_match(err, "URPS")
  expect_false(grepl("\\bGO\\b/", err))          # GO's cells are fine and unreported

  # With that cell fixed, the pooled hazard for the band is the summed ratio,
  # and it stays a probability.
  w$urps_events[2] <- 4
  out <- hazard_pooled_long(w, c("go", "urps"))
  by_band <- tapply(seq_len(nrow(out)), out$band,
                    function(i) sum(out$ev[i]) / sum(out$py[i]))
  expect_true(all(by_band >= 0 & by_band <= 1))
  expect_equal(unname(by_band[["50-54"]]), (3 + 4) / (80 + 80))
})
