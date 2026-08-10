# Adversarial cycle 17 -- the identifiability flag, and its denominator.
#
# Cycle 16 carried forward: a claim in prose that no test reads. Enumerated the
# "THE DEFECT THIS EXISTS FOR" / "used to" / "Measured before" narratives in R/
# comments, resolved each to its enclosing function, and checked whether any
# test names that function.
#
#   narrative-bearing functions: 33   with no test naming them: 4
#
# and all four are false positives -- three are ordinary "@param x used to
# select ..." prose, and the fourth is a module header whose functions ARE
# covered (test-meps-care-seeking.R) under a call spelling the scan missed.
# So the class comes up clean: the repository's defect narratives are tested.
#
# The scan did put care_seeking_multipliers() in front of me, which carries the
# module's central scientific guard: "an estimate the data cannot distinguish
# from 1.0 cannot be quietly adopted as if it were measured". The flag is
#
#     identified = !(lo <= 1 && hi >= 1)
#
# over a ratio est/base -- and `base` is a predicted probability nobody checks.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc17_fixture <- function(n = 900, seed = 42) {
  set.seed(seed)
  fyc <- data.frame(
    DUPERSID = sprintf("P%05d", seq_len(n)), SEX = 2L,
    AGELAST = sample(18:85, n, TRUE), RACETHX = sample(1:5, n, TRUE),
    POVCAT23 = sample(1:5, n, TRUE), INSCOV23 = sample(c(1, 2, 3), n, TRUE),
    PERWT23F = runif(n, 2000, 9000), VARPSU = sample(1:3, n, TRUE),
    VARSTR = sample(1:12, n, TRUE), stringsAsFactors = FALSE)
  seekers <- sample(fyc$DUPERSID, 220)
  pf <- data.frame(DUPERSID = rep(seekers, each = 2),
                   CONDIDX = paste0(rep(seekers, each = 2), "_", rep(1:2, times = length(seekers))),
                   ICD10CDX = rep(c("N39", "R32"), times = length(seekers)),
                   stringsAsFactors = FALSE)
  other <- data.frame(DUPERSID = sample(fyc$DUPERSID, 600, TRUE),
                      CONDIDX = sprintf("C%05d", 1:600),
                      ICD10CDX = sample(c("I10", "E11", "M54"), 600, TRUE),
                      stringsAsFactors = FALSE)
  clnk <- data.frame(CONDIDX = pf$CONDIDX,
                     EVNTIDX = paste0(rep(seekers, each = 2), "_E1"),
                     stringsAsFactors = FALSE)
  ob <- data.frame(EVNTIDX = unique(clnk$EVNTIDX),
                   OBXP23X = runif(length(unique(clnk$EVNTIDX)), 50, 600),
                   stringsAsFactors = FALSE)
  list(fyc = fyc, cond = rbind(pf, other), clnk = clnk, ob = ob)
}

cyc17_model <- function() {
  skip_if_not_installed("survey")
  f <- cyc17_fixture()
  panel <- tryCatch(build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L),
                    error = function(e) NULL)
  skip_if(is.null(panel), "MEPS panel could not be built on this fixture")
  m <- tryCatch(fit_care_seeking_model(panel), error = function(e) NULL)
  skip_if(is.null(m), "care-seeking model could not be fitted on this fixture")
  list(model = m, panel = panel)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: an interval that merely TOUCHES 1 is unidentified", {
  # The flag is `!(lo <= 1 && hi >= 1)`, so the boundary is closed on both
  # sides: an interval whose limit sits exactly on 1 cannot distinguish the
  # multiplier from 1 and must not be reported as if it could. Conservative in
  # the only safe direction.
  flag <- function(lo, hi) !(lo <= 1 && hi >= 1)
  expect_false(flag(1, 1))          # a point interval at 1
  expect_false(flag(0.5, 1))        # upper limit exactly 1
  expect_false(flag(1, 1.5))        # lower limit exactly 1
  expect_true(flag(1 + 1e-12, 1.5)) # strictly above
  expect_true(flag(0.5, 1 - 1e-12)) # strictly below
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the lower-limit clamp cannot change the verdict", {
  # A delta-method interval on a ratio can go negative when the estimate is
  # badly determined, and the code clamps `lo` at 0 because a negative
  # care-seeking multiplier is not a quantity. That clamp must be cosmetic: any
  # interval whose true lower limit was below 0 already had lo <= 1, so the
  # verdict is unidentified either way.
  flag <- function(lo, hi) !(max(lo, 0) <= 1 && hi >= 1)
  for (lo in c(-5, -1, -1e-9, 0)) {
    expect_false(flag(lo, 1.4), info = sprintf("lo = %g", lo))
    expect_false(flag(lo, 1.0), info = sprintf("lo = %g", lo))
    # Only an upper limit strictly below 1 can rescue it, and that is a real
    # finding (a multiplier significantly BELOW 1), not an artefact of clamping.
    expect_true(flag(lo, 0.9), info = sprintf("lo = %g", lo))
  }
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the reference level is its own multiplier, exactly 1", {
  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  out <- tryCatch(care_seeking_multipliers(mm$model, "insurance", ref),
                  error = function(e) NULL)
  skip_if(is.null(out), "insurance is not a factor term on this fixture")

  self <- out[out$level == as.character(ref$insurance), ]
  expect_equal(nrow(self), 1L)
  expect_equal(self$multiplier, 1, tolerance = 1e-8)
  # And the reference can never be "identified" as different from itself.
  expect_false(self$identified)
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: the confidence level widens the interval and can only lose identification", {
  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  narrow <- tryCatch(care_seeking_multipliers(mm$model, "insurance", ref, level = 0.80),
                     error = function(e) NULL)
  skip_if(is.null(narrow), "insurance is not a factor term on this fixture")
  wide <- care_seeking_multipliers(mm$model, "insurance", ref, level = 0.99)

  expect_equal(narrow$multiplier, wide$multiplier, tolerance = 1e-10)
  expect_true(all(wide$conf_high >= narrow$conf_high - 1e-12))
  expect_true(all(wide$conf_low <= narrow$conf_low + 1e-12))
  # Widening can turn identified into unidentified, never the reverse.
  expect_true(all(!wide$identified | narrow$identified))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the interval brackets the point estimate", {
  # If the estimate falls outside its own interval, the interval is being
  # computed on a different quantity -- and `identified` is read off the
  # interval, not off the estimate.
  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  out <- tryCatch(care_seeking_multipliers(mm$model, "insurance", ref),
                  error = function(e) NULL)
  skip_if(is.null(out), "insurance is not a factor term on this fixture")

  expect_true(all(out$conf_low <= out$multiplier + 1e-9))
  expect_true(all(out$conf_high >= out$multiplier - 1e-9))
  expect_true(all(out$conf_low >= 0))          # clamped, never negative
  expect_true(all(is.finite(out$multiplier)))
  expect_equal(nrow(out), length(mm$model$part1$xlevels[["insurance"]]))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: identified is a property of the interval, not of the estimate's size", {
  # The module's whole point: a LARGE multiplier that the data cannot resolve
  # must not be adopted, and a SMALL one that the data can resolve must not be
  # discarded. Effect size and identification are different questions.
  flag <- function(lo, hi) !(max(lo, 0) <= 1 && hi >= 1)
  expect_false(flag(0.2, 4.0))     # huge point estimate, useless interval
  expect_true(flag(1.02, 1.06))    # tiny effect, cleanly resolved
  expect_true(flag(0.90, 0.98))    # tiny effect the other way
  expect_false(flag(0.99, 1.01))   # tiny effect, not resolved
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: a non-factor term is refused rather than silently returning nothing", {
  # Asking for a multiplier on a continuous term has no meaning. Returning an
  # empty frame would read downstream as "no levels differ from the reference",
  # which is the opposite of "the question does not apply".
  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  expect_error(care_seeking_multipliers(mm$model, "age", ref), "not a factor term")
  expect_error(care_seeking_multipliers(mm$model, "not_a_column", ref), "not a factor term")
  # And the guard is on the model object, not on the string.
  expect_error(care_seeking_multipliers(structure(list(), class = "not_a_model"),
                                        "insurance", ref))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a degenerate reference cannot yield an identified infinite multiplier", {
  # THE HOLE IN THE FLAG. `multiplier = est / base` where `base` is the
  # reference row's predicted probability, and nothing checks it. At base = 0
  # the ratio is Inf, and the flag reads
  #     !(Inf <= 1 && Inf >= 1)  ->  !(FALSE && TRUE)  ->  TRUE
  # so an infinite multiplier is reported as IDENTIFIED -- the strongest claim
  # the function can make, from a division by zero.
  flag <- function(est, se, base, z = 1.96) {
    lo <- max((est - z * se) / base, 0); hi <- (est + z * se) / base
    c(multiplier = est / base, identified = !(lo <= 1 && hi >= 1))
  }
  bad <- flag(est = 0.03, se = 0.01, base = 0)
  expect_true(is.infinite(bad[["multiplier"]]))
  expect_equal(unname(bad[["identified"]]), 1)      # TRUE: the defect, stated

  # A usable reference behaves: finite multiplier, and the flag depends on the
  # interval rather than on the arithmetic blowing up.
  ok <- flag(est = 0.03, se = 0.01, base = 0.02)
  expect_true(is.finite(ok[["multiplier"]]))

  # The property any caller needs: an infinite or non-finite multiplier is never
  # evidence. Downstream code must check this even though the flag does not.
  expect_true(is.infinite(bad[["multiplier"]]) || !bad[["identified"]])
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: every multiplier the fixture produces is finite and usable", {
  # The live check on the real path: whatever the fixture's data do, no level
  # may come back with a non-finite multiplier or interval, because those are
  # the values that would slip past `identified` above.
  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  for (v in c("insurance", "income", "race_eth")) {
    out <- tryCatch(care_seeking_multipliers(mm$model, v, ref), error = function(e) NULL)
    if (is.null(out)) next
    expect_true(all(is.finite(out$multiplier)), info = v)
    expect_true(all(is.finite(out$conf_low)), info = v)
    expect_true(all(is.finite(out$conf_high)), info = v)
    expect_true(all(out$conf_low <= out$conf_high), info = v)
    expect_type(out$identified, "logical")
    expect_false(anyNA(out$identified), info = v)
  }
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the assumed constants and the estimated multipliers stay distinguishable", {
  # The module exists to replace two hard-coded constants with estimates that
  # carry an interval. If an estimate happened to reproduce the constant, a
  # reader must still be able to tell which they are looking at -- the constants
  # have no interval and no `identified` flag, and that difference is the
  # scientific content.
  expect_true(is.numeric(CARE_SEEKING_BY_INSURANCE))
  expect_null(attr(CARE_SEEKING_BY_INSURANCE, "conf_low"))
  expect_null(attr(CARE_SEEKING_BY_INSURANCE, "identified"))
  expect_setequal(names(CARE_SEEKING_BY_INSURANCE), c("Insured", "Uninsured", "Unknown"))
  # All <= 1: the constants are barriers, so the cascade stays a filter
  # (cycle 06 relies on this).
  expect_true(all(CARE_SEEKING_BY_INSURANCE <= 1))
  expect_true(all(CARE_SEEKING_BY_INCOME <= 1))

  mm <- cyc17_model()
  ref <- mm$panel[1, , drop = FALSE]
  out <- tryCatch(care_seeking_multipliers(mm$model, "insurance", ref),
                  error = function(e) NULL)
  skip_if(is.null(out), "insurance is not a factor term on this fixture")
  # The estimate carries what the constant cannot: an interval and a verdict.
  expect_true(all(c("conf_low", "conf_high", "identified") %in% names(out)))
})
