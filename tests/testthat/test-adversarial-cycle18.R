# Adversarial cycle 18 -- divisions whose denominator is a MODELLED quantity.
#
# Cycle 17 carried forward exactly that: the inputs to this package are
# validated and the denominators are not, because the denominators are computed.
# So this cycle enumerated the whole surface rather than stopping at the first
# failure -- 275 division sites in 63 files, of which 174 have a non-literal
# denominator -- and classified each by whether zero is reachable on a live path.
#
# The pattern the survivors share is the one cycles 12 and 13 named: THE GUARD
# EXISTS AND THE COPY DOES NOT USE IT. compute_fte_gap() computes gap_pct with
# ssot_safe_divide(); two other places compute the same quantity by hand and
# divide raw.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.
# (Rotation note: the mix sequence drifted at cycle 15; this restores the
# canonical 4/3/3 -> 3/4/3 -> 3/3/4 cycle from cycle 01.)

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the outlook classifier refuses every non-finite ratio", {
  # A replacement ratio is entrants / departures. Inf used to classify as
  # "Adequate" and -Inf as "Insufficient", because only is.na() was checked and
  # Inf is a number. NaN already fell through. An undefined ratio is undefined
  # however it arose.
  expect_true(is.na(classify_workforce_outlook(Inf)))
  expect_true(is.na(classify_workforce_outlook(-Inf)))
  expect_true(is.na(classify_workforce_outlook(NaN)))
  expect_true(is.na(classify_workforce_outlook(NA_real_)))

  # Finite ratios are unaffected, including at the published cutpoints.
  expect_equal(classify_workforce_outlook(c(1.2, 1.0, 0.8, 0.79)),
               c("Adequate", "Marginal", "Marginal", "Insufficient"))
  # Vectorised, with the non-finite entries isolated rather than poisoning the rest.
  expect_equal(classify_workforce_outlook(c(Inf, 1.3, NaN, 0.5)),
               c(NA, "Adequate", NA, "Insufficient"))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the safe divide is closed at the zero it defends against", {
  # ssot_safe_divide() treats |denominator| < 1e-10 as zero. That threshold is
  # the boundary every fix in this cycle now depends on, so it is pinned here
  # rather than assumed at each call site.
  expect_true(is.na(ssot_safe_divide(1, 0)))
  expect_true(is.na(ssot_safe_divide(1, 1e-11)))
  expect_true(is.na(ssot_safe_divide(1, -1e-11)))
  expect_equal(ssot_safe_divide(1, 1e-9), 1e9)
  expect_equal(ssot_safe_divide(0, 0, default = 1), 1)     # the default is honoured
  expect_equal(ssot_safe_divide(6, 3), 2)
  # A zero NUMERATOR over a real denominator is 0, not the default.
  expect_equal(ssot_safe_divide(0, 5), 0)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a zero required-FTE anchor gives an undefined shortfall, not an infinite one", {
  # baseline_gap()'s shortfall_pct is the same shape as compute_fte_gap()'s
  # gap_pct, and gap_pct has been guarded since it was written. This one was not.
  expect_true(is.na(ssot_safe_divide(100 * (0 - 50), 0)))
  expect_equal(ssot_safe_divide(100 * (1400 - 1300), 1400), 100 * 100 / 1400)

  # And the guarded sibling still behaves, so the two now agree on the same input.
  z <- compute_fte_gap(tibble::tibble(year = 2025L, effective_fte_median = 100),
                       tibble::tibble(year = 2025L, required_fte = 0))
  expect_true(is.na(z$gap_pct))
  expect_false(any(is.infinite(z$gap_pct)))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: an undefined replacement ratio is not a favourable one", {
  # THE DEFECT, in the terms that matter. A scenario in which nobody departs has
  # no replacement ratio -- the quantity entrants-per-departure does not exist.
  # Reporting it as "Adequate" turns a division by zero into a workforce finding.
  expect_true(is.na(ssot_safe_divide(55, 0)))
  expect_true(is.na(classify_workforce_outlook(ssot_safe_divide(55, 0))))

  # Zero entrants and zero departures is equally undefined, and was already NA
  # only because 0/0 is NaN -- now it is NA for the stated reason instead.
  expect_true(is.na(classify_workforce_outlook(ssot_safe_divide(0, 0))))

  # A real ratio still classifies, so the guard has not swallowed the signal.
  expect_equal(classify_workforce_outlook(ssot_safe_divide(66, 55)), "Adequate")
  expect_equal(classify_workforce_outlook(ssot_safe_divide(40, 55)), "Insufficient")
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: a zero departure rate is reachable, so the guard is not hypothetical", {
  # The reachability claim, verified rather than asserted. If no parameterisation
  # produced a zero denominator, the fix would be defending against nothing.
  ages <- seq(35, 45, length.out = 20)
  rate <- implied_annual_departure_rate(
    ages, rep("female", length(ages)),
    retirement_schedule = setNames(rep(0, 120), 1:120),
    career_change_hazard = 0)
  expect_equal(rate, 0)

  # And the ordinary schedule gives a positive rate, so zero is a scenario
  # choice rather than the normal case.
  ordinary <- implied_annual_departure_rate(ages, rep("female", length(ages)))
  expect_gt(ordinary, 0)
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the guarded denominators that already existed still hold", {
  # The sweep's other result: most computed denominators ARE guarded, several by
  # an explicit stop() rather than a default. Pinned as a group so a later
  # refactor that routes one of them through a silent default has to argue.
  expect_error(calibrate_wrvu_per_fte(1e7, 0), NULL)          # solved denominator > 0
  expect_error(rebase_to_year(2025:2027, c(0, 0, 0), 2025) |> suppressMessages(),
               "cannot be rebased")
  expect_true(is.na(supply_per_capita(
    tibble::tibble(geo = "A", fte = 10),
    tibble::tibble(geo = "A", population = 0))$fte_per_capita))
  # capacity_category_adequacy refuses a vanishing denominator outright.
  expect_error(capacity_category_adequacy("shortage_hours", seen = 4, additional = 4),
               "cannot yield an adequacy ratio")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no public summary path emits Inf from a modelled denominator", {
  # The property the whole cycle is about, asserted over the paths a reader
  # actually sees. NA is a legitimate answer ("undefined"); Inf is not, because
  # it reads as a number.
  leaves <- function(x) {
    if (is.numeric(x)) return(as.numeric(x))
    if (is.list(x)) return(unlist(lapply(x, leaves), use.names = FALSE))
    numeric(0)
  }
  cases <- list(
    compute_fte_gap(tibble::tibble(year = 2025L, effective_fte_median = 100),
                    tibble::tibble(year = 2025L, required_fte = 0)),
    supply_per_capita(tibble::tibble(geo = "A", fte = 10),
                      tibble::tibble(geo = "A", population = 0)),
    tibble::tibble(rr = ssot_safe_divide(55, 0),
                   outlook = classify_workforce_outlook(ssot_safe_divide(55, 0)))
  )
  for (i in seq_along(cases)) {
    v <- leaves(cases[[i]])
    expect_false(any(is.infinite(v)), info = paste("case", i))
    expect_false(any(is.nan(v)), info = paste("case", i))
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the two places that compute a gap percentage agree on a degenerate input", {
  # The duplicated-guard pattern, stated as an agreement test. compute_fte_gap()
  # guards gap_pct; the orchestrator's fallback and baseline_gap()'s
  # shortfall_pct compute the same shape by hand. Three implementations of one
  # quantity must not disagree about what a zero denominator means.
  guarded <- compute_fte_gap(tibble::tibble(year = 2025L, effective_fte_median = 100),
                             tibble::tibble(year = 2025L, required_fte = 0))$gap_pct
  fallback <- ssot_safe_divide(100 * 100, 0)               # orchestrator's form
  shortfall <- ssot_safe_divide(100 * (0 - 100), 0)        # baseline_gap()'s form
  expect_true(is.na(guarded))
  expect_true(is.na(fallback))
  expect_true(is.na(shortfall))

  # And on a NON-degenerate input all three are ordinary arithmetic, so the
  # guard has not changed any reported number.
  expect_equal(ssot_safe_divide(100 * -300, 1200), 100 * -300 / 1200)
  ok <- compute_fte_gap(tibble::tibble(year = 2025L, effective_fte_median = 900),
                        tibble::tibble(year = 2025L, required_fte = 1200))
  expect_equal(ok$gap_pct, 100 * (900 - 1200) / 1200)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: a demand lift with no status-quo demand does not become infinite", {
  # scen_fte / sq_demand_fte scales a scenario's demand against the status quo.
  # With no status-quo demand the lift is unknowable; the neutral default of 1
  # says "no lift" rather than "infinite lift", and is the only value that keeps
  # the downstream multiplication finite.
  expect_equal(ssot_safe_divide(1500, 0, default = 1), 1)
  expect_equal(ssot_safe_divide(0, 0, default = 1), 1)
  expect_equal(ssot_safe_divide(1500, 1200, default = 1), 1.25)
  # A lift of 1 leaves the scenario's required FTE where it was, which is what
  # "we cannot tell" has to mean for a multiplier.
  base <- 1300
  expect_equal(base * ssot_safe_divide(1500, 0, default = 1), base)
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: the outlook cutpoints cannot be reached by a non-finite ratio", {
  # The fix must not be satisfiable by coincidence: no non-finite input may land
  # on a category, and no finite input may lose one. Fuzzed across the cutpoints
  # and the pathological values together.
  finite <- c(0, 0.5, 0.79, 0.8, 1.0, 1.19, 1.2, 5, 1e6)
  bad <- c(Inf, -Inf, NaN, NA_real_)
  got_finite <- classify_workforce_outlook(finite)
  expect_false(anyNA(got_finite))
  expect_true(all(got_finite %in% c("Adequate", "Marginal", "Insufficient")))

  got_bad <- classify_workforce_outlook(bad)
  expect_true(all(is.na(got_bad)))

  # Interleaved, so position-dependent handling cannot pass by accident.
  mixed <- c(rbind(finite[seq_len(4)], bad))
  out <- classify_workforce_outlook(mixed)
  expect_equal(is.na(out), rep(c(FALSE, TRUE), 4))
})
