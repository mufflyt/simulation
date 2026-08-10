# Adversarial cycle 16 -- shapes, tested as shapes.
#
# Cycle 15 carried forward: an identity asserted at a point but never across the
# range. `retirement_survival()` was checked at individual ages for years and
# was never checked for monotonicity, which is how a survival curve that RISES
# survived undetected.
#
# So this cycle enumerated the roxygen in R/ for claims about SHAPE -- monotone,
# non-decreasing, cumulative, telescopes, conserved, sums to a total, bounded --
# and tested the shape rather than a point on it.
#
# Nothing was broken. That is the result: the documented shapes hold, including
# the ones that are only true over part of a range. The tests below are the
# thing that was missing, not a fix.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc16_poolings <- c("urps", "pooled", "pooled_migs", "go")
cyc16_hazard <- function(p, ...) {
  tryCatch(urps_empirical_hazard_by_ageband(p, ...), error = function(e) NULL)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the sparse-cell guard applies from 60-64 onward and nowhere earlier", {
  # The guard is a cummax anchored at one band. Anchored one band too early it
  # would flatten real non-monotonicity in mid-career; one too late and the 70+
  # cell it exists for is not covered.
  h <- cyc16_hazard("urps")
  skip_if(is.null(h), "pooled-hazard artifact not resolvable")
  raw <- cyc16_hazard("urps", floor_sparse = FALSE)
  from <- match("60-64", MICROSIM_AGE_BAND_LABELS)

  # Below the anchor the guard changes nothing at all.
  expect_equal(unname(h[seq_len(from - 1L)]), unname(raw[seq_len(from - 1L)]))
  # At and above it, the result is the running maximum of the raw series.
  expect_equal(unname(h[from:length(h)]), unname(cummax(raw[from:length(raw)])))
  # And the raw series is where the problem is: the 70+ cell is observed 0/16.
  expect_equal(unname(raw[["70+"]]), 0)
  expect_gt(unname(h[["70+"]]), 0)
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the single-year schedule splices observed and literature at exactly 70", {
  s <- tryCatch(urps_empirical_retirement_schedule(), error = function(e) NULL)
  skip_if(is.null(s), "pooled-hazard artifact not resolvable")
  ages <- as.integer(names(s))
  expect_equal(min(ages), 50L)

  # 69 is the last observed year and 70 the first literature year, so the value
  # must change exactly there and nowhere inside either regime.
  expect_equal(unname(s[["66"]]), unname(s[["69"]]))     # flat within the 65-69 band
  expect_false(isTRUE(all.equal(unname(s[["69"]]), unname(s[["70"]]))))
  expect_equal(unname(s[["70"]]), unname(s[["71"]]))     # flat at the start of the tail
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the Wu component rates are non-negative and sum to the combined table", {
  cmp <- WU2011_SURGERY_RATE_COMPONENTS
  expect_true(all(cmp$sui >= 0))
  expect_true(all(cmp$pop >= 0))
  expect_setequal(cmp$age_band, DEMAND_AGE_BANDS)
  # The combined per-1,000 table must BE the component sum, or D3 computed two
  # ways disagrees by construction.
  expect_equal(unname(cmp$sui + cmp$pop),
               unname(WU2011_SURGERY_RATE_PER_1000[cmp$age_band]), tolerance = 1e-12)
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the hazard is non-decreasing from 60-64 under EVERY pooling choice", {
  # The documented shape, tested as a shape and across all four series rather
  # than on the default one. A pooling option that broke it would be reached
  # only by whoever selected that option.
  any_ran <- FALSE
  from <- match("60-64", MICROSIM_AGE_BAND_LABELS)
  for (p in cyc16_poolings) {
    h <- cyc16_hazard(p)
    if (is.null(h)) next
    any_ran <- TRUE
    expect_named(h, MICROSIM_AGE_BAND_LABELS, info = p)
    expect_true(all(h >= 0), info = p)
    expect_false(is.unsorted(h[from:length(h)]),
                 info = sprintf("%s: %s", p, paste(round(h, 4), collapse = " ")))
    # The 70+ cell can never read as "nobody retires after 70".
    expect_gt(unname(h[["70+"]]), 0)
    expect_gte(unname(h[["70+"]]), unname(h[["65-69"]]))
  }
  skip_if(!any_ran, "pooled-hazard artifact not resolvable")
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the single-year schedule is non-decreasing across its whole range", {
  # Not "the two ends look right" -- every consecutive pair. A dip anywhere
  # means a cohort's departure probability falls as it ages, which is what the
  # sparse-cell guard exists to prevent and must hold after the splice too.
  s <- tryCatch(urps_empirical_retirement_schedule(), error = function(e) NULL)
  skip_if(is.null(s), "pooled-hazard artifact not resolvable")
  expect_false(is.unsorted(s))
  expect_true(all(diff(s) >= -1e-12))
  expect_true(all(s >= 0 & s <= 1))
  expect_false(anyNA(s))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: age-specific surgical demand is linear in population and additive in condition", {
  # Two shapes at once: scaling the population scales the cases exactly, and the
  # by-condition split sums back to the combined total. If the second fails, D3
  # computed two ways disagrees and either could be the one reported.
  pop <- data.frame(year = rep(2025L, length(DEMAND_AGE_BANDS)),
                    age_band = DEMAND_AGE_BANDS,
                    female_pop = rep(1e6, length(DEMAND_AGE_BANDS)),
                    stringsAsFactors = FALSE)
  one <- apply_age_specific_surgery_demand(pop)
  two <- apply_age_specific_surgery_demand(dplyr::mutate(pop, female_pop = .data$female_pop * 2))
  expect_equal(two$surgical_cases, one$surgical_cases * 2)

  split <- apply_age_specific_surgery_demand(pop, by_condition = TRUE)
  expect_equal(split$sui_cases + split$pop_cases, one$surgical_cases, tolerance = 1e-9)
  expect_true(all(split$sui_cases > 0))
  expect_true(all(split$pop_cases > 0))
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the two conditions have genuinely different age curves", {
  # The module documents SUI and POP as different clinical pathways with
  # different age curves, which is why the cascade models them separately. If
  # the shipped component table made them proportional, modelling them
  # separately would be arithmetic, not evidence -- the same objection
  # assert_estimands_independent() raises about the demand estimands.
  cmp <- WU2011_SURGERY_RATE_COMPONENTS
  ratio <- cmp$sui / cmp$pop
  expect_gt(diff(range(ratio)), 0.1)

  # Both rise to a peak inside 65-79 and fall at 80+, which is the shape the
  # source describes; neither is monotone over the whole range.
  for (v in list(cmp$sui, cmp$pop)) {
    peak <- which.max(v)
    expect_true(peak %in% c(3L, 4L))                 # 60-64 or 65-79
    expect_false(is.unsorted(v[seq_len(peak)]))      # rising up to the peak
    expect_lt(v[length(v)], v[peak])                 # and falling at 80+
  }
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a band-label mismatch is refused, not silently dropped", {
  # The module records this as a live past defect: cliff labels the top band
  # "80plus" where this table says "80+", and a filter(!is.na(rate)) quietly
  # lost 15% of the cases (21,900 -> 18,700 on a flat test population). Silent
  # population loss is never the right default for a demand total.
  pop <- data.frame(year = rep(2025L, length(DEMAND_AGE_BANDS)),
                    age_band = DEMAND_AGE_BANDS,
                    female_pop = rep(1e6, length(DEMAND_AGE_BANDS)),
                    stringsAsFactors = FALSE)
  bad <- pop; bad$age_band[bad$age_band == "80+"] <- "80plus"
  expect_error(apply_age_specific_surgery_demand(bad), "not in the rate table")
  expect_error(apply_age_specific_surgery_demand(bad), "understate")

  # And the loss it would have caused is real: the top band is a material share.
  full <- apply_age_specific_surgery_demand(pop)$surgical_cases
  without <- apply_age_specific_surgery_demand(pop[pop$age_band != "80+", ])$surgical_cases
  expect_gt((full - without) / full, 0.05)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: every documented shape in the supply lifecycle holds across its range", {
  # A shape battery. Each of these is documented somewhere in R/ as a property
  # rather than a value, and each was previously only ever checked at a point.
  # Survival is the one that failed that way in cycle 15.
  s <- retirement_survival(45, 40:95, sex = "female")
  expect_true(all(diff(s) <= 1e-12))                       # non-increasing
  expect_true(all(s >= 0 & s <= 1))                        # bounded
  expect_equal(unname(s[as.character(40:45)]), rep(1, 6))  # nothing elapsed yet

  # Incremental band weights telescope to the cumulative ones at every band.
  w <- e2sfca_band_weights(E2SFCA_DEFAULT_WEIGHTS)
  inc <- e2sfca_incremental_weights(E2SFCA_DEFAULT_WEIGHTS)
  for (b in seq_along(w)) expect_equal(sum(inc[b:length(inc)]), unname(w[b]))

  # Prevalence stays in [0, 1] over a long grid, not just at the ends.
  p <- prevalence_from_incidence(rep(0.03, 60), remission = 0.02, p0 = 0)
  expect_true(all(p >= 0 & p <= 1))
  expect_true(all(diff(p) >= -1e-12))                      # no remission-driven dip here
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: turning the sparse-cell guard off changes the shape, so the guard is load-bearing", {
  # A guard that makes no difference is not protecting anything. The raw series
  # must actually violate the shape the guarded one satisfies, or this whole
  # mechanism is decoration.
  raw <- cyc16_hazard("urps", floor_sparse = FALSE)
  skip_if(is.null(raw), "pooled-hazard artifact not resolvable")
  from <- match("60-64", MICROSIM_AGE_BAND_LABELS)
  expect_true(is.unsorted(raw[from:length(raw)]),
              info = "the raw series is already monotone, so the guard tests nothing")

  guarded <- cyc16_hazard("urps")
  expect_false(is.unsorted(guarded[from:length(guarded)]))
  # The guard only ever raises a hazard; it never lowers an observed one.
  expect_true(all(guarded >= raw - 1e-12))
})
