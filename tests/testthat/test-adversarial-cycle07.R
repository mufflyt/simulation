# Adversarial cycle 07 -- the E2SFCA port against its canonical source.
#
# R/geography-spatial_access_e2sfca.R is a port of twostep's floating-catchment
# module. Cycle 03 found a port regression by diffing against canonical (cliff's
# concentration guard); this cycle did the same against ~/twostep and found the
# port had kept the shapes and dropped FOUR guards. Two of them produce a
# negative or inflated access surface rather than an error.
#
# The standing rule -- prefer the canonical function, and when diverging, say so
# out loud -- is what makes this findable. None of these is visible from inside
# the port; all four are visible in one diff against twostep.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

cyc07_membership <- function() {
  # Three demand units, two providers, nested cumulative bands.
  tibble::tibble(
    demand_id = c("d1", "d1", "d2", "d2", "d3", "d3"),
    provider_id = c("p1", "p2", "p1", "p2", "p1", "p2"),
    band = c(30L, 120L, 60L, 60L, 180L, 30L)
  )
}
cyc07_supply <- function() tibble::tibble(provider_id = c("p1", "p2"), supply = c(4, 6))
cyc07_demand <- function() tibble::tibble(demand_id = c("d1", "d2", "d3"),
                                          population = c(50000, 30000, 20000))

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: band weights are monotone non-increasing, and the boundary is equality", {
  # Equal adjacent weights are legal (a flat decay over two bands); an INCREASE
  # is not, because the incremental weight it implies is negative.
  expect_silent(e2sfca_band_weights(c("30" = 0.7, "60" = 0.7)))
  expect_silent(e2sfca_band_weights(c("30" = 0.7, "60" = 0.7 - 1e-9)))
  expect_error(e2sfca_band_weights(c("30" = 0.5, "60" = 1.0)), "monotone non-increasing")
  # The tolerance is 1e-9, so a difference inside float noise is still accepted.
  expect_silent(e2sfca_band_weights(c("30" = 0.7, "60" = 0.7 + 1e-12)))

  # Order is taken from the band NAMES, not the vector order, so an unsorted
  # input is sorted rather than rejected.
  expect_equal(names(e2sfca_band_weights(c("60" = 0.5, "30" = 1.0))), c("30", "60"))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: weights are closed at 0 and the step-2 power at 1", {
  expect_silent(e2sfca_band_weights(c("30" = 1, "60" = 0)))       # zero is legal
  expect_error(e2sfca_band_weights(c("30" = 1, "60" = -1e-12)), "non-negative")
  expect_error(e2sfca_band_weights(c("30" = 1, "60" = NA_real_)), "finite")

  expect_silent(e2sfca_incremental_weights(step2_power = 1))
  expect_silent(e2sfca_incremental_weights(step2_power = 2))
  # Below 1 the incremental weights stop being monotone: at 0.5 the 60-minute
  # band gets 0.356 against the 30-minute band's 0.175, inverting the decay.
  expect_error(e2sfca_incremental_weights(step2_power = 0.5), ">= 1")
  expect_error(e2sfca_incremental_weights(step2_power = 1 - 1e-9), ">= 1")
  expect_error(e2sfca_incremental_weights(step2_power = c(1, 2)), "single number")
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: a single band is the degenerate case and does not trip the descending range", {
  # incr <- wp - c(wp[-1], 0). The old form indexed wp[2:n], which at n = 1 is
  # the DESCENDING range c(2, 1) -- the same trap cycle 05 found live in the
  # aging recurrence. Masked here by an empty assignment index, pinned so it
  # cannot become live.
  one <- e2sfca_incremental_weights(c("30" = 1))
  expect_length(one, 1L)
  expect_equal(unname(one), 1)
  expect_equal(names(one), "30")
  expect_equal(unname(e2sfca_incremental_weights(c("30" = 0.4))), 0.4)
  # And at power 2 the single band is its own square.
  expect_equal(unname(e2sfca_incremental_weights(c("30" = 0.5), step2_power = 2)), 0.25)
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: M2SFCA is closed at a cumulative weight of exactly 1", {
  # The nearest band is conventionally 1.0, so the guard must admit exactly 1
  # while refusing anything above it.
  expect_silent(e2sfca_incremental_weights(c("30" = 1, "60" = 0.5), step2_power = 2))
  expect_error(e2sfca_incremental_weights(c("30" = 1 + 1e-9, "60" = 0.5), step2_power = 2),
               "would INCREASE access")
  # At power 1 a weight above 1 is merely unusual, not incoherent, so it passes.
  expect_silent(e2sfca_incremental_weights(c("30" = 1.5, "60" = 0.5), step2_power = 1))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: incremental weights telescope back to the cumulative weights", {
  # The whole point of the incremental form is that summing over NESTED
  # isochrone populations equals weighting each ring once. That identity is
  # sum(incr[b:n]) == W_b for every b, and it is what makes cumulative
  # artifacts usable without differencing the geometry.
  w <- e2sfca_band_weights(E2SFCA_DEFAULT_WEIGHTS)
  incr <- e2sfca_incremental_weights(E2SFCA_DEFAULT_WEIGHTS)
  n <- length(w)
  for (b in seq_len(n)) {
    expect_equal(sum(incr[b:n]), unname(w[b]),
                 info = sprintf("tail sum from band %s did not telescope", names(w)[b]))
  }
  expect_true(all(incr >= 0))

  # M2SFCA telescopes to the SQUARED cumulative weights, not the squared
  # incremental ones -- diff(W^2), never diff(W)^2.
  incr2 <- e2sfca_incremental_weights(E2SFCA_DEFAULT_WEIGHTS, step2_power = 2)
  for (b in seq_len(n)) expect_equal(sum(incr2[b:n]), unname(w[b])^2)
  expect_false(isTRUE(all.equal(unname(incr2), unname(incr)^2)))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: M2SFCA penalises distance strictly more than E2SFCA", {
  # Delamater's correction exists to stop far providers counting as much as
  # near ones. If squaring does not shift weight toward the nearest band, the
  # penalty is not being applied where it is claimed.
  w <- e2sfca_band_weights(E2SFCA_DEFAULT_WEIGHTS)
  e <- e2sfca_incremental_weights(E2SFCA_DEFAULT_WEIGHTS, step2_power = 1)
  m <- e2sfca_incremental_weights(E2SFCA_DEFAULT_WEIGHTS, step2_power = 2)

  # Nearest band gains share; the outermost band loses it.
  expect_gt(m[[1]] / sum(m), e[[1]] / sum(e))
  expect_lt(m[[length(m)]], e[[length(e)]])
  # And every band beyond the first is weighted no more heavily than before.
  expect_true(all(m[-1] <= e[-1] + 1e-12))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: zero weighted demand yields an undefined ratio, not zero access", {
  # The module documents this explicitly: a provider whose weighted demand is 0
  # has an UNDEFINED ratio, not an infinite or zero one, and its supply must be
  # booked into the audit rather than silently discarded.
  acc <- compute_e2sfca_access(cyc07_membership(), cyc07_supply(),
                               dplyr::mutate(cyc07_demand(), population = 0))
  expect_true(all(is.na(acc$provider_ratios$ratio)))
  expect_false(any(is.infinite(acc$provider_ratios$ratio)))
  expect_true(all(is.na(acc$access$access) | acc$access$access == 0))

  # With population present the ratios are finite and positive.
  ok <- compute_e2sfca_access(cyc07_membership(), cyc07_supply(), cyc07_demand())
  expect_true(all(is.finite(ok$provider_ratios$ratio)))
  expect_true(all(ok$provider_ratios$ratio > 0))
  expect_true(all(ok$access$access >= 0))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a non-monotone weight table can no longer produce negative demand weight", {
  # THE DEFECT. c("30" = 0.5, "60" = 1.0) produced an incremental weight of
  # -0.5 for the 30-minute band. A negative demand weight SUBTRACTS population
  # from a provider's catchment, inflating its supply-to-demand ratio and its
  # access contribution. The port only warned -- and warned with .msg_warn(),
  # a message, so it never reached warnings() and could not be promoted with
  # options(warn = 2). Canonical twostep stops.
  expect_error(e2sfca_incremental_weights(c("30" = 0.5, "60" = 1.0)),
               "monotone non-increasing")
  expect_error(compute_e2sfca_access(cyc07_membership(), cyc07_supply(), cyc07_demand(),
                                     weights = c("30" = 0.5, "60" = 1.0)),
               "monotone non-increasing")

  # Any admissible table yields non-negative incremental weights.
  for (w in list(c("30" = 1, "60" = 1), c("30" = 1, "60" = 0), c("30" = 0.9, "60" = 0.2),
                 E2SFCA_DEFAULT_WEIGHTS)) {
    expect_true(all(e2sfca_incremental_weights(w) >= 0))
  }
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: band labels that are not minutes are refused, not silently reordered", {
  # order(as.numeric(c("near", "far"))) is order(c(NA, NA)) -- an arbitrary
  # order. The weights were then attached to whichever band came out first, and
  # the only signal was R's own "NAs introduced by coercion" warning, which says
  # nothing about access.
  expect_error(e2sfca_band_weights(c("near" = 1.0, "far" = 0.5)), "band-in-minutes")
  expect_error(e2sfca_band_weights(c("30min" = 1.0, "60min" = 0.5)), "band-in-minutes")
  expect_error(e2sfca_band_weights(setNames(c(1, 0.5), c("", "60"))), "band-in-minutes")

  # Numeric-looking names in any order still work, and sort by value not by string.
  w <- e2sfca_band_weights(c("120" = 0.22, "30" = 1.0, "60" = 0.68))
  expect_equal(names(w), c("30", "60", "120"))
  # String sorting would have put "120" first; this is the bug that check prevents.
  expect_false(identical(names(w), sort(c("120", "30", "60"))))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: a membership band outside the weight table stops the access run", {
  # A band with no weight maps to NA and would be silently dropped from the
  # access sum -- understating access for exactly the demand units that are
  # hardest to reach. The module already refuses it; pinned here alongside the
  # weight guards so the whole entry path is covered by one file.
  mem <- cyc07_membership()
  mem$band[1] <- 240L
  expect_error(compute_e2sfca_access(mem, cyc07_supply(), cyc07_demand()),
               "not in the weight table")

  # Restricting the weight table has the same effect, from the other side.
  expect_error(compute_e2sfca_access(cyc07_membership(), cyc07_supply(), cyc07_demand(),
                                     weights = c("30" = 1.0, "60" = 0.68)),
               "not in the weight table")

  # And the M2SFCA path reaches the same guards as the E2SFCA path, rather than
  # taking a shortcut around them.
  expect_error(compute_e2sfca_access(mem, cyc07_supply(), cyc07_demand(), step2_power = 2),
               "not in the weight table")
})
