# Delegated quantities agree with their fallbacks ----
#
# WHY THIS EXISTS. `zero_access_share()` and `weighted_mean_all()` now forward
# to the mufflyaccess contract but keep a local body, because mufflyaccess is a
# Suggests dependency and the package must stay usable without it. That fallback
# is the delegation's own loophole: it is a second implementation of the same
# quantity, which is the defect the delegation was meant to remove, reintroduced
# one line lower.
#
# The bodies were byte-identical when the delegation was written. Nothing keeps
# them that way except this file. Without it, an upstream correction would be
# picked up by every installation WITH the contract and silently missed by every
# installation without one -- two answers, and the divergence visible only to
# whoever happened to run both.
#
# This is the same reasoning as the Wilson-pair recommendation in
# docs/CANONICAL_SOURCES_AUDIT.md: where two implementations must coexist, the
# cheap insurance is a test that they agree on a shared grid.

skip_if_not(requireNamespace("mufflyaccess", quietly = TRUE),
            "mufflyaccess not installed")

# The fallback bodies, extracted so they can be exercised with the contract
# present. Calling the exported function would take the delegation branch and
# compare the contract against itself.
fallback_zero_access_share <- function(access, w) {
  stopifnot(length(access) == length(w))
  sw <- sum(w)
  if (!is.finite(sw) || sw == 0) return(NA_real_)
  100 * sum(w * (access == 0)) / sw
}

fallback_weighted_mean_all <- function(a, w) {
  stopifnot(length(a) == length(w))
  sw <- sum(w)
  if (!is.finite(sw) || sw == 0) return(NA_real_)
  sum(a * w) / sw
}

# Ordinary values, plus the cases each function's guards single out: an exact
# zero in the access vector, a zero weight sum, a non-finite weight, and a
# length-one input.
CASES <- list(
  list(a = c(0, 1, 2.5, 10),        w = c(1, 2, 3, 4)),
  list(a = c(0, 0, 0),              w = c(5, 5, 5)),
  list(a = c(1, 2, 3),              w = c(0, 0, 0)),
  list(a = c(1, 2, 3),              w = c(1, Inf, 3)),
  list(a = c(0, 1),                 w = c(0, 1)),
  list(a = 0,                       w = 1),
  list(a = 7.25,                    w = 3),
  list(a = c(-1, 0, 1),             w = c(2, 2, 2)),
  list(a = c(0.0, 1e-12, 1),        w = c(1, 1, 1))
)

test_that("zero_access_share agrees with the contract on every case", {
  for (i in seq_along(CASES)) {
    cs <- CASES[[i]]
    expect_equal(mufflyaccess::zero_access_share(cs$a, cs$w),
                 fallback_zero_access_share(cs$a, cs$w),
                 info = sprintf("case %d", i))
  }
})

test_that("weighted_mean_all agrees with the contract on every case", {
  for (i in seq_along(CASES)) {
    cs <- CASES[[i]]
    expect_equal(mufflyaccess::weighted_mean_all(cs$a, cs$w),
                 fallback_weighted_mean_all(cs$a, cs$w),
                 info = sprintf("case %d", i))
  }
})

test_that("the exported functions take the delegation branch", {
  # A delegation nobody exercises is indistinguishable from no delegation, so
  # assert the exported name reaches the contract rather than the fallback.
  expect_true(grepl("mufflyaccess::zero_access_share",
                    paste(deparse(zero_access_share), collapse = " "), fixed = TRUE))
  expect_true(grepl("mufflyaccess::weighted_mean_all",
                    paste(deparse(weighted_mean_all), collapse = " "), fixed = TRUE))
})

test_that("every delegated name is declared in the contract pin", {
  # MUFFLYACCESS_REQUIRED_EXPORTS exists so an installed build missing a needed
  # function is rejected as unusable rather than failing later at a call site. A
  # delegation that skipped the list would defeat it.
  expect_true(all(c("zero_access_share", "weighted_mean_all") %in%
                    MUFFLYACCESS_REQUIRED_EXPORTS))
})

test_that("the delegated functions exist at the PINNED contract commit", {
  # The reason these two were separable from calculate_rural_metro_comparison.
  # Delegating to a function absent from the pin would force a pin bump, which
  # is what 72a7e13 reverted for turning main red under --as-cran.
  build <- mufflyaccess_build()
  expect_length(build$missing_exports, 0L)
})
