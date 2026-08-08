# Regression tests for the batch D/E hardening guards (the cheaply constructable
# ones). Each pins the failure the guard converts from silent/cryptic to loud.

test_that("assign_entrant_geography rejects a degenerate share vector", {
  expect_error(
    assign_entrant_geography(10, tibble::tibble(geo = c("A", "B"), share = c(NA_real_, 1))),
    "finite")
  expect_error(
    assign_entrant_geography(10, tibble::tibble(geo = c("A", "B"), share = c(0, 0))),
    "sum")
  # a valid share vector still works
  out <- assign_entrant_geography(5, tibble::tibble(geo = c("A", "B"), share = c(0.5, 0.5)))
  expect_length(out, 5L)
})

test_that("apply_provider_migration fails loudly when entry_year is missing", {
  agents <- data.frame(state = c("CA", "TX"), age = c(40, 50), stringsAsFactors = FALSE)
  shares <- tibble::tibble(geo = c("CA", "TX"), share = c(0.5, 0.5))
  expect_error(apply_provider_migration(agents, year = 2030, shares = shares), "entry_year")
})

test_that("pathway_stage_entrants rejects an unnamed treated vector", {
  expect_error(pathway_stage_entrants(c(1, 2, 3)), "named", ignore.case = TRUE)
})

test_that("weighted_interval_score rejects a mis-length y instead of partial-recycling", {
  q <- matrix(c(1, 2, 3,
                2, 3, 4,
                3, 4, 5), nrow = 3, byrow = TRUE)
  lv <- c(0.25, 0.5, 0.75)
  expect_error(weighted_interval_score(y = c(1, 2), quantiles = q, quantile_levels = lv),
               "length")
  # length 1 (recycled) and length nrow both remain valid
  expect_silent(invisible(weighted_interval_score(y = 2, quantiles = q, quantile_levels = lv)))
})
