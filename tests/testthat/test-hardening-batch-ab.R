# Regression tests for the hardening batch A/B fixes: each pins the specific
# silent-wrong / phantom-row behaviour the guard removes, so a revert fails loudly.

test_that("validation_report fails required_fte_positive when the column is absent or empty (no silent pass)", {
  supply <- tibble::tibble(year = 2025L, headcount = 100)

  # A `required` table whose fte column is MIS-NAMED: the check must FAIL, not
  # pass. Before the guard, `all(NULL > 0, na.rm = TRUE)` was TRUE -> recorded
  # PASS having validated nothing.
  rep_bad <- validation_report(supply, required = tibble::tibble(reqd_fte = c(5, 10)))
  row_bad <- rep_bad[rep_bad$check == "required_fte_positive", ]
  expect_equal(nrow(row_bad), 1L)
  expect_false(row_bad$passed)

  # An empty table also fails (was the same all(logical(0)) == TRUE trap).
  rep_empty <- validation_report(supply, required = tibble::tibble(required_fte = numeric(0)))
  expect_false(rep_empty[rep_empty$check == "required_fte_positive", ]$passed)

  # A well-formed table with positive FTE still passes.
  rep_ok <- validation_report(supply, required = tibble::tibble(required_fte = c(5, 10)))
  expect_true(rep_ok[rep_ok$check == "required_fte_positive", ]$passed)
})

test_that("opportunity_placement_shares keeps retirement-only geos (full join, not left)", {
  demand_growth <- tibble::tibble(geo = c("A", "B"), demand_growth_fte = c(10, 5))
  retirements   <- tibble::tibble(geo = c("A", "C"), retirements_fte   = c(2, 3))

  out <- opportunity_placement_shares(demand_growth, retirements)

  # "C" has retirements but no demand-growth row: a left join dropped it and its
  # openings never entered the placement distribution.
  expect_true("C" %in% out$geo)
  expect_equal(out$requirements_fte[out$geo == "C"], 3)
  expect_equal(sum(out$share), 1)
})

test_that("thin_roster_by_p_active does not inject phantom NA rows on an NA age", {
  set.seed(1)
  agents <- data.frame(
    age = c(45, NA, 60),
    sex = c("female", "female", "male"),
    id  = 1:3,
    stringsAsFactors = FALSE
  )

  out <- thin_roster_by_p_active(agents, stochastic = TRUE)

  # Before the `!is.na(p) &` guard, an NA activity probability made `keep` NA and
  # `agents[NA, ]` inserted a phantom all-NA row.
  expect_false(anyNA(out$age))
  expect_true(all(out$id %in% 1:3))
  expect_lte(nrow(out), 3L)
})
