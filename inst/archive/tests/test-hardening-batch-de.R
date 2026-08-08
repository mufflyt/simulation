# ARCHIVED tests, moved from tests/testthat/test-hardening-batch-de.R.
# They exercise functions now in inst/archive/ and are NOT run.

test_that("apply_provider_migration fails loudly when entry_year is missing", {
  agents <- data.frame(state = c("CA", "TX"), age = c(40, 50), stringsAsFactors = FALSE)
  shares <- tibble::tibble(geo = c("CA", "TX"), share = c(0.5, 0.5))
  expect_error(apply_provider_migration(agents, year = 2030, shares = shares), "entry_year")
})

