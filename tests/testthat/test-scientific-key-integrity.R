# tests/testthat/test-scientific-key-integrity.R
# Scientific Hardening Section 4 P0: Scientific Key Integrity Tests

test_that("assert_scientific_key_integrity passes on unique composite keys", {
  df <- tibble::tibble(
    year = c(2015L, 2015L, 2016L),
    RecordType20ID = c(101L, 102L, 101L),
    value = c(10, 20, 30)
  )
  expect_true(assert_scientific_key_integrity(df, c("year", "RecordType20ID"), expected_relationship = "one-to-one"))
})

test_that("assert_scientific_key_integrity catches duplicate encounter keys in one-to-one relationship", {
  df_dup <- tibble::tibble(
    year = c(2015L, 2015L),
    RecordType20ID = c(101L, 101L), # Duplicate!
    value = c(10, 20)
  )
  expect_error(
    assert_scientific_key_integrity(df_dup, c("year", "RecordType20ID"), expected_relationship = "one-to-one"),
    "duplicate key rows found"
  )
})

