test_that("validate_hrr_reference validates 306 HRR codes", {
  ref <- tibble::tibble(
    hrr_code = sprintf("HRR%03d", 1:306),
    hrr_name = paste("HRR Region", 1:306)
  )

  res <- validate_hrr_reference(ref, expected_hrr_n = 306L)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 306L)
})

test_that("aggregate_hrr_workforce_balance performs 5-step spatial balance accounting", {
  ref <- tibble::tibble(
    hrr_code = sprintf("HRR%03d", 1:306),
    hrr_name = paste("HRR Region", 1:306)
  )

  roster <- tibble::tibble(
    provider_id = c("P1", "P2", "P3"),
    hrr_code = c("HRR001", "HRR001", "HRR002"),
    fte = c(1.0, 0.8, 1.0)
  )

  demand <- tibble::tibble(
    hrr_code = c("HRR001", "HRR002", "HRR003"),
    demand_fte = c(2.5, 1.0, 3.0)
  )

  bal <- aggregate_hrr_workforce_balance(
    provider_roster = roster,
    hrr_demand_tbl = demand,
    hrr_reference_tbl = ref,
    expected_hrr_n = 306L
  )

  expect_s3_class(bal, "tbl_df")
  expect_equal(nrow(bal), 306L)
  expect_true(all(c("gap_fte", "deficit_fte", "surplus_fte", "adequacy_ratio", "shortage_20pct", "shortage_severity") %in% names(bal)))

  # National summary check
  summ <- summarize_hrr_workforce_balance(bal)
  expect_s3_class(summ, "tbl_df")
  expect_equal(summ$hrr_n, 306L)
  expect_equal(summ$supply_fte, 2.8)
  expect_equal(summ$demand_fte, 6.5)
})
