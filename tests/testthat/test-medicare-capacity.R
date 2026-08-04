test_that("Medicare workload is roster-linked and does not infer specialty from claims", {
  roster <- tibble::tibble(npi = c("a", "b", "c"), year = 2024L,
    age_band = c("<45", "45-54", "55+"), coverage = c(0.5, 1, 1))
  claims <- tibble::tibble(npi = c("a", "a", "b", "outside"), year = 2024L,
    hcpcs = c("99204", "57288", "99213", "99213"), services = c(10, 2, 20, 99))
  p <- medicare_work_rvu_by_provider(claims, roster, coverage_col = "coverage", minimum_wrvu = 10)
  expect_equal(nrow(p), 3)
  expect_true("age_band" %in% names(p))
  expect_equal(p$medicare_work_rvu[p$npi == "c"], 0)
  expect_false(p$included_in_reference[p$npi == "c"])
  expect_equal(p$coverage_adjusted_work_rvu[p$npi == "a"], 2 * (10 * 2.6 + 2 * 12.13))
})

test_that("workload index is normalized to its roster reference and remains labelled relative", {
  p <- tibble::tibble(npi = c("a", "b", "c"), coverage_adjusted_work_rvu = c(100, 200, 0),
    included_in_reference = c(TRUE, TRUE, FALSE), age_band = c("<45", "55+", "55+"))
  x <- medicare_workload_index(p, direct_care_hours = 2063)
  expect_equal(x$medicare_workload_index, c(2 / 3, 4 / 3, 0))
  expect_equal(x$implied_direct_care_hours[2], 2063 * 4 / 3)
  expect_match(attr(x, "estimand"), "not observed clinical-hours FTE")
  s <- summarise_medicare_capacity(x, by = "age_band")
  expect_equal(sum(s$relative_capacity), 2)
})

test_that("Medicare capacity rejects unobserved CPTs and invalid coverage", {
  claims <- tibble::tibble(npi = "a", year = 2024L, hcpcs = "99999", services = 1)
  roster <- tibble::tibble(npi = "a", year = 2024L, coverage = 0)
  expect_error(medicare_work_rvu_by_provider(claims, roster), "no work RVU")
  claims$hcpcs <- "99213"
  expect_error(medicare_work_rvu_by_provider(claims, roster, coverage_col = "coverage"), "coverage")
})
