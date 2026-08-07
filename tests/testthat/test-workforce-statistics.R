# Guards for the cliff-harvested workforce statistics + sensitivity registry.

test_that("Wilson proportion CI matches known bounds and handles n=0", {
  ci <- calculate_proportion_ci(5, 10)
  expect_equal(ci$proportion, 0.5)
  expect_equal(ci$lower_ci, 0.2366, tolerance = 1e-3)
  expect_equal(ci$upper_ci, 0.7634, tolerance = 1e-3)
  expect_equal(ci$method, "Wilson")
  z <- calculate_proportion_ci(0, 0)
  expect_true(is.na(z$proportion))
  expect_equal(z$note, "Zero denominator")
})

test_that("two-proportion test refuses small samples but tests large ones", {
  small <- calculate_two_prop_test(3, 10, 4, 12)
  expect_equal(small$method, "descriptive_only")
  expect_false(small$significant)
  expect_true(is.na(small$p_value))

  big <- calculate_two_prop_test(60, 100, 100, 500)   # 60% vs 20%, clearly different
  expect_equal(big$method, "prop.test")
  expect_true(big$significant)
  expect_lt(big$p_value, 0.05)
})

test_that("rural-metro comparison bundles rates, CIs and a test", {
  cmp <- calculate_rural_metro_comparison(45, 150, 200, 1000)
  expect_equal(cmp$rural$rate_pct, 30, tolerance = 1e-6)
  expect_equal(cmp$metro$rate_pct, 20, tolerance = 1e-6)
  expect_equal(cmp$comparison$rate_difference_pct, 10, tolerance = 1e-6)
  expect_true(cmp$rural$ci_lower < cmp$rural$rate_pct)   # CI brackets the point rate
  expect_true(cmp$rural$ci_upper > cmp$rural$rate_pct)
  expect_true(cmp$comparison$significant)                # 30% vs 20%, n large
})

test_that("replacement gap computes ratio, net gap and adequacy", {
  retirees <- tibble::tibble(subspecialty = c("URPS", "GO"),
                             retiring_count = c(100, 300))
  grads <- tibble::tibble(subspecialty = c("URPS", "URPS", "GO", "GO"),
                          graduates = c(20, 22, 40, 44))   # means 21 and 42
  res <- calculate_replacement_gap(retirees, grads, horizon_years = 5)
  urps <- dplyr::filter(res$by_subspecialty, subspecialty == "URPS")
  expect_equal(urps$projected_grads, 21 * 5)              # 105
  expect_equal(urps$net_gap, 100 - 105)                   # -5 (surplus)
  expect_true(urps$adequate_replacement)                  # ratio 1.05 >= 1
  go <- dplyr::filter(res$by_subspecialty, subspecialty == "GO")
  expect_equal(go$projected_grads, 42 * 5)                # 210 vs 300 retiring
  expect_false(go$adequate_replacement)                   # ratio 0.70 < 1
  expect_equal(res$overall$total_retiring, 400)
})

test_that("state vulnerability ranks by loss, weighted by log active count", {
  impacts <- tibble::tibble(
    state = c("CA", "WY", "TX"),
    count_active = c(300, 3, 200),
    count_at_risk = c(30, 2, 40),
    pct_loss_if_retire = c(10, 67, 20),
    zero_coverage_if_retire = c(FALSE, TRUE, FALSE)
  )
  top <- calculate_state_vulnerability(impacts, top_n = 2)
  expect_equal(nrow(top), 2L)
  expect_equal(top$state[1], "TX")                        # highest log10(active)-weighted vulnerability_score
  expect_true("vulnerability_score" %in% names(top))
})

# ---- sensitivity registry --------------------------------------------------

sens_available <- function() {
  isTRUE(tryCatch({ resolve_canonical("sensitivity_departure_rate"); TRUE },
                  error = function(e) FALSE))
}

test_that("sensitivity registry lists 13 tables", {
  expect_length(sensitivity_registry(), 13L)
  expect_true(all(c("departure_rate", "mortality", "grid") %in% sensitivity_registry()))
})

test_that("load_sensitivity_table resolves a checksummed table and rejects unknowns", {
  skip_if_not(sens_available(), "sensitivity files not present")
  tbl <- load_sensitivity_table("departure_rate", mode = "strict")
  expect_s3_class(tbl, "tbl_df")
  expect_gt(nrow(tbl), 0)
  expect_error(load_sensitivity_table("nope"), "Unknown sensitivity table")
})
