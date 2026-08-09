# Primary historical back-test selection (config backtest$compare drives the MAPE).
# Verifies that the comparison metric is REAL, not cosmetic: the primary MAPE is
# computed over the configured categories only, sling is demoted to a secondary
# diagnostic (but kept), supply-side config entries never enter the demand MAPE,
# and an empty configured set fails loudly.

mk_sv <- function() {
  base <- tibble::tibble(
    service = c("new_consultation", "return_visit", "sling_procedure", "prolapse_procedure"),
    volume  = c(2e6, 1e6, 1e5, 8e4))                       # office=3e6, sling=1e5, POP=8e4
  dplyr::bind_rows(
    dplyr::mutate(base, year = 2017L),
    dplyr::mutate(base, year = 2023L, volume = volume * 1.10))   # +10% by target year
}
obs_fit <- tibble::tibble(                                        # == 2017 prediction (scalar 1)
  category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
  observed = c(3e6, 1e5, 8e4))
# target: office & POP land exactly (+10%, error 0); sling is off (+50%, big error)
obs_tgt <- tibble::tibble(
  category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
  observed = c(3.3e6, 1.65e5, 8.8e4))

bt <- function(compare) backtest_lifecourse(mk_sv(), obs_fit, obs_tgt,
                                            fit_through_year = 2017L, target_year = 2023L,
                                            compare_categories = compare)

test_that("the configured comparator selects the primary category", {
  pop   <- bt("prolapse_procedure_volume")
  sling <- bt("sling_procedure_volume")
  expect_equal(pop$summary$primary_categories, "prolapse_procedure_volume")
  expect_equal(sling$summary$primary_categories, "sling_procedure_volume")
  # by_category flags primary vs secondary correctly
  expect_true(pop$by_category$primary_backtest[pop$by_category$category == "prolapse_procedure_volume"])
  expect_false(pop$by_category$primary_backtest[pop$by_category$category == "sling_procedure_volume"])
})

test_that("POP drives primary MAPE and sling does NOT after the switch", {
  pop <- bt("prolapse_procedure_volume")
  # POP error is ~0, so primary MAPE ~0 even though sling is badly off
  expect_lt(pop$summary$mape, 1e-6)
  expect_equal(pop$summary$n, 1L)
  # sling's large error is present in diagnostics but excluded from the primary score
  sling_row <- pop$by_category[pop$by_category$category == "sling_procedure_volume", ]
  expect_gt(sling_row$abs_pct_error, 20)          # ~33% off
  expect_false(sling_row$primary_backtest)
  # and if sling WERE primary, the MAPE would be large -> proves it was excluded
  expect_gt(bt("sling_procedure_volume")$summary$mape, 20)
})

test_that("sling stays a full calibration anchor (selection is back-test-only)", {
  observed <- tibble::tibble(
    category = c("urps_office_visits", "sling_procedure_volume", "prolapse_procedure_volume"),
    observed = c(3e6, 1e5, 8e4))
  cal <- calibrate_lifecourse_demand(mk_sv(), observed, base_year = 2017L)
  # calibration fits scalars for ALL anchors, including sling, regardless of the
  # back-test comparator
  expect_true("sling_procedure_volume" %in% cal$scalars$category)
  expect_true(all(c("urps_office_visits", "prolapse_procedure_volume") %in% cal$scalars$category))
})

test_that("unrelated supply-side config entries never enter the demand MAPE", {
  # config compare may list supply metrics; only the demand intersection matters
  out <- bt(c("urps_headcount", "urps_state_distribution", "prolapse_procedure_volume"))
  expect_equal(out$summary$primary_categories, "prolapse_procedure_volume")
  expect_equal(out$summary$n, 1L)
  expect_lt(out$summary$mape, 1e-6)
})

test_that("an empty configured demand set fails loudly, never scores against all", {
  expect_error(bt(c("urps_headcount", "urps_state_distribution")),
               "empty set|none of the configured", ignore.case = TRUE)
})

test_that("NULL compare_categories is unchanged (all categories primary)", {
  out <- bt(NULL)
  expect_equal(out$summary$n, 3L)
  expect_true(all(out$by_category$primary_backtest))
  # MAPE over all three = mean(0, ~33%, 0) ~ 11%
  expect_gt(out$summary$mape, 5)
  expect_lt(out$summary$mape, 20)
})
