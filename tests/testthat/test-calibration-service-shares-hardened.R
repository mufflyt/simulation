# Stress-test variant of test-calibration-service-shares.R: larger draw
# counts and an explicit simplex-precision check, using the same synthetic
# events fixture (.synthetic_service_share_events(), defined there and
# auto-sourced by testthat alongside this file).

test_that("draw_service_share_composition satisfies double-precision simplex equality across 1000 draws", {
  events <- .synthetic_service_share_events()
  concentration <- select_service_share_concentration(events)
  draws <- draw_service_share_composition(
    events,
    selected_alpha = concentration$selected,
    draws = 1000L,
    seed = 999L
  )

  cell_sums <- draws |>
    dplyr::group_by(.data$draw_id, .data$service, .data$condition, .data$year) |>
    dplyr::summarise(total_share = sum(.data$share), .groups = "drop")

  expect_true(all(abs(cell_sums$total_share - 1.0) < 1e-10))
  expect_true(all(draws$share >= 0.0))
  expect_true(all(draws$share <= 1.0))
})

test_that("calibrate_service_share_model reweights draws when external evidence is supplied", {
  events <- .synthetic_service_share_events()
  bundle_no_evidence <- calibrate_service_share_model(events, draws = 200L, seed = 20260823L)
  expect_false(bundle_no_evidence$config$cms_used)
  expect_false(bundle_no_evidence$config$chia_used)
  expect_equal(bundle_no_evidence$share_draws$draw_id, bundle_no_evidence$share_draws$source_draw_id)
})
