test_that("calibrate_service_share_model fits held-out concentrations from real events", {
  events <- .synthetic_service_share_events()
  bundle <- calibrate_service_share_model(events, draws = 20L)

  expect_type(bundle, "list")
  expect_true(all(c(
    "share_draws", "selected_alpha", "holdout_scores",
    "source_fit", "provenance", "config", "valid"
  ) %in% names(bundle)))
  expect_true(bundle$valid)
  expect_true(all(bundle$selected_alpha$selected_alpha > 0))
})

test_that("draw_service_share_composition produces a valid compositional simplex", {
  events <- .synthetic_service_share_events()
  concentration <- select_service_share_concentration(events)
  draws <- draw_service_share_composition(
    events,
    selected_alpha = concentration$selected,
    draws = 50L,
    seed = 20260823L
  )

  cell_sums <- draws |>
    dplyr::group_by(.data$draw_id, .data$service, .data$condition, .data$year) |>
    dplyr::summarise(total_share = sum(.data$share), .groups = "drop")

  expect_true(all(abs(cell_sums$total_share - 1.0) < 1e-8))
  expect_true(all(draws$share >= 0))
  expect_true(all(draws$share <= 1))
})

test_that("calibrate_service_share_model is reproducible given the same seed", {
  events <- .synthetic_service_share_events()
  bundle1 <- calibrate_service_share_model(events, draws = 20L, seed = 42L)
  bundle2 <- calibrate_service_share_model(events, draws = 20L, seed = 42L)

  expect_equal(bundle1$share_draws$share, bundle2$share_draws$share)
})

test_that("validate_service_share_bundle accepts a real calibrated bundle", {
  events <- .synthetic_service_share_events()
  bundle <- calibrate_service_share_model(events, draws = 20L)
  expect_true(validate_service_share_bundle(bundle))
})
