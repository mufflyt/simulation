service_share_known_truth_fixture <- function() {
  years <- 2020:2024
  purrr::map_dfr(years, function(year) {
    tibble::tribble(
      ~service, ~condition, ~year, ~provider_group, ~service_events,
      "sling_procedure", "sui", year, "urps", 700,
      "sling_procedure", "sui", year, "general_obgyn", 300,
      "pessary_care", "pop", year, "urps", 200,
      "pessary_care", "pop", year, "general_obgyn", 500,
      "pessary_care", "pop", year, "app", 300
    )
  })
}


test_that("concentration is selected by held-out predictive score", {
  events <- service_share_known_truth_fixture()
  fit <- select_service_share_concentration(
    events,
    alpha_grid = c(0.5, 2, 10, 50)
  )

  expect_setequal(
    fit$selected$service,
    c("sling_procedure", "pessary_care")
  )
  expect_true(all(fit$selected$selected_alpha %in% c(0.5, 2, 10, 50)))
  expect_true(all(fit$selected$holdout_year == 2024L))

  best_scores <- fit$scores |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      best_cross_entropy = base::min(.data$cross_entropy),
      .groups = "drop"
    )
  observed <- fit$selected |>
    dplyr::select(.data$service, .data$cross_entropy) |>
    dplyr::left_join(best_scores, by = "service")

  expect_equal(
    observed$cross_entropy,
    observed$best_cross_entropy,
    tolerance = 1e-12
  )
})


test_that("calibration refuses services without a held-out year", {
  events <- tibble::tribble(
    ~service, ~condition, ~year, ~provider_group, ~service_events,
    "sling_procedure", "sui", 2024L, "urps", 70,
    "sling_procedure", "sui", 2024L, "general_obgyn", 30
  )

  expect_error(
    select_service_share_concentration(events),
    "at least two years"
  )
})


test_that("joint draws are normalized and reproducible", {
  events <- service_share_known_truth_fixture()
  alpha <- select_service_share_concentration(
    events,
    alpha_grid = c(0.5, 2, 10)
  )$selected

  draws_a <- draw_service_share_composition(
    events,
    selected_alpha = alpha,
    draws = 100L,
    seed = 20260822L
  )
  draws_b <- draw_service_share_composition(
    events,
    selected_alpha = alpha,
    draws = 100L,
    seed = 20260822L
  )

  expect_identical(draws_a, draws_b)

  sums <- draws_a |>
    dplyr::group_by(
      .data$service,
      .data$condition,
      .data$year,
      .data$draw_id
    ) |>
    dplyr::summarise(
      share_sum = base::sum(.data$share),
      .groups = "drop"
    )
  expect_equal(
    sums$share_sum,
    base::rep(1, base::nrow(sums)),
    tolerance = 1e-12
  )
  expect_true(all(draws_a$share >= 0 & draws_a$share <= 1))
})


test_that("large cells recover known provider composition", {
  bundle <- calibrate_service_share_model(
    service_share_known_truth_fixture(),
    alpha_grid = c(0.5, 2, 10),
    draws = 500L,
    seed = 20260822L
  )

  observed <- bundle$share_draws |>
    dplyr::filter(
      .data$service == "sling_procedure",
      .data$condition == "sui",
      .data$year == 2024L,
      .data$provider_group == "urps"
    ) |>
    dplyr::summarise(mean_share = base::mean(.data$share)) |>
    dplyr::pull(.data$mean_share)

  expect_equal(observed, 0.70, tolerance = 0.03)
  expect_invisible(validate_service_share_bundle(bundle))
})


test_that("CMS and CHIA evidence are scored separately", {
  events <- service_share_known_truth_fixture()
  cms <- base::list(
    service_bounds = tibble::tribble(
      ~service, ~lower_bound, ~upper_bound,
      "sling_procedure", 0.60, 0.80,
      "pessary_care", 0.10, 0.40
    )
  )
  chia <- base::list(
    physician_share = tibble::tribble(
      ~service, ~year, ~payer_group, ~setting, ~urps_events,
      ~physician_events, ~urps_given_physician,
      "sling_procedure", 2024L, "Commercial", "inpatient", 65,
      100, 0.65,
      "pessary_care", 2024L, "Commercial", "outpatient", 25,
      70, 25 / 70
    )
  )

  bundle <- calibrate_service_share_model(
    events,
    cms_evidence = cms,
    chia_evidence = chia,
    alpha_grid = c(0.5, 2, 10),
    draws = 100L,
    seed = 42L
  )

  expect_true(base::is.data.frame(bundle$source_fit$cms))
  expect_true(base::is.data.frame(bundle$source_fit$chia))
  expect_true(all(base::is.finite(bundle$source_fit$draw_weights$weight)))
  expect_equal(
    base::sum(bundle$source_fit$draw_weights$weight),
    1,
    tolerance = 1e-12
  )
  expect_true(all(bundle$source_fit$chia$transport_sd > 0))
})


test_that("bundle validator catches broken compositions and unknown groups", {
  bundle <- calibrate_service_share_model(
    service_share_known_truth_fixture(),
    alpha_grid = c(0.5, 2),
    draws = 20L,
    seed = 11L
  )

  broken <- bundle
  broken$share_draws$share[[1L]] <- 1.5
  expect_error(
    validate_service_share_bundle(broken),
    "share"
  )

  broken <- bundle
  broken$share_draws$provider_group[[1L]] <- "unknown"
  expect_error(
    validate_service_share_bundle(broken),
    "provider_group"
  )
})
