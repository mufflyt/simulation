testthat::test_that("provider routing priors sum to one", {
  prior_tbl <- provider_routing_prior()

  sums_tbl <- prior_tbl |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      share_sum = base::sum(.data$prior_mean),
      .groups = "drop"
    )

  testthat::expect_true(
    base::all(base::abs(sums_tbl$share_sum - 1) < 1e-12)
  )
  testthat::expect_true(
    base::all(prior_tbl$alpha_prior > 0)
  )
})

testthat::test_that("historical surgical priors preserve published shares", {
  prior_tbl <- provider_routing_prior(
    surgical_strength = 20
  )

  sling_tbl <- prior_tbl |>
    dplyr::filter(
      .data$service == "sling_procedure",
      .data$provider_group %in% base::c(
        "urps",
        "general_urology",
        "general_obgyn",
        "other"
      )
    ) |>
    dplyr::select(
      "provider_group",
      "prior_mean"
    )

  urps_share <- sling_tbl |>
    dplyr::filter(.data$provider_group == "urps") |>
    dplyr::pull(.data$prior_mean)

  testthat::expect_equal(
    urps_share,
    0.606,
    tolerance = 1e-04
  )
})

testthat::test_that("Dirichlet routing draws conserve probability", {
  posterior_tbl <- provider_routing_prior() |>
    dplyr::filter(.data$service == "sling_procedure") |>
    dplyr::transmute(
      geography = "US",
      service = .data$service,
      provider_group = .data$provider_group,
      alpha_posterior = .data$alpha_prior + 10,
      evidence_status = "cms_ffs_updated"
    )

  draw_tbl <- draw_provider_routing(
    posterior = posterior_tbl,
    draws = 100,
    seed = 123
  )

  sums_tbl <- draw_tbl |>
    dplyr::group_by(.data$draw) |>
    dplyr::summarise(
      share_sum = base::sum(.data$probability),
      .groups = "drop"
    )

  testthat::expect_true(
    base::all(base::abs(sums_tbl$share_sum - 1) < 1e-10)
  )
})

testthat::test_that("provider routing conserves service volume", {
  posterior_tbl <- tibble::tribble(
    ~geography, ~service, ~provider_group,
    ~posterior_mean, ~evidence_status,
    "US", "sling_procedure", "urps",
    0.60, "cms_ffs_updated",
    "US", "sling_procedure", "general_urology",
    0.25, "cms_ffs_updated",
    "US", "sling_procedure", "general_obgyn",
    0.15, "cms_ffs_updated"
  )

  volume_tbl <- tibble::tibble(
    year = 2030,
    service = "sling_procedure",
    volume = 100
  )

  routed_tbl <- apply_provider_routing(
    service_volume = volume_tbl,
    routing = posterior_tbl
  )

  testthat::expect_equal(
    base::sum(routed_tbl$provider_volume),
    100,
    tolerance = 1e-10
  )

  urps_volume <- routed_tbl |>
    dplyr::filter(.data$provider_group == "urps") |>
    dplyr::pull(.data$provider_volume)

  testthat::expect_equal(urps_volume, 60)
})

testthat::test_that("prior-only routing remains unresolved by default", {
  posterior_tbl <- tibble::tribble(
    ~geography, ~service, ~provider_group,
    ~posterior_mean, ~evidence_status,
    "US", "new_consultation", "urps",
    0.50, "prior_only",
    "US", "new_consultation", "general_obgyn",
    0.50, "prior_only"
  )

  volume_tbl <- tibble::tibble(
    year = 2030,
    service = "new_consultation",
    volume = 200
  )

  routed_tbl <- apply_provider_routing(
    service_volume = volume_tbl,
    routing = posterior_tbl,
    prior_only = "unresolved"
  )

  testthat::expect_equal(
    base::unique(routed_tbl$provider_group),
    "unresolved"
  )
  testthat::expect_equal(
    base::sum(routed_tbl$provider_volume),
    200
  )
})
