service_share_full_routing_fixture <- function() {
  services <- c(
    "sling_procedure",
    "prolapse_surgery",
    "sacral_neuromodulation",
    "botox_injection",
    "ptns_procedure",
    "urodynamics",
    "pessary_fitting",
    "cystoscopy",
    "bladder_instillation",
    "new_consultation",
    "return_visit"
  )
  shares <- tibble::tribble(
    ~provider_group, ~share,
    "urps", 0.50,
    "app", 0.20,
    "general_obgyn", 0.30
  )
  draw_tbl <- tidyr::crossing(
    service = services,
    year = 2024L,
    draw_id = 1L,
    condition = "all",
    shares
  ) |>
    dplyr::mutate(
      source_draw_id = 1L,
      cell_events = 100,
      selected_alpha = 2
    )

  base::list(
    share_draws = draw_tbl,
    selected_alpha = tibble::tibble(
      service = services,
      holdout_year = 2024L,
      selected_alpha = 2,
      log_score = -100,
      holdout_events = 100,
      cross_entropy = 1
    ),
    holdout_scores = tibble::tibble(
      service = services,
      holdout_year = 2024L,
      alpha = 2,
      log_score = -100,
      holdout_events = 100,
      cross_entropy = 1
    ),
    source_fit = base::list(
      cms = tibble::tibble(),
      chia = tibble::tibble(),
      draw_weights = tibble::tibble(draw_id = 1L, weight = 1)
    ),
    provenance = base::list(events_sha256 = "fixture"),
    config = base::list(
      seed = 1L,
      draws = 1L,
      projection_policy = "carry_forward_latest",
      provider_groups = provider_routing_groups()
    ),
    valid = TRUE
  )
}


test_that("calibrated routing carries latest evidence forward without backcast", {
  bundle <- service_share_full_routing_fixture()
  routing <- service_share_routing_for_year(
    bundle,
    year = 2026L,
    draw_id = 1L,
    required_services = service_share_required_routing_services()
  )

  expect_true(all(routing$evidence_year == 2024L))
  expect_true(all(routing$draw_id == 1L))
  sums <- routing |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      probability = base::sum(.data$probability),
      .groups = "drop"
    )
  expect_equal(sums$probability, rep(1, nrow(sums)), tolerance = 1e-12)

  expect_error(
    service_share_routing_for_year(bundle, year = 2023L, draw_id = 1L),
    "backcast"
  )
})


test_that("calibrated routing fails if a pathway service lacks evidence", {
  bundle <- service_share_full_routing_fixture()
  bundle$share_draws <- bundle$share_draws |>
    dplyr::filter(.data$service != "return_visit")
  bundle$selected_alpha <- bundle$selected_alpha |>
    dplyr::filter(.data$service != "return_visit")
  bundle$holdout_scores <- bundle$holdout_scores |>
    dplyr::filter(.data$service != "return_visit")

  expect_error(
    service_share_routing_for_year(
      bundle,
      year = 2025L,
      draw_id = 1L,
      required_services = service_share_required_routing_services()
    ),
    "missing calibrated evidence"
  )
})


test_that("service workload bridge uses CMS RVUs including sacral neuromodulation", {
  workload <- service_share_routing_workload()
  expect_true(all(service_share_required_routing_services() %in% workload$service))
  snm <- workload |>
    dplyr::filter(.data$service == "sacral_neuromodulation")
  expect_equal(nrow(snm), 1L)
  expect_equal(snm$work_rvu, 12.20, tolerance = 1e-12)
  expect_match(snm$source, "64581")
})


test_that("URPS workload conserves service volume and work RVUs", {
  routed <- tibble::tribble(
    ~year, ~service, ~provider_group, ~provider_volume,
    2025L, "sling_procedure", "urps", 10,
    2025L, "sling_procedure", "app", 5,
    2025L, "pessary_fitting", "urps", 20,
    2025L, "pessary_fitting", "general_obgyn", 10
  )
  workload <- tibble::tribble(
    ~service, ~work_rvu, ~source,
    "sling_procedure", 12, "fixture",
    "pessary_fitting", 1, "fixture"
  )

  result <- allocate_urps_service_workload(routed, workload = workload)
  expect_equal(result$total_urps_services, 30)
  expect_equal(result$total_urps_wrvu, 140)
  expect_equal(sum(result$service_workload$urps_volume), 30)
  expect_equal(sum(result$service_workload$work_rvu_total), 140)
})


test_that("provider allocation uses active providers only and closes", {
  providers <- tibble::tribble(
    ~provider_id, ~fte, ~active,
    "a", 1.0, TRUE,
    "b", 0.5, TRUE,
    "c", 1.0, FALSE
  )
  allocation <- allocate_urps_workload_to_active_providers(
    providers,
    total_urps_wrvu = 1500,
    year = 2025L
  )

  expect_setequal(allocation$provider_id, c("a", "b"))
  expect_equal(sum(allocation$annual_wrvu), 1500, tolerance = 1e-10)
  expect_equal(
    allocation$annual_wrvu[allocation$provider_id == "a"],
    1000,
    tolerance = 1e-10
  )
})
