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


service_share_full_routing_fixture <- function() {
  services <- service_share_required_routing_services()
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
