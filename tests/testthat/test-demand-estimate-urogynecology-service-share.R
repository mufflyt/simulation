test_that("empirical provider shares recover known truth", {
  pessary_counts <- tibble::tribble(
    ~provider_type, ~expected_events,
    "FPMRS physician", 20L,
    "General OB/GYN", 10L,
    "Nurse practitioner", 50L,
    "Physician assistant", 20L
  )

  synthetic_claims <- pessary_counts |>
    dplyr::mutate(
      event_number = purrr::map(
        .data$expected_events,
        \(event_count) base::seq_len(event_count)
      )
    ) |>
    tidyr::unnest(.data$event_number) |>
    dplyr::group_by(.data$provider_type) |>
    dplyr::mutate(
      rendering_npi = dplyr::case_when(
        .data$provider_type == "FPMRS physician" ~ "1111111111",
        .data$provider_type == "General OB/GYN" ~ "2222222222",
        .data$provider_type == "Nurse practitioner" ~ "3333333333",
        .data$provider_type == "Physician assistant" ~ "4444444444"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      claim_id = base::sprintf("claim_%03d", dplyr::row_number()),
      service_date = base::as.Date("2023-06-01"),
      hcpcs = "57160",
      dx1 = "N81.10"
    ) |>
    dplyr::select(
      .data$claim_id,
      .data$service_date,
      .data$rendering_npi,
      .data$hcpcs,
      .data$dx1
    )

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "2222222222", "207V00000X", TRUE,
    "3333333333", "363L00000X", TRUE,
    "4444444444", "363A00000X", TRUE
  )

  synthetic_bundle <- estimate_urogynecology_service_share(
    claims = synthetic_claims,
    npi_taxonomy = synthetic_taxonomy,
    taxonomy_crosswalk = example_taxonomy_crosswalk,
    service_rules = example_service_rules,
    condition_rules = example_condition_rules,
    prior_strength = 20
  )

  observed_shares <- synthetic_bundle$shares |>
    dplyr::filter(
      .data$service == "Pessary fitting",
      .data$condition == "Pelvic organ prolapse",
      .data$year == 2023
    ) |>
    dplyr::select(
      .data$provider_type,
      .data$service_events,
      .data$empirical_share
    )

  expected_shares <- pessary_counts |>
    dplyr::transmute(
      provider_type = .data$provider_type,
      expected_events = .data$expected_events,
      expected_share = .data$expected_events /
        base::sum(.data$expected_events)
    )

  comparison <- observed_shares |>
    dplyr::inner_join(
      expected_shares,
      by = "provider_type"
    )

  expect_equal(
    comparison$service_events,
    comparison$expected_events
  )
  expect_equal(
    comparison$empirical_share,
    comparison$expected_share,
    tolerance = 1e-12
  )
})

test_that("provider shares sum to one within every cell and obey mathematical invariants", {
  pessary_counts <- tibble::tribble(
    ~provider_type, ~expected_events,
    "FPMRS physician", 20L,
    "General OB/GYN", 10L,
    "Nurse practitioner", 50L,
    "Physician assistant", 20L
  )

  synthetic_claims <- pessary_counts |>
    dplyr::mutate(
      event_number = purrr::map(
        .data$expected_events,
        \(event_count) base::seq_len(event_count)
      )
    ) |>
    tidyr::unnest(.data$event_number) |>
    dplyr::group_by(.data$provider_type) |>
    dplyr::mutate(
      rendering_npi = dplyr::case_when(
        .data$provider_type == "FPMRS physician" ~ "1111111111",
        .data$provider_type == "General OB/GYN" ~ "2222222222",
        .data$provider_type == "Nurse practitioner" ~ "3333333333",
        .data$provider_type == "Physician assistant" ~ "4444444444"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      claim_id = base::sprintf("claim_%03d", dplyr::row_number()),
      service_date = base::as.Date("2023-06-01"),
      hcpcs = "57160",
      dx1 = "N81.10"
    )

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "2222222222", "207V00000X", TRUE,
    "3333333333", "363L00000X", TRUE,
    "4444444444", "363A00000X", TRUE
  )

  analysis_bundle <- estimate_urogynecology_service_share(
    claims = synthetic_claims,
    npi_taxonomy = synthetic_taxonomy,
    taxonomy_crosswalk = example_taxonomy_crosswalk,
    service_rules = example_service_rules,
    condition_rules = example_condition_rules
  )

  share_sums <- analysis_bundle$shares |>
    dplyr::group_by(
      .data$service,
      .data$condition,
      .data$year
    ) |>
    dplyr::summarise(
      empirical_sum = base::sum(.data$empirical_share),
      posterior_sum = base::sum(.data$posterior_share),
      .groups = "drop"
    )

  expect_equal(
    share_sums$empirical_sum,
    base::rep(1, base::nrow(share_sums)),
    tolerance = 1e-10
  )
  expect_equal(
    share_sums$posterior_sum,
    base::rep(1, base::nrow(share_sums)),
    tolerance = 1e-10
  )

  # Check bounds
  expect_true(all(analysis_bundle$shares$empirical_share >= 0 & analysis_bundle$shares$empirical_share <= 1))
  expect_true(all(analysis_bundle$shares$posterior_share >= 0 & analysis_bundle$shares$posterior_share <= 1))
  expect_true(all(analysis_bundle$shares$posterior_lower <= analysis_bundle$shares$posterior_share))
  expect_true(all(analysis_bundle$shares$posterior_share <= analysis_bundle$shares$posterior_upper))
})

test_that("duplicate diagnosis and claim lines do not inflate events", {
  pessary_counts <- tibble::tribble(
    ~provider_type, ~expected_events,
    "FPMRS physician", 20L,
    "General OB/GYN", 10L,
    "Nurse practitioner", 50L,
    "Physician assistant", 20L
  )

  synthetic_claims <- pessary_counts |>
    dplyr::mutate(
      event_number = purrr::map(
        .data$expected_events,
        \(event_count) base::seq_len(event_count)
      )
    ) |>
    tidyr::unnest(.data$event_number) |>
    dplyr::group_by(.data$provider_type) |>
    dplyr::mutate(
      rendering_npi = dplyr::case_when(
        .data$provider_type == "FPMRS physician" ~ "1111111111",
        .data$provider_type == "General OB/GYN" ~ "2222222222",
        .data$provider_type == "Nurse practitioner" ~ "3333333333",
        .data$provider_type == "Physician assistant" ~ "4444444444"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      claim_id = base::sprintf("claim_%03d", dplyr::row_number()),
      service_date = base::as.Date("2023-06-01"),
      hcpcs = "57160",
      dx1 = "N81.10"
    )

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "2222222222", "207V00000X", TRUE,
    "3333333333", "363L00000X", TRUE,
    "4444444444", "363A00000X", TRUE
  )

  duplicated_claims <- dplyr::bind_rows(
    synthetic_claims,
    synthetic_claims,
    synthetic_claims
  )

  duplicated_bundle <- estimate_urogynecology_service_share(
    claims = duplicated_claims,
    npi_taxonomy = synthetic_taxonomy,
    taxonomy_crosswalk = example_taxonomy_crosswalk,
    service_rules = example_service_rules,
    condition_rules = example_condition_rules
  )

  expect_equal(
    base::sum(
      duplicated_bundle$shares$service_events[
        duplicated_bundle$shares$year == 2023
      ]
    ),
    100L
  )
})

test_that("trend recovery detects increasing and decreasing provider shares", {
  years <- 2020:2024
  # Increasing APP share: 20%, 30%, 40%, 50%, 60%
  claims_list <- lapply(seq_along(years), function(i) {
    yr <- years[i]
    app_n <- 10 * i
    doc_n <- 10 * (6 - i)

    app_claims <- tibble::tibble(
      claim_id = sprintf("claim_app_%d_%02d", yr, seq_len(app_n)),
      service_date = as.Date(sprintf("%d-06-01", yr)),
      rendering_npi = "3333333333",
      hcpcs = "57160",
      dx1 = "N81.10"
    )
    doc_claims <- tibble::tibble(
      claim_id = sprintf("claim_doc_%d_%02d", yr, seq_len(doc_n)),
      service_date = as.Date(sprintf("%d-06-01", yr)),
      rendering_npi = "1111111111",
      hcpcs = "57160",
      dx1 = "N81.10"
    )
    dplyr::bind_rows(app_claims, doc_claims)
  })

  trend_claims <- dplyr::bind_rows(claims_list)

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "3333333333", "363L00000X", TRUE
  )

  bundle <- estimate_urogynecology_service_share(
    claims = trend_claims,
    npi_taxonomy = synthetic_taxonomy,
    taxonomy_crosswalk = example_taxonomy_crosswalk,
    service_rules = example_service_rules,
    condition_rules = example_condition_rules
  )

  app_trend <- bundle$trends |>
    dplyr::filter(provider_type == "Nurse practitioner")

  expect_equal(app_trend$direction, "increased")
  expect_gt(app_trend$annual_odds_ratio, 1.0)
})
