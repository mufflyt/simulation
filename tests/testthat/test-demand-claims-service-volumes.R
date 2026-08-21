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
    dplyr::select(.data$claim_id, .data$service_date, .data$rendering_npi, .data$hcpcs, .data$dx1)

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
    dplyr::select(.data$provider_type, .data$service_events, .data$empirical_share)

  expected_shares <- pessary_counts |>
    dplyr::transmute(
      provider_type = .data$provider_type,
      expected_events = .data$expected_events,
      expected_share = .data$expected_events / base::sum(.data$expected_events)
    )

  comparison <- observed_shares |>
    dplyr::inner_join(expected_shares, by = "provider_type")

  expect_equal(comparison$service_events, comparison$expected_events)
  expect_equal(comparison$empirical_share, comparison$expected_share, tolerance = 1e-12)
})

test_that("provider shares sum to one within every cell", {
  synthetic_claims <- tibble::tribble(
    ~claim_id, ~service_date, ~rendering_npi, ~hcpcs, ~dx1,
    "C1", "2023-01-10", "1111111111", "57160", "N81.10",
    "C2", "2023-02-10", "2222222222", "57160", "N81.10"
  )

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE,
    "2222222222", "207V00000X", TRUE
  )

  analysis_bundle <- estimate_urogynecology_service_share(
    claims = synthetic_claims,
    npi_taxonomy = synthetic_taxonomy
  )

  share_sums <- analysis_bundle$shares |>
    dplyr::group_by(.data$service, .data$condition, .data$year) |>
    dplyr::summarise(
      empirical_sum = base::sum(.data$empirical_share),
      posterior_sum = base::sum(.data$posterior_share),
      .groups = "drop"
    )

  expect_equal(share_sums$empirical_sum, base::rep(1, base::nrow(share_sums)), tolerance = 1e-10)
  expect_equal(share_sums$posterior_sum, base::rep(1, base::nrow(share_sums)), tolerance = 1e-10)
})

test_that("duplicate diagnosis and claim lines do not inflate events", {
  synthetic_claims <- tibble::tribble(
    ~claim_id, ~service_date, ~rendering_npi, ~hcpcs, ~dx1,
    "C1", "2023-01-10", "1111111111", "57160", "N81.10"
  )

  synthetic_taxonomy <- tibble::tribble(
    ~rendering_npi, ~taxonomy_code, ~is_primary,
    "1111111111", "207VF0040X", TRUE
  )

  duplicated_claims <- dplyr::bind_rows(synthetic_claims, synthetic_claims, synthetic_claims)

  duplicated_bundle <- estimate_urogynecology_service_share(
    claims = duplicated_claims,
    npi_taxonomy = synthetic_taxonomy
  )

  expect_equal(
    base::sum(duplicated_bundle$shares$service_events[duplicated_bundle$shares$year == 2023]),
    1
  )
})
