test_that("accounting validator accepts a conserved calibrated result", {
  bundle <- service_share_full_routing_fixture()
  result <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    initial_provider_count = 25L,
    fellowship_entrants = 1L,
    service_share_engine = "calibrated",
    service_share_bundle = bundle,
    service_share_draw = 1L,
    seed = 515L,
    save_outputs = FALSE
  )

  checks <- validate_service_share_accounting(result)
  expect_true(all(checks$passed))
  expect_true(all(checks$max_abs_error <= checks$tolerance))
})


test_that("accounting validator catches provider workload drift", {
  bundle <- service_share_full_routing_fixture()
  result <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    initial_provider_count = 20L,
    fellowship_entrants = 1L,
    service_share_engine = "calibrated",
    service_share_bundle = bundle,
    service_share_draw = 1L,
    seed = 616L,
    save_outputs = FALSE
  )
  result$service_share_provider_workload$annual_wrvu[[1L]] <-
    result$service_share_provider_workload$annual_wrvu[[1L]] + 10

  expect_error(
    validate_service_share_accounting(result),
    "provider work-RVU"
  )
})


test_that("CMS accounting identity is validated independently", {
  cms <- base::list(
    service_bounds = tibble::tibble(
      service = "sling_procedure",
      T_s = 100,
      U = 30,
      O = 20,
      N = 10,
      M = 40
    )
  )
  expect_true(all(validate_cms_service_share_accounting(cms)$passed))

  cms$service_bounds$M <- 39
  expect_error(
    validate_cms_service_share_accounting(cms),
    "T = U + O + N + M"
  )
})


test_that("reproducibility digest is stable and evidence-sensitive", {
  events <- service_share_known_truth_fixture()
  bundle_a <- calibrate_service_share_model(
    events,
    alpha_grid = c(0.5, 2),
    draws = 30L,
    seed = 77L
  )
  bundle_b <- calibrate_service_share_model(
    events,
    alpha_grid = c(0.5, 2),
    draws = 30L,
    seed = 77L
  )
  expect_identical(
    service_share_reproducibility_digest(bundle_a),
    service_share_reproducibility_digest(bundle_b)
  )

  changed <- bundle_b
  changed$provenance$events_sha256 <- "different-evidence"
  expect_false(base::identical(
    service_share_reproducibility_digest(bundle_a),
    service_share_reproducibility_digest(changed)
  ))
})


test_that("source-dropout evaluation labels all evidence configurations", {
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

  dropout <- evaluate_service_share_source_dropout(
    events,
    cms_evidence = cms,
    chia_evidence = chia,
    alpha_grid = c(0.5, 2),
    draws = 30L,
    seed = 88L
  )

  expect_setequal(
    unique(dropout$source_configuration),
    c("claims+cms", "claims+chia", "claims+cms+chia")
  )
  expect_true(all(dropout$mean_urps_share >= 0 & dropout$mean_urps_share <= 1))
  expect_true(all(dropout$p25_urps_share <= dropout$p75_urps_share))
})


test_that("provenance manifest exposes source hashes and model configuration", {
  bundle <- calibrate_service_share_model(
    service_share_known_truth_fixture(),
    alpha_grid = c(0.5, 2),
    draws = 10L,
    seed = 99L
  )
  manifest <- service_share_provenance_manifest(bundle)
  expect_true(all(c("key", "value") %in% names(manifest)))
  expect_true("events_sha256" %in% manifest$key)
  expect_true("seed" %in% manifest$key)
  expect_true("reproducibility_digest" %in% manifest$key)
})
