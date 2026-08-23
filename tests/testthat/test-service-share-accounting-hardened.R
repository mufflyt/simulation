test_that("allocate_urps_service_workload enforces total service volume conservation across 100 random allocations", {
  .skip_unless_cms_service_share_data()
  service_registry <- build_urogynecology_service_registry()
  taxonomy_registry <- build_urogynecology_provider_taxonomy_registry()
  share_draws <- draw_compositional_service_shares(n_draws = 10L, seed = 123L)

  provider_cohort <- tibble::tibble(
    rendering_npi = paste0("NPI_", 1:10),
    provider_type = sample(taxonomy_registry$provider_type, 10, replace = TRUE),
    is_active = sample(c(TRUE, FALSE), 10, replace = TRUE, prob = c(0.8, 0.2)),
    status = "active"
  )

  service_demand <- tibble::tibble(
    service = sample(service_registry$service, 15, replace = TRUE),
    condition = "Pelvic Floor Disorder",
    demand_services = sample(100:5000, 15, replace = TRUE)
  )

  res <- allocate_urps_service_workload(
    service_demand = service_demand,
    provider_cohort = provider_cohort,
    share_draws = share_draws
  )

  audit <- res$accounting_audit
  expect_true(audit$accounting_passed)
  expect_true(audit$services_match)
  expect_equal(audit$inactive_provider_wrvu, 0.0)
  expect_gt(audit$total_urps_wrvu, 0.0)
})

test_that("allocate_urps_service_workload assigns zero wRVU to inactive or retired providers", {
  .skip_unless_cms_service_share_data()
  service_demand <- tibble::tibble(
    service = "Midurethral sling",
    condition = "Pelvic Floor Disorder",
    demand_services = 1000
  )

  provider_cohort <- tibble::tibble(
    rendering_npi = c("ACTIVE_1", "RETIRED_1"),
    provider_type = c("FPMRS physician", "FPMRS physician"),
    is_active = c(TRUE, FALSE),
    status = c("active", "retired")
  )

  res <- allocate_urps_service_workload(service_demand, provider_cohort)
  workload <- res$allocated_workload

  inactive_workload <- workload |> dplyr::filter(rendering_npi == "RETIRED_1")
  expect_equal(nrow(inactive_workload), 0)
})
