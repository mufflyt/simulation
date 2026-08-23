test_that("allocate_urps_service_workload passes all critical accounting identities", {
  .skip_unless_cms_service_share_data()
  service_demand <- tibble::tibble(
    service = c("Midurethral sling", "Pessary fitting"),
    condition = "Pelvic Floor Disorder",
    demand_services = c(500, 1000)
  )

  provider_cohort <- tibble::tibble(
    rendering_npi = c("NPI1", "NPI2", "NPI3", "NPI4"),
    provider_type = c("FPMRS physician", "General OB/GYN", "Urologist", "Nurse practitioner"),
    is_active = c(TRUE, TRUE, TRUE, FALSE),
    status = c("active", "active", "active", "retired")
  )

  res <- allocate_urps_service_workload(service_demand, provider_cohort)

  expect_type(res, "list")
  expect_true(res$accounting_audit$services_match)
  expect_equal(res$accounting_audit$inactive_provider_wrvu, 0)
  expect_true(res$accounting_audit$accounting_passed)
})
