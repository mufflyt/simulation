test_that("canonical service-share registry is production-grade", {
  registry <- urogynecology_service_share_registry()
  taxonomy <- urogynecology_provider_taxonomy_registry()

  expect_invisible(validate_service_share_registry(registry, strict = TRUE))
  expect_false(anyDuplicated(registry$hcpcs) > 0L)
  expect_true(all(nzchar(registry$source_id)))
  expect_true(all(nzchar(registry$source_citation)))
  expect_true(all(registry$evidence_quality %in% c("A", "B")))
  expect_true(all(registry$calibration_role %in% c("primary", "secondary")))

  tier_a <- registry |>
    dplyr::filter(.data$cms_tier == "A")
  expect_equal(base::nrow(tier_a), 13L)
  expect_true(all(tier_a$sex_specific))
  expect_false(any(tier_a$service %in% c(
    "new_consultation", "return_visit", "postoperative_care"
  )))

  urps_codes <- taxonomy |>
    dplyr::filter(.data$provider_group == "urps") |>
    dplyr::pull(.data$taxonomy_code)
  expect_setequal(urps_codes, c("207VF0040X", "2088F0040X"))
})


test_that("strict registry rejects example or unsourced active rules", {
  bad <- urogynecology_service_share_registry()
  bad$evidence_status[[1L]] <- "example"
  expect_error(
    validate_service_share_registry(bad, strict = TRUE),
    "example"
  )

  bad <- urogynecology_service_share_registry()
  bad$source_citation[[1L]] <- ""
  expect_error(
    validate_service_share_registry(bad, strict = TRUE),
    "source_citation"
  )
})


test_that("CMS builder closes suppression accounting and computes bounds", {
  service_registry <- urogynecology_service_share_registry() |>
    dplyr::filter(.data$hcpcs == "57160")

  provider_service <- tibble::tribble(
    ~Rndrng_NPI, ~Rndrng_Prvdr_Type, ~HCPCS_Cd, ~Tot_Srvcs,
    "1111111111", "Obstetrics & Gynecology", "57160", 30,
    "2222222222", "Urology", "57160", 20,
    "3333333333", "Nurse Practitioner", "57160", 10
  )
  geography_service <- tibble::tribble(
    ~Rndrng_Prvdr_Geo_Lvl, ~HCPCS_Cd, ~Tot_Srvcs,
    "National", "57160", 100
  )
  roster <- tibble::tribble(
    ~npi,
    "1111111111"
  )
  provider_type_map <- tibble::tribble(
    ~cms_provider_type, ~provider_class,
    "Obstetrics & Gynecology", "physician",
    "Urology", "physician",
    "Nurse Practitioner", "nonphysician"
  )

  evidence <- build_cms_service_share_evidence(
    provider_service = provider_service,
    geography_service = geography_service,
    roster = roster,
    provider_type_map = provider_type_map,
    service_registry = service_registry,
    workload = tibble::tibble(
      service = "pessary_care",
      work_rvu = 0.89
    )
  )

  row <- evidence$service_bounds
  expect_equal(row$T_s, 100)
  expect_equal(row$U, 30)
  expect_equal(row$O, 20)
  expect_equal(row$N, 10)
  expect_equal(row$M, 40)
  expect_equal(row$U + row$O + row$N + row$M, row$T_s)
  expect_equal(row$lower_bound, 30 / 90)
  expect_equal(row$upper_bound, 70 / 90)
  expect_equal(row$observed_cell_share, 30 / 50)
  expect_equal(row$capture_share, 60 / 100)
  expect_equal(evidence$aggregate_bounds$lower_bound, 30 / 90)
  expect_equal(evidence$aggregate_bounds$upper_bound, 70 / 90)
})


test_that("roster membership overrides a conflicting CMS provider class", {
  provider_service <- tibble::tribble(
    ~Rndrng_NPI, ~Rndrng_Prvdr_Type, ~HCPCS_Cd, ~Tot_Srvcs,
    "1111111111", "Nurse Practitioner", "57160", 30
  )
  geography_service <- tibble::tribble(
    ~Rndrng_Prvdr_Geo_Lvl, ~HCPCS_Cd, ~Tot_Srvcs,
    "National", "57160", 50
  )
  roster <- tibble::tibble(npi = "1111111111")
  provider_type_map <- tibble::tribble(
    ~cms_provider_type, ~provider_class,
    "Nurse Practitioner", "nonphysician"
  )

  evidence <- build_cms_service_share_evidence(
    provider_service,
    geography_service,
    roster,
    provider_type_map,
    service_registry = urogynecology_service_share_registry() |>
      dplyr::filter(.data$hcpcs == "57160"),
    workload = tibble::tibble(
      service = "pessary_care",
      work_rvu = 0.89
    )
  )

  expect_equal(evidence$service_bounds$U, 30)
  expect_equal(evidence$service_bounds$N, 0)
  expect_equal(evidence$diagnostics$roster_nonphysician_services, 30)
})


test_that("CMS builder fails closed on unknown provider types and negative M", {
  registry <- urogynecology_service_share_registry() |>
    dplyr::filter(.data$hcpcs == "57160")
  roster <- tibble::tibble(npi = character())

  unknown_provider <- tibble::tribble(
    ~Rndrng_NPI, ~Rndrng_Prvdr_Type, ~HCPCS_Cd, ~Tot_Srvcs,
    "2222222222", "New CMS Type", "57160", 20
  )
  geo <- tibble::tribble(
    ~Rndrng_Prvdr_Geo_Lvl, ~HCPCS_Cd, ~Tot_Srvcs,
    "National", "57160", 100
  )
  map <- tibble::tribble(
    ~cms_provider_type, ~provider_class,
    "Urology", "physician"
  )

  expect_error(
    build_cms_service_share_evidence(
      unknown_provider, geo, roster, map,
      service_registry = registry,
      workload = tibble::tibble(
        service = "pessary_care", work_rvu = 0.89
      )
    ),
    "unmapped CMS provider type"
  )

  too_large <- tibble::tribble(
    ~Rndrng_NPI, ~Rndrng_Prvdr_Type, ~HCPCS_Cd, ~Tot_Srvcs,
    "2222222222", "Urology", "57160", 120
  )
  expect_error(
    build_cms_service_share_evidence(
      too_large, geo, roster, map,
      service_registry = registry,
      workload = tibble::tibble(
        service = "pessary_care", work_rvu = 0.89
      )
    ),
    "negative unidentified volume"
  )
})
