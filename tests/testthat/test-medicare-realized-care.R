test_that("Medicare realized-care aggregation keeps its FFS estimand distinct", {
  claims <- tibble::tibble(
    year = c(2022L, 2022L, 2023L, 2023L), HCPCS_Cd = c("57288", "52000", "99999", "57288"),
    Tot_Srvcs = c(12, 8, 4, 20), Rndrng_Prvdr_State_Abrvtn = c("CO", "CO", "CO", "UT"),
    Rndrng_Prvdr_Type = "Obstetrics & Gynecology", Place_Of_Srvc = "O",
    Rndrng_NPI = c("a", "a", "b", "c")
  )
  out <- urpssim:::aggregate_medicare_realized_care(claims)
  expect_equal(sum(out$billed_services), 40)
  expect_equal(attr(out, "unmapped_service_fraction"), 4 / 44)
  expect_true(all(grepl("not latent all-payer", out$estimand, fixed = TRUE)))
  expect_true(all(c("state", "provider_type", "place_of_service", "billing_npis") %in% names(out)))
})

test_that("default Medicare crosswalk excludes generic E/M visit codes", {
  crosswalk <- urpssim:::urps_medicare_service_crosswalk()
  expect_false(any(grepl("^99", crosswalk$hcpcs)))
  expect_true("57288" %in% crosswalk$hcpcs)
  expect_true("99213" %in% urpssim:::urps_medicare_service_crosswalk(include_non_specific = TRUE)$hcpcs)
})

test_that("realized-care comparison does not produce a latent-demand scalar", {
  observed <- tibble::tibble(service = c("sling_procedure", "cystoscopy"), billed_services = c(20, 10))
  predicted <- tibble::tibble(service = c("sling_procedure", "cystoscopy"), predicted_services = c(10, 20))
  out <- urpssim:::compare_medicare_realized_care(predicted, observed)
  expect_equal(out$calibration_ratio[out$service == "sling_procedure"], 2)
  expect_equal(out$calibration_ratio[out$service == "cystoscopy"], .5)
  expect_true(all(grepl("not a latent", out$comparison_estimand, fixed = TRUE)))
})
