# build_cms_service_share_evidence()'s real inputs are the 2024 CMS
# Provider-and-Service / Geography PUFs, which this checkout does not vendor
# (see .skip_unless_cms_service_share_data()'s fixture-based tests above and
# tests/testthat/test-calibration-service-shares.R). These two tests instead
# build a small SYNTHETIC provider_service/geography_service/roster/
# provider_type_map fixture matching the real function's contract, so the
# suppression-accounting math (T = U + O + N + M, L <= H) gets exercised on
# every checkout regardless of whether the real PUFs are present.
#
# National totals are built as a multiple of the summed provider-level
# volumes (not independently random) so M = T - U - O - N is non-negative by
# construction -- a fixture that could accidentally produce negative M would
# be testing .cms_require_columns()'s own stop(), not the bounds logic.
.synthetic_cms_service_share_fixture <- function(seed, national_multiplier = 3) {
  withr::local_seed(seed)
  registry <- urogynecology_service_share_registry()
  hcpcs_codes <- unique(registry$hcpcs)

  provider_type_map <- tibble::tribble(
    ~cms_provider_type, ~provider_class,
    "Obstetrics & Gynecology", "physician",
    "Urology", "physician",
    "Family Practice", "physician",
    "Nurse Practitioner", "nonphysician",
    "Physician Assistant", "nonphysician"
  )

  urps_npis <- sprintf("1%09d", 1:5)
  other_physician_npis <- sprintf("2%09d", 1:5)
  nonphysician_npis <- sprintf("3%09d", 1:5)

  provider_service <- tibble::tibble(
    Rndrng_NPI = sample(
      c(urps_npis, other_physician_npis, nonphysician_npis),
      500, replace = TRUE
    ),
    HCPCS_Cd = sample(hcpcs_codes, 500, replace = TRUE),
    Tot_Srvcs = sample(c(0, 1, 10, 100, 1000), 500, replace = TRUE)
  ) |>
    dplyr::mutate(
      Rndrng_Prvdr_Type = dplyr::case_when(
        Rndrng_NPI %in% c(urps_npis, other_physician_npis) ~
          sample(c("Obstetrics & Gynecology", "Urology", "Family Practice"), dplyr::n(), replace = TRUE),
        TRUE ~ sample(c("Nurse Practitioner", "Physician Assistant"), dplyr::n(), replace = TRUE)
      )
    )

  provider_totals <- provider_service |>
    dplyr::group_by(HCPCS_Cd) |>
    dplyr::summarise(services = sum(Tot_Srvcs), .groups = "drop")

  geography_service <- tibble::tibble(HCPCS_Cd = hcpcs_codes) |>
    dplyr::left_join(provider_totals, by = "HCPCS_Cd") |>
    dplyr::mutate(
      services = tidyr::replace_na(services, 0),
      Rndrng_Prvdr_Geo_Lvl = "National",
      Tot_Srvcs = services * national_multiplier + 10
    ) |>
    dplyr::select(Rndrng_Prvdr_Geo_Lvl, HCPCS_Cd, Tot_Srvcs)

  roster <- tibble::tibble(npi = urps_npis)

  list(
    provider_service = provider_service,
    geography_service = geography_service,
    roster = roster,
    provider_type_map = provider_type_map
  )
}

test_that("build_cms_service_share_evidence holds mathematical bounds L <= H, L >= 0, H <= 1 across noisy inputs", {
  fx <- .synthetic_cms_service_share_fixture(seed = 20260823L)

  res <- build_cms_service_share_evidence(
    provider_service = fx$provider_service,
    geography_service = fx$geography_service,
    roster = fx$roster,
    provider_type_map = fx$provider_type_map
  )
  bounds <- res$service_bounds

  # 1. Suppression accounting identity: T = U + O + N + M
  expect_equal(bounds$T_s, bounds$U + bounds$O + bounds$N + bounds$M)

  # 2. Lower bound <= Upper bound
  expect_true(all(bounds$lower_bound <= bounds$upper_bound + 1e-9))

  # 3. Bounds non-negativity
  expect_true(all(bounds$lower_bound >= 0.0))
  expect_true(all(bounds$upper_bound >= 0.0))

  # 4. Upper bound does not exceed 1 (a share, not a raw count)
  expect_true(all(bounds$upper_bound <= 1 + 1e-9))
})

test_that("build_cms_service_share_evidence handles all-zero provider volumes gracefully", {
  # "Empty claims" in the real function's contract means every provider-level
  # cell is zero, not zero ROWS -- geography_service must still cover every
  # registry HCPCS code (real CMS Geography files always do), so a genuinely
  # empty provider_service is the "noisy but valid" edge case, not a
  # zero-row data frame the function was never designed to accept.
  fx <- .synthetic_cms_service_share_fixture(seed = 20260823L)
  zero_provider_service <- fx$provider_service
  zero_provider_service$Tot_Srvcs <- 0

  res <- build_cms_service_share_evidence(
    provider_service = zero_provider_service,
    geography_service = fx$geography_service,
    roster = fx$roster,
    provider_type_map = fx$provider_type_map
  )
  expect_type(res, "list")
  expect_s3_class(res$service_bounds, "tbl_df")
  expect_true(all(res$service_bounds$U == 0))
  expect_true(all(res$service_bounds$lower_bound == 0))
})
