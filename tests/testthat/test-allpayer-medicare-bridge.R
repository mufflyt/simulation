# CHIA all-payer <-> Medicare FFS workload bridge. These tests run the whole
# pipeline (build -> join -> fit -> project -> age curve) on synthetic in-memory
# claims, so they exercise the contract without the external CHIA/Medicare files
# or the optional arrow reader. A known all-payer/Medicare multiplier is planted
# and recovered.

sd_bridge_claims <- function(seed = 1L, n_prov = 120L, multiplier = 1.8) {
  set.seed(seed)
  npi <- sprintf("1%09d", seq_len(n_prov))
  age <- 40L + (seq_len(n_prov) %% 31L)            # spreads 40-70; >=20 in 45-54
  sex <- rep(c("F", "M"), length.out = n_prov)
  base_wrvu <- stats::runif(n_prov, 120, 320)      # Medicare wRVU per provider-year

  mk <- function(scale, source_tag) {
    rows <- lapply(seq_len(n_prov), function(i) {
      do.call(rbind, lapply(c(2018L, 2019L), function(y) {
        k <- 6L
        target <- base_wrvu[i] * scale * stats::runif(1, 0.9, 1.1)
        data.frame(
          npi = npi[i], year = y, provider_age = age[i],
          provider_sex = sex[i], provider_state = "MA",
          wrvu = rep(target / k, k), units = 1L,
          hcpcs = rep(c("57288", "57260"), length.out = k),
          patient_id = paste0("pt", (i * 10L):(i * 10L + k - 1L)),
          stringsAsFactors = FALSE
        )
      }))
    })
    tibble::as_tibble(do.call(rbind, rows))
  }

  list(
    chia = mk(multiplier, "chia"),   # all-payer = multiplier x Medicare
    medicare = mk(1, "medicare")
  )
}

test_that("normalize_npi keeps 10-digit ids and nulls the rest", {
  out <- normalize_npi(c("1234567890", "12-345-67890", "999", NA, "abc1234567"))
  expect_equal(out[1], "1234567890")
  expect_true(is.na(out[3]))          # too short
  expect_true(is.na(out[4]))          # NA in
})

test_that("build_claims_provider_year aggregates lines to provider-years", {
  fx <- sd_bridge_claims()
  py <- suppressMessages(build_claims_provider_year(fx$medicare, "Medicare_FFS"))
  expect_true(all(c("npi", "year", "total_wrvu", "provider_age") %in% names(py)))
  expect_equal(nrow(py), 120L * 2L)   # one row per provider-year
  expect_true(all(py$total_wrvu > 0))
})

test_that("the planted all-payer/Medicare multiplier is recovered", {
  fx <- sd_bridge_claims(multiplier = 1.8)
  chia_py <- suppressMessages(build_chia_provider_year(fx$chia))
  med_py  <- suppressMessages(build_medicare_provider_year(fx$medicare))
  overlap <- suppressMessages(join_chia_medicare_overlap(chia_py, med_py))
  expect_equal(nrow(overlap), 120L * 2L)

  fit <- suppressMessages(fit_chia_medicare_bridge(overlap))
  expect_equal(fit$workload_metric, "wrvu")
  expect_equal(fit$calibration_status, "measured_input_unvalidated_response")
  # Planted 1.8x, recovered within noise.
  expect_gt(fit$ratio_summary$median_ratio, 1.5)
  expect_lt(fit$ratio_summary$median_ratio, 2.1)
})

test_that("national projection and age curve are well-formed", {
  fx <- sd_bridge_claims()
  chia_py <- suppressMessages(build_chia_provider_year(fx$chia))
  med_py  <- suppressMessages(build_medicare_provider_year(fx$medicare))
  overlap <- suppressMessages(join_chia_medicare_overlap(chia_py, med_py))
  fit <- suppressMessages(fit_chia_medicare_bridge(overlap))

  projected <- suppressMessages(predict_allpayer_from_medicare(fit, med_py))
  expect_true(all(projected$estimated_allpayer_workload > 0))
  expect_true(all(c("estimated_allpayer_low", "estimated_allpayer_high",
                    "allpayer_medicare_multiplier") %in% names(projected)))
  expect_equal(unique(projected$calibration_status),
               "measured_input_unvalidated_response")

  age_curve <- suppressMessages(estimate_workload_age_curve(projected))
  expect_true("relative_workload" %in% names(age_curve))
  # 45-54 is the normalization reference, so its relative workload is exactly 1.
  ref <- age_curve$relative_workload[age_curve$age_group == "45-54"]
  expect_equal(ref, 1)
})

test_that("filter_claims_to_urps keeps only roster NPIs and auto-detects the column", {
  fx <- sd_bridge_claims(n_prov = 40L)
  roster <- data.frame(npi = sprintf("1%09d", 1:20), stringsAsFactors = FALSE)
  kept <- suppressMessages(filter_claims_to_urps(fx$medicare, roster))
  kept_npi <- unique(normalize_npi(kept$npi))
  expect_true(all(kept_npi %in% roster$npi))
  expect_true(all(sprintf("1%09d", 21:40) %in% kept$npi == FALSE))
})
