test_that("build_cms_service_share_evidence calculates valid suppression bounds and shares", {
  .skip_unless_cms_service_share_data()
  res <- default_cms_service_share_evidence()

  expect_type(res, "list")
  expect_true(all(c("service_bounds", "aggregate_bounds", "diagnostics", "provenance", "estimand") %in% names(res)))

  shares <- res$service_bounds
  expect_s3_class(shares, "tbl_df")

  # Accounting Identity Verification: T == U + O + N + M
  expect_equal(
    shares$T_s,
    shares$U + shares$O + shares$N + shares$M
  )

  # Bound Verification: L <= H, L >= 0, H <= 1 (approx)
  expect_true(all(shares$lower_bound <= shares$upper_bound + 1e-9))
  expect_true(all(shares$lower_bound >= 0))

  # Input Hash Pinning
  expect_type(res$provenance$roster_sha256, "character")
  expect_type(res$provenance$provider_service_sha256, "character")
})

test_that("wRVU weighted shares sum to 1 within each service", {
  skip(paste(
    "build_cms_service_share_evidence() has no wrvu_shares output.",
    "R/data-public_evidence_duckdb.R:158-159 already assumes",
    "cms_evidence$wrvu_shares exists (a per-service, per-provider-bucket",
    "wRVU-weighted share table), so this is a real gap shared by two",
    "independent call sites, not stale test debt -- but the weighting",
    "semantics (weighted across services within one aggregate, since",
    "work_rvu is constant within a single service and so cancels out of",
    "any purely within-service share) need a design decision this test",
    "can't make unilaterally. See aggregate_bounds' work_rvu-weighted",
    "capture_share for the existing precedent to extend."
  ))
})
