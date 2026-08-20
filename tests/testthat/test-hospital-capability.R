test_that("hcris_urps_capability_spec returns required cost centers", {
  spec <- hcris_urps_capability_spec()

  expect_s3_class(spec, "tbl_df")
  expect_true(all(c("signal", "line_num", "measurement_role", "wksht_cd", "clmn_num") %in% names(spec)))
  expect_true("operating_room" %in% spec$signal)
  expect_true("05000" %in% spec$line_num)
})

test_that(".normalize_hcris_ccn pads CCNs to 6 digits", {
  expect_equal(.normalize_hcris_ccn("6001"), "006001")
  expect_equal(.normalize_hcris_ccn(12345), "012345")
  expect_equal(.normalize_hcris_ccn("  "), NA_character_)
})

test_that("classify_urps_hospital_capability correctly tiers hospital infrastructure", {
  sample_panel <- tibble::tribble(
    ~ccn, ~fiscal_year, ~operating_room_evidence, ~pacu_evidence, ~anesthesia_evidence, ~basic_lab_evidence, ~blood_bank_evidence, ~sterile_processing_proxy, ~pharmacy_evidence, ~imaging_evidence,
    "000001", 2024L, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
    "000002", 2024L, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE,
    "000003", 2024L, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE
  )

  classified <- classify_urps_hospital_capability(sample_panel)

  expect_s3_class(classified, "tbl_df")
  expect_equal(as.character(classified$urps_site_tier[1]), "not_confirmed")
  expect_equal(as.character(classified$urps_site_tier[2]), "core_operative_confirmed")
  expect_equal(as.character(classified$urps_site_tier[3]), "full_scope_confirmed")
})
