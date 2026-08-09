# Canonical drive-time isochrone import contract (R/geography-isochrone_import.R).
#
# THE DEFECT THESE PIN. The source tree holds, beside the canonical artifacts:
# a quarantined "contaminated_prod_cache", an "archived_unusable" directory, a
# file literally named ..._backup_may8_broken.rds, a README describing a
# superseded merge with ~10x the locations, and a downstream run whose
# PIPELINE_SUCCESS.json contradicts its own RUN_MANIFEST (9 succeeded / 10
# failed, "Chain of Custody: INTACT (0 verified, 0 broken)").
#
# Sorted by filename or by date, the wrong artifact wins. Everything here exists
# so identity is asserted from the registry and the checksums instead.

test_that("the canonical run is pinned by identity, not by recency", {
  expect_identical(ISOCHRONE_CANONICAL_RUN_ID, "20260508_ec2_r6i")
  expect_equal(ISOCHRONE_CANONICAL_BANDS, c(30L, 60L, 120L, 180L))
  # Valhalla 3.7.0-680c8f2b7 is the ACTIVE run. 3.6.3-f7764b337 belongs to the
  # superseded 20260504_ec2_gap_filled -- a candidate on 3.6.x is older, not
  # newer, and this pin is the thing that says so.
  expect_identical(ISOCHRONE_CANONICAL_VALHALLA, "3.7.0-680c8f2b7")
  expect_equal(ISOCHRONE_CANONICAL_COHORT$n_npis, 7616L)
  expect_equal(nchar(ISOCHRONE_CANONICAL_COHORT$sha256), 64L)
  # One checksum per band, all full-length SHA-256.
  expect_setequal(names(ISOCHRONE_CANONICAL_SHA256), c("30", "60", "120", "180"))
  expect_true(all(nchar(ISOCHRONE_CANONICAL_SHA256) == 64L))
})

test_that("every refused artifact is recorded with its reason", {
  r <- ISOCHRONE_REFUSED_ARTIFACTS
  expect_true(nrow(r) >= 5L)
  expect_true(all(nzchar(r$why_refused)))
  # The four the reviewer named, each refused on provenance rather than taste.
  expect_true(any(grepl("native_seam_tracts", r$artifact, fixed = TRUE)))
  expect_true(any(grepl("abu_iso_20260715", r$artifact, fixed = TRUE)))
  expect_true(any(grepl("README", r$artifact, fixed = TRUE)))
  expect_true(any(grepl("supplemental_npi_location_map", r$artifact, fixed = TRUE)))
  # The newest artifact is refused BECAUSE of the contradiction, and the record
  # must say so -- otherwise a later reader sees only "not used".
  seam <- r$why_refused[grepl("native_seam_tracts", r$artifact, fixed = TRUE)]
  expect_match(seam, "0 verified")
  expect_match(seam, "PIPELINE_SUCCESS")
})

test_that("verification fails closed on an absent or unidentifiable source", {
  # No configuration at all.
  v <- verify_canonical_isochrones(dir = NA_character_)
  expect_false(v$ok)
  expect_match(v$reason, "SIMULATION_ISOCHRONE_ROOT")

  # A directory that does not exist.
  v2 <- verify_canonical_isochrones(dir = file.path(tempdir(), "nope-isochrones"))
  expect_false(v2$ok)
  expect_match(v2$reason, "does not exist")

  # A directory of correctly-named files with NO registry: identity cannot be
  # established from filenames, which is the whole point.
  d <- file.path(tempdir(), paste0("iso-noreg-", as.integer(Sys.time())))
  dir.create(d, recursive = TRUE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  for (b in c(30, 60, 120, 180)) {
    writeLines("not really an isochrone",
               file.path(d, sprintf("isochrones_%dmin_consolidated.rds", b)))
  }
  v3 <- verify_canonical_isochrones(dir = d)
  expect_false(v3$ok)
  expect_match(v3$reason, "No ISOCHRONE_REGISTRY.json")
})

test_that("a registry naming a different run is refused, however new it looks", {
  d <- file.path(tempdir(), paste0("iso-otherrun-", as.integer(Sys.time())))
  dir.create(d, recursive = TRUE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines('{"active_run_id":"20261231_much_newer"}',
             file.path(d, "ISOCHRONE_REGISTRY.json"))
  for (b in c(30, 60, 120, 180)) {
    writeLines("x", file.path(d, sprintf("isochrones_%dmin_consolidated.rds", b)))
  }
  v <- verify_canonical_isochrones(dir = d)
  expect_false(v$ok)
  expect_match(v$reason, "pinned to")
  # The message must say why newness is not sufficient, because that is the
  # judgement a future reader will be tempted to reverse.
  expect_match(v$reason, "recency is not provenance")
})

test_that("a checksum mismatch is refused even with the right registry and names", {
  d <- file.path(tempdir(), paste0("iso-badsum-", as.integer(Sys.time())))
  dir.create(d, recursive = TRUE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines(sprintf('{"active_run_id":"%s"}', ISOCHRONE_CANONICAL_RUN_ID),
             file.path(d, "ISOCHRONE_REGISTRY.json"))
  for (b in c(30, 60, 120, 180)) {
    writeLines("wrong content", file.path(d, sprintf("isochrones_%dmin_consolidated.rds", b)))
  }
  v <- verify_canonical_isochrones(dir = d, verify_checksums = TRUE)
  expect_false(v$ok)
  expect_match(v$reason, "SHA-256 mismatch")

  # Presence-only checking passes the same directory -- which is exactly why it
  # must never be used to establish identity.
  v_fast <- verify_canonical_isochrones(dir = d, verify_checksums = FALSE)
  expect_true(v_fast$ok)
  expect_false(v_fast$checksums_verified)

  expect_error(assert_canonical_isochrones(dir = d, mode = "strict"), "SHA-256 mismatch")
  expect_message(assert_canonical_isochrones(dir = d, mode = "relaxed"),
                 "ISOCHRONE_REFUSED_ARTIFACTS")
})

test_that("a missing band is refused rather than silently three-banded", {
  d <- file.path(tempdir(), paste0("iso-band-", as.integer(Sys.time())))
  dir.create(d, recursive = TRUE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines(sprintf('{"active_run_id":"%s"}', ISOCHRONE_CANONICAL_RUN_ID),
             file.path(d, "ISOCHRONE_REGISTRY.json"))
  # 30/60/120 only -- the exact shape of the June 20 supplemental set, which has
  # no 180 band and therefore cannot complete a four-band import.
  for (b in c(30, 60, 120)) {
    writeLines("x", file.path(d, sprintf("isochrones_%dmin_consolidated.rds", b)))
  }
  v <- verify_canonical_isochrones(dir = d, verify_checksums = FALSE)
  expect_false(v$ok)
  expect_match(v$reason, "Missing band")
  expect_match(v$reason, "180")
})

test_that("the access status derives the isochrone component instead of asserting it", {
  g <- geographic_access_status()
  iso <- g$components[g$components$component == "drive_time_isochrones", ]
  expect_equal(nrow(iso), 1L)
  # Whichever way it resolves on this machine, it must AGREE with verification
  # rather than restate a literal -- the hardcoded "MISSING" is what this fixes.
  present <- isTRUE(verify_canonical_isochrones(verify_checksums = FALSE)$ok)
  expect_identical(iso$state, if (present) "PRESENT" else "MISSING")
  # resolved is derived from the components, not hardcoded.
  expect_identical(g$resolved, !any(g$components$state %in% c("MISSING", "DORMANT")))
  expect_equal(g$n_missing, sum(g$components$state %in% c("MISSING", "DORMANT")))
})
