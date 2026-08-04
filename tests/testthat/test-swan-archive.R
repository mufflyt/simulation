# SWAN archive loader (R/50-swan_archive.R).
#
# Every test writes its own tiny .rds to a temp dir and computes the expected
# checksum from it, so nothing here needs SIMULATION_DATA_ROOT, the external
# drive, or the 2.6 GB archive. The behaviours locked here are the ones that
# make the loader worth having: a mismatch must be loud, an unverifiable file
# must not masquerade as verified, and the provenance must reach the caveats.

with_mode <- function(mode, code) {
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE")
          else Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)
  Sys.setenv(REPRODUCIBILITY_MODE = mode)
  force(code)
}

write_fixture <- function(obj = data.frame(SWANID = 1:3, AGE0 = c(45, 46, 47))) {
  p <- tempfile(fileext = ".rds")
  saveRDS(obj, p)
  list(path = p, sha = digest::digest(file = p, algo = "sha256"))
}

test_that("a verified read records the checksum and marks it verified", {
  f <- write_fixture()
  obj <- suppressMessages(load_swan_archive(path = f$path, expected_sha256 = f$sha,
                                            verbose = FALSE))
  prov <- swan_archive_provenance(obj)
  expect_true(prov$verified)
  expect_identical(prov$sha256, f$sha)
  expect_identical(prov$reference_source, "caller")
  expect_true(prov$approved_local_copy)
  expect_equal(prov$n_rows, 3L)
})

test_that("a checksum mismatch stops in strict mode and warns in relaxed", {
  f <- write_fixture()
  wrong <- paste(rep("0", 64), collapse = "")
  with_mode("strict", {
    expect_error(load_swan_archive(path = f$path, expected_sha256 = wrong, verbose = FALSE),
                 "checksum mismatch")
  })
  with_mode("relaxed", {
    expect_message(load_swan_archive(path = f$path, expected_sha256 = wrong, verbose = FALSE),
                   "checksum mismatch")
    obj <- suppressMessages(load_swan_archive(path = f$path, expected_sha256 = wrong,
                                              verbose = FALSE))
    # It proceeds, but it must not claim to be verified.
    expect_false(swan_archive_provenance(obj)$verified)
  })
})

test_that("a file with no reference checksum is recorded but not called verified", {
  f <- write_fixture()
  expect_message(load_swan_archive(path = f$path, verbose = FALSE),
                 "No reference checksum")
  obj <- suppressMessages(load_swan_archive(path = f$path, verbose = FALSE))
  prov <- swan_archive_provenance(obj)
  expect_false(prov$verified)
  expect_identical(prov$reference_source, "none")
  expect_false(is.na(prov$sha256))          # still recorded
})

test_that("verify = FALSE records verified = FALSE rather than silently passing", {
  f <- write_fixture()
  obj <- suppressMessages(load_swan_archive(path = f$path, expected_sha256 = f$sha,
                                            verify = FALSE, verbose = FALSE))
  prov <- swan_archive_provenance(obj)
  expect_false(prov$verified)
  expect_true(is.na(prov$sha256))
})

test_that("a missing file names the actual problem", {
  # The two failure modes need different fixes: mount a disk, or fix a path.
  missing <- file.path(tempfile("nodir_"), "swan_all_visits.rds")
  expect_error(load_swan_archive(path = missing, verbose = FALSE),
               "SWAN archive not found")
})

test_that("archive provenance reaches the panel and the caveats", {
  f <- write_fixture(data.frame(
    SWANID = 1:3, NUMCHILD = c("(0) No children", "(1) 1 child", "(2) 2 children"),
    HIGH_BP = rep("(1) No", 3), HYSTERE = rep("(1) No", 3),
    AGE0 = c(45, 46, 47), BMI0 = 27, STATUS0 = rep("(5) Pre-menopausal", 3),
    INVOLEA0 = c("(2) Yes", "(1) No", "(2) Yes"), stringsAsFactors = FALSE))

  panel <- suppressMessages(swan_dmdm_panel_from_archive(
    path = f$path, expected_sha256 = f$sha, visits = 0, verbose = FALSE))

  prov <- attr(panel, "swan_dmdm_provenance")
  expect_false(is.null(prov$archive))
  expect_true(prov$archive$verified)
  # The accessor reaches the nested record from the panel too.
  expect_identical(swan_archive_provenance(panel)$sha256, f$sha)

  caveats <- swan_panel_fit_caveats(panel)
  expect_true(any(grepl("^SOURCE:", caveats)))
  expect_true(any(grepl("verified against", caveats)))
})

test_that("a panel built without the loader says so instead of implying provenance", {
  w <- data.frame(
    SWANID = 1:3, NUMCHILD = c("(0) No children", "(1) 1 child", "(2) 2 children"),
    HIGH_BP = rep("(1) No", 3), HYSTERE = rep("(1) No", 3),
    AGE0 = c(45, 46, 47), BMI0 = 27, STATUS0 = rep("(5) Pre-menopausal", 3),
    INVOLEA0 = c("(2) Yes", "(1) No", "(2) Yes"), stringsAsFactors = FALSE)
  panel <- build_swan_dmdm_panel(w, visits = 0, verbose = FALSE)
  expect_null(attr(panel, "swan_dmdm_provenance")$archive)
  expect_true(any(grepl("provenance not recorded", swan_panel_fit_caveats(panel))))
})

test_that("the shipped reference describes the validated archive", {
  # Guards against the constant drifting from the file it documents.
  expect_true("swan_all_visits.rds" %in% names(SWAN_ARCHIVE_SHA256))
  expect_match(SWAN_ARCHIVE_SHA256[["swan_all_visits.rds"]], "^[0-9a-f]{64}$")
  expect_equal(SWAN_ARCHIVE_BYTES[["swan_all_visits.rds"]], 29537163)
})
