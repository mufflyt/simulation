# Deleted intermediates stay regenerable ----
#
# ~208 MB of derived files were deleted from a session scratchpad on the
# argument that each is superseded or regenerable. That argument is only true
# while the paths behind it exist. Delete
# scripts/data_acquisition/06_download_meps_2023.R and the MEPS claim quietly
# becomes false; drop the canonical_sources.yml entry for the CMS PUF and the
# basket-extract claim goes with it. Nothing would notice, because the files it
# would have made recoverable are already gone.
#
# So this asserts the regeneration PATH, not the artifact.
#
# It deliberately does NOT reach the network. A test that pinged AHRQ or
# data.cms.gov would turn a third party's outage into a red build, and would
# assert something this repository cannot be responsible for. What the
# repository owes is a resolvable route; whether the far end answers today is a
# different question, and the acquisition scripts fail loudly on their own when
# it does not.

root <- tryCatch(rprojroot::find_root(rprojroot::has_file("DESCRIPTION")),
                 error = function(e) NA_character_)
skip_if(is.na(root), "repository root not reachable")

decl <- file.path(root, "scripts", "dev", "regenerate_intermediates.R")
skip_if_not(file.exists(decl), "repository root not reachable")
source(decl, local = TRUE)

test_that("every declared regeneration path resolves", {
  st <- check_intermediates(root)
  bad <- st[!st$ok, , drop = FALSE]
  expect_equal(
    nrow(bad), 0L,
    info = paste0(
      "A deleted intermediate is no longer regenerable:\n  ",
      paste(sprintf("%s -> missing %s", bad$id, bad$missing), collapse = "\n  "),
      "\nEither restore the prerequisite, or correct ",
      "docs/DATA_PROVENANCE_INTERMEDIATES.md to stop claiming the file can be ",
      "recovered."))
})

test_that("each intermediate declares a recognised disposal reason", {
  # The three reasons are not interchangeable and the distinction is the point:
  # `superseded` means DO NOT restore, while the other two mean here is how.
  kinds <- vapply(INTERMEDIATES, `[[`, character(1), "kind")
  expect_equal(setdiff(unique(kinds), c("regenerable", "superseded", "refetchable")),
               character())
})

test_that("every declaration carries a command and a justification", {
  for (x in INTERMEDIATES) {
    expect_true(nzchar(x$command), info = x$id)
    expect_true(nzchar(x$why_safe), info = x$id)
    expect_true(nzchar(x$what), info = x$id)
  }
})

test_that("the superseded prototype names what replaced it", {
  # The weakest link in the whole argument: `superseded` is the one reason with
  # no rebuild command, so it has to point at shipped code, and that code has to
  # exist. A prototype declared superseded by something absent is just deleted.
  sup <- Filter(function(x) x$kind == "superseded", INTERMEDIATES)
  expect_gt(length(sup), 0L)
  for (x in sup) {
    expect_true(any(grepl("^R/", x$requires)), info = x$id)
    expect_true(all(file.exists(file.path(root, x$requires))), info = x$id)
  }
})

test_that("analysis 05 reads the raw PUF, not the deleted extract", {
  # The specific claim that makes cms_basket_extract disposable. If 05 ever
  # starts reading a prebuilt extract, that extract becomes an input to a
  # manuscript-citable result and stops being a scratch file.
  src <- readLines(file.path(root, "scripts", "validation",
                             "05_urps_share_partial_identification.R"), warn = FALSE)
  expect_true(any(grepl("PHY_R26_P05_V10_D24_Prov_Svc.csv", src, fixed = TRUE)))
  expect_false(any(grepl("urps_basket_prov_svc", src, fixed = TRUE)))
})
