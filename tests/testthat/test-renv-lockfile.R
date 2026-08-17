# renv.lock is the answer to "can this exact scientific environment still be
# reconstructed?". Until it existed that question had no mechanism at all.
#
# It is DELIBERATELY not activated: there is no .Rprofile and no renv/ directory,
# so nothing auto-switches libraries for developers or for the existing
# workflows. The lockfile is a reproducibility RECORD that the nightly
# frozen-restore job consumes explicitly.

.lock <- function() jsonlite::fromJSON("../../renv.lock", simplifyVector = FALSE)
.desc <- function() read.dcf("../../DESCRIPTION")

test_that("the lockfile exists, parses, and pins an R version", {
  skip_if_not(file.exists("../../renv.lock"))
  l <- .lock()
  expect_true(nzchar(l$R$Version))
  expect_gt(length(l$Packages), 50)
  expect_true(length(l$R$Repositories) > 0)
})

test_that("every hard dependency in DESCRIPTION is pinned", {
  skip_if_not(file.exists("../../renv.lock"))
  d <- .desc()
  imports <- trimws(gsub("\\s*\\(.*?\\)", "", unlist(strsplit(d[1, "Imports"], ",\\s*"))))
  base <- rownames(installed.packages(priority = "base"))
  need <- setdiff(imports[nzchar(imports)], base)
  have <- names(.lock()$Packages)
  missing <- setdiff(need, have)
  expect_equal(length(missing), 0L,
               info = paste("Imports absent from renv.lock:", paste(missing, collapse = ", ")))
})

test_that("no package is recorded with an unusable source", {
  # renv::snapshot() records a locally-built package as Source "unknown" with no
  # remote, which restore() cannot install -- the lockfile looks complete and is
  # not. mufflyaccess was recorded exactly this way and had to be repaired by
  # hand. This guard stops it recurring for any package.
  skip_if_not(file.exists("../../renv.lock"))
  pk <- .lock()$Packages
  bad <- names(pk)[vapply(pk, function(p) identical(p$Source, "unknown"), logical(1))]
  expect_equal(length(bad), 0L,
               info = paste("unrestorable Source 'unknown':", paste(bad, collapse = ", ")))
})

test_that("mufflyaccess is pinned to the SAME commit in all three places", {
  # DESCRIPTION Remotes, renv.lock, and the nightly workflow must agree. An
  # extra-packages entry does not inherit the Remotes pin, so these drift
  # silently -- this repository has already been bitten by two builds both
  # calling themselves 0.10.0 and disagreeing about whether n_retired is NA.
  skip_if_not(file.exists("../../renv.lock"))
  # unname: regmatches on a named DESCRIPTION field carries the field name
  sha_of <- function(x) unname(regmatches(x, regexpr("[0-9a-f]{40}", x)))

  d_sha <- sha_of(.desc()[1, "Remotes"])
  l_sha <- .lock()$Packages$mufflyaccess$RemoteSha
  expect_length(d_sha, 1L)
  expect_identical(l_sha, d_sha)
  expect_identical(.lock()$Packages$mufflyaccess$Source, "GitHub")

  wf <- "../../.github/workflows/nightly.yaml"
  if (file.exists(wf)) {
    w_sha <- sha_of(grep("MUFFLYACCESS_PIN", readLines(wf, warn = FALSE), value = TRUE))
    expect_length(w_sha, 1L)
    expect_identical(w_sha, d_sha)
  }
})

test_that("mysterycall is deliberately absent from the lockfile", {
  # It is a GitHub-only Suggests intentionally kept out of Remotes (its GLMM
  # graph conflicts with the pinned lme4/blme), so it is UNRESOLVABLE in CI.
  # Pinning it would make every restore fail. Its callers guard with
  # requireNamespace() and degrade explicitly.
  skip_if_not(file.exists("../../renv.lock"))
  expect_false("mysterycall" %in% names(.lock()$Packages))
})

test_that("flexsurv is pinned, because its absence changes the science", {
  # Without flexsurv the retirement hazard silently falls back to a Weibull
  # approximation (R/supply-retirement_hazard.R). That is a MODEL difference,
  # not a cosmetic one, so the reproducible environment must pin it rather than
  # leave the fallback as the de facto default.
  skip_if_not(file.exists("../../renv.lock"))
  expect_true("flexsurv" %in% names(.lock()$Packages))
})

test_that("renv is not auto-activated for the project", {
  # An activating .Rprofile would switch libraries for every developer and every
  # existing workflow, none of which use renv. The lockfile is a record consumed
  # explicitly by the nightly, not an ambient mode.
  expect_false(file.exists("../../.Rprofile"))
  expect_false(dir.exists("../../renv"))
})
