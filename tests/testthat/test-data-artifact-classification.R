# No manuscript analysis may read a non-canonical artifact ----
#
# THE INVARIANT. A script under scripts/validation/ produces run-identified
# evidence that reaches a manuscript. It may read canonical inputs -- registered
# in config/canonical_sources.yml with a SHA-256 and hashed into the run
# manifest. It may NOT read a derived intermediate, a download cache, or an
# obsolete prototype, because none of those carries a hash, a manifest entry, or
# any guarantee about which source produced it.
#
# WHY A TEST AND NOT A CONVENTION. The failure this prevents is not a crash. A
# validation script reading `urps_basket_prov_svc.rds` instead of the raw PUF
# would run fine, produce plausible bounds, and reproduce at zero tolerance --
# while the number's actual provenance quietly became "whatever RDS happened to
# be on that laptop". Reproducibility machinery cannot detect that, because the
# arithmetic really would be identical on both runs.
#
# The file names below are checked even though the files are deleted. That is
# deliberate: the prohibition has to outlive the artifact, or the first person to
# regenerate one reintroduces the defect with nothing objecting.
#
# Classes and rationale: docs/DATA_ARTIFACT_INVENTORY.md

root <- tryCatch(rprojroot::find_root(rprojroot::has_file("DESCRIPTION")),
                 error = function(e) NA_character_)
skip_if(is.na(root), "repository root not reachable")

# Non-canonical artifacts, by the class that forbids them.
FORBIDDEN <- c(
  # derived intermediates
  "urps_basket_prov_svc",
  # download caches
  "meps_FYC_2023", "meps_COND_2023", "meps_CLNK_2023", "meps_ob_2023",
  "cms_datajson",
  # obsolete exploratory
  "fitted_model"
)

validation_scripts <- function() {
  d <- file.path(root, "scripts", "validation")
  list.files(d, pattern = "^[0-9]+_.*[.]R$", full.names = TRUE)
}

test_that("no validation script reads a non-canonical artifact", {
  scripts <- validation_scripts()
  skip_if(length(scripts) == 0L, "repository root not reachable")

  offenders <- character()
  for (f in scripts) {
    # Comments are stripped first. These names are DISCUSSED in headers -- 04
    # explains that its extract used to live in a scratchpad, and that sentence
    # must not be read as a call site. A gate that cannot tell a warning from a
    # violation trains people to work around it.
    src <- readLines(f, warn = FALSE)
    src <- sub("#.*$", "", src)
    for (bad in FORBIDDEN) {
      if (any(grepl(bad, src, fixed = TRUE)))
        offenders <- c(offenders, sprintf("%s reads %s", basename(f), bad))
    }
  }

  expect_equal(
    offenders, character(),
    info = paste0(
      "A validation script reads a non-canonical artifact:\n  ",
      paste(offenders, collapse = "\n  "),
      "\nEither read the canonical source instead (see ",
      "config/canonical_sources.yml), or promote the artifact to a canonical ",
      "input with a SHA-256 and a manifest entry. Do not relax this test: the ",
      "number would still be reproducible and would no longer be attributable."))
})

test_that("analysis 05 reads the raw PUF, by name", {
  # The positive half. Asserting only the absence of the wrong path would still
  # pass if 05 stopped reading any CMS file at all.
  f <- file.path(root, "scripts", "validation",
                 "05_urps_share_partial_identification.R")
  skip_if_not(file.exists(f), "repository root not reachable")
  src <- readLines(f, warn = FALSE)
  expect_true(any(grepl("PHY_R26_P05_V10_D24_Prov_Svc.csv", src, fixed = TRUE)))
  expect_true(any(grepl("MUP_PHY_R26_P05_V10_D24_Geo.csv", src, fixed = TRUE)))
})

test_that("both CMS PUFs are registered with a SHA-256", {
  # What makes them canonical rather than merely present.
  cfg <- file.path(root, "config", "canonical_sources.yml")
  skip_if_not(file.exists(cfg), "canonical source registry not reachable")
  y <- yaml::read_yaml(cfg)
  src <- y$sources %||% y
  for (id in c("cms_mup_phy_2024_prov_svc", "cms_mup_phy_2024_geo")) {
    expect_true(!is.null(src[[id]]), info = id)
    expect_true(nzchar(src[[id]]$sha256 %||% ""), info = id)
    expect_match(src[[id]]$sha256, "^[0-9a-f]{64}$", info = id)
  }
})

test_that("the convenience builder declares itself non-canonical", {
  # The banner is load-bearing: it is what a reader who finds the RDS in six
  # months will follow back. If it is edited away, this fails.
  f <- file.path(root, "scripts", "data", "build_urps_basket_prov_svc.R")
  skip_if_not(file.exists(f), "repository root not reachable")
  src <- paste(readLines(f, warn = FALSE), collapse = "\n")
  expect_true(grepl("NOT a canonical input", src, fixed = TRUE))
  expect_true(grepl("NOT consumed by manuscript analysis 05", src, fixed = TRUE))
  # And it must resolve its source through the registry, not by filename.
  expect_true(grepl("resolve_canonical", src, fixed = TRUE))
})

test_that("the inventory documents every forbidden artifact", {
  # A prohibition with no explanation gets deleted by whoever it inconveniences.
  inv <- file.path(root, "docs", "DATA_ARTIFACT_INVENTORY.md")
  skip_if_not(file.exists(inv), "repository root not reachable")
  txt <- paste(readLines(inv, warn = FALSE), collapse = "\n")
  undocumented <- Filter(function(b) !grepl(b, txt, fixed = TRUE), FORBIDDEN)
  expect_equal(undocumented, character())
})
