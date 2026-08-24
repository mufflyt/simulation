# Shipped config copies must match their config/ originals -------------------
#
# Several config files exist TWICE: once in config/, which is the source of
# truth a human edits, and once in inst/extdata/, which is the copy that gets
# installed. The duplication is not gratuitous -- config/ is .Rbuildignore'd,
# so an installed package cannot see it, and functions that need these files at
# runtime (read_scientific_boundaries(), the AI-claims and recurrence readers)
# would have nothing to read.
#
# The duplication had no guard. Three pairs were already being kept in sync by
# hand, which works right up until someone edits config/ and the installed copy
# quietly keeps serving the old values -- a divergence that produces wrong
# numbers rather than an error, and that no other test in this suite would
# notice. This asserts the pair is byte-identical.
#
# Adding a file to config/ does NOT oblige you to ship it. Only files that an
# installed package must read belong in the list below; most of config/ is
# build-time or repo-local (paths.yml, canonical_sources.yml) and correctly
# ships nowhere.

.shipped_config_files <- c(
  "ai_claims_basket.yml",
  "ai_treatment_evidence.csv",
  "recurrence_evidence.csv",
  "scientific_boundaries.yml"
)

testthat::test_that("every shipped config file matches its config/ original", {
  root <- .source_tree_root()
  testthat::skip_if(length(root) == 0, "repository root not reachable")
  root <- root[1]

  for (file_name in .shipped_config_files) {
    source_path <- file.path(root, "config", file_name)
    shipped_path <- file.path(root, "inst", "extdata", file_name)

    testthat::expect_true(
      file.exists(source_path),
      info = paste0("config/", file_name, " is missing; it is the source of ",
                    "truth for inst/extdata/", file_name)
    )
    testthat::expect_true(
      file.exists(shipped_path),
      info = paste0("inst/extdata/", file_name, " is missing, so the ",
                    "installed package cannot read it. Copy it from config/.")
    )

    if (file.exists(source_path) && file.exists(shipped_path)) {
      # Compared by content hash rather than parsed value: a YAML/CSV that
      # parses the same today can still be the stale copy, and the point is to
      # catch the edit that touched only one of the two.
      testthat::expect_identical(
        digest::digest(readBin(shipped_path, "raw", file.size(shipped_path))),
        digest::digest(readBin(source_path, "raw", file.size(source_path))),
        info = paste0(
          "inst/extdata/", file_name, " has drifted from config/", file_name,
          ". The installed package serves the inst/extdata copy, so this ",
          "divergence changes results silently. Re-copy config/", file_name,
          " over it."
        )
      )
    }
  }
})

testthat::test_that("the boundary registry resolves and carries its boundaries", {
  # The regression this file exists for: read_scientific_boundaries() returned
  # nothing under R CMD check, and callers reported a missing boundary ID
  # instead of a missing file.
  registry <- read_scientific_boundaries()

  testthat::expect_true(is.list(registry))
  testthat::expect_true(length(registry) > 0L)
  testthat::expect_true(
    all(c("drive_time_30", "probability_lower", "supply_demand_balance",
          "hospital_capability_threshold") %in% names(registry))
  )
})
