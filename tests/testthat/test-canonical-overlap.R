# The canonical-overlap gate ----
#
# Fails when a function name is NEWLY shared with a sibling repository and has
# no row in tests/canonical-overlap-registry.csv.
#
# IT DOES NOT FAIL ON OVERLAP. Three of the registry's classifications describe
# divergence somebody chose, and one describes a byte-identical port that is not
# urgent to remove. Failing on overlap would pressure whoever hit it into
# deleting a deliberate extension to get a green build. What must not happen
# quietly is a NEW twin, which is how this repository reached twelve of them
# before anyone counted.
#
# The gate cannot run where the siblings are absent -- CI, and R CMD check's
# <pkg>.Rcheck/ -- so it skips with a declared reason rather than passing
# vacuously. tests/skip-budget.csv carries the budget.

root <- tryCatch(rprojroot::find_root(rprojroot::has_file("DESCRIPTION")),
                 error = function(e) NA_character_)
skip_if(is.na(root), "repository root not reachable")

audit_script <- file.path(root, "scripts", "dev", "audit_canonical_overlap.R")
skip_if_not(file.exists(audit_script), "repository root not reachable")
source(audit_script, local = TRUE)

# One implementation of the extraction, shared with the script. A gate that
# re-derived the intersection its own way could disagree with the tool the
# author runs to fix it, which is the worst possible failure for a gate.
present <- names(sibling_repo_paths(root))
skip_if(length(present) == 0L, "sibling repositories not present")

test_that("every name shared with a sibling repository is classified", {
  st <- overlap_status(root)

  # Only judge siblings actually on disk. A partial checkout must not report
  # every mufflyaccess row as stale merely because cliff is missing.
  reg_here <- st$registry[st$registry$sibling %in% present, , drop = FALSE]
  stale <- st$stale[st$stale$sibling %in% present, , drop = FALSE]

  expect_equal(
    nrow(st$unclassified), 0L,
    info = paste0(
      "Unclassified collision(s) with a sibling repository:\n  ",
      paste(sprintf("%s (%s)", st$unclassified$fn, st$unclassified$sibling),
            collapse = "\n  "),
      "\nOpen each, compare it, and add a row to ",
      "tests/canonical-overlap-registry.csv. `unexamined` is an allowed ",
      "classification and is not a way to dismiss one."))

  expect_equal(
    nrow(stale), 0L,
    info = paste0(
      "Registry row(s) for collisions that no longer exist:\n  ",
      paste(sprintf("%s (%s)", stale$fn, stale$sibling), collapse = "\n  "),
      "\nDelete them; a registry carrying rows for functions that are gone ",
      "stops being evidence of anything."))

  expect_gt(nrow(reg_here), 0L)
})

test_that("the registry uses only the declared classifications", {
  reg <- read_overlap_registry(root)
  allowed <- c("exact_copy", "contract_collision", "stronger_here",
               "ported_weaker", "equivalent", "script_local_copy",
               "utility_name_only", "unexamined")
  expect_equal(setdiff(unique(reg$classification), allowed), character())
  expect_false(any(is.na(reg$note) | !nzchar(trimws(reg$note))))
})

test_that("contract collisions stay visible", {
  # The class the reviewer called the correctness risk, as against the
  # maintenance debt of an exact copy. Pinned so that resolving one is a
  # deliberate edit here rather than a silent reclassification.
  reg <- read_overlap_registry(root)
  collisions <- reg$fn[reg$classification == "contract_collision"]
  expect_setequal(collisions, c("urps_p_active", "urps_survival_curve"))
})
