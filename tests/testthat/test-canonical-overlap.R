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
               "utility_name_only", "unexamined", "delegated")
  expect_equal(setdiff(unique(reg$classification), allowed), character())
  expect_false(any(is.na(reg$note) | !nzchar(trimws(reg$note))))
})

test_that("no contract collision is outstanding", {
  # THE CLASS IS EMPTY, AND THIS ASSERTS THAT IT STAYS EMPTY. A contract
  # collision -- same public-looking name, different arguments, guards or
  # return shape -- is a correctness risk rather than the maintenance debt an
  # exact copy represents, because a reader assumes a parity the code does not
  # provide. The two that existed (`urps_p_active`, `urps_survival_curve`) were
  # resolved on 2026-08-09 by renaming to `supply_p_active()` and
  # `supply_survival_curve()`, not by documenting them.
  #
  # A test asserting emptiness is not vacuous here: it is the difference
  # between the next collision being an explicit decision and being a row
  # somebody adds while getting a build green.
  reg <- read_overlap_registry(root)
  expect_equal(reg$fn[reg$classification == "contract_collision"], character())
})

test_that("the renamed supply functions no longer collide with the SSOT", {
  # The rename is only real if the intersection actually stops containing them.
  live <- canonical_overlap(root)
  expect_false(any(live$fn %in% c("urps_p_active", "urps_survival_curve")))
  expect_true(all(c("supply_p_active", "supply_survival_curve") %in%
                    top_level_defs(file.path(root, "R"))))
})
