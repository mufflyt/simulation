# Exported-but-unwired surface (tests/export-registry.csv).
#
# THE DEFECT THIS EXISTS FOR, found repeatedly and always by accident:
#
#   * assert_demand_calibrated() was defined, tested and called by nothing, so
#     the orchestrator accepted `calibration`, stored it in metadata and never
#     checked it. Demand anchored to no observed quantity passed silently.
#   * opportunity_placement_shares() and the whole geography layer were
#     reachable only by calling the engine directly. Every run was
#     national-headcount-only.
#   * hours_coef, apply_hrsa_surgical_fte(), apply_calibration_scalars() and
#     param_spec in run_backtest() were each the same shape.
#
# None of these were broken. Each had tests, and the tests passed. The failure
# is invisible to a test suite by construction, because a test calls the
# function -- that is what makes it a test -- and so proves nothing about
# whether the PACKAGE does.
#
# This file makes the surface explicit instead of waiting to trip over it.

ew_root <- function() {
  r <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
              c(".", "..", file.path("..", "..")))
  if (length(r) == 0) NULL else r[1]
}

# A symbol counts as CALLED if it appears anywhere in R/, scripts/ or
# vignettes/ outside its own definition line. Deliberately generous: it matches
# a bare mention, not just `name(`, so a constant passed as a value
# (URPS_DELEGATION_MATRIX, RETIREMENT_HAZARD_BY_AGE) counts as wired. A stricter
# rule would flag those and the noise would sink the gate.
#
# TESTS ARE NOT EVIDENCE OF WIRING and are excluded from the haystack. A test
# calling a function is what a test IS; counting it here would make every
# orphan look connected, which is exactly the illusion that hid the defects
# above.
ew_orphans <- function(root) {
  rd <- function(f) tryCatch(sub("#.*$", "", readLines(f, warn = FALSE)),
                             error = function(e) character())
  ns <- readLines(file.path(root, "NAMESPACE"), warn = FALSE)
  # Strip BOTH quoting forms. NAMESPACE writes non-syntactic names as
  # export("%>%") and backticked ones as export(`x`); leaving either in place
  # makes the symbol un-matchable and it shows up as a phantom orphan.
  ex <- gsub("[`\"]", "", sub("^export[(](.*)[)]$", "\\1", grep("^export[(]", ns, value = TRUE)))
  code <- unlist(lapply(list.files(file.path(root, "R"), "[.]R$", full.names = TRUE), rd))
  scr  <- unlist(lapply(list.files(file.path(root, "scripts"), "[.]R$",
                                   full.names = TRUE, recursive = TRUE), rd))
  vig  <- unlist(lapply(list.files(file.path(root, "vignettes"), "[.]Rmd$",
                                   full.names = TRUE), rd))
  defs <- grepl("^[[:space:]]*[A-Za-z._][A-Za-z0-9._]*[[:space:]]*<-[[:space:]]*function", code)
  hay <- c(code[!defs], scr, vig)
  used <- vapply(ex, function(f) any(grepl(
    paste0("(^|[^A-Za-z0-9._])", gsub("([.])", "[.]", f),
           "([[:space:]]*[(]|[^A-Za-z0-9._(]|$)"), hay)), logical(1))
  list(exports = ex, orphans = sort(ex[!used]))
}

# The schema. Declared once so the "every orphan declares a kind" check and the
# "unwired_gate still exists" check cannot drift apart -- if they did, the
# category could be deleted from one and silently accepted by the other.
EW_CATEGORIES <- c("api", "unwired_gate", "dormant")

ew_registry <- function(root) {
  utils::read.csv(file.path(root, "tests", "export-registry.csv"),
                  stringsAsFactors = FALSE, comment.char = "#")
}

test_that("every unwired export is registered, and the register has no stale rows", {
  root <- ew_root()
  skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
  o <- ew_orphans(root)
  reg <- ew_registry(root)

  # A NEW orphan is the case that matters: someone exported a capability and
  # wired it to nothing, which is how every defect above began.
  expect_setequal(setdiff(o$orphans, reg$export), character(0))

  # Stale rows in the other direction. A row for something now WIRED means the
  # debt was paid and the register should shrink; a row for something no longer
  # exported means it was dropped. Both should delete the row, and leaving it
  # makes the register lie about the size of the surface.
  expect_setequal(setdiff(reg$export, o$orphans), character(0))
})

test_that("every registered orphan declares which kind of orphan it is", {
  root <- ew_root()
  skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
  reg <- ew_registry(root)
  expect_true(all(reg$category %in% EW_CATEGORIES))
  expect_equal(anyDuplicated(reg$export), 0L)
  expect_true(all(nzchar(reg$export)))
})

test_that("the dormant list is short enough to be decided entry by entry", {
  root <- ew_root()
  skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
  reg <- ew_registry(root)
  dormant <- sort(reg$export[reg$category == "dormant"])

  # 30 -> 0. Most of the fall was a CORRECTION: a sensitivity sweep or a summary
  # table is orphaned by construction because the package has no reason to call
  # what a user calls, so 23 were re-read individually and moved to `api`. A
  # register that overstates its debt is no more useful than one that hides it.
  #
  # The five that were genuinely dormant are ARCHIVED in inst/archive/, source
  # and tests together, rather than deleted -- the implementation is the
  # expensive part. They are no longer exports, so they leave this register
  # entirely rather than sitting in it forever as permanent debt.
  expect_equal(dormant, character(0))
})

test_that("no guard is left unwired, and the category stays in the schema", {
  root <- ew_root()
  skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
  reg <- ew_registry(root)

  # THIS LIST REACHED ZERO. It was pinned by NAME rather than by count so that
  # it could only shrink and no entry could be silently swapped for another; all
  # ten have now been wired. Six went into validation_report(), which the
  # orchestrator runs on every projection, and check_legacy_canonical() into
  # scripts/ci/check_suite.R -- it is a property of the source tree, not of a
  # projection, so wiring it into a run would have been the wrong home.
  expect_equal(sort(reg$export[reg$category == "unwired_gate"]), character(0))

  # The category stays valid in the schema deliberately. Removing it would force
  # the next guard that arrives unwired to be filed as `api`, which is precisely
  # how "implemented and connected to nothing" stays invisible.
  expect_true("unwired_gate" %in% EW_CATEGORIES)
})

test_that("the unwired surface does not grow", {
  root <- ew_root()
  skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
  o <- ew_orphans(root)
  # A ratchet, not a target: 52 of 413 exports reach no pipeline, down from 67.
  # Every unwired_gate entry was wired and every dormant one archived, so what
  # remains is entirely `api` -- functions a user calls, orphaned by
  # construction, which are not debt.
  #
  # It is 52 rather than 51 because archiving real_access_surface() orphaned
  # load_tract_demand(), its only in-package caller. The gate caught that on the
  # same run, which is the behaviour worth having: removing code moves work, it
  # does not always remove it.
  expect_lte(length(o$orphans), 52L)
  expect_lte(length(o$orphans) / length(o$exports), 0.13)
})
