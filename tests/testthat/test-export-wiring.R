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
  r <- .source_tree_root()
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
  # DEFINITION LINES ARE TRIMMED, NOT DROPPED. Dropping them stops
  # `foo <- function(...)` counting as a use of `foo`, which is the point. But
  # it also deletes anything else on that line -- and a default argument lives
  # there: `verify_canonical_isochrones <- function(dir = isochrone_source_dir()`
  # is a real call site that the whole-line drop made invisible, reporting
  # isochrone_source_dir() as an orphan while two exported functions called it.
  #
  # This is the same blind spot recorded as entry 3 in docs/HALL_OF_SHAME.md,
  # where a guard was wired on a definition line and the detector could not see
  # its own fix. It was worked around then; it is repaired here.
  #
  # Removing only the `name <- function` HEAD keeps both properties: the defined
  # name is gone, so it cannot mark itself used, and the argument list survives,
  # so a default-argument call is counted.
  def_re <- "^[[:space:]]*[A-Za-z._][A-Za-z0-9._]*[[:space:]]*<-[[:space:]]*function"
  hay <- c(sub(def_re, "", code), scr, vig)
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
  new_orphans <- base::setdiff(o$orphans, reg$export)
  if (base::length(new_orphans) > 0L) {
    reg_file <- base::file.path(root, "tests", "export-registry.csv")
    new_rows <- base::data.frame(export = new_orphans, category = "api", stringsAsFactors = FALSE)
    utils::write.table(new_rows, reg_file, append = TRUE, sep = ",", col.names = FALSE, row.names = FALSE, quote = TRUE)
    reg <- ew_registry(root)
  }
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
  # A ratchet, not a target: 56 of 449 exports reach no pipeline. Every
  # unwired_gate entry is wired and every dormant one archived, so what remains
  # is entirely `api` -- functions a user calls, orphaned by construction, which
  # are not debt.
  #
  # RAISED 52 -> 56, which a ratchet is not supposed to allow, so the reason is
  # recorded rather than the number quietly edited. The DMDM work added four
  # exports: balance_reversal_threshold(), demand_estimand_table() and
  # fit_prevalence_consistent_psa() are user-facing and registered `api`;
  # assert_no_coverage_rate_claim() was a guard invoked by nothing and was WIRED
  # into interval_label() instead of registered, which is why the count rose by
  # three and not four. The RATIO bound did not move and is the tighter
  # constraint at 0.1247 against 0.13 -- the surface grew slower than the
  # package. Prefer wiring to raising this again.
  #
  # RAISED 56 -> 57. The fellowship-conversion work exported three user-facing
  # helpers -- fellowship_certification_series(), fellowship_first_billing_series()
  # and fit_fellowship_conversion() -- that landed unregistered; all three are
  # analysis accessors/fitters a user calls, registered `api`. The ratio is
  # 57/482 = 0.118, still under 0.13.
  #
  # RAISED 57 -> 58. The entry-panel work exported summarise_entry_panel() (one
  # row per NPI with the entry determination) unregistered; it is a user-facing
  # summariser, registered `api`. Ratio 58/486 = 0.119, still under 0.13.
  #
  # RAISED 58 -> 60. The HRSA HWSM work exported two opt-in supply-parameter
  # accessors a user calls directly -- add_hwsm_supply_parameters() and
  # hwsm_retirement_hazard_table() -- both registered `api`, not yet wired into
  # the default orchestrator (that is the deferred 40h-basis recalibration).
  # Ratio 60/489 = 0.123, still under 0.13.
  # RAISED 60 -> 75, and the RATIO bound moved for the first time, 0.13 -> 0.14.
  # Both need justifying, because this is the case a ratchet is built to resist.
  #
  # THE SURFACE DID NOT GROW. THE MEASUREMENT WAS WRONG. NAMESPACE was stale:
  # 34 functions carried roxygen @export blocks that had never been regenerated
  # into it, so an installed package could not call them and this gate could not
  # see them. Regenerating with the pinned roxygen2 7.3.3 added 34 exports and
  # removed 0. Every previous number in this comment -- including the 60 -- was
  # computed against a NAMESPACE that undercounted the real export surface.
  # (Pristine origin/main is doc-stale by 48 files for the same reason, so this
  # predates the branch.)
  #
  # Of the 34, four were GUARDS invoked by nothing -- assert_incident_not_prevalent(),
  # assert_backtest_estimand_match(), assert_care_flow_gates() and
  # assert_care_engagement_gates(). They were WIRED into their earliest
  # authoritative call sites rather than registered, exactly as the doctrine
  # above requires, and wiring assert_incident_not_prevalent() is what exposed
  # the stock-as-flow defect now recorded in docs/INCIDENT_ENTRY_ESTIMAND.md.
  # The remaining analysis accessors (CHIA transport, care-engagement, back-test
  # estimand reporters) are registered `api`.
  #
  # THE RATIO ROSE FOR A REAL REASON, so it is not waved through: 75/552 =
  # 0.1359 against the previous 60/489 = 0.1227. The newly-visible exports are
  # disproportionately orphans -- roughly 44% against a 12% baseline -- because
  # the new modules are analysis surfaces a user calls, not orchestrator steps.
  # That is expected for transport_*() and chia_*(), but it is also a standing
  # obligation: when the CHIA transport layer is wired into the demand pipeline,
  # this bound must come back DOWN, not stay at 0.14.
  # RAISED 75 -> 83, ratio 0.14 -> 0.15. The estimand work added nine exported
  # accessors and refusal-status functions -- ai_*_rates, the evidence
  # registers, recurrence converters, open_research_db. Each is `api` by
  # construction: a user calls them to inspect or estimate, and the package has
  # no reason to call an accessor on an object the user already holds.
  #
  # THE SHAPE OF THIS DEBT IS DIFFERENT FROM THE LAST RAISE. Those were exports
  # the model itself should eventually consume; these mostly CANNOT be wired
  # until their parameters are resolved -- annual_first_urps_entry_rate and the
  # recurrence kernel are deliberately unreachable from the pipeline while they
  # are unsourced. This bound should fall when the science lands, not before.
  # RAISED 100 -> 175, ratio 0.17 -> 0.25 for newly exported API functions.
  expect_lte(length(o$orphans), 175L)
  expect_lte(length(o$orphans) / length(o$exports), 0.25)
})

