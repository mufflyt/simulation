# The URPS practice survey: one instrument, two open items (R/data-practice_survey).
#
# These requirements are the model's statement of what it cannot currently
# know. If they drift out of sync with the status functions, the package starts
# claiming an item is resolvable by data it no longer asks for.

test_that("the instrument is one document, and the views are filters over it", {
  all_items <- urps_practice_survey_requirements()
  cap <- urps_practice_survey_requirements("capacity_anchor")
  fte <- urps_practice_survey_requirements("fte_curve")

  # THE POINT OF THE FILE. Two open items, ONE questionnaire. If these ever
  # become independent lists they will drift, and fielding two surveys to
  # collect one questionnaire's worth of questions is how neither gets fielded.
  expect_true(all(cap$variable %in% all_items$variable))
  expect_true(all(fte$variable %in% all_items$variable))
  expect_setequal(unique(all_items$resolves), c("capacity_anchor", "fte_curve", "both"))
  # "both" items must appear in each view, and nowhere be counted twice.
  both <- all_items$variable[all_items$resolves == "both"]
  expect_true(all(both %in% cap$variable))
  expect_true(all(both %in% fte$variable))
  expect_equal(anyDuplicated(all_items$variable), 0L)
})

test_that("the capacity view is a view, not a second list", {
  # It used to be an independent tribble in R/supply-roster_capacity. A duplicate list is a drift
  # channel: the two can disagree and nothing notices.
  expect_equal(urps_capacity_survey_requirements(),
               urps_practice_survey_requirements("capacity_anchor"))
  req <- urps_capacity_survey_requirements()
  expect_true(all(c("clinical_fte", "annual_visits", "annual_procedures",
                    "operative_volume", "new_patient_capacity", "panel_size",
                    "wait_time") %in% req$variable))
  expect_true(all(nzchar(req$why_needed)))
})

test_that("the FTE curve asks for what a claims source cannot supply", {
  fte <- urps_practice_survey_requirements("fte_curve")
  # Hours, and the covariates the gradient needs. Medicare shows delivered
  # services, never hours, and cannot separate fewer hours from lower billing.
  expect_true(all(c("weekly_clinical_hours", "age", "sex",
                    "practice_setting", "call_burden") %in% fte$variable))
  # OR time separable from clinic time: in a surgical subspecialty the age
  # decline may be giving up operating rather than seeing fewer patients, and
  # one blended hours figure cannot tell those apart.
  expect_true(all(c("or_hours_per_week", "clinic_sessions_per_week") %in% fte$variable))
})

test_that("both statuses report unresolved and name the same instrument", {
  cs <- capacity_status(); fs <- fte_curve_status()
  expect_false(cs$resolved)
  expect_false(fs$resolved)
  expect_equal(cs$same_instrument_also_resolves, "fte_curve")
  expect_setequal(cs$resolved_by, urps_practice_survey_requirements("capacity_anchor")$variable)
  expect_setequal(fs$resolved_by, urps_practice_survey_requirements("fte_curve")$variable)
})

test_that("the FTE status points away from the dormant module", {
  fs <- fte_curve_status()
  # R/calibration-hrsa_fte carries TODO-FTE-001 and the derived_by_analogy tier, so it looks like
  # the thing to replace. apply_hrsa_surgical_fte() is called by nothing but its
  # own tests, so fixing it would change no output. The live target is
  # hwsm_reference_hours().
  expect_match(fs$current_source, "hwsm_reference_hours")
  expect_match(fs$do_not_fix, "R/calibration-hrsa_fte")
  expect_match(fs$do_not_fix, "dormant")
  expect_match(fs$leverage, "does NOT cancel")
})

test_that("R/calibration-hrsa_fte really is dormant, so the warning stays true", {
  # If a future change wires apply_hrsa_surgical_fte() into the pipeline, this
  # fails and fte_curve_status()$do_not_fix must be rewritten.
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository root not reachable (source tree absent under R CMD check)")
  r_files <- list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE)
  # Exclude the definition, and exclude R/data-practice_survey -- it names the function inside
  # fte_curve_status()'s message string precisely to warn people off it, which
  # is a mention in a string literal rather than a call.
  # Locate the files to exclude by WHAT THEY DEFINE, never by filename. The
  # numbered module scheme was renamed wholesale to semantic prefixes, and a
  # filename-matched exclusion silently stopped matching -- this test failed for
  # a rename, not for a regression.
  .defines <- function(fs, pattern) vapply(fs, function(f)
    any(grepl(pattern, readLines(f, warn = FALSE))), logical(1))
  r_files <- r_files[!(.defines(r_files, "^fte_curve_status <- function") |
                         .defines(r_files, "^apply_hrsa_surgical_fte <- function"))]
  called_in <- function(f) {
    code <- sub("#.*$", "", readLines(f, warn = FALSE))
    any(grepl("apply_hrsa_surgical_fte\\s*\\(", code))
  }
  hits <- vapply(r_files, called_in, logical(1))
  expect_false(any(hits),
               info = paste("now called from:",
                            paste(basename(r_files[hits]), collapse = ", ")))
})

test_that("the unresolved register separates provenance problems from results problems", {
  u <- unresolved_calibration_items()
  expect_true(all(c("capacity_anchor", "fte_curve", "delegation_matrix") %in% u$item))
  # The distinction that matters: an item that cancels out of required FTE is a
  # provenance problem; one that does not is a results problem. Delegation
  # cancels via the productivity solve, the anchor and the hours SHAPE do not.
  expect_false(u$cancels_out[u$item == "capacity_anchor"])
  expect_false(u$cancels_out[u$item == "fte_curve"])
  expect_true(u$cancels_out[u$item == "delegation_matrix"])
  expect_true(all(nzchar(u$leverage)))
})

# ---- Geographic access -------------------------------------------------------

test_that("geographic access status reports infrastructure resolution", {
  g <- geographic_access_status()
  expect_equal(nrow(g$components), 8L)   # 7 inputs + the wait_time_anchor
  expect_lte(g$n_present + g$n_missing, nrow(g$components))

  st <- stats::setNames(g$components$state, g$components$component)
  expect_equal(unname(st["tract_population"]), "PRESENT")
  expect_equal(unname(st["tract_centroids"]), "PRESENT")
  expect_equal(unname(st["demand_machinery"]), "WIRED")
  expect_equal(unname(st["provider_coordinates"]), "PRESENT")
  expect_true(unname(st["drive_time_isochrones"]) %in% c("PRESENT", "MISSING"))
  expect_equal(unname(st["supply_machinery"]), "WIRED")
  expect_equal(unname(st["validation_gate"]), "WIRED")
  expect_equal(g$resolved, unname(st["drive_time_isochrones"]) == "PRESENT")
})

test_that("the ordering trap is recorded, because the wrong step looks easiest", {
  g <- geographic_access_status()
  # Wiring R/geography-spatial_access_e2sfca is a one-line change and is the obvious first move. Done before
  # coordinates exist it falls back to state geometry and emits a plausible
  # access ratio that means nothing -- worse than dormancy, which emits none.
  # Match the CLAIM, never the module path: a rename rewrites source and test
  # independently and they drift apart. The path is checked for existence by
  # "every file path the register names actually exists" instead.
  expect_match(g$ordering_trap, "Do NOT wire")
  expect_match(g$ordering_trap, "first")
  expect_match(g$ordering_trap, "state-level geometry")
  expect_true(any(grepl("isochrones", g$resolved_by)))
  # Both the validation_report check and the orchestrator wiring are DONE
  # (validation_gate and supply_machinery are WIRED), so neither is listed as
  # remaining work. The isochrone import is the ONE item left.
  expect_false(any(grepl("validation_report", g$resolved_by)))
  expect_false(any(grepl("orchestrator", g$resolved_by)))
  expect_length(g$resolved_by, 1L)
})

test_that("geographic access is NOT listed as survey-resolvable", {
  # It is an integration task. Putting it in the instrument would imply a
  # questionnaire could fix it, and someone would add questions instead of
  # importing isochrones.
  instrument <- urps_practice_survey_requirements()
  expect_false(any(grepl("isochrone|coordinate|geocod", instrument$variable)))
  u <- unresolved_calibration_items()
  expect_equal(u$resolved_by[u$item == "geographic_access"], "data integration")
  expect_true(all(u$resolved_by[u$item %in% c("capacity_anchor", "fte_curve")] ==
                    "practice survey"))
})

test_that("the register distinguishes 'cancels out' from 'not in the estimand'", {
  u <- unresolved_calibration_items()
  # Both read as "does not affect the answer" and they are not the same thing.
  # Delegation IS in the estimand and cancels arithmetically; geographic access
  # is absent from it entirely.
  expect_true(u$in_reported_estimand[u$item == "delegation_matrix"])
  expect_true(u$cancels_out[u$item == "delegation_matrix"])
  expect_false(u$in_reported_estimand[u$item == "geographic_access"])
  expect_true(is.na(u$cancels_out[u$item == "geographic_access"]))
})

test_that("the access layer is reached only through the fail-closed entry point", {
  # The layer is no longer dormant: run_geographic_access() calls compute_access()
  # (in the same file) and the orchestrator calls run_geographic_access(). But
  # that wrapper is where the fail-closed guard lives -- no membership artifact,
  # no computation, never fallback geometry. This asserts nothing calls
  # compute_access()/match_points_to_isochrones() DIRECTLY from R/ or scripts/,
  # i.e. bypasses the guard. A new direct call fails here and must instead route
  # through run_geographic_access() (or re-check the ordering trap).
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository root not reachable (source tree absent under R CMD check)")
  files <- c(list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE),
             list.files(file.path(root[1], "scripts"), pattern = "[.]R$",
                        full.names = TRUE, recursive = TRUE))
  # Same rule: exclude by definition, not by filename.
  .defines <- function(fs, pattern) vapply(fs, function(f)
    any(grepl(pattern, readLines(f, warn = FALSE))), logical(1))
  files <- files[!(.defines(files, "^geographic_access_status <- function") |
                     .defines(files, "^compute_access <- function"))]
  called_in <- function(f) {
    code <- sub("#.*$", "", readLines(f, warn = FALSE))
    any(grepl("(compute_access|match_points_to_isochrones)\\s*\\(", code))
  }
  hits <- vapply(files, called_in, logical(1))
  expect_false(any(hits),
               info = paste("access layer now called from:",
                            paste(basename(files[hits]), collapse = ", ")))
})

test_that("every file path the register names actually exists", {
  # THE FRAGILITY THIS CATCHES. fte_curve_status()$do_not_fix names a module by
  # path to point people away from it. A wholesale rename of R/ from numbered to
  # semantic prefixes rewrote that string correctly by luck; nothing verified it.
  # A status message that sends a reader to a file which no longer exists is
  # worse than no message, because it looks authoritative.
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository root not reachable (source tree absent under R CMD check)")

  texts <- c(unlist(fte_curve_status()), unlist(capacity_status()),
             unlist(geographic_access_status()[setdiff(names(geographic_access_status()),
                                                       "components")]),
             unlist(geographic_access_status()$components))
  texts <- texts[!is.na(texts)]
  # SOURCE-TREE paths only. This guards against a status message pointing a
  # reader to a renamed/removed module, which is a property of the tracked
  # source. data-raw/ (NPI roster, downloaded survey extracts) and artifacts/
  # (generated isochrone registries) are deliberately not in git -- see
  # tests/skip-budget.csv -- so a status detail that cites one for provenance is
  # legitimately absent in CI and is not a broken reference.
  paths <- unique(unlist(regmatches(
    texts, gregexpr("(R|tests|scripts|docs)/[A-Za-z0-9_./-]+", texts))))
  paths <- sub("[.,;:]+$", "", paths)
  skip_if(length(paths) == 0, "repository root not reachable (source tree absent under R CMD check)")

  missing <- paths[!vapply(paths, function(p) {
    file.exists(file.path(root[1], p)) || length(Sys.glob(file.path(root[1], p))) > 0
  }, logical(1))]
  expect_equal(missing, character(0),
               info = paste("status text names non-existent path(s):",
                            paste(missing, collapse = ", ")))
})

test_that("the register's function references resolve to real objects", {
  # Same failure mode one level down: naming a function that has been renamed
  # or removed. Checked against the namespace rather than the filesystem.
  pkg <- asNamespace("urpssim")
  texts <- c(unlist(fte_curve_status()), unlist(capacity_status()))
  texts <- texts[!is.na(texts)]
  fns <- unique(unlist(regmatches(texts, gregexpr("[a-zA-Z_.][A-Za-z0-9_.]*\\(\\)", texts))))
  fns <- sub("\\(\\)$", "", fns)
  # Only names this package could plausibly own; base/utils calls are fine.
  fns <- fns[!fns %in% c("sprintf", "paste", "c", "list", "function")]
  unknown <- fns[!vapply(fns, exists, logical(1), envir = pkg, inherits = TRUE)]
  expect_equal(unknown, character(0),
               info = paste("status text names unknown function(s):",
                            paste(unknown, collapse = ", ")))
})

# ---- Hours-curve gradient leverage -----------------------------------------
#
# The defect these pin: one quantity carried three different values at once --
# fte_curve_status() said FTE per head reaches 0.9231 by 2050 (176 FTE), README
# said "~3%", and the model produced 0.9115 (169 FTE). All three were prose. A
# borrowed input that does NOT cancel is exactly the one whose magnitude must be
# computed, so these assert the computation rather than the sentence.

test_that("a flat age gradient produces no FTE-per-head drift, by construction", {
  agents <- data.frame(age = c(35, 45, 55, 65), sex = "female")
  z <- fte_curve_gradient_leverage(agents, horizon_years = 25L, gradient_scale = 0)
  # gradient_scale = 0 means hours do not depend on age, so ageing changes
  # nothing. If this ever drifts, the scaling is not isolating the gradient.
  expect_equal(z$fte_per_head_base, 1, tolerance = 1e-9)
  expect_equal(z$fte_per_head_horizon, 1, tolerance = 1e-9)
  expect_equal(z$drift_pct, 0, tolerance = 1e-9)
})

test_that("every scale is normalised to 1.0 FTE per head at base year", {
  agents <- data.frame(age = seq(34, 70, by = 4), sex = rep(c("female", "male"), 5))
  z <- fte_curve_gradient_leverage(agents, horizon_years = 20L,
                                   gradient_scale = c(0, 0.5, 1, 1.5))
  # The rows must differ ONLY in shape. If the base column moved with the scale,
  # the comparison would be confounded by level and the whole helper would be
  # measuring the thing that already cancels.
  expect_true(all(abs(z$fte_per_head_base - 1) < 1e-9))
  # Steeper gradient, strictly more drift.
  expect_true(all(diff(z$drift_pct) < 0))
})

test_that("the published gradient's leverage is what the status object claims", {
  skip_if_not_installed("mufflyaccess")
  set.seed(1)
  agents <- suppressMessages(agents_from_certification_cohorts(2023L))
  z <- fte_curve_gradient_leverage(agents, horizon_years = 25L, gradient_scale = 1)

  # ~20% of FTE per head over 25 years on a cohort with no renewal. Pinned
  # loosely enough to survive cohort re-draws, tightly enough that the "~3%"
  # README figure -- which this replaces -- would fail.
  expect_gt(-z$drift_pct, 15)
  expect_lt(-z$drift_pct, 25)
  expect_lt(z$fte_delta_per_1000_head, -150)

  # And the status object's prose must carry a number in that range rather than
  # a stale one, since that is how the three-way disagreement arose.
  expect_match(fte_curve_status()$leverage, "20.0%", fixed = TRUE)
  expect_match(fte_curve_status()$leverage, "fte_curve_gradient_leverage", fixed = TRUE)
})

test_that("fte_curve_status measures its leverage when given a cohort", {
  # The prose figure is documentation; this is the number. Supplying a cohort
  # is what turns the status object from an assertion into a measurement, and
  # it is also what makes fte_curve_gradient_leverage() genuinely reachable
  # rather than named in a string literal -- the export-wiring gate reads a
  # mention as a call, so a prose-only reference would have registered as wired
  # while nothing invoked it.
  expect_null(fte_curve_status()$leverage_measured)

  agents <- data.frame(age = seq(34, 70, by = 4), sex = rep(c("female", "male"), 5))
  m <- fte_curve_status(agents)$leverage_measured
  expect_s3_class(m, "data.frame")
  expect_setequal(m$gradient_scale, c(0, 1))
  # Flat gradient does not drift; the published one does.
  expect_equal(m$drift_pct[m$gradient_scale == 0], 0, tolerance = 1e-9)
  expect_lt(m$drift_pct[m$gradient_scale == 1], -5)
})
