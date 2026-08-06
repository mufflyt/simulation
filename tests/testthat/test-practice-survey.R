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
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  r_files <- list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE)
  # Exclude the definition, and exclude R/data-practice_survey -- it names the function inside
  # fte_curve_status()'s message string precisely to warn people off it, which
  # is a mention in a string literal rather than a call.
  r_files <- r_files[!grepl("40-hrsa_fte_calibration|56-practice_survey", r_files)]
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

test_that("geographic access is registered as absent, not as miscalibrated", {
  g <- geographic_access_status()
  expect_false(g$resolved)
  expect_equal(nrow(g$components), 7L)
  expect_equal(g$n_present + g$n_missing, nrow(g$components))

  st <- stats::setNames(g$components$state, g$components$component)
  # Two of the three inputs the methods doc names are DONE. Reporting this item
  # as "build a geographic bundle" would send someone to rebuild them.
  expect_equal(unname(st["tract_population"]), "PRESENT")
  expect_equal(unname(st["tract_centroids"]), "PRESENT")
  expect_equal(unname(st["demand_machinery"]), "WIRED")
  # What is actually missing.
  expect_equal(unname(st["provider_coordinates"]), "MISSING")
  expect_equal(unname(st["drive_time_isochrones"]), "MISSING")
  expect_equal(unname(st["supply_machinery"]), "DORMANT")
  expect_equal(unname(st["validation_gate"]), "MISSING")
})

test_that("the ordering trap is recorded, because the wrong step looks easiest", {
  g <- geographic_access_status()
  # Wiring R/geography-spatial_access_e2sfca is a one-line change and is the obvious first move. Done before
  # coordinates exist it falls back to state geometry and emits a plausible
  # access ratio that means nothing -- worse than dormancy, which emits none.
  expect_match(g$ordering_trap, "Do NOT wire R/geography-spatial_access_e2sfca first")
  expect_match(g$ordering_trap, "state-level geometry")
  expect_true(any(grepl("isochrones", g$resolved_by)))
  expect_true(any(grepl("validation_report", g$resolved_by)))
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

test_that("R/geography-spatial_access_e2sfca really is dormant, so the trap warning stays true", {
  # If a future change calls the access layer from R/ or scripts/, this fails
  # and geographic_access_status() must be re-checked -- especially whether
  # provider coordinates arrived first.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  files <- c(list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE),
             list.files(file.path(root[1], "scripts"), pattern = "[.]R$",
                        full.names = TRUE, recursive = TRUE))
  files <- files[!grepl("14-spatial_access_e2sfca|56-practice_survey", files)]
  called_in <- function(f) {
    code <- sub("#.*$", "", readLines(f, warn = FALSE))
    any(grepl("(compute_access|match_points_to_isochrones)\\s*\\(", code))
  }
  hits <- vapply(files, called_in, logical(1))
  expect_false(any(hits),
               info = paste("access layer now called from:",
                            paste(basename(files[hits]), collapse = ", ")))
})
