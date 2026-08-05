# The URPS practice survey: one instrument, two open items (R/56).
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
  # It used to be an independent tribble in R/55. A duplicate list is a drift
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
  # R/40 carries TODO-FTE-001 and the derived_by_analogy tier, so it looks like
  # the thing to replace. apply_hrsa_surgical_fte() is called by nothing but its
  # own tests, so fixing it would change no output. The live target is
  # hwsm_reference_hours().
  expect_match(fs$current_source, "hwsm_reference_hours")
  expect_match(fs$do_not_fix, "R/40")
  expect_match(fs$do_not_fix, "dormant")
  expect_match(fs$leverage, "does NOT cancel")
})

test_that("R/40 really is dormant, so the warning stays true", {
  # If a future change wires apply_hrsa_surgical_fte() into the pipeline, this
  # fails and fte_curve_status()$do_not_fix must be rewritten.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  r_files <- list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE)
  # Exclude the definition, and exclude R/56 -- it names the function inside
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
