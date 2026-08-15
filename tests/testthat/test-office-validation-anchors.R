# These anchors validate a decomposition. They are NOT transition parameters,
# and the tests exist to stop them being assigned as such.

.va <- function() yaml::read_yaml("../../config/office_visit_validation_anchors.yml")

test_that("no validation anchor may be used as a transition parameter", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  for (nm in names(.va())) {
    a <- .va()[[nm]]
    expect_false(isTRUE(a$is_transition_parameter), info = nm)
    expect_false(isTRUE(a$production_scalar_eligible), info = nm)
    expect_identical(a$role, "external_validation_only", info = nm)
  }
})

test_that("the CMS E/M mix is recorded with its within-cell caveat", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  a <- .va()$cms_fpmrs_em_mix
  expect_equal(a$measures$new_em_share, 0.1904, tolerance = 1e-4)
  # the per-beneficiary ratio must be labelled a LOWER BOUND, since CMS
  # beneficiary counts are distinct only within NPI x HCPCS x POS
  expect_true(any(grepl("LOWER BOUND", unlist(a$caveats))))
})

test_that("the utilization parameters remain unsourced", {
  # CMS answers a service-share question; it cannot answer a person-level
  # first-year-versus-continuing question. Gate 4 must still fail.
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  p <- care_engagement_params()
  still_needed <- p$parameter[p$calibration_status == "requires_source"]
  expect_true("first_year_followup_rate" %in% still_needed)
  expect_true("annual_followup_rate" %in% still_needed)
})

test_that("two independent sources converge on the new-patient share", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  cms <- .va()$cms_fpmrs_em_mix$measures$new_em_share
  nam <- .va()$namcs_2019_visit_mix$measures$new_patient_visit_share
  # convergence is worth asserting; it is the strongest signal available that
  # ~19% is the right order, though it is still a SERVICE share
  expect_lt(abs(cms - nam), 0.02)
})
