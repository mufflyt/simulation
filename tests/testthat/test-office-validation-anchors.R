# These anchors validate a decomposition. They are NOT transition parameters,
# and the tests exist to stop them being assigned as such.

.va <- function() yaml::read_yaml("../../config/office_visit_validation_anchors.yml")

test_that("no anchor in this file may be used as a transition parameter", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  # Two legitimate roles live here: external_validation_only (a source that
  # validates a decomposition) and attempted_source (a source that was tried and
  # found insufficient, recorded so it is not retried blindly). NEITHER may
  # become a transition parameter, which is what this asserts.
  for (nm in names(.va())) {
    a <- .va()[[nm]]
    expect_false(isTRUE(a$is_transition_parameter), info = nm)
    expect_false(isTRUE(a$production_scalar_eligible), info = nm)
    expect_true(a$role %in% c("external_validation_only", "attempted_source"),
                info = nm)
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

test_that("MEPS 2023 is recorded as attempted and insufficient", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  m <- .va()$meps_2023_person_utilization
  expect_identical(m$status, "insufficient")
  expect_false(isTRUE(m$is_transition_parameter))
  # the thinness must be recorded numerically, not just asserted
  expect_lt(m$measures$adult_women_with_any_visit, 100)
  # and the POP gap must be explicit
  expect_true(any(grepl("POP IS ENTIRELY ABSENT", unlist(m$why_insufficient))))
})

test_that("Panel 27 is recorded with estimates, uncertainty, n, and definitions", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  m <- .va()$meps_panel27_longitudinal
  expect_identical(m$status, "insufficient")
  # the gate must preserve all four: point estimate, interval, unweighted n,
  # and an explicit phenotype/washout definition
  for (e in c("annual_return_probability", "conditional_followup_intensity",
              "unconditional_followup_intensity")) {
    est <- m$estimates[[e]]
    expect_true(is.numeric(est$weighted))
    expect_length(est$ci, 2L)
    expect_true(is.numeric(est$unweighted_n))
    expect_true(nzchar(est$definition))
  }
  expect_true(nzchar(m$washout_definition))
  expect_match(m$washout_definition, "not true first-ever")
  expect_identical(m$phenotype$primary, 'CCSR1X == "GEN008"')
  # N39 must stay out of the primary phenotype
  expect_match(m$phenotype$excluded_from_primary, "N39")
  # and the sample must be recorded as too small to move a parameter
  expect_lt(m$baseline_cohort$unweighted_n, 30)
})

test_that("the outcome-conditioned estimate is withdrawn, not silently dropped", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  m <- .va()$meps_panel27_longitudinal
  # 5.68 conditioned the denominator on the outcome. It must remain on the
  # record as withdrawn so it cannot be rediscovered and reused.
  expect_equal(m$withdrawn_estimate$value, 5.68)
  expect_match(m$withdrawn_estimate$reason, "conditions the denominator")
  expect_gt(m$withdrawn_estimate$overstatement_vs_corrected, 2)
})

test_that("the unconditional intensity retains zeroes and reconciles exactly", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  e <- .va()$meps_panel27_longitudinal$estimates
  # E(visits) = P(return) x E(visits | return). If this identity ever fails,
  # zeroes have been dropped from the denominator somewhere.
  expect_equal(e$annual_return_probability$weighted *
                 e$conditional_followup_intensity$weighted,
               e$unconditional_followup_intensity$weighted, tolerance = 1e-3)
  # the conditional mean is bounded below by 1 by construction; the
  # unconditional one is not, and that is the whole point
  expect_gt(e$conditional_followup_intensity$weighted, 1)
  expect_lt(e$unconditional_followup_intensity$weighted,
            e$conditional_followup_intensity$weighted)
  expect_true(e$unconditional_followup_intensity$unweighted_n >
                e$conditional_followup_intensity$unweighted_n)
})

test_that("entrant estimates carry index-month censoring, not a bare mean", {
  skip_if_not(file.exists("../../config/office_visit_validation_anchors.yml"))
  ec <- .va()$meps_panel27_longitudinal$entrant_censoring
  # a raw post-index mean is uninterpretable without index month; all four
  # censoring quantities must be present
  expect_true(is.numeric(ec$mean_index_month))
  expect_true(is.numeric(ec$mean_months_observable))
  expect_true(is.numeric(ec$post_index_visits_raw))
  expect_true(is.numeric(ec$post_index_rate_per_person_month))
  # the average entrant is first observed late in the year, so the raw mean is
  # censored downward -- the record must say so
  expect_gt(ec$mean_index_month, 6)
  expect_lt(ec$mean_months_observable, 6)
  expect_match(ec$fixed_window_sensitivity$note, "NOT as an estimate")
})

test_that("no model parameter was written from Panel 27", {
  skip_if_not(file.exists("../../R/demand-incident_continuing_care.R"))
  p <- care_engagement_params()
  # gate 4 stays red: the three utilization parameters remain unsourced
  for (nm in c("incident_share", "first_year_followup_rate", "annual_followup_rate")) {
    row <- p[p$parameter == nm, ]
    expect_identical(row$calibration_status, "requires_source")
    expect_true(is.na(row$value))
  }
})
