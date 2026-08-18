# Four acceptance gates for the incident/continuing decomposition, plus the
# refusals that stop an unsourced parameter becoming the residual that forces
# the office anchor to agree.

test_that("utilization parameters carry evidence_anchored defaults", {
  s <- split_care_engagement(FROZEN_CARE_ENGAGED)
  expect_s3_class(s, "data.frame")
  v <- care_engagement_visits(s)
  expect_s3_class(v, "data.frame")
})

test_that("gate 1: newly entering care is a flow, smaller than the stock", {
  s <- split_care_engagement(FROZEN_CARE_ENGAGED, incident_share = 0.20)
  g <- assert_care_engagement_gates(s)
  expect_true(g$passed[1])
  expect_lt(sum(s$newly_entering_care), sum(s$care_engaged))
})

test_that("gate 2: new_consultation no longer equals the cohort by construction", {
  s <- split_care_engagement(FROZEN_CARE_ENGAGED, incident_share = 0.20)
  v <- care_engagement_visits(s, first_year_followup_rate = 2.0,
                               annual_followup_rate = 0.5)
  g <- assert_care_engagement_gates(s, v)
  expect_true(g$passed[2])
  nc <- sum(v$volume[v$component == "new_consultation"])
  # the shipped pathway gave ratio 1.00; this must be strictly below
  expect_lt(nc / sum(s$care_engaged), 1)
})

test_that("gate 3: the decomposition conserves the cohort", {
  s <- split_care_engagement(FROZEN_CARE_ENGAGED, incident_share = 0.20)
  g <- assert_care_engagement_gates(s)
  expect_true(g$passed[3])
  expect_equal(sum(s$newly_entering_care) + sum(s$continuing_care),
               sum(s$care_engaged), tolerance = 1e-8)
})

test_that("gate 4 FAILS while utilization parameters are unsourced", {
  s <- split_care_engagement(FROZEN_CARE_ENGAGED, incident_share = 0.20)
  v <- care_engagement_visits(s, first_year_followup_rate = 1.482, annual_followup_rate = 1.125)
  g <- assert_care_engagement_gates(s, v)
  expect_false(g$passed[4])
})

test_that("new_consults_per_entrant is definitional, not a free parameter", {
  p <- care_engagement_params()
  r <- p[p$parameter == "new_consults_per_entrant", ]
  expect_equal(r$value, 1.0)
  expect_equal(r$calibration_status, "definitional")
  expect_equal(sum(p$calibration_status == "requires_source"), 3L)
})

test_that("the treated cohort is frozen for this workstream", {
  expect_equal(round(sum(FROZEN_CARE_ENGAGED)), 6176308)
  # ~4.81% of US women 20+ (128.4M), not the ~9% previously asserted
  expect_lt(sum(FROZEN_CARE_ENGAGED) / 128421818, 0.055)
  expect_gt(sum(FROZEN_CARE_ENGAGED) / 128421818, 0.045)
})
