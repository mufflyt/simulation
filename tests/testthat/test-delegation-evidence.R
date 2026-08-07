# Delegation evidence and sensitivity (R/supply-delegation_evidence).
#
# The matrix stays an assumption. These tests pin the two things that keep it
# an HONEST assumption: that the corroboration reports its own limits, and that
# the least-evidenced constant in the workload path has its influence measured
# rather than asserted to be small.

de_realized <- function() {
  tibble::tibble(
    service = c("pessary_care", "pessary_care", "sling_procedure",
                "sling_procedure", "cystoscopy"),
    provider_type = c("Nurse Practitioner", "Urology", "Physician Assistant",
                      "Urology", "Urology"),
    billed_services = c(20, 80, 5, 95, 100)
  )
}

test_that("the APP share is measured from the APP provider types only", {
  c1 <- medicare_delegation_corroboration(de_realized())
  expect_equal(c1$measured_app_share[c1$service == "pessary_care"], 0.20)
  expect_equal(c1$measured_app_share[c1$service == "sling_procedure"], 0.05)
  expect_equal(c1$measured_app_share[c1$service == "cystoscopy"], 0)
})

test_that("a zero measured share yields NA, not an infinite ratio", {
  c1 <- medicare_delegation_corroboration(de_realized())
  # Inf reads as "infinitely overstated"; it means "no APP-billed service in
  # this cell", which is a different claim.
  expect_true(is.na(c1$ratio[c1$service == "cystoscopy"]))
  expect_true(is.finite(c1$ratio[c1$service == "pessary_care"]))
})

test_that("the corroboration carries the limits that stop it being adopted", {
  c1 <- medicare_delegation_corroboration(de_realized())
  cav <- attr(c1, "caveats")
  # Incident-to billing is the reason a claims-measured APP share is a LOWER
  # bound. Dropping this caveat would make the comparison look like a refutation
  # of the matrix rather than a bounded check on it.
  expect_true(any(grepl("[Ii]ncident-to", cav)))
  expect_true(any(grepl("LOWER BOUND", cav)))
  expect_true(any(grepl("no urogynaecology provider type", cav, ignore.case = TRUE)))
  # E/M services are absent from Medicare's URPS basket, and they are where
  # delegation is highest -- so the comparison must declare them unobserved.
  expect_true(all(c("new_consultation", "return_visit", "postoperative_care") %in%
                    attr(c1, "services_not_observed")))
})

test_that("the delegation matrix is still declared an assumption", {
  # If this ever flips to "calibrated", it must be because a survey or an
  # NPPES-linked claims extract was fielded -- not because this module exists.
  expect_equal(urpssim:::URPS_DELEGATION_STATUS, "derived_by_analogy")
  expect_match(urpssim:::URPS_DELEGATION_SOURCE, "NOT a urogynaecology survey")
})

de_volumes <- function() {
  tidyr::expand_grid(
    year = c(2025, 2050),
    service = c("new_consultation", "return_visit", "sling_procedure",
                "urodynamics", "pessary_care", "cystoscopy", "botox_bladder",
                "ptns", "bladder_instillation", "prolapse_procedure",
                "postoperative_care")
  ) |>
    dplyr::mutate(volume = rep(c(3e6, 7e6, 3e5, 9e5, 1e6, 5e5, 1e5, 3e5, 2e5,
                                 2e5, 1e6), 2) * rep(c(1, 1.15), each = 11))
}

test_that("required FTE is INVARIANT to the delegation capacity factor", {
  s <- delegation_capacity_sensitivity(de_volumes())

  # THE CONTRACT, and it corrects an intuition worth stating: work RVUs scale
  # linearly with the factor, but calibrate_wrvu_per_fte() SOLVES productivity
  # against the base-year anchor, so the denominator scales identically and the
  # ratio cancels. The projected gap is therefore robust to the least-evidenced
  # constant in the workload path. A sweep that reported only work RVUs would
  # imply the opposite.
  expect_true(all(diff(s$urps_wrvu) > 0))
  expect_equal(diff(range(s$required_fte_base)), 0, tolerance = 1e-8)
  expect_equal(diff(range(s$required_fte_target)), 0, tolerance = 1e-8)
  # Productivity absorbs it, in proportion.
  expect_equal(s$solved_wrvu_per_fte / s$urps_wrvu,
               rep(s$solved_wrvu_per_fte[1] / s$urps_wrvu[1], nrow(s)),
               tolerance = 1e-8)
})

test_that("the capacity factor is falsifiable through implied productivity", {
  s <- delegation_capacity_sensitivity(de_volumes())
  # It moves nothing in the gap, but it does move a quantity with a published
  # plausible range -- which is what makes the Medicare-measured share testable
  # rather than merely different.
  expect_true(all(diff(s$solved_wrvu_per_fte) > 0))
  expect_true(any(s$productivity_plausible))
  expect_false(all(s$productivity_plausible))
})

test_that("the real artifact corroborates the borrowed ordering", {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0, "repository root not reachable (source tree absent under R CMD check)")
  skip_if_not(length(list.files(file.path(root[1], "artifacts"),
                                pattern = "^medicare_realized_care_.*\\.rds$")) > 0)
  c1 <- medicare_delegation_corroboration(
    urpssim:::.load_realized_medicare(file.path(root[1], "artifacts")))
  # The Forte SHAPE transfers -- care-management services are the most delegated
  # in both -- while the LEVEL does not, which is the same shape-not-level split
  # rescale_delegation_to_capacity() found for the subspecialist column.
  expect_gt(attr(c1, "rank_correlation"), 0.5)
  expect_gt(max(c1$ratio, na.rm = TRUE), 2)
})
