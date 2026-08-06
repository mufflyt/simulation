# Reporting a baseline gap (R/61).
#
# The method/tier split means identical arithmetic can carry entirely different
# evidentiary weight. These tests pin the consequence: what a headline number
# must travel with, and which sentence it is allowed to license.

bg <- function(...) {
  args <- list(base_supply_fte = 1306, adequacy = 0.948, method = "capacity_survey",
               calibration_status = "derived_by_analogy",
               source_specialty = "physical therapy", externally_measured = FALSE,
               population = "US urogynaecologists", year = 2023L,
               evidence = "Zarek 2025 stand-in")
  do.call(baseline_gap, utils::modifyList(args, list(...)))
}

test_that("provenance reports every field a headline number must carry", {
  p <- baseline_gap_provenance(bg(adequacy_ci = c(0.90, 0.99)))
  expect_setequal(p$field,
                  c("method", "calibration_status", "population", "year",
                    "source_specialty", "externally_measured",
                    "uncertainty_interval", "base_supply_fte", "required_fte",
                    "shortfall_fte", "evidence"))
  expect_true(all(nzchar(p$value)))
  expect_equal(p$value[p$field == "source_specialty"], "physical therapy")
  expect_equal(p$value[p$field == "externally_measured"], "FALSE")
})

test_that("an undeclared field says UNDECLARED rather than vanishing", {
  bare <- baseline_gap(1306, 0.948, method = "capacity_survey",
                       calibration_status = "derived_by_analogy", evidence = "x")
  p <- baseline_gap_provenance(bare)
  # Silently omitting a field lets a reader assume it was considered. Saying
  # UNDECLARED is the difference between "not applicable" and "nobody recorded".
  for (f in c("population", "year", "source_specialty", "externally_measured",
              "uncertainty_interval")) {
    expect_equal(p$value[p$field == f], "UNDECLARED", info = f)
  }
})

test_that("the shortfall interval inverts, because required = supply / adequacy", {
  g <- bg(adequacy_ci = c(0.90, 0.99))
  # LOW adequacy means MORE required FTE. Carrying the adequacy CI through
  # without the swap would report an interval pointing the wrong way, and it
  # would look perfectly plausible.
  expect_lt(g$required_fte_ci[1], g$required_fte)
  expect_gt(g$required_fte_ci[2], g$required_fte)
  expect_equal(g$required_fte_ci[1], 1306 / 0.99, tolerance = 1e-6)
  expect_equal(g$required_fte_ci[2], 1306 / 0.90, tolerance = 1e-6)
  expect_equal(g$shortfall_fte_ci, g$required_fte_ci - 1306, tolerance = 1e-6)
  expect_error(baseline_gap(1306, 0.9, method = "assumed", evidence = "x",
                            adequacy_ci = c(0.9, -1)),
               "two finite positive numbers")
})

test_that("an external anchor requires tier AND measurement AND this specialty", {
  # All three, because any one alone is satisfiable by the borrowed case the
  # method/tier split exists to expose.
  expect_false(has_external_anchor(bg()))                                   # none
  expect_false(has_external_anchor(bg(calibration_status = "calibrated")))  # not measured
  expect_false(has_external_anchor(bg(externally_measured = TRUE)))         # wrong tier
  # Measured and calibrated, but the distribution is still another specialty's.
  expect_false(has_external_anchor(
    bg(calibration_status = "calibrated", externally_measured = TRUE,
       source_specialty = "physical therapy")))
  # The real thing.
  expect_true(has_external_anchor(
    bg(calibration_status = "calibrated", externally_measured = TRUE,
       source_specialty = "urogynaecology")))
  expect_false(has_external_anchor("not a gap"))
})

test_that("the claim language switches on the anchor, not on the number", {
  weak <- baseline_gap_claim(bg())
  expect_match(weak, "model-implied gap under the specified calibration")
  expect_match(weak, "not a measured shortage")
  expect_false(grepl("The current shortage is", weak, fixed = TRUE))
  # It must name WHY, so the sentence is self-justifying when pasted into prose.
  expect_match(weak, "physical therapy")
  expect_match(weak, "derived_by_analogy")

  strong <- baseline_gap_claim(
    bg(calibration_status = "calibrated", externally_measured = TRUE,
       source_specialty = "urogynaecology"))
  expect_match(strong, "The current shortage is")
  expect_match(strong, "US urogynaecologists")
  expect_match(strong, "2023")
})

test_that("the interval appears in the claim when it exists", {
  with_ci <- baseline_gap_claim(bg(adequacy_ci = c(0.90, 0.99)))
  expect_match(with_ci, "to 145 FTE")
  expect_false(grepl("FTE ()", baseline_gap_claim(bg()), fixed = TRUE))
})

test_that("shortage language is refused without a direct external anchor", {
  expect_error(assert_external_anchor(bg(), mode = "strict"),
               "cannot support the claim")
  expect_message(assert_external_anchor(bg(), mode = "relaxed"), "no direct external anchor")
  expect_false(suppressMessages(assert_external_anchor(bg(), mode = "relaxed")))
  # The refusal must hand back the sentence that IS supportable, or it just
  # blocks without helping.
  msg <- tryCatch(assert_external_anchor(bg(), mode = "strict"), error = conditionMessage)
  expect_match(msg, "model-implied gap")
  expect_true(assert_external_anchor(
    bg(calibration_status = "calibrated", externally_measured = TRUE,
       source_specialty = "urogynaecology"), mode = "strict"))
})

test_that("anchor sensitivity is the closed form, and finds where the sign flips", {
  s <- baseline_anchor_sensitivity(bg(), supply_at_target = 2116, demand_growth = 1.1479)
  # gap = supply - anchor * growth, so break-even is supply / growth exactly.
  expect_equal(s$breakeven_anchor_fte, 2116 / 1.1479, tolerance = 1e-9)
  expect_equal(s$gap_at_anchor, 2116 - s$anchor_fte * 1.1479, tolerance = 1e-9)
  # The decision-relevant numbers: a 34% anchor uplift erases the surplus, which
  # corresponds to base-year adequacy near 0.71 rather than the assumed 0.948.
  expect_equal(s$anchor_uplift_to_flip, 0.338, tolerance = 0.01)
  expect_equal(s$implied_adequacy_at_breakeven, 0.708, tolerance = 0.005)
  expect_lt(s$elasticity, 0)   # a bigger anchor shrinks the gap

  tab <- s$sensitivity
  expect_true(all(diff(tab$gap_fte) < 0))          # monotone decreasing
  expect_true(any(tab$conclusion == "surplus"))
  expect_true(any(tab$conclusion == "shortage"))   # the sweep must span the flip
})

test_that("required FTE scales with the anchor, so the gap is linear in it", {
  s1 <- baseline_anchor_sensitivity(bg(), 2116, 1.1479)
  # Doubling the anchor doubles required FTE at the horizon. If this ever became
  # invariant, the anchor would have stopped mattering and the model would have
  # no demand side at all.
  r <- s1$sensitivity$required_at_target
  m <- s1$sensitivity$anchor_multiplier
  expect_equal(r / m, rep(r[m == 1], length(r)), tolerance = 1e-8)
})
