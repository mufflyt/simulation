# Demand estimands (R/reporting-demand_estimands.R).
#
# What these protect: a single calibration_status makes two opposite errors at
# once. It blocks reporting of realized-care demand, which needs no adequacy
# estimate, and it would license reporting of adequate-need demand the moment
# one procedure anchor landed. The estimands must gate independently.

test_that("the dimensions are distinct quantities, not synonyms", {
  expect_length(DEMAND_CALIBRATION_DIMENSIONS, 4L)
  expect_true(all(c("disease_burden", "care_seeking", "access_barriers",
                    "baseline_adequacy") %in% DEMAND_CALIBRATION_DIMENSIONS))
})

test_that("realized_care does NOT require baseline adequacy", {
  # The central claim. Utilization cannot identify adequacy, but it does not
  # need to in order to answer "what if current patterns continue".
  expect_false("baseline_adequacy" %in% DEMAND_ESTIMANDS$realized_care)
  expect_true("baseline_adequacy" %in% DEMAND_ESTIMANDS$adequate_need)
  # The equity counterfactual needs an access model; status quo does not.
  expect_false("access_barriers" %in% DEMAND_ESTIMANDS$realized_care)
  expect_true("access_barriers" %in% DEMAND_ESTIMANDS$reduced_barrier)
})

test_that("estimands gate independently on the same evidence", {
  # The whole point of splitting the status: one evidence state, different
  # verdicts per question.
  st <- c(disease_burden = "fitted", care_seeking = "calibrated",
          access_barriers = "fitted", baseline_adequacy = "derived_by_analogy")

  expect_true(demand_estimand_status(st, "realized_care")$reportable)
  expect_true(demand_estimand_status(st, "reduced_barrier")$reportable)
  # derived_by_analogy is the tier the adequacy anchor sits at, and it is below
  # the bar -- so the adequacy-dependent estimand alone is blocked.
  expect_false(demand_estimand_status(st, "adequate_need")$reportable)
  expect_equal(demand_estimand_status(st, "adequate_need")$weakest_dimension,
               "baseline_adequacy")
})

test_that("the weakest required dimension governs", {
  st <- c(disease_burden = "placeholder_uncalibrated", care_seeking = "calibrated",
          access_barriers = "calibrated", baseline_adequacy = "calibrated")
  s <- demand_estimand_status(st, "realized_care")
  expect_false(s$reportable)
  expect_equal(s$weakest_dimension, "disease_burden")
  expect_match(s$note, "disease_burden")
  expect_match(s$note, "weakest required dimension governs")
})

test_that("undeclared dimensions are uncalibrated, not assumed fine", {
  # Silence must never buy reportability.
  expect_false(demand_estimand_status(c(disease_burden = "calibrated"),
                                      "realized_care")$reportable)
  expect_false(demand_estimand_status(NULL, "realized_care")$reportable)
  expect_equal(demand_estimand_status(NULL, "realized_care")$dimension_status[["care_seeking"]],
               "undeclared")
  # An unrecognised status string is not a promotion either.
  expect_false(demand_estimand_status(
    c(disease_burden = "looks_fine", care_seeking = "calibrated"),
    "realized_care")$reportable)
})

test_that("unknown dimension names are refused, not silently ignored", {
  # A typo that is quietly dropped would make an estimand look better supported
  # than it is.
  expect_error(
    demand_estimand_status(c(disease_burdn = "calibrated"), "realized_care"),
    "unrecognised dimension")
  expect_error(demand_estimand_status(c("calibrated"), "realized_care"),
               "must be named by dimension")
})

test_that("everything is reportable only when every dimension clears the bar", {
  all_ok <- setNames(rep("calibrated", 4), DEMAND_CALIBRATION_DIMENSIONS)
  tab <- demand_estimand_table(all_ok)
  expect_equal(nrow(tab), 3L)
  expect_true(all(tab$reportable))
})

test_that("the table reports the repo's ACTUAL current position", {
  # As of the demand pipeline status: UI fitted, POP by analogy, AI placeholder,
  # calibration illustrative, adequacy an analogy. Nothing is reportable yet --
  # but the table says WHY per estimand, which a single status cannot.
  now <- c(disease_burden = "placeholder_uncalibrated",   # dmdm_ai still placeholder
           care_seeking = "uncalibrated_illustrative",    # anchors=illustrative_fallback
           access_barriers = "derived_by_analogy",
           baseline_adequacy = "derived_by_analogy")
  tab <- demand_estimand_table(now)
  expect_false(any(tab$reportable))
  expect_equal(tab$weakest_dimension[tab$estimand == "realized_care"], "disease_burden")

  # And the point of the split: fixing disease burden and care seeking alone
  # releases realized_care WITHOUT releasing adequate_need.
  fixed <- now
  fixed[["disease_burden"]] <- "fitted"
  fixed[["care_seeking"]] <- "calibrated"
  tab2 <- demand_estimand_table(fixed)
  expect_true(tab2$reportable[tab2$estimand == "realized_care"])
  expect_false(tab2$reportable[tab2$estimand == "adequate_need"])
})
