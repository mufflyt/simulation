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

# ---- no dimension inherits credibility from another ------------------------
#
# THE RULE: evidence about one dimension must never raise the standing of a
# different one. Two specific failures it forbids:
#
#   realized_care = calibrated   must never promote   baseline_adequacy
#   strong access_barriers       must never convert a DIRECTIONAL adequacy
#                                concern into an identified MAGNITUDE
#
# This is the whole reason the dimensions are separate. If they could borrow
# from each other, splitting them would be decoration: one well-anchored
# utilization dataset would quietly license every downstream claim, which is
# precisely the "assume the base year is in equilibrium" move the Dall lineage
# moved away from.

test_that("raising one dimension never changes another's recorded status", {
  # Property test over every ordered pair: perturb one dimension, confirm no
  # other dimension's status moves. Independence proven, not asserted.
  base <- setNames(rep("placeholder_uncalibrated", length(DEMAND_CALIBRATION_DIMENSIONS)),
                   DEMAND_CALIBRATION_DIMENSIONS)
  for (a in DEMAND_CALIBRATION_DIMENSIONS) {
    bumped <- base
    bumped[[a]] <- "calibrated"
    for (e in names(DEMAND_ESTIMANDS)) {
      s0 <- demand_estimand_status(base, e)$dimension_status
      s1 <- demand_estimand_status(bumped, e)$dimension_status
      others <- setdiff(names(s1), a)
      expect_identical(s1[others], s0[others],
                       info = paste("bumping", a, "moved another dimension in", e))
    }
  }
})

test_that("a calibrated realized_care does not release adequate_need", {
  # The headline case. Perfect utilization evidence, adequacy still an analogy.
  st <- c(disease_burden = "calibrated", care_seeking = "calibrated",
          access_barriers = "calibrated", baseline_adequacy = "derived_by_analogy")
  expect_true(demand_estimand_status(st, "realized_care")$reportable)
  expect_true(demand_estimand_status(st, "reduced_barrier")$reportable)
  expect_false(demand_estimand_status(st, "adequate_need")$reportable)
  expect_equal(demand_estimand_status(st, "adequate_need")$weakest_dimension,
               "baseline_adequacy")
})

test_that("only baseline_adequacy can release adequate_need", {
  # Every other dimension maxed; adequate_need still blocked. Then raise
  # adequacy alone and it releases. No substitute exists for that evidence.
  st <- setNames(rep("calibrated", length(DEMAND_CALIBRATION_DIMENSIONS)),
                 DEMAND_CALIBRATION_DIMENSIONS)
  st[["baseline_adequacy"]] <- "derived_by_analogy"
  expect_false(demand_estimand_status(st, "adequate_need")$reportable)

  st[["baseline_adequacy"]] <- "calibrated"
  expect_true(demand_estimand_status(st, "adequate_need")$reportable)
})

test_that("strong access evidence cannot turn direction into magnitude", {
  # access_barriers = calibrated is exactly the state the mystery-caller work
  # aims at. It must still leave the adequacy interpretation directional: the
  # vocabulary offers no term asserting a level, whatever the access evidence.
  st <- setNames(rep("calibrated", length(DEMAND_CALIBRATION_DIMENSIONS)),
                 DEMAND_CALIBRATION_DIMENSIONS)
  st[["baseline_adequacy"]] <- "derived_by_analogy"
  expect_false(demand_estimand_status(st, "adequate_need")$reportable)

  # And the evidence layer still refuses to name a magnitude.
  expect_error(
    adequacy_evidence_table(
      evidence = "Mystery-caller access", model_implication = "~5% shortfall",
      observed = "32% obtained an appointment",
      interpretation = "adequacy is 1.5x", citation = "audit 2026"),
    "controlled vocabulary")
})

# ---- Live dimension statuses ------------------------------------------------
#
# demand_estimand_status() existed with no caller supplying a real
# dimension_status -- only tests passing hypothetical vectors. The framework
# could say what each estimand WOULD need and not what any of them IS.

test_that("dimension statuses are read from the objects that own them", {
  st <- demand_dimension_status()
  expect_setequal(names(st), DEMAND_CALIBRATION_DIMENSIONS)

  # Access is ABSENT rather than wrong -- the layer is called by nothing -- and
  # absence must still block, so it sits at the floor of the ranking.
  expect_identical(unname(st["access_barriers"]),
                   if (isTRUE(geographic_access_status()$resolved)) "calibrated"
                   else "uncalibrated_illustrative")

  # Adequacy is undeclared without a gap object: an adequacy nobody stated is
  # not an adequacy, and silence must not buy reportability.
  expect_identical(unname(st["baseline_adequacy"]), "undeclared")

  g <- baseline_gap(1306, 0.948, method = "capacity_survey",
                    calibration_status = "derived_by_analogy")
  expect_identical(unname(demand_dimension_status(gap = g)["baseline_adequacy"]),
                   "derived_by_analogy")
  # A measured gap propagates too -- the tier is read, not assumed.
  g2 <- baseline_gap(1306, 0.948, method = "capacity_survey",
                     calibration_status = "calibrated")
  expect_identical(unname(demand_dimension_status(gap = g2)["baseline_adequacy"]),
                   "calibrated")
})

test_that("no estimand is reportable under the current evidence state", {
  # Records the state rather than asserting it is acceptable. If a fit or an
  # access import later lifts a dimension, this fails and must be re-read --
  # which is the point: reportability should never change silently.
  tab <- demand_estimand_table(demand_dimension_status())
  expect_false(any(tab$reportable))
  expect_identical(tab$weakest_dimension[tab$estimand == "realized_care"],
                   "disease_burden")
  expect_identical(tab$weakest_dimension[tab$estimand == "reduced_barrier"],
                   "access_barriers")

  # realized_care is ONE fit away: supplying an onset model lifts disease_burden
  # to `fitted`, which is the reportability floor.
  st <- demand_dimension_status(onset_model = structure(list(), class = "stub"))
  tab2 <- demand_estimand_table(st)
  expect_true(tab2$reportable[tab2$estimand == "realized_care"])
  # ...and does NOT rescue the two that need access.
  expect_false(any(tab2$reportable[tab2$estimand != "realized_care"]))
})
