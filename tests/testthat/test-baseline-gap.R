# Regression guards for base-year supply adequacy.
#
# The central assertion of this file: the starting shortfall must never be
# forced to zero. HWMM v5.19.20 names base-year equilibrium as a conceptual
# limitation -- it "essentially presents future adequacy relative to current
# levels" and lets any current imbalance persist invisibly into the projection.

test_that("capacity-survey adequacy reproduces the published PT calculation", {
  # Zarek 2025 PTJ: four categories, weighted mean 94.8% -> 5.2% gap ->
  # 12,070 FTE on a 233,890 FTE workforce.
  responses <- tibble::tribble(
    ~category,         ~n,  ~seen, ~additional,
    "equilibrium",    343,     NA,          NA,
    "surplus",        268,     25,           5,
    "shortage_hours", 449,     31,           4,
    "shortage_unmet", 363,     35,           5
  )
  res <- capacity_survey_adequacy(responses)

  # Per-category adequacy ratios published in the paper.
  d <- res$detail
  expect_equal(d$adequacy[d$category == "equilibrium"], 1.000, tolerance = 1e-3)
  expect_equal(d$adequacy[d$category == "surplus"], 1.167, tolerance = 1e-3)
  expect_equal(d$adequacy[d$category == "shortage_hours"], 0.852, tolerance = 1e-3)
  expect_equal(d$adequacy[d$category == "shortage_unmet"], 0.858, tolerance = 1e-3)

  # The published weighted mean: 94.8% adequacy -> a 5.2% national shortfall.
  # Compared at the precision the paper reports, since 0.9482 rounds to 94.8%.
  expect_equal(round(res$adequacy, 3), 0.948)
  expect_equal(round(res$gap_fraction, 3), 0.052)

  # ...which on the published 233,890 FTE workforce is the published 12,070 FTE.
  expect_equal(required_fte_base_year(233890, res$adequacy) - 233890,
               12070, tolerance = 250)
})

test_that("the bottom-up route lets a spare-capacity group offset the shortfall", {
  # Dall 2021 physiatry: 410 + 650 - 120 = 940 FTE on 8,850 FTE = 10.6%.
  # The "not busy enough" group contributes NEGATIVE 120 FTE; a single scalar
  # adequacy fraction cannot represent that offset.
  responses <- tibble::tribble(
    ~category,            ~national_fte, ~over_under_capacity,
    "at_capacity",                 5230,                0.000,
    "over_capacity_able",          1730,                0.239,
    "over_capacity_too_busy",      1530,                0.423,
    "not_busy_enough",              360,               -0.337
  )
  res <- capacity_survey_shortfall_bottom_up(responses)
  expect_equal(res$shortfall_fte, 940, tolerance = 5)
  expect_equal(res$shortfall_pct, 10.6, tolerance = 0.2)
  # The offset is real: dropping the not-busy-enough group inflates the estimate.
  without <- capacity_survey_shortfall_bottom_up(responses[1:3, ])
  expect_gt(without$shortfall_fte, res$shortfall_fte)
})

test_that("required FTE is supply divided by adequacy", {
  expect_equal(required_fte_base_year(233890, 0.948), 233890 / 0.948)
  # The user-facing form supply / (1 - gap) agrees.
  expect_equal(required_fte_base_year(1306, 1 - 0.052), 1306 / (1 - 0.052))
})

test_that("HPSA-removal counts are an accepted second route", {
  res <- hpsa_removal_shortfall(providers_to_remove = 200, base_supply_fte = 1306)
  expect_equal(res$required_fte, 1506)
  expect_equal(res$adequacy, 1306 / 1506)
  expect_equal(res$shortfall_fte, 200)
})

test_that("an assumed gap demands an evidence ledger", {
  # Dall 2013 assumed 10% adult / 20% child neurology and justified it with wait
  # times, Medicaid non-acceptance and vacancies.
  expect_message(assumed_baseline_gap(0.10, character(0), 16366), "no supporting evidence")
  res <- assumed_baseline_gap(
    0.10,
    evidence = c("new-patient wait 34.8 business days, up from 28.1",
                 "39% of children's hospitals with 12-month vacancies"),
    base_supply_fte = 16366
  )
  expect_equal(res$required_fte, 16366 / 0.9)
  expect_equal(res$method, "assumed")
  expect_length(res$evidence, 2)
})

test_that("base-year equilibrium is refused, not silently accepted", {
  equilibrium <- baseline_gap(1306, adequacy = 1.0, method = "assumed")
  expect_false(assert_baseline_gap_estimated(equilibrium, mode = "relaxed"))
  expect_error(assert_baseline_gap_estimated(equilibrium, mode = "strict"),
               "base-year equilibrium")
  expect_error(assert_baseline_gap_estimated(NULL, mode = "strict"),
               "No base-year supply-adequacy estimate")

  measured <- baseline_gap(1306, 0.948, method = "capacity_survey")
  expect_true(assert_baseline_gap_estimated(measured, mode = "strict"))
  expect_gt(measured$shortfall_fte, 0)
})

test_that("an access deficit cannot be counted twice", {
  # Dall 2013: the +460 FTE non-metropolitan equity effect "is part of the
  # assumed current shortfall" -- adding a reduced-barriers scenario on top of a
  # gap derived from access indicators books the same deficit twice.
  gap <- baseline_gap(1306, 0.90, method = "capacity_survey",
                      access_components_counted = c("nonmetro", "uninsured"))
  expect_error(
    assert_access_not_double_counted(gap, c("nonmetro", "racial_equity"), mode = "strict"),
    "double-count"
  )
  overlap <- assert_access_not_double_counted(gap, "racial_equity", mode = "strict")
  expect_length(overlap, 0)
})

test_that("published reference gaps are internally consistent", {
  ref <- published_baseline_gaps()
  expect_true(all(ref$shortfall_pct > 0))
  # shortfall_pct is expressed as a share of DEMAND (supply + shortfall).
  implied <- 100 * ref$shortfall_fte / (ref$supply_fte + ref$shortfall_fte)
  expect_equal(implied, ref$shortfall_pct, tolerance = 0.6)
})
