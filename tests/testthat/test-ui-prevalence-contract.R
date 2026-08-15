# UI prevalence is a POPULATION state. These tests assert the contract, not the
# implementation: the primary definition is the validated Incontinence Severity
# Index at >= 3, and the leakage phenotypes are never labelled as UI prevalence.

.ui_contract <- function() {
  yaml::read_yaml("../../config/calibration_targets.yml")$anchors$ui_prevalence
}

test_that("primary UI prevalence uses validated ISI >= 3", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  cd <- .ui_contract()$estimand$case_definition$primary
  expect_identical(cd$threshold, ">= 3")
  expect_identical(cd$method, "incontinence_severity_index")
  expect_false(identical(cd$method, "any_leakage"))
  expect_identical(cd$variables$frequency, "KIQ005")
  expect_identical(cd$variables$amount, "KIQ010")
})

test_that("the retired any-leakage definition is explicitly excluded", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  ex <- .ui_contract()$estimand$excluded_definitions
  expect_true("KIQ005_non_never_alone" %in% ex)
  expect_true(all(c("diagnosed_ui","treated_ui","care_seeking_ui") %in% ex))
})

test_that("raw leakage phenotypes are not labelled UI prevalence", {
  skip_if_not(file.exists("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds"))
  d <- readRDS("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds")
  prohibited <- c("stress_ui", "urgency_ui", "mixed_ui")
  expect_length(intersect(prohibited, unique(d$outcome)), 0L)
  # they must exist under phenotype names instead
  expect_true(all(c("stress_leakage_12m","urgency_leakage_12m") %in% d$outcome))
})

test_that("ISI >= 3 is materially stricter than any leakage", {
  skip_if_not(file.exists("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds"))
  d <- readRDS("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds")
  oldest <- d[d$outcome == "ui" & d$group == "75+", ]
  # the retired definition reached 0.781 at 75+; a severity-qualified state
  # must be well below that or the regeneration did not take
  expect_lt(oldest$prevalence, 0.65)
})

test_that("phenotypes may exceed primary prevalence without being an error", {
  skip_if_not(file.exists("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds"))
  d <- readRDS("../../data-raw/nhanes/nhanes_ui_prevalence_by_age.rds")
  ui <- d$prevalence[d$outcome == "ui" & d$group == "20-34"]
  st <- d$prevalence[d$outcome == "stress_leakage_12m" & d$group == "20-34"]
  # this is EXPECTED: different NHANES questions, not nested sets
  expect_gt(st, ui)
})

test_that("the UI anchor is time-aligned to the model fit year", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  cfg <- yaml::read_yaml("../../config/calibration_targets.yml")
  sel <- cfg$anchors$ui_prevalence$empirical_validation$selected_cycle
  bt  <- cfg$backtest$fit_through_year

  # the selected cycle must contain or abut the fit-through year
  yrs <- as.integer(strsplit(sel$cycle, "-")[[1]])
  expect_true(bt >= yrs[1] - 1 && bt <= yrs[2])

  # and it must be the CAPI cycle, so the series against the 2005-2006
  # replication is not mode-confounded
  expect_identical(sel$mode, "CAPI")

  # the later, larger cycle must be explicitly rejected with a reason, not
  # silently unused -- selection is by model year, never by sample size
  rejected <- vapply(sel$rejected, function(r) r$cycle, character(1))
  expect_true("2021-2023" %in% rejected)
  expect_length(cfg$anchors$ui_prevalence$empirical_validation$remaining_blockers, 0L)
})
