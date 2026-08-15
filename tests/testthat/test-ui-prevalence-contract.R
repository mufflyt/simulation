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
