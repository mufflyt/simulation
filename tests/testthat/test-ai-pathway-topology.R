# AI pathway topology.
#
# The AI limb is mis-specified UPSTREAM of recurrence: PTNS occupies the
# definitive-procedure state, so "recurrence after definitive treatment" is
# undefined. These tests pin the structural separations that must hold once
# SNM and sphincteroplasty are modelled, and they contain NO numerical AI
# probability -- the topology is the subject, not the values.
# See docs/AI_PATHWAY_TOPOLOGY_AUDIT.md.

.ai_rows <- function() {
  pw <- condition_service_pathway()
  pw[pw$condition == "ai", , drop = FALSE]
}

test_that("PTNS is recorded as a MIS-SPECIFIED index treatment", {
  ai <- .ai_rows()
  proc <- ai[ai$stage == "procedure", , drop = FALSE]
  testthat::expect_true(any(proc$service == "ptns"))
  # It must be flagged, not silently accepted as definitive.
  testthat::expect_match(paste(proc$source, collapse = " "), "MIS-SPECIFIED")
  testthat::expect_match(paste(proc$notes, collapse = " "), "NONSURGICAL|not a definitive procedure")
})

test_that("the definitive AI treatments are named, and absent from the basket", {
  ai <- .ai_rows()
  notes <- paste(ai$notes, collapse = " ")
  testthat::expect_match(notes, "SACRAL NEUROMODULATION|sacral neuromodulation")
  testthat::expect_match(notes, "SPHINCTEROPLASTY|sphincteroplasty")
  basket <- urps_service_workload()$service
  testthat::expect_false(any(grepl("snm|sacral|sphinctero", basket, ignore.case = TRUE)))
})

test_that("the AI diagnostic is a urinary stand-in and is labelled as such", {
  # Renaming it would transport a urinary procedure's workload into the bowel
  # pathway, which is harder to see than leaving it visibly wrong.
  ai <- .ai_rows()
  testing <- ai[ai$stage == "testing", , drop = FALSE]
  testthat::expect_true(any(testing$service == "urodynamics"))
  testthat::expect_match(paste(testing$notes, collapse = " "), "STAND-IN")
})

test_that("AI has NO retreatment row, unlike UI and POP", {
  # A structural asymmetry that must stay visible until decided.
  pw <- condition_service_pathway()
  rec <- pw[pw$stage == "recurrence" & grepl("procedure", pw$service), ]
  testthat::expect_true(all(c("ui", "pop") %in% rec$condition))
  testthat::expect_false("ai" %in% rec$condition)
})

# ---------------------------------------------------------------------------
# Separations that must hold once SNM and sphincteroplasty exist
# ---------------------------------------------------------------------------

test_that("SNM and sphincteroplasty cannot silently share a kernel", {
  gc <- c("condition", "index_treatment")
  cohorts <- tibble::tribble(
    ~condition, ~index_treatment, ~treatment_year, ~treated_n,
    "ai", "snm",             2019L, 100,
    "ai", "sphincteroplasty", 2019L, 100)
  # A kernel defined for sphincteroplasty ONLY must refuse the SNM cohort
  # rather than being applied by analogy.
  kern <- tibble::tribble(
    ~condition, ~index_treatment, ~years_since_treatment, ~recurrence_prob,
    "ai", "sphincteroplasty", 1L, 0.10)
  testthat::expect_error(
    suppressMessages(compute_recurrence_convolution(cohorts, kern, 2020, group_cols = gc)),
    "no recurrence kernel")
})

test_that("collapsing the two treatments into one AI group is REFUSED", {
  # Dropping index_treatment makes the cohorts non-unique, and the error says
  # to add the stratifier rather than silently summing them.
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "ai", 2019L, 100,
                             "ai", 2019L, 100)
  kern <- tibble::tribble(~condition, ~years_since_treatment, ~recurrence_prob,
                          "ai", 1L, 0.10)
  testthat::expect_error(
    suppressMessages(compute_recurrence_convolution(cohorts, kern, 2020)),
    "not unique by group")
})

test_that("device management is registered as having NO route into g_k", {
  # A device revision is not necessarily recurrent FI, and recurrent FI is not
  # necessarily a revision -- the AI analogue of the POP 0.40 error.
  err <- tryCatch(assert_recurrence_kernel_compatible("ai", "device_management_events"),
                  error = conditionMessage)
  testthat::expect_match(err, "repeat_treatment_rate")
  reg <- recurrence_evidence_register()
  row <- reg[reg$parameter == "device_management_events", ]
  testthat::expect_equal(row$measure_type, "repeat_treatment_rate")
  testthat::expect_match(row$incompatibility_reason, "not necessarily")
})

test_that("SNM clinical recurrence is unresolved AT THE ENDPOINT LEVEL", {
  # Not merely missing a number: the model has not decided whether it needs
  # loss-of-efficacy recurrence, device workload, or both.
  err <- tryCatch(assert_recurrence_kernel_compatible("ai", "recurrent_fi_care_kernel"),
                  error = conditionMessage)
  testthat::expect_match(err, "NOT kernel-compatible")
  reg <- recurrence_evidence_register()
  snm <- reg[reg$index_treatment == "snm" & reg$parameter == "recurrent_fi_care_kernel", ]
  testthat::expect_match(snm$incompatibility_reason, "BOTH AS DISTINCT QUANTITIES")
})

test_that("device-maintenance volume cannot alter clinical recurrence counts", {
  # Two processes, one cohort. Adding device events must leave the recurrent-care
  # convolution untouched, because they are not inputs to it.
  gc <- c("condition", "index_treatment")
  cohorts <- tibble::tribble(
    ~condition, ~index_treatment, ~treatment_year, ~treated_n,
    "ai", "sphincteroplasty", 2019L, 100)
  kern <- tibble::tribble(
    ~condition, ~index_treatment, ~years_since_treatment, ~recurrence_prob,
    "ai", "sphincteroplasty", 1L, 0.10)
  base_calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, kern, 2020, group_cols = gc, tail_policy = "zero_after_kernel"))
  # a device-events column on the cohort table is simply not consumed
  with_device <- dplyr::mutate(cohorts, device_events_n = 9999)
  dev_calc <- suppressMessages(compute_recurrence_convolution(
    with_device, kern, 2020, group_cols = gc, tail_policy = "zero_after_kernel"))
  testthat::expect_equal(dev_calc$annual$recurrence_n, base_calc$annual$recurrence_n)
})

test_that("no AI probability has been sourced", {
  reg <- recurrence_evidence_register()
  ai <- reg[reg$condition == "ai", ]
  testthat::expect_true(nrow(ai) >= 3L)
  testthat::expect_false(any(as.logical(ai$kernel_compatible)))
  # values remain empty -- nothing invented
  vals <- ai$value[ai$index_treatment %in% c("snm", "sphincteroplasty")]
  testthat::expect_true(all(is.na(vals) | !nzchar(as.character(vals))))
})
