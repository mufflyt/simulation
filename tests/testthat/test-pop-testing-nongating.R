# POP testing is utilization, not a clinical gate.
#
# The live POP pathway previously multiplied the conservative-to-surgical
# transition (0.35) by a second testing-to-procedure transition (0.55), even
# though the testing stage emits only 0.50 testing services per entrant. These
# tests lock the estimand correction: testing utilization may change without
# changing who reaches surgery.

testthat::test_that("POP testing is a structural pass-through", {
  pathway <- valid_pathway()   # shipped table is refused; see helper-setup.R
  pop_testing <- pathway[
    pathway$condition == "pop" & pathway$stage == "testing",
    ,
    drop = FALSE
  ]

  testthat::expect_true(nrow(pop_testing) > 0L)
  testthat::expect_true(all(pop_testing$p_advance == 1))
  testthat::expect_true(all(is.na(pop_testing$ci_low)))
  testthat::expect_true(all(is.na(pop_testing$ci_high)))
})

testthat::test_that("POP procedure entry is governed by conservative transition", {
  pathway <- valid_pathway()   # shipped table is refused; see helper-setup.R
  entrants <- pathway_stage_entrants(c(pop = 1000), pathway)
  pop_entering <- stats::setNames(entrants$entering, entrants$stage)

  testthat::expect_equal(
    unname(pop_entering[["conservative"]]),
    1000
  )
  testthat::expect_equal(
    unname(pop_entering[["testing"]]),
    350
  )
  testthat::expect_equal(
    unname(pop_entering[["procedure"]]),
    350
  )
  testthat::expect_equal(
    unname(pop_entering[["followup"]]),
    350
  )
  testthat::expect_equal(
    unname(pop_entering[["recurrence"]]),
    42
  )
})

testthat::test_that("testing utilization cannot gate POP surgery", {
  pathway <- valid_pathway()   # shipped table is refused; see helper-setup.R
  baseline <- pathway_service_volumes(
    treated = c(pop = 1000),
    year = 2025L,
    pathway = pathway,
    by_stage = TRUE
  )

  no_urodynamics <- pathway
  row_to_change <-
    no_urodynamics$condition == "pop" &
    no_urodynamics$stage == "testing" &
    no_urodynamics$service == "urodynamics"
  no_urodynamics$per_entering[row_to_change] <- 0

  perturbed <- pathway_service_volumes(
    treated = c(pop = 1000),
    year = 2025L,
    pathway = no_urodynamics,
    by_stage = TRUE
  )

  baseline_procedure <- baseline$volume[
    baseline$condition == "pop" &
    baseline$stage == "procedure" &
    baseline$service == "prolapse_procedure"
  ]
  perturbed_procedure <- perturbed$volume[
    perturbed$condition == "pop" &
    perturbed$stage == "procedure" &
    perturbed$service == "prolapse_procedure"
  ]
  baseline_urodynamics <- baseline$volume[
    baseline$condition == "pop" &
    baseline$stage == "testing" &
    baseline$service == "urodynamics"
  ]
  perturbed_urodynamics <- perturbed$volume[
    perturbed$condition == "pop" &
    perturbed$stage == "testing" &
    perturbed$service == "urodynamics"
  ]

  testthat::expect_equal(baseline_urodynamics, 105)
  testthat::expect_equal(perturbed_urodynamics, 0)
  testthat::expect_equal(baseline_procedure, 350)
  testthat::expect_equal(perturbed_procedure, baseline_procedure)
})

testthat::test_that("conservative transition remains the causal POP lever", {
  pathway <- valid_pathway()   # shipped table is refused; see helper-setup.R
  baseline <- pathway_service_volumes(
    treated = c(pop = 1000),
    year = 2025L,
    pathway = pathway,
    by_stage = TRUE
  )

  half_transition <- pathway
  pop_conservative <-
    half_transition$condition == "pop" &
    half_transition$stage == "conservative"
  half_transition$p_advance[pop_conservative] <- 0.175

  perturbed <- pathway_service_volumes(
    treated = c(pop = 1000),
    year = 2025L,
    pathway = half_transition,
    by_stage = TRUE
  )

  pop_procedure <- function(service_volumes) {
    service_volumes$volume[
      service_volumes$condition == "pop" &
      service_volumes$stage == "procedure" &
      service_volumes$service == "prolapse_procedure"
    ]
  }
  pop_recurrence <- function(service_volumes) {
    service_volumes$volume[
      service_volumes$condition == "pop" &
      service_volumes$stage == "recurrence" &
      service_volumes$service == "prolapse_procedure"
    ]
  }

  baseline_primary <- pop_procedure(baseline)
  perturbed_primary <- pop_procedure(perturbed)
  baseline_recurrent <- pop_recurrence(baseline)
  perturbed_recurrent <- pop_recurrence(perturbed)

  testthat::expect_equal(perturbed_primary / baseline_primary, 0.5)
  testthat::expect_equal(perturbed_recurrent / baseline_recurrent, 0.5)
  testthat::expect_equal(
    (perturbed_primary + perturbed_recurrent) /
      (baseline_primary + baseline_recurrent),
    0.5
  )
})

testthat::test_that("UI testing transition is unchanged", {
  pathway <- valid_pathway()   # shipped table is refused; see helper-setup.R
  ui_testing <- pathway[
    pathway$condition == "ui" & pathway$stage == "testing",
    ,
    drop = FALSE
  ]

  testthat::expect_true(all(ui_testing$p_advance == 0.40))

  entrants <- pathway_stage_entrants(c(ui = 1000), pathway)
  ui_entering <- stats::setNames(entrants$entering, entrants$stage)
  testthat::expect_equal(unname(ui_entering[["testing"]]), 450)
  testthat::expect_equal(unname(ui_entering[["procedure"]]), 180)
})

testthat::test_that("POP pathway remains uncalibrated after restructure", {
  testthat::expect_identical(
    condition_pathway_status(),
    "uncalibrated_illustrative"
  )
})
