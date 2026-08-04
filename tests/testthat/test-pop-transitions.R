# Literature-derived POP transitions (R/33) and staged progression fit (R/31).
#
# (a) pop_transition_parameters() + dmdm_transitions_with_pop_literature() compile
#     the cited MOAD/WHI/SWEPOP table into an engine-usable transition object with
#     calibration_status = "derived_by_analogy".
# (b) dmdm_transition_data(stage_cols=) + fit_dmdm_transitions(stage_conditions=)
#     handle a graded POP stage, fitting per-stage progression/regression.

# ---- (a) literature POP transitions ---------------------------------------

test_that("pop_transition_parameters ships a cited onset/progression/regression table", {
  p <- pop_transition_parameters()
  expect_true(all(c("transition", "term", "stage_from", "stage_to", "measure",
                    "value", "confidence", "source") %in% names(p)))
  expect_setequal(unique(p$transition),
                  c("onset", "progression", "regression", "remission"))
  expect_true(all(nzchar(p$source)))                       # every row is cited
  probs <- p[p$measure == "annual_prob", "value"]
  expect_true(all(probs > 0 & probs < 1))
})

test_that("dmdm_transitions_with_pop_literature overlays only POP and is engine-usable", {
  def <- dmdm_default_transitions()
  tr <- dmdm_transitions_with_pop_literature()
  # provenance is explicit and mixed: POP derived-by-analogy, UI/AI still placeholder
  expect_identical(tr$calibration_status, "derived_by_analogy")
  expect_identical(tr$provenance$pop, "derived_by_analogy")
  expect_identical(tr$provenance$ui, "placeholder_uncalibrated")
  # UI/AI onset rows untouched; POP replaced
  expect_identical(tr$onset$ui, def$onset$ui)
  expect_identical(tr$onset$ai, def$onset$ai)
  expect_false(identical(tr$onset$pop, def$onset$pop))
  # vaginal delivery remains the strongest positive POP onset driver
  expect_gt(tr$onset$pop[["avag"]], 0)
  # staged elements attached and ordered by stage
  expect_named(tr$pop_progression, c("1", "2", "3"))
  expect_named(tr$pop_regression, c("1", "2", "3"))
  # mild POP regresses more than it progresses (the feature UI lacks)
  expect_gt(tr$pop_regression[["1"]], tr$pop_progression[["1"]])
})

test_that("the engine runs unchanged in shape with the literature POP transitions", {
  co <- data.frame(age = 50:70, cumulative_vaginal_deliveries = 3L,
                   years_since_last_vaginal_birth = 20, bmi = 28, hysterectomy = 0,
                   menopause_status = as.integer(50:70 >= 51), comorbidity = 0)
  # A shape assertion. The literature POP transitions carry status
  # "derived_by_analogy", which the calibration gate does not accept as an
  # estimated tier, so the run is declared exploratory.
  out <- suppressMessages(simulate_dmdm(
    co, 2025, 2035, transitions = dmdm_transitions_with_pop_literature(), seed = 1,
    allow_uncalibrated = TRUE))
  expect_equal(nrow(out), 11L)
  expect_true(all(out$prev_pop >= 0 & out$prev_pop <= 1))
})

# ---- (b) graded (staged) POP transitions ----------------------------------

test_that("dmdm_transition_data carries from_stage/to_stage for a staged condition", {
  panel <- tibble::tibble(
    person_id = c(1, 1, 1, 2, 2), year = c(2000L, 2001L, 2002L, 2000L, 2001L),
    age = 50, cumulative_vaginal_deliveries = 2,
    years_since_last_vaginal_birth = 20, bmi = 28, hysterectomy = 0,
    menopause_status = 1, comorbidity = 0,
    has_pop = c(0, 1, 1, 1, 0), pop_stage = c(0L, 1L, 2L, 3L, 0L))
  td <- dmdm_transition_data(panel, conditions = "pop", stage_cols = c(pop = "pop_stage"))
  expect_true(all(c("from_stage", "to_stage") %in% names(td)))
  # binary from/event are derived from the stage, so they stay consistent
  expect_equal(td$from, as.integer(td$from_stage > 0))
  # person 1: 0->1 (onset), 1->2 (progression); person 2: 3->0 (regression to none)
  p1 <- td[td$person_id == 1, ]
  expect_equal(p1$from_stage, c(0L, 1L))
  expect_equal(p1$to_stage, c(1L, 2L))
  p2 <- td[td$person_id == 2, ]
  expect_equal(p2$from_stage, 3L)
  expect_equal(p2$to_stage, 0L)
})

test_that("two-state conditions get NA stage columns and are unaffected", {
  panel <- tibble::tibble(
    person_id = c(1, 1), year = c(2000L, 2001L), age = c(50, 51),
    cumulative_vaginal_deliveries = 2, years_since_last_vaginal_birth = 20,
    bmi = 28, hysterectomy = 0, menopause_status = 0, comorbidity = 0,
    has_ui = c(0L, 1L), has_pop = 0L, has_ai = 0L, pop_stage = 0L)
  td <- dmdm_transition_data(panel, conditions = c("ui", "pop"),
                             stage_cols = c(pop = "pop_stage"))
  ui <- td[td$condition == "ui", ]
  expect_true(all(is.na(ui$from_stage)))
  expect_equal(ui$from, 0L); expect_equal(ui$event, 1L)   # binary path unchanged
  pop <- td[td$condition == "pop", ]
  expect_false(any(is.na(pop$from_stage)))
})

test_that("fit_dmdm_transitions recovers per-stage progression/regression rates", {
  set.seed(11)
  N <- 60000
  from_stage <- sample(0:3, N, replace = TRUE)
  prog_true <- c(`0` = 0.10, `1` = 0.08, `2` = 0.05, `3` = 0.03)
  regr_true <- c(`1` = 0.20, `2` = 0.08, `3` = 0.03)
  to_stage <- vapply(seq_len(N), function(i) {
    s <- from_stage[i]; u <- stats::runif(1)
    p_up <- prog_true[[as.character(s)]]
    p_dn <- if (s > 0) regr_true[[as.character(s)]] else 0
    if (u < p_up && s < 4L) s + 1L else if (u < p_up + p_dn && s > 0L) s - 1L else s
  }, integer(1))
  td <- tibble::tibble(condition = "pop", from = as.integer(from_stage > 0),
                       event = as.integer((to_stage > 0) != (from_stage > 0)),
                       from_stage = from_stage, to_stage = to_stage)
  fit <- fit_dmdm_transitions(td, conditions = "pop", stage_conditions = "pop")
  expect_identical(fit$status, "fitted")
  expect_true(all(c("pop_progression", "pop_regression") %in% names(fit)))
  expect_lt(abs(fit$pop_progression[["1"]] - 0.08), 0.02)
  expect_lt(abs(fit$pop_regression[["1"]] - 0.20), 0.02)
  expect_lt(abs(fit$pop_progression[["2"]] - 0.05), 0.02)
})

test_that("stage_conditions requires the staged columns", {
  td <- tibble::tibble(condition = "pop", from = 0L, event = 0L)
  expect_error(fit_dmdm_transitions(td, conditions = "pop", stage_conditions = "pop"),
               "from_stage")
})
