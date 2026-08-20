# tests/testthat/test-swan-incontinence-markov.R
# Synthetic-fixture coverage for R/demand-swan_incontinence_markov.R. Builds
# a small panel directly at the shape score_sandvik_severity() produces
# (swan_id, visit, leakage_ever, sandvik_category) plus covariates, rather
# than routing through the full raw-SWAN-file loader -- that loader is
# already covered by its own tests; this file is about the Markov layer.

.build_swan_markov_fixture <- function(n_participants = 60L, visits = 5:10, seed = 1L) {
  set.seed(seed)
  categories <- c(NA, "slight", "moderate", "severe", "very_severe")

  rows <- list()
  for (pid in seq_len(n_participants)) {
    age0 <- stats::runif(1, 45, 55)
    bmi0 <- stats::runif(1, 22, 32)
    parity <- sample(0:3, 1)
    hyst <- sample(0:1, 1, prob = c(0.8, 0.2))
    ht <- sample(0:1, 1, prob = c(0.85, 0.15))
    # A persistent per-participant tendency, so transitions aren't pure noise.
    tendency <- sample(1:5, 1, prob = c(0.35, 0.25, 0.2, 0.12, 0.08))

    for (i in seq_along(visits)) {
      v <- visits[i]
      drift <- sample(c(-1, 0, 0, 0, 1), 1)
      cat_idx <- min(max(tendency + drift, 1), 5)
      tendency <- cat_idx
      cat_val <- categories[cat_idx]
      rows[[length(rows) + 1L]] <- tibble::tibble(
        swan_id = pid,
        visit = v,
        leakage_ever = !is.na(cat_val),
        sandvik_category = cat_val,
        age = age0 + (v - visits[1]),
        bmi = bmi0 + stats::rnorm(1, 0, 0.3),
        parity = parity,
        hysterectomy = hyst,
        hormone_therapy = ht
      )
    }
  }
  dplyr::bind_rows(rows)
}

test_that("add_swan_ui_markov_state derives state 0 for continent women, NA for unmapped categories", {
  panel <- .build_swan_markov_fixture(n_participants = 5, visits = 5:6)
  out <- add_swan_ui_markov_state(panel)
  expect_true(all(out$ui_state[!out$leakage_ever] == 0L))
  expect_true(all(out$ui_state[out$leakage_ever] %in% 1:4))
})

test_that("fit_swan_ui_markov fits both models and predict/propagate run end to end", {
  panel <- .build_swan_markov_fixture()
  scored <- add_swan_ui_markov_state(panel)

  fit <- fit_swan_ui_markov(scored, visits = 5:10)
  expect_s3_class(fit, "swan_ui_markov_fit")
  expect_true(all(c("initial_model", "transition_model", "empirical_transitions") %in% names(fit)))
  # 5 from_states x 5 to_states, fully completed by tidyr::complete()
  expect_equal(nrow(fit$empirical_transitions), 25L)

  one_row <- scored[1, , drop = FALSE]
  init_probs <- predict_swan_ui_markov_probabilities(fit, one_row, initial = TRUE)
  expect_equal(nrow(init_probs), 1L)
  expect_equal(names(init_probs), c("p_none", "p_slight", "p_moderate", "p_severe", "p_very_severe"))
  expect_equal(sum(as.numeric(init_probs[1, ])), 1, tolerance = 1e-8)

  trans_probs <- predict_swan_ui_markov_probabilities(fit, one_row, current_state = 2L, initial = FALSE)
  expect_equal(sum(as.numeric(trans_probs[1, ])), 1, tolerance = 1e-8)

  expect_error(predict_swan_ui_markov_probabilities(fit, one_row, initial = FALSE),
               "current_state is required")

  propagated <- propagate_swan_ui_markov(fit, scored, seed = 42L)
  expect_true(all(c("propagated_ui_state", "propagated_ui_category", "state_source") %in% names(propagated)))
  expect_true(all(propagated$propagated_ui_state %in% 0:4))
  expect_true(all(propagated$state_source %in% c("observed", "markov")))
  # preserve_observed = TRUE (default): every non-NA input ui_state is kept exactly.
  observed_rows <- !is.na(propagated$ui_state)
  expect_equal(propagated$propagated_ui_state[observed_rows], propagated$ui_state[observed_rows])
  expect_true(all(propagated$state_source[observed_rows] == "observed"))
})

test_that("propagate_swan_ui_markov is deterministic given the same seed", {
  panel <- .build_swan_markov_fixture()
  scored <- add_swan_ui_markov_state(panel)
  fit <- fit_swan_ui_markov(scored, visits = 5:10)

  # Force some markov-generated rows by dropping observed states for one participant.
  scored_gapped <- scored
  scored_gapped$ui_state[scored_gapped$swan_id == 1] <- NA_integer_
  scored_gapped$leakage_ever[scored_gapped$swan_id == 1] <- NA
  scored_gapped$sandvik_category[scored_gapped$swan_id == 1] <- NA_character_

  a <- propagate_swan_ui_markov(fit, scored_gapped, seed = 99L)
  b <- propagate_swan_ui_markov(fit, scored_gapped, seed = 99L)
  expect_identical(a$propagated_ui_state, b$propagated_ui_state)
})

test_that("prepare_swan_ui_markov_panel rejects duplicate participant-visit rows", {
  panel <- .build_swan_markov_fixture(n_participants = 3, visits = 5:6)
  scored <- add_swan_ui_markov_state(panel)
  dup <- dplyr::bind_rows(scored, scored[1, ])
  expect_error(prepare_swan_ui_markov_panel(dup), "Duplicate SWAN participant-visit rows")
})
