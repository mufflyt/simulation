# Retirement contract: identity gate and provider-year state machine --------
#
# The anchor is the six-year fixture. It encodes the two failures a cumulative
# exit flag CANNOT avoid, and both produce plausible provider-years rather
# than errors, so only an explicit assertion catches them:
#
#     2018 ACTIVE
#     2019 UNKNOWN    <- activity in 2018 and 2020 must not manufacture 2019
#     2020 ACTIVE
#     2021 EXITED
#     2022 EXITED
#     2023 ACTIVE     <- after valid reactivation; the 2021 exit must not
#                        resurrect itself
#
# Hermetic: plain data frames, no DuckDB, no roster, no network.

.rsm_panel_row <- function(year,
                           event_type = NA_character_,
                           terminal_decision = "candidate_only",
                           positive_activity = FALSE,
                           activity_confidence = NA_real_,
                           explicit_reinstatement = FALSE,
                           provider_id = "P1") {
  tibble::tibble(
    provider_id = provider_id,
    year = as.integer(year),
    event_type = event_type,
    terminal_decision = terminal_decision,
    positive_activity = positive_activity,
    activity_confidence = activity_confidence,
    explicit_reinstatement = explicit_reinstatement
  )
}

.rsm_canonical_panel <- function() {
  dplyr::bind_rows(
    .rsm_panel_row(2018, positive_activity = TRUE, activity_confidence = 0.95),
    .rsm_panel_row(2019),
    .rsm_panel_row(2020, positive_activity = TRUE, activity_confidence = 0.95),
    .rsm_panel_row(2021, event_type = "retired",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2022),
    .rsm_panel_row(2023, event_type = "retired",
                   terminal_decision = "reactivated",
                   positive_activity = TRUE, activity_confidence = 0.95,
                   explicit_reinstatement = TRUE)
  )
}

testthat::test_that("the canonical six-year lifecycle is exact", {
  states <- suppressMessages(
    derive_provider_year_states(.rsm_canonical_panel())
  )

  testthat::expect_identical(
    states$activity_state,
    c("ACTIVE", "UNKNOWN", "ACTIVE", "EXITED", "EXITED", "ACTIVE")
  )
  testthat::expect_identical(states$year, 2018:2023)
  testthat::expect_identical(base::unique(states$state_semantics), "year_end")
})

testthat::test_that("activity in adjacent years does not manufacture the gap year", {
  states <- suppressMessages(
    derive_provider_year_states(.rsm_canonical_panel())
  )
  gap <- states[states$year == 2019L, ]

  # The precise failure of cumsum()/cummax(): a year with no evidence inherits
  # its neighbours. UNKNOWN and ACTIVE are different scientific claims -- one
  # says "we did not observe", the other says "they were practising".
  testthat::expect_identical(gap$activity_state, "UNKNOWN")
  testthat::expect_identical(gap$state_reason, "gap_not_filled")
})

testthat::test_that("a confirmed exit does not resurrect itself after reactivation", {
  states <- suppressMessages(
    derive_provider_year_states(.rsm_canonical_panel())
  )

  testthat::expect_identical(
    states$activity_state[states$year == 2022L], "EXITED"
  )
  testthat::expect_identical(
    states$activity_state[states$year == 2023L], "ACTIVE"
  )
  testthat::expect_true(
    base::is.na(states$active_terminal_event[states$year == 2023L])
  )

  # Extend past the reactivation: a cumulative flag would snap back to EXITED
  # the moment evidence stops, because the exit is still in the cumsum.
  extended <- dplyr::bind_rows(.rsm_canonical_panel(), .rsm_panel_row(2024))
  extended_states <- suppressMessages(derive_provider_year_states(extended))
  testthat::expect_identical(
    extended_states$activity_state[extended_states$year == 2024L], "UNKNOWN"
  )
})

testthat::test_that("an adjudicated reactivation ends EXITED on its own", {
  # THIS TEST EXISTS BECAUSE MUTATION TESTING CAUGHT ITS ABSENCE. The
  # canonical fixture's 2023 row carries both a "reactivated" decision AND
  # positive activity, so disabling the reactivation branch entirely left the
  # suite green -- the activity branch produced ACTIVE instead and the test
  # passed for the wrong reason.
  #
  # Here activity is absent, so only the adjudicator's reactivation decision
  # can end the exit. Removing that branch leaves the provider EXITED and this
  # goes red.
  panel <- dplyr::bind_rows(
    .rsm_panel_row(2020, event_type = "retired",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2021, event_type = "retired",
                   terminal_decision = "reactivated",
                   positive_activity = FALSE)
  )
  states <- suppressMessages(derive_provider_year_states(panel))

  testthat::expect_identical(states$activity_state, c("EXITED", "ACTIVE"))
  testthat::expect_identical(
    states$state_reason[[2]], "explicit_or_evidence_reactivation"
  )
  testthat::expect_true(base::is.na(states$active_terminal_event[[2]]))
})

testthat::test_that("death is absorbing and later activity is a conflict, never a return", {
  panel <- dplyr::bind_rows(
    .rsm_panel_row(2020, event_type = "deceased",
                   terminal_decision = "confirmed_death"),
    .rsm_panel_row(2021),
    .rsm_panel_row(2022, positive_activity = TRUE, activity_confidence = 0.99)
  )
  states <- suppressMessages(derive_provider_year_states(panel))

  testthat::expect_identical(
    states$activity_state, c("DECEASED", "DECEASED", "CONFLICT")
  )
  testthat::expect_identical(
    states$state_reason[[3]], "positive_activity_after_death"
  )
})

testthat::test_that("activity after revocation requires explicit reinstatement", {
  base_panel <- function(reinstated) {
    dplyr::bind_rows(
      .rsm_panel_row(2020, event_type = "revoked",
                     terminal_decision = "confirmed_exit"),
      .rsm_panel_row(2021, positive_activity = TRUE,
                     activity_confidence = 0.95,
                     explicit_reinstatement = reinstated)
    )
  }

  without <- suppressMessages(derive_provider_year_states(base_panel(FALSE)))
  testthat::expect_identical(without$activity_state[[2]], "CONFLICT")
  testthat::expect_identical(
    without$state_reason[[2]], "activity_without_reinstatement"
  )

  with_reinstatement <- suppressMessages(
    derive_provider_year_states(base_panel(TRUE))
  )
  testthat::expect_identical(with_reinstatement$activity_state[[2]], "ACTIVE")
})

testthat::test_that("exit and activity in the same year is a conflict, not a precedence puzzle", {
  panel <- .rsm_panel_row(
    2021, event_type = "retired", terminal_decision = "confirmed_exit",
    positive_activity = TRUE, activity_confidence = 0.95
  )
  states <- suppressMessages(derive_provider_year_states(panel))

  testthat::expect_identical(states$activity_state, "CONFLICT")
  testthat::expect_identical(states$state_reason, "exit_and_activity_same_year")
})

testthat::test_that("low-confidence activity leaves the year UNKNOWN", {
  panel <- .rsm_panel_row(2020, positive_activity = TRUE,
                          activity_confidence = 0.40)
  states <- suppressMessages(derive_provider_year_states(panel))
  testthat::expect_identical(states$activity_state, "UNKNOWN")
})

testthat::test_that("a duplicated provider-year is refused rather than arbitrated", {
  panel <- dplyr::bind_rows(.rsm_panel_row(2020), .rsm_panel_row(2020))
  testthat::expect_error(
    suppressMessages(derive_provider_year_states(panel)),
    "duplicated provider-year"
  )
})

# ---- identity gate ---------------------------------------------------------

.rsm_event <- function(event_type,
                       identity_confidence = 0.99,
                       linkage_class = "direct_npi",
                       event_confidence = 0.99,
                       timing_confidence = 0.99,
                       event_year = 2021L,
                       later_activity = FALSE,
                       explicit_reinstatement = FALSE,
                       confirmation_matured = TRUE) {
  tibble::tibble(
    provider_id = "P1",
    event_type = event_type,
    event_year = event_year,
    identity_confidence = identity_confidence,
    event_confidence = event_confidence,
    timing_confidence = timing_confidence,
    linkage_class = linkage_class,
    later_activity = later_activity,
    explicit_reinstatement = explicit_reinstatement,
    confirmation_matured = confirmation_matured
  )
}

testthat::test_that("a weak name-only match can never produce a terminal event", {
  # The core claim: temporal sophistication cannot repair a wrong-person match.
  for (event in c("deceased", "revoked", "surrendered", "retired")) {
    weak <- suppressMessages(adjudicate_terminal_events(
      .rsm_event(event, linkage_class = "name_only", identity_confidence = 0.99)
    ))
    testthat::expect_identical(
      weak$terminal_decision, "quarantine_identity",
      info = paste("weak linkage produced a terminal decision for", event)
    )
  }
})

testthat::test_that("death carries a stricter identity threshold than other exits", {
  # 0.96 clears the ordinary bar and fails the death bar.
  retired <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("retired", identity_confidence = 0.96, later_activity = FALSE)
  ))
  deceased <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("deceased", identity_confidence = 0.96)
  ))

  testthat::expect_identical(retired$terminal_decision, "confirmed_exit")
  testthat::expect_identical(deceased$terminal_decision, "quarantine_identity")

  testthat::expect_error(
    suppressMessages(adjudicate_terminal_events(
      .rsm_event("retired"), identity_min = 0.99, death_identity_min = 0.90
    )),
    "death_identity_min"
  )
})

testthat::test_that("missing identity confidence fails the gate rather than passing it", {
  missing_identity <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("deceased", identity_confidence = NA_real_)
  ))
  testthat::expect_identical(
    missing_identity$terminal_decision, "quarantine_identity"
  )
})

testthat::test_that("CONTROL 1 (positive): a licence lapse ends active provider-years", {
  # A lapse is an EXIT from the active workforce, not missingness. Treating it
  # as administrative noise leaves the provider-year standing and
  # systematically overcounts supply after a known licence termination -- a
  # bias with a direction, not noise, since it inflates the denominator of
  # every access measure downstream.
  for (event in c("expired", "lapsed", "inactive", "not renewed", "suspended")) {
    decision <- suppressMessages(adjudicate_terminal_events(.rsm_event(event)))
    testthat::expect_identical(
      decision$terminal_decision, "confirmed_exit",
      info = paste(event, "did not end the active provider-year")
    )
  }

  panel <- dplyr::bind_rows(
    .rsm_panel_row(2019, positive_activity = TRUE, activity_confidence = 0.95),
    .rsm_panel_row(2020, event_type = "lapsed",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2021)
  )
  states <- suppressMessages(derive_provider_year_states(panel))
  testthat::expect_identical(
    states$activity_state, c("ACTIVE", "EXITED", "EXITED")
  )
})

testthat::test_that("CONTROL 2 (negative): an unreinstated lapse cannot generate later active years", {
  # Billing activity is evidence of billing, not of a licence. If activity
  # alone cleared a lapse, the exit would be cosmetic and the overcount would
  # return through the back door.
  for (event in c("expired", "lapsed", "inactive", "not renewed", "suspended",
                  "revoked", "surrendered")) {
    panel <- dplyr::bind_rows(
      .rsm_panel_row(2020, event_type = event,
                     terminal_decision = "confirmed_exit"),
      .rsm_panel_row(2021, positive_activity = TRUE,
                     activity_confidence = 0.99,
                     explicit_reinstatement = FALSE)
    )
    states <- suppressMessages(derive_provider_year_states(panel))
    testthat::expect_false(
      base::identical(states$activity_state[[2]], "ACTIVE"),
      info = paste("activity alone reactivated an unreinstated", event)
    )
    testthat::expect_identical(
      states$state_reason[[2]], "activity_without_reinstatement",
      info = paste("unreinstated", event, "did not flag for adjudication")
    )
  }

  # Same law at the adjudicator: activity after a licensure exit without a
  # documented reinstatement is a conflict, not a return.
  for (event in c("lapsed", "suspended", "revoked")) {
    decision <- suppressMessages(adjudicate_terminal_events(
      .rsm_event(event, later_activity = TRUE, explicit_reinstatement = FALSE)
    ))
    testthat::expect_identical(
      decision$terminal_decision, "quarantine_conflict",
      info = paste("unreinstated activity after", event, "was not quarantined")
    )
  }
})

testthat::test_that("CONTROL 3 (positive): documented renewal legitimately restores ACTIVE", {
  for (event in c("expired", "lapsed", "inactive", "not renewed", "suspended",
                  "revoked", "surrendered")) {
    panel <- dplyr::bind_rows(
      .rsm_panel_row(2020, event_type = event,
                     terminal_decision = "confirmed_exit"),
      .rsm_panel_row(2021, positive_activity = TRUE,
                     activity_confidence = 0.99,
                     explicit_reinstatement = TRUE)
    )
    states <- suppressMessages(derive_provider_year_states(panel))
    testthat::expect_identical(
      states$activity_state, c("EXITED", "ACTIVE"),
      info = paste("documented reinstatement did not restore ACTIVE after", event)
    )

    decision <- suppressMessages(adjudicate_terminal_events(
      .rsm_event(event, later_activity = TRUE, explicit_reinstatement = TRUE)
    ))
    testthat::expect_identical(
      decision$terminal_decision, "reactivated",
      info = paste("documented reinstatement was not honoured after", event)
    )
  }
})

testthat::test_that("the exit taxonomy states the reversibility tiers explicitly", {
  taxonomy <- retirement_exit_taxonomy()

  # Death is the only absorbing state.
  testthat::expect_identical(
    taxonomy$event_type[taxonomy$absorbing], "deceased"
  )
  # Every licensure status needs documented reinstatement; self-declared
  # retirement does not, because no licence action occurred.
  testthat::expect_setequal(
    taxonomy$event_type[
      !base::is.na(taxonomy$reinstatement_required) &
        taxonomy$reinstatement_required
    ],
    c("revoked", "surrendered", "suspended", "expired", "lapsed", "inactive",
      "not renewed")
  )
  testthat::expect_identical(
    taxonomy$event_type[
      !base::is.na(taxonomy$reinstatement_required) &
        !taxonomy$reinstatement_required
    ],
    "retired"
  )
  # No status may be silently dropped: nothing outside the taxonomy can reach
  # a confirmed exit.
  unknown <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("some_status_nobody_classified")
  ))
  testthat::expect_identical(unknown$terminal_decision, "candidate_only")
})

testthat::test_that("post-death activity quarantines instead of resolving", {
  conflict <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("deceased", later_activity = TRUE)
  ))
  testthat::expect_identical(conflict$terminal_decision, "quarantine_conflict")
})

testthat::test_that("revocation followed by activity needs reinstatement to be a return", {
  without <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("revoked", later_activity = TRUE)
  ))
  with_reinstatement <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("revoked", later_activity = TRUE, explicit_reinstatement = TRUE)
  ))

  testthat::expect_identical(without$terminal_decision, "quarantine_conflict")
  testthat::expect_identical(with_reinstatement$terminal_decision, "reactivated")
})

testthat::test_that("the identity gate precedes event and timing interpretation", {
  # Order is the contract. A record that fails identity AND has unusable event
  # and timing evidence must report the identity failure -- if it reported
  # candidate_only, the gate would be sitting below the later arms and a weak
  # match with good event evidence would slip through.
  gated <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("deceased", linkage_class = "name_only",
               event_confidence = 0.10, timing_confidence = 0.10)
  ))
  testthat::expect_identical(gated$terminal_decision, "quarantine_identity")
})

testthat::test_that("an unmatured retirement is a candidate, not a confirmed exit", {
  immature <- suppressMessages(adjudicate_terminal_events(
    .rsm_event("retired", confirmation_matured = FALSE)
  ))
  testthat::expect_identical(immature$terminal_decision, "candidate_only")
})
