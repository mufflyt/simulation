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

# ---- time dimension: effective dates, no retroactive rewriting -------------

testthat::test_that("a lapse ends active years at its effective date and not before", {
  # The exit must begin AT the effective year. Rewriting earlier years would
  # erase practice that was observed, and shifting it later would keep a
  # terminated licence in the active workforce.
  panel <- dplyr::bind_rows(
    .rsm_panel_row(2017, positive_activity = TRUE, activity_confidence = 0.99),
    .rsm_panel_row(2018, positive_activity = TRUE, activity_confidence = 0.99),
    .rsm_panel_row(2019, event_type = "lapsed",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2020),
    .rsm_panel_row(2021)
  )
  states <- suppressMessages(derive_provider_year_states(panel))

  testthat::expect_identical(
    states$activity_state,
    c("ACTIVE", "ACTIVE", "EXITED", "EXITED", "EXITED")
  )
  # Explicitly: the two pre-lapse years are untouched.
  testthat::expect_identical(
    states$activity_state[states$year < 2019L], c("ACTIVE", "ACTIVE")
  )
})

testthat::test_that("reinstatement restores ACTIVE from its own year, not retroactively", {
  panel <- dplyr::bind_rows(
    .rsm_panel_row(2018, positive_activity = TRUE, activity_confidence = 0.99),
    .rsm_panel_row(2019, event_type = "lapsed",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2020),
    .rsm_panel_row(2021, event_type = "lapsed",
                   terminal_decision = "reactivated",
                   positive_activity = TRUE, activity_confidence = 0.99,
                   explicit_reinstatement = TRUE),
    .rsm_panel_row(2022, positive_activity = TRUE, activity_confidence = 0.99)
  )
  states <- suppressMessages(derive_provider_year_states(panel))

  # 2020 must stay EXITED: a 2021 renewal is not evidence about 2020.
  testthat::expect_identical(
    states$activity_state,
    c("ACTIVE", "EXITED", "EXITED", "ACTIVE", "ACTIVE")
  )
})

# ---- multiple licences -----------------------------------------------------

.rsm_license_row <- function(year, license_id, activity_state,
                             provider_id = "P1", qualifying = TRUE) {
  tibble::tibble(
    provider_id = provider_id, license_id = license_id,
    year = as.integer(year), activity_state = activity_state,
    qualifying = qualifying
  )
}

testthat::test_that("one state's lapse does not end a career while another licence is active", {
  # The Colorado/Wyoming case. Declaring a career exit from a single licence
  # event overstates attrition, and does so selectively: multi-state
  # physicians have the most licences to lapse.
  licenses <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "ACTIVE"),
    .rsm_license_row(2020, "WY", "EXITED")
  )
  career <- suppressMessages(derive_provider_career_states(licenses))

  testthat::expect_identical(career$career_state, "ACTIVE")
  testthat::expect_identical(
    career$career_reason, "qualifying_active_license_remains"
  )
  testthat::expect_equal(career$n_active_licenses, 1L)
})

testthat::test_that("a career exit requires that no qualifying active licence remains", {
  licenses <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "EXITED"),
    .rsm_license_row(2020, "WY", "EXITED")
  )
  career <- suppressMessages(derive_provider_career_states(licenses))

  testthat::expect_identical(career$career_state, "EXITED")
  testthat::expect_identical(
    career$career_reason, "no_qualifying_active_license_remains"
  )
})

testthat::test_that("exit is not asserted while any licence status is unobserved", {
  # That unobserved licence might be the active one, so claiming a career exit
  # would be asserting something unmeasured.
  licenses <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "EXITED"),
    .rsm_license_row(2020, "WY", "UNKNOWN")
  )
  career <- suppressMessages(derive_provider_career_states(licenses))

  testthat::expect_identical(career$career_state, "UNKNOWN")
  testthat::expect_identical(career$career_reason, "license_status_unobserved")
})

testthat::test_that("a non-qualifying licence cannot hold a provider in the workforce", {
  licenses <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "EXITED"),
    .rsm_license_row(2020, "XX", "ACTIVE", qualifying = FALSE)
  )
  career <- suppressMessages(derive_provider_career_states(licenses))

  testthat::expect_identical(career$career_state, "EXITED")
  testthat::expect_equal(career$n_qualifying_licenses, 1L)
})

testthat::test_that("death applies to the person, not the credential", {
  deceased_only <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "DECEASED"),
    .rsm_license_row(2020, "WY", "EXITED")
  )
  testthat::expect_identical(
    suppressMessages(derive_provider_career_states(deceased_only))$career_state,
    "DECEASED"
  )

  # A licence showing practice after death is a conflict, never a return.
  contradictory <- dplyr::bind_rows(
    .rsm_license_row(2020, "CO", "DECEASED"),
    .rsm_license_row(2020, "WY", "ACTIVE")
  )
  contradiction <- suppressMessages(
    derive_provider_career_states(contradictory)
  )
  testthat::expect_identical(contradiction$career_state, "CONFLICT")
  testthat::expect_identical(
    contradiction$career_reason,
    "activity_under_another_license_after_death"
  )
})

testthat::test_that("career state tracks licence changes across years", {
  licenses <- dplyr::bind_rows(
    .rsm_license_row(2019, "CO", "ACTIVE"), .rsm_license_row(2019, "WY", "ACTIVE"),
    .rsm_license_row(2020, "CO", "ACTIVE"), .rsm_license_row(2020, "WY", "EXITED"),
    .rsm_license_row(2021, "CO", "EXITED"), .rsm_license_row(2021, "WY", "EXITED")
  )
  career <- suppressMessages(derive_provider_career_states(licenses))

  # The career exit lands in 2021, when the LAST licence goes -- not in 2020
  # when the first one lapsed.
  testthat::expect_identical(career$career_state, c("ACTIVE", "ACTIVE", "EXITED"))
  testthat::expect_equal(career$n_active_licenses, c(2L, 1L, 0L))
})

# ---- what activity may reverse a self-declared retirement ------------------

.rsm_src_row <- function(year, source, provider_id = "P1") {
  tibble::tibble(
    provider_id = provider_id, year = as.integer(year),
    event_type = NA_character_, terminal_decision = "candidate_only",
    positive_activity = TRUE, activity_confidence = 0.99,
    explicit_reinstatement = FALSE, activity_source = source
  )
}

.rsm_retired_then <- function(source) {
  dplyr::bind_rows(
    .rsm_panel_row(2020, event_type = "retired",
                   terminal_decision = "confirmed_exit"),
    .rsm_src_row(2021, source)
  )
}

testthat::test_that("clinical evidence of care delivered reverses a retirement", {
  for (source in c("medicare_claims", "medicaid_claims", "commercial_claims",
                   "encounter_record", "procedure_log", "hospital_privileging")) {
    states <- suppressMessages(
      derive_provider_year_states(.rsm_retired_then(source))
    )
    testthat::expect_identical(
      states$activity_state, c("EXITED", "ACTIVE"),
      info = paste(source, "did not reverse a retirement")
    )
  }
})

testthat::test_that("a registry entry or a live credential NEVER reverses a retirement", {
  # THE FAILURE THIS PREVENTS. An NPPES record persists after a physician stops
  # practising and its deactivation is notoriously lagged; board certification
  # and an unexpired licence outlive practice by design. Admitting any of them
  # would let one stale source resurrect a genuinely retired physician, which
  # is the overcount the lapse correction exists to prevent, arriving by a
  # different door.
  for (source in c("nppes_record", "provider_directory", "roster_membership",
                   "affiliation_listing", "board_certification",
                   "license_active", "dea_registration")) {
    states <- suppressMessages(
      derive_provider_year_states(.rsm_retired_then(source))
    )
    testthat::expect_identical(
      states$activity_state, c("EXITED", "EXITED"),
      info = paste(source, "resurrected a retired physician")
    )
    testthat::expect_identical(
      states$state_reason[[2]], "activity_source_cannot_reverse_retirement"
    )
  }
})

testthat::test_that("an unrecognised or undeclared source fails closed", {
  # An unassessed source is not a qualifying one. Defaulting the other way
  # would mean every new data feed silently gains the power to un-retire
  # people the day it is added.
  for (source in list("some_new_feed", NA_character_)) {
    states <- suppressMessages(
      derive_provider_year_states(.rsm_retired_then(source))
    )
    testthat::expect_identical(
      states$activity_state, c("EXITED", "EXITED"),
      info = "an unassessed activity source was treated as qualifying"
    )
  }

  # And when the column is absent entirely.
  panel <- dplyr::bind_rows(
    .rsm_panel_row(2020, event_type = "retired",
                   terminal_decision = "confirmed_exit"),
    .rsm_panel_row(2021, positive_activity = TRUE, activity_confidence = 0.99)
  )
  testthat::expect_identical(
    suppressMessages(derive_provider_year_states(panel))$activity_state,
    c("EXITED", "EXITED")
  )
})

testthat::test_that("registry activity after retirement is not a CONFLICT", {
  # Deliberate: a directory listing after retirement is EXPECTED, not
  # contradictory. Routing it to CONFLICT would bury the real conflicts --
  # post-death activity, unlicensed practice -- under routine registry noise.
  states <- suppressMessages(
    derive_provider_year_states(.rsm_retired_then("nppes_record"))
  )
  testthat::expect_false(base::any(states$activity_state == "CONFLICT"))
})

testthat::test_that("the source threshold does not weaken licensure exits", {
  # Clinical evidence reverses a RETIREMENT. It must not reverse a lapse or a
  # revocation, which still require documented reinstatement.
  for (event in c("lapsed", "suspended", "revoked")) {
    panel <- dplyr::bind_rows(
      .rsm_panel_row(2020, event_type = event,
                     terminal_decision = "confirmed_exit"),
      .rsm_src_row(2021, "medicare_claims")
    )
    states <- suppressMessages(derive_provider_year_states(panel))
    testthat::expect_identical(
      states$activity_state[[2]], "CONFLICT",
      info = paste("clinical activity cleared a", event, "without reinstatement")
    )
  }
})

testthat::test_that("the adjudicator applies the same evidence hierarchy", {
  event <- function(source) tibble::tibble(
    provider_id = "P1", event_type = "retired", event_year = 2021L,
    identity_confidence = 0.99, event_confidence = 0.99,
    timing_confidence = 0.99, linkage_class = "direct_npi",
    later_activity = TRUE, explicit_reinstatement = FALSE,
    confirmation_matured = TRUE, activity_source = source
  )
  testthat::expect_identical(
    suppressMessages(adjudicate_terminal_events(event("medicare_claims")))$terminal_decision,
    "reactivated"
  )
  testthat::expect_identical(
    suppressMessages(adjudicate_terminal_events(event("nppes_record")))$terminal_decision,
    "candidate_only"
  )
  testthat::expect_identical(
    suppressMessages(adjudicate_terminal_events(event(NA_character_)))$terminal_decision,
    "candidate_only"
  )
})

testthat::test_that("the evidence tier table states the hierarchy explicitly", {
  tiers <- retirement_activity_evidence_tiers()
  testthat::expect_setequal(
    tiers$evidence_tier[tiers$reverses_retirement], "clinical_contemporaneous"
  )
  testthat::expect_setequal(
    base::unique(tiers$evidence_tier[!tiers$reverses_retirement]),
    c("administrative_registry", "credential_status")
  )
  testthat::expect_true(base::all(base::nzchar(tiers$rationale)))
})
