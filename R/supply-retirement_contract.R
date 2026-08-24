# Retirement contract: identity gate, terminal adjudication, state machine ---
#
# WHAT THIS REPLACES, AND WHY IT IS NOT A REFACTOR.
#
# The defect is a cumulative exit flag -- `cumsum(any_exit) > 0` -- which makes
# exit ABSORBING by construction. Two wrong things follow, and both produce
# plausible provider-years rather than errors:
#
#   1. Once any exit signal appears, every later year inherits EXITED, even
#      after the same pipeline separately observes a return to practice.
#   2. Activity observed in one year silently fills the years around it,
#      because a cumulative flag has no way to say "no evidence this year".
#
# The replacement is an explicit state machine over (provider, year) with four
# states -- ACTIVE, UNKNOWN, EXITED, CONFLICT -- and one rule that the cumsum
# formulation cannot express: ACTIVE EVIDENCE APPLIES ONLY TO THE YEAR IT WAS
# OBSERVED IN. A gap is UNKNOWN, never a carried-forward ACTIVE and never a
# carried-forward EXITED once reactivation is established.
#
# ORDER IS THE CONTRACT, NOT AN IMPLEMENTATION DETAIL:
#
#       identity admissibility
#              v
#       event interpretation
#              v
#       temporal arbitration
#              v
#       provider-year state
#
# A record failing the identity gate must never reach the later stages. Weak
# name-only linkage may raise a CANDIDATE signal; it may never produce
# DECEASED, REVOKED, SURRENDERED, or any confirmed permanent exit. Temporal
# sophistication cannot repair a wrong-person match: arbitrating dates between
# two different physicians is a category error, not a hard problem.
#
# NO IN-REPO CALLER YET. These are contract functions for the retirement
# pipeline that lives outside this package; urpssim ships and tests the law,
# the pipeline calls it. They are written to be callable with plain data
# frames so the contract can be tested without any of that pipeline's data.

#' Linkage classes strong enough to support a terminal event
#'
#' Identity evidence that ties a record to one specific physician rather than
#' to a name. Anything outside this set can raise a candidate signal but can
#' never confirm a death, revocation, surrender, or permanent exit.
#'
#' @return Character vector of admissible linkage classes.
#' @family retirement contract
#' @concept supply
#' @export
retirement_strong_linkage_classes <- function() {
  base::c("direct_npi", "license_state_exact", "verified_crosswalk")
}

#' Career-exit taxonomy: which statuses end active practice, and how reversibly
#'
#' @description
#' The scientific law this contract enforces, as a table rather than as
#' conditions scattered through a `case_when()`, so it can be read and asserted
#' directly.
#'
#' **A licence lapse IS an exit from the active workforce.** Treating
#' `expired`/`lapsed`/`inactive`/`not renewed` as mere missingness -- an
#' administrative footnote that leaves the provider-year untouched -- makes the
#' pipeline systematically OVERCOUNT supply after a known licence termination.
#' That is a bias with a direction, not noise: it inflates the denominator of
#' every access measure built downstream.
#'
#' Exit is not the same as absorbing, and the difference is the whole table:
#'
#' \describe{
#'   \item{`licensure_lapse`}{`expired`, `lapsed`, `inactive`, `not renewed`.
#'     Ends active years at the effective date. Reversible, but only by a
#'     DOCUMENTED renewal -- billing activity alone does not restore a licence
#'     that expired.}
#'   \item{`licensure_suspension`}{`suspended`. Out of the active workforce for
#'     the duration; documented reinstatement required.}
#'   \item{`licensure_revocation`}{`revoked`, `surrendered`. Stronger: activity
#'     without an explicit reinstatement is a conflict for a human, because it
#'     means either unlicensed practice or a wrong-person match.}
#'   \item{`self_declared_retirement`}{`retired`. Not a licensure status. A
#'     documented return to practice reactivates it without a licence action,
#'     since nothing was revoked.}
#'   \item{`terminal_death`}{`deceased`. Permanently absorbing. Nothing
#'     reactivates it; later activity is always a conflict.}
#' }
#'
#' @return A tibble with `event_type`, `exit_class`, `reinstatement_required`
#'   and `absorbing`.
#' @family retirement contract
#' @concept supply
#' @export
retirement_exit_taxonomy <- function() {
  tibble::tribble(
    ~event_type,   ~exit_class,                ~reinstatement_required, ~absorbing,
    "deceased",    "terminal_death",           NA,                      TRUE,
    "revoked",     "licensure_revocation",     TRUE,                    FALSE,
    "surrendered", "licensure_revocation",     TRUE,                    FALSE,
    "suspended",   "licensure_suspension",     TRUE,                    FALSE,
    "expired",     "licensure_lapse",          TRUE,                    FALSE,
    "lapsed",      "licensure_lapse",          TRUE,                    FALSE,
    "inactive",    "licensure_lapse",          TRUE,                    FALSE,
    "not renewed", "licensure_lapse",          TRUE,                    FALSE,
    "retired",     "self_declared_retirement", FALSE,                   FALSE
  )
}

#' Adjudicate terminal career events behind an identity gate
#'
#' @description
#' Classifies each candidate terminal event into a `terminal_decision`. The
#' identity gate is evaluated FIRST: a record whose linkage class is weak, or
#' whose `identity_confidence` falls below the threshold for its event type,
#' is quarantined and never reaches event or timing interpretation.
#'
#' Death carries a stricter identity threshold than other exits because it is
#' irreversible downstream -- a wrong-person death is unrecoverable in a way a
#' wrong-person retirement is not.
#'
#' @param event_tbl Data frame with `provider_id`, `event_type`, `event_year`,
#'   `identity_confidence`, `event_confidence`, `timing_confidence`,
#'   `linkage_class`, `later_activity`, `explicit_reinstatement`,
#'   `confirmation_matured`.
#' @param identity_min Minimum identity confidence for a non-death terminal
#'   event.
#' @param death_identity_min Minimum identity confidence for a death. Strictly
#'   greater than `identity_min` by intent.
#' @param event_min Minimum confidence that the event itself occurred.
#' @param timing_min Minimum confidence in the event's year.
#'
#' @return `event_tbl` with `linkage_ok`, `identity_ok`, `event_ok`,
#'   `timing_ok` and `terminal_decision`.
#' @family retirement contract
#' @concept supply
#' @export
adjudicate_terminal_events <- function(event_tbl,
                                       identity_min = 0.95,
                                       death_identity_min = 0.98,
                                       event_min = 0.90,
                                       timing_min = 0.80) {
  base::message("[retirement] Adjudicating terminal events.")

  required_columns <- base::c(
    "provider_id", "event_type", "event_year", "identity_confidence",
    "event_confidence", "timing_confidence", "linkage_class",
    "later_activity", "explicit_reinstatement", "confirmation_matured"
  )
  missing_columns <- base::setdiff(required_columns, base::names(event_tbl))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Terminal adjudication is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  if (death_identity_min < identity_min) {
    base::stop(
      "death_identity_min must be at least identity_min: a wrong-person ",
      "death is less recoverable than a wrong-person retirement.",
      call. = FALSE
    )
  }

  strong_linkage <- retirement_strong_linkage_classes()
  taxonomy <- retirement_exit_taxonomy()

  adjudicated_tbl <- event_tbl |>
    dplyr::mutate(
      event_type = base::tolower(base::as.character(.data$event_type)),
      later_activity = dplyr::coalesce(.data$later_activity, FALSE),
      explicit_reinstatement = dplyr::coalesce(
        .data$explicit_reinstatement, FALSE
      ),
      confirmation_matured = dplyr::coalesce(
        .data$confirmation_matured, FALSE
      )
    ) |>
    dplyr::left_join(taxonomy, by = "event_type") |>
    dplyr::mutate(
      # An event type absent from the taxonomy is NOT assumed harmless. It
      # cannot confirm an exit, but it is surfaced rather than silently
      # dropped, because a status nobody classified is how a real exit goes
      # unrecorded.
      exit_class = dplyr::coalesce(.data$exit_class, "unclassified"),
      linkage_ok = .data$linkage_class %in% strong_linkage,
      # NA identity is not "unknown, proceed" -- it is a failed gate.
      identity_ok = dplyr::case_when(
        base::is.na(.data$identity_confidence) ~ FALSE,
        .data$event_type == "deceased" ~
          .data$identity_confidence >= death_identity_min,
        TRUE ~ .data$identity_confidence >= identity_min
      ),
      event_ok = !base::is.na(.data$event_confidence) &
        .data$event_confidence >= event_min,
      timing_ok = !base::is.na(.data$event_year) &
        !base::is.na(.data$timing_confidence) &
        .data$timing_confidence >= timing_min,
      terminal_decision = dplyr::case_when(
        # IDENTITY GATE, EVALUATED BEFORE EVENT AND TIMING. Its position in
        # this case_when() is the contract: moving it below the event or
        # timing arms would let a weak match reach a terminal decision.
        !.data$linkage_ok | !.data$identity_ok ~ "quarantine_identity",
        !.data$event_ok ~ "candidate_only",
        !.data$timing_ok ~ "candidate_only",
        .data$exit_class == "unclassified" ~ "candidate_only",
        # Death first, and absorbing: later activity can only ever be a
        # conflict, never a return.
        .data$exit_class == "terminal_death" & .data$later_activity ~
          "quarantine_conflict",
        .data$exit_class == "terminal_death" ~ "confirmed_death",
        # Every licensure exit -- lapse, suspension, revocation -- needs a
        # DOCUMENTED reinstatement to become a return. Billing activity does
        # not renew a licence, so activity alone must not resurrect the
        # provider; that is the overcount this contract exists to prevent.
        .data$reinstatement_required & .data$later_activity &
          .data$explicit_reinstatement ~ "reactivated",
        .data$reinstatement_required & .data$later_activity ~
          "quarantine_conflict",
        # Self-declared retirement is not a licensure action, so a documented
        # return to practice reactivates it without a licence event.
        !.data$reinstatement_required & .data$later_activity ~ "reactivated",
        .data$event_type == "retired" & !.data$confirmation_matured ~
          "candidate_only",
        TRUE ~ "confirmed_exit"
      )
    )

  decision_counts <- adjudicated_tbl |>
    dplyr::count(.data$terminal_decision, name = "n")
  base::message(
    "[retirement] Terminal decisions: ",
    base::paste(
      decision_counts$terminal_decision, decision_counts$n,
      sep = "=", collapse = ", "
    )
  )

  adjudicated_tbl
}

#' Derive provider-year activity states from adjudicated evidence
#'
#' @description
#' Walks each provider's years in order and assigns ACTIVE, UNKNOWN, EXITED or
#' CONFLICT. Replaces a cumulative exit flag, which could not represent a gap
#' and could not be undone by a later return.
#'
#' The rules, in the order they are applied per year:
#'
#' \itemize{
#'   \item Death is absorbing. Positive activity after a confirmed death is a
#'     CONFLICT for a human to resolve, never a silent return.
#'   \item A quarantined record yields CONFLICT and never a terminal state.
#'   \item Exit and positive activity in the SAME year is a CONFLICT, not a
#'     precedence puzzle to be resolved by ordering.
#'   \item Positive activity establishes ACTIVE for that year only, except
#'     after revocation or surrender, which requires explicit reinstatement.
#'   \item A confirmed exit carries forward until reactivation.
#'   \item Anything else is UNKNOWN. A gap is never filled from either side.
#' }
#'
#' @param panel_tbl Data frame with `provider_id`, `year`, `event_type`,
#'   `terminal_decision`, `positive_activity`, `activity_confidence`,
#'   `explicit_reinstatement`. One row per provider-year.
#' @param activity_confidence_min Minimum confidence for activity evidence to
#'   establish ACTIVE. Low-confidence activity leaves the year UNKNOWN rather
#'   than asserting presence.
#'
#' @return `panel_tbl` with `activity_state`, `state_reason`,
#'   `active_terminal_event` and `state_semantics` (`"year_end"`).
#' @family retirement contract
#' @concept supply
#' @export
derive_provider_year_states <- function(panel_tbl,
                                        activity_confidence_min = 0.80) {
  base::message("[retirement] Deriving provider-year states.")

  required_columns <- base::c(
    "provider_id", "year", "event_type", "terminal_decision",
    "positive_activity", "activity_confidence", "explicit_reinstatement"
  )
  missing_columns <- base::setdiff(required_columns, base::names(panel_tbl))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Provider-year panel is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  duplicate_years <- panel_tbl |>
    dplyr::count(.data$provider_id, .data$year, name = "n") |>
    dplyr::filter(.data$n > 1L)
  if (base::nrow(duplicate_years) > 0L) {
    base::stop(
      "Provider-year panel has ", base::nrow(duplicate_years),
      " duplicated provider-year row(s); a state machine over an ambiguous ",
      "sequence has no defined meaning.",
      call. = FALSE
    )
  }

  taxonomy <- retirement_exit_taxonomy()
  reinstatement_required_types <- taxonomy$event_type[
    !base::is.na(taxonomy$reinstatement_required) &
      taxonomy$reinstatement_required
  ]

  ordered_tbl <- panel_tbl |>
    dplyr::arrange(.data$provider_id, .data$year)

  state_tbl <- ordered_tbl |>
    dplyr::group_by(.data$provider_id) |>
    dplyr::group_modify(function(provider_tbl, provider_key) {
      row_count <- base::nrow(provider_tbl)
      state_values <- base::rep("UNKNOWN", row_count)
      reason_values <- base::rep("no_current_year_evidence", row_count)
      terminal_values <- base::rep(NA_character_, row_count)

      current_state <- "UNKNOWN"
      current_terminal <- NA_character_

      for (row_id in base::seq_len(row_count)) {
        decision <- provider_tbl$terminal_decision[[row_id]]
        event_type <- provider_tbl$event_type[[row_id]]

        activity_ok <- base::isTRUE(
          provider_tbl$positive_activity[[row_id]]
        ) &&
          !base::is.na(provider_tbl$activity_confidence[[row_id]]) &&
          provider_tbl$activity_confidence[[row_id]] >=
            activity_confidence_min

        reinstated <- base::isTRUE(
          provider_tbl$explicit_reinstatement[[row_id]]
        )

        if (base::identical(current_state, "DECEASED")) {
          if (activity_ok) {
            current_state <- "CONFLICT"
            reason_values[[row_id]] <- "positive_activity_after_death"
          } else {
            reason_values[[row_id]] <- "death_is_absorbing"
          }
        } else if (decision %in%
                   base::c("quarantine_identity", "quarantine_conflict")) {
          current_state <- "CONFLICT"
          reason_values[[row_id]] <- decision
        } else if (base::identical(decision, "confirmed_death")) {
          current_state <- "DECEASED"
          current_terminal <- "deceased"
          reason_values[[row_id]] <- "confirmed_death"
        } else if (base::identical(decision, "reactivated")) {
          current_state <- "ACTIVE"
          current_terminal <- NA_character_
          reason_values[[row_id]] <- "explicit_or_evidence_reactivation"
        } else if (base::identical(decision, "confirmed_exit") && activity_ok) {
          current_state <- "CONFLICT"
          reason_values[[row_id]] <- "exit_and_activity_same_year"
        } else if (base::identical(decision, "confirmed_exit")) {
          current_state <- "EXITED"
          current_terminal <- event_type
          reason_values[[row_id]] <- "confirmed_exit"
        } else if (activity_ok) {
          # EVERY licensure exit needs documented reinstatement, not just
          # revocation. A physician whose licence expired does not become
          # active again because a claim appeared: the claim is evidence of
          # billing, not of a licence. Letting activity alone clear a lapse is
          # exactly how the workforce gets overcounted after a known
          # termination.
          requires_reinstatement <-
            base::identical(current_state, "EXITED") &&
            !base::is.na(current_terminal) &&
            current_terminal %in% reinstatement_required_types
          if (requires_reinstatement && !reinstated) {
            current_state <- "CONFLICT"
            reason_values[[row_id]] <- "activity_without_reinstatement"
          } else {
            current_state <- "ACTIVE"
            current_terminal <- NA_character_
            reason_values[[row_id]] <- "current_year_positive_activity"
          }
        } else if (base::identical(current_state, "EXITED")) {
          reason_values[[row_id]] <- "confirmed_exit_carries_forward"
        } else if (base::identical(current_state, "CONFLICT")) {
          reason_values[[row_id]] <- "unresolved_conflict"
        } else {
          # THE LINE THE cumsum() FORMULATION COULD NOT WRITE. An ACTIVE year
          # does not survive into the next year on its own; absence of
          # evidence returns the provider to UNKNOWN.
          current_state <- "UNKNOWN"
          reason_values[[row_id]] <- "gap_not_filled"
        }

        state_values[[row_id]] <- current_state
        terminal_values[[row_id]] <- current_terminal
      }

      provider_tbl |>
        dplyr::mutate(
          activity_state = state_values,
          state_reason = reason_values,
          active_terminal_event = terminal_values,
          state_semantics = "year_end"
        )
    }) |>
    dplyr::ungroup()

  state_summary <- state_tbl |>
    dplyr::count(.data$activity_state, name = "provider_years")
  base::message(
    "[retirement] Provider-year states: ",
    base::paste(
      state_summary$activity_state, state_summary$provider_years,
      sep = "=", collapse = ", "
    )
  )

  state_tbl
}

#' Roll licence-level states up to a provider-level career state
#'
#' @description
#' A LAPSE IS AN EXIT FROM A LICENCE, NOT NECESSARILY FROM THE PROFESSION.
#' A physician licensed in Colorado and Wyoming whose Wyoming licence expires
#' has not left the workforce; they have left Wyoming's. Declaring a career
#' exit from a single licence event overstates attrition, and it does so
#' selectively -- multi-state physicians are the ones with the most licences to
#' lapse, so the bias concentrates in exactly the group least likely to have
#' actually retired.
#'
#' The rule is therefore: **a provider-level career exit requires that no
#' qualifying active licence remains.**
#'
#' Precedence, applied per provider-year, and deliberately fail-closed against
#' OVERSTATING supply rather than against overstating exit:
#'
#' \enumerate{
#'   \item Any `DECEASED` licence together with any `ACTIVE` licence is a
#'     `CONFLICT` -- death is a fact about a person, so it cannot coexist with
#'     practice under another licence.
#'   \item Any `DECEASED` licence makes the provider `DECEASED`; death applies
#'     to the person, not the credential.
#'   \item Any qualifying `ACTIVE` licence makes the provider `ACTIVE`.
#'   \item Any `CONFLICT` makes the provider `CONFLICT`.
#'   \item Any `UNKNOWN` licence leaves the provider `UNKNOWN`. Exit cannot be
#'     asserted while some licence's status is unobserved -- that licence might
#'     be the active one.
#'   \item Only when every qualifying licence is `EXITED` is the provider
#'     `EXITED`.
#' }
#'
#' Non-qualifying licences (`qualifying = FALSE`) are excluded before any of
#' this: a licence outside the study's scope must not keep a provider in the
#' active workforce.
#'
#' @param license_state_tbl Licence-level provider-year states, as returned by
#'   [derive_provider_year_states()] per licence, with `provider_id`,
#'   `license_id`, `year`, `activity_state` and optionally `qualifying`.
#'
#' @return One row per provider-year: `provider_id`, `year`, `career_state`,
#'   `career_reason`, `n_qualifying_licenses`, `n_active_licenses`.
#' @family retirement contract
#' @concept supply
#' @export
derive_provider_career_states <- function(license_state_tbl) {
  base::message("[retirement] Rolling licence states up to career states.")

  required_columns <- base::c(
    "provider_id", "license_id", "year", "activity_state"
  )
  missing_columns <- base::setdiff(
    required_columns, base::names(license_state_tbl)
  )
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Licence-level state table is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  scoped_tbl <- license_state_tbl
  if (!"qualifying" %in% base::names(scoped_tbl)) {
    scoped_tbl$qualifying <- TRUE
  }
  scoped_tbl$qualifying <- dplyr::coalesce(scoped_tbl$qualifying, FALSE)
  scoped_tbl <- scoped_tbl[scoped_tbl$qualifying, , drop = FALSE]

  career_tbl <- scoped_tbl |>
    dplyr::group_by(.data$provider_id, .data$year) |>
    dplyr::summarise(
      n_qualifying_licenses = dplyr::n(),
      n_active_licenses = base::sum(.data$activity_state == "ACTIVE"),
      .any_deceased = base::any(.data$activity_state == "DECEASED"),
      .any_active = base::any(.data$activity_state == "ACTIVE"),
      .any_conflict = base::any(.data$activity_state == "CONFLICT"),
      .any_unknown = base::any(.data$activity_state == "UNKNOWN"),
      .all_exited = base::all(.data$activity_state == "EXITED"),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      career_state = dplyr::case_when(
        .data$.any_deceased & .data$.any_active ~ "CONFLICT",
        .data$.any_deceased ~ "DECEASED",
        .data$.any_active ~ "ACTIVE",
        .data$.any_conflict ~ "CONFLICT",
        .data$.any_unknown ~ "UNKNOWN",
        .data$.all_exited ~ "EXITED",
        TRUE ~ "UNKNOWN"
      ),
      career_reason = dplyr::case_when(
        .data$.any_deceased & .data$.any_active ~ "activity_under_another_license_after_death",
        .data$.any_deceased ~ "death_applies_to_the_person",
        .data$.any_active ~ "qualifying_active_license_remains",
        .data$.any_conflict ~ "unresolved_license_conflict",
        .data$.any_unknown ~ "license_status_unobserved",
        .data$.all_exited ~ "no_qualifying_active_license_remains",
        TRUE ~ "no_qualifying_license"
      )
    ) |>
    dplyr::select(
      "provider_id", "year", "career_state", "career_reason",
      "n_qualifying_licenses", "n_active_licenses"
    )

  career_summary <- career_tbl |>
    dplyr::count(.data$career_state, name = "provider_years")
  base::message(
    "[retirement] Career states: ",
    base::paste(
      career_summary$career_state, career_summary$provider_years,
      sep = "=", collapse = ", "
    )
  )

  career_tbl
}
