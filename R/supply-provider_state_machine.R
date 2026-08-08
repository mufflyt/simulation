# Provider Career State Machine (Supply) ----
#
# Formalises the physician career lifecycle as an EXPLICIT state variable and a
# single diffable transition registry, replacing the age-implicit staging that
# R/supply-provider_microsimulation.R and R/supply-provider_lifecycle.R carry today. The
# canonical Dall/HWSM supply models define agents through explicit states rather
# than scattered age conditionals:
#
#   resident -> fellow -> early_career -> mid_career -> late_career -> retired
#
# This module is the SUPPLY analogue of the demand-transition registry (R/25b):
# one independently reviewable table where each transition carries a value, a
# CALIBRATION TIER from the package's canonical `CALIBRATION_TIERS` vocabulary
# (R/23), and a citation. A reviewer can diff "which transition changed, at what
# tier, on what evidence" in one place without reading loop code.
#
# TWO DESIGN COMMITMENTS, both load-bearing:
#
#  1. ADDITIVE AND OUTPUT-PRESERVING. The registry RECONSTRUCTS the existing
#     physician retirement schedule (RETIREMENT_HAZARD_PHYSICIAN, R/16) and the
#     career-change hazard byte-identically -- it is the SSOT those constants are
#     read back from, guarded by test-provider-state-machine.R. Turning on state
#     tracking in the engine adds state-stratified columns and never changes
#     headcount / effective_fte / mean_age.
#
#  2. HAZARDS STAY AGE-CONTINUOUS WITHIN A STATE. The retirement schedule steps
#     every five years of age; the departure transition mid_career/late_career ->
#     retired therefore carries an age-graded hazard, NOT a single per-state
#     constant. Collapsing it to one number per state would discard the within-
#     state age gradient the HWSM survival curves are built from -- the same
#     false-precision trap R/16 documents. The state machine makes the states
#     first-class; it does not flatten the evidence.
#
# The mockup's remaining transitions -- burnout, parental leave, and
# Medicare/Medicaid participation -- have NO in-domain URPS evidence, so they
# enter the registry as NEUTRAL (multiplier 1), `uncalibrated_illustrative`
# SCENARIO LEVERS, gated by assert_publishable_supply_transitions(), exactly as
# the demand placeholders are. A scenario range is preferable to a falsely
# precise estimate.

# ---- Canonical career states ----------------------------------------------

# Ordered career lifecycle. `resident` and `fellow` are the PIPELINE states,
# upstream of this repo's supply engine, which injects entrants post-certification
# at MICROSIM_ENTRY_AGE (R/12) and is owned by the entrant regime (R/49, R/54).
# `early_career` / `mid_career` / `late_career` are the ACTIVE practice states the
# microsimulation traverses; `retired` is absorbing.
CAREER_STATES <- c("resident", "fellow", "early_career", "mid_career",
                   "late_career", "retired")
CAREER_PIPELINE_STATES <- c("resident", "fellow")
CAREER_ACTIVE_STATES <- c("early_career", "mid_career", "late_career")
CAREER_ABSORBING_STATE <- "retired"

# Clinical career-stage boundaries (LABELS ONLY; they do not gate the hazard).
# Mid-career from 45; late-career from 60 -- 60 is where HWSM Exhibit 14 hours
# begin to fall and the physician retirement hazard first steps up. These are
# definitional bands, so they are tiered "solved" in the registry. They are
# deliberately DISTINCT from the hazard's own age-50 regime boundary: the state
# is a clinical label, the hazard is a continuous age function, and conflating
# the two would mis-assign a 47-year-old's departure hazard.
CAREER_STATE_MID_ONSET_AGE <- 45L
CAREER_STATE_LATE_ONSET_AGE <- 60L

#' Label the career state of a provider
#'
#' Deterministic mapping from age and lifecycle status to a career state. This is
#' a labelling VIEW over the existing agent table: it never alters departures.
#'
#' @param age Numeric age(s).
#' @param entered Logical; has the provider entered practice (recycled).
#' @param retired Logical; has the provider retired (recycled). Takes precedence.
#' @return An ordered factor with levels `CAREER_STATES`.
#' @family provider state machine
#' @concept supply
#' @examples
#' # Age maps to career state (boundaries: mid-career >= 45, late-career >= 60).
#' career_state_of(c(38, 52, 66))
#' # A provider who has retired is in the absorbing state regardless of age:
#' career_state_of(58, retired = TRUE)
#' @export
career_state_of <- function(age, entered = TRUE, retired = FALSE) {
  age <- as.numeric(age)
  n <- length(age)
  entered <- rep_len(as.logical(entered), n)
  retired <- rep_len(as.logical(retired), n)

  out <- rep(NA_character_, n)
  out[!entered] <- "fellow"                       # pipeline (pre-practice)
  active <- entered & !retired
  band <- ifelse(age < CAREER_STATE_MID_ONSET_AGE, "early_career",
          ifelse(age < CAREER_STATE_LATE_ONSET_AGE, "mid_career", "late_career"))
  out[active] <- band[active]
  out[entered & retired] <- "retired"             # absorbing (overrides band)
  factor(out, levels = CAREER_STATES)
}

# ---- The diffable transition registry -------------------------------------

#' Provider career-transition registry
#'
#' One row per career transition (or per age band of a graded transition),
#' carrying its calibration tier (from `CALIBRATION_TIERS`), source, and a `role`:
#'   "progression"     definitional age/stage advance (tier "solved")
#'   "departure"       exit-to-retired hazards reconstructed from R/16
#'                     (tier "derived_by_analogy": physician, not URPS-specific)
#'   "participation"   the part-time / FTE attribute (FutureDocs)
#'   "scenario_lever"  neutral, evidence-absent knobs (burnout, parental leave,
#'                     Medicare/Medicaid participation); tier
#'                     "uncalibrated_illustrative", value at the neutral identity.
#'
#' The "departure" retirement rows are the SSOT for `RETIREMENT_HAZARD_PHYSICIAN`
#' (R/16): `.career_retirement_schedule()` reconstructs that constant from them
#' byte-identically (regression-guarded). Written as code rather than a link
#' because it is an unexported internal with no Rd topic to point at, and
#' `R CMD check` fails an unresolvable cross-reference as a WARNING.
#'
#' @return A tibble with `from_state`, `to_state`, `trigger`, `param`, `value`,
#'   `age_lo`, `age_hi`, `ci_low`, `ci_high`, `calibration_tier`, `source`,
#'   `role`, `notes`.
#' @family provider state machine
#' @concept supply
#' @export
career_transition_registry <- function() {
  row <- function(from_state, to_state, trigger, param, value,
                  age_lo = NA_integer_, age_hi = NA_integer_,
                  ci_low = NA_real_, ci_high = NA_real_,
                  calibration_tier, source, role, notes = "") {
    tibble::tibble(
      from_state = from_state, to_state = to_state, trigger = trigger,
      param = param, value = value, age_lo = age_lo, age_hi = age_hi,
      ci_low = ci_low, ci_high = ci_high, calibration_tier = calibration_tier,
      source = source, role = role, notes = notes)
  }

  # -- Progression (definitional; tier "solved") ----------------------------
  prog_src <- "definitional stage boundary"
  progression <- rbind(
    row("resident", "fellow", "training completion", "stage_progression",
        NA_real_, calibration_tier = "solved",
        source = "training pipeline (entrant regime, R/49, R/54)",
        role = "progression",
        notes = "pipeline state; upstream of this engine"),
    row("fellow", "early_career", "certification / entry", "entry_age",
        as.numeric(MICROSIM_ENTRY_AGE), calibration_tier = "solved",
        source = "cliff WC_ENTRY_AGE (R/12)", role = "progression",
        notes = "engine injects entrants at this age"),
    row("early_career", "mid_career", "age 45", "mid_onset_age",
        as.numeric(CAREER_STATE_MID_ONSET_AGE), calibration_tier = "solved",
        source = prog_src, role = "progression"),
    row("mid_career", "late_career", "age 60", "late_onset_age",
        as.numeric(CAREER_STATE_LATE_ONSET_AGE), calibration_tier = "solved",
        source = prog_src, role = "progression",
        notes = "HWSM Exhibit 14: hours begin declining at 60")
  )

  # -- Departure to retired (age-graded; SSOT for RETIREMENT_HAZARD_PHYSICIAN) --
  # Below age 50 the exit process is career change (age-flat, allied-health
  # occupational separation); at 50+ it is the all-cause physician retirement
  # schedule. Both are physician / cross-specialty, not measured on URPS, so
  # both are "derived_by_analogy".
  hwsm_src <- "HWSM Exhibit 17 / Fraher & Knapton FutureDocs (physician survival)"
  ret_bands <- tibble::tribble(
    ~age_lo, ~age_hi, ~value,
    50L,     54L,     0.022,
    55L,     59L,     0.023,
    60L,     64L,     0.073,
    65L,     69L,     0.115,
    70L,     74L,     0.170,
    75L,     79L,     0.240,
    80L,     89L,     0.350
  )
  retirement <- do.call(rbind, lapply(seq_len(nrow(ret_bands)), function(k) {
    b <- ret_bands[k, ]
    from <- if (b$age_lo < CAREER_STATE_LATE_ONSET_AGE) "mid_career" else "late_career"
    row(from, "retired", sprintf("age %d-%d", b$age_lo, b$age_hi),
        "retirement_hazard", b$value, age_lo = b$age_lo, age_hi = b$age_hi,
        calibration_tier = "derived_by_analogy", source = hwsm_src,
        role = "departure",
        notes = "annual all-cause retirement hazard (includes mortality above 50)")
  }))

  departures <- rbind(
    row("early_career", "retired", "age < 50", "career_change_hazard",
        as.numeric(CAREER_CHANGE_HAZARD_UNDER_50), calibration_tier = "derived_by_analogy",
        source = "Zarek 2025 (CPS ASEC; Wolf & Lockard occupational separations)",
        role = "departure",
        notes = "age-flat permanent occupational separation below the retirement age"),
    retirement,
    row("mid_career", "retired", "sex", "retirement_sex_multiplier_female",
        as.numeric(RETIREMENT_SEX_HAZARD_MULTIPLIER[["female"]]),
        calibration_tier = "derived_by_analogy", source = hwsm_src, role = "departure",
        notes = "female physicians retire slightly earlier (applies mid + late)"),
    row("late_career", "retired", "mortality", "death",
        NA_real_, calibration_tier = "derived_by_analogy",
        source = "Dall 2013 (professional-occupation mortality 0.75 M / 0.85 F vs national)",
        role = "departure",
        notes = paste("folded into the all-cause retirement hazard above 50 and",
                      "applied to an external life table by the caller; NOT an",
                      "additive hazard in this engine"))
  )

  # -- Participation (part-time; realised via participation_fte(), FutureDocs) --
  participation <- row(
    "late_career", "late_career", "part-time transition", "part_time_share",
    NA_real_, calibration_tier = "derived_by_analogy",
    source = FUTUREDOCS_PARTICIPATION_SOURCE, role = "participation",
    notes = "realised as an FTE attribute via participation_fte() (R/16), not a departure")

  # -- Scenario levers (NEUTRAL, evidence-absent; tier uncalibrated) ---------
  lever <- function(param, notes) row(
    "early_career|mid_career|late_career", "retired|self", "scenario",
    param, 1.0, calibration_tier = "uncalibrated_illustrative",
    source = "placeholder (no in-domain URPS estimate)", role = "scenario_lever",
    notes = notes)
  levers <- rbind(
    lever("burnout_hazard_multiplier",
          paste("neutral (1.0); scales the age-flat early-exit hazard when",
                "activated. Wired via the `burnout_reduction` supply scenario",
                "(career_change_multiplier -> run_supply_microsimulation's",
                "career_change_hazard)")),
    lever("parental_leave_fte_multiplier",
          "neutral (1.0); temporary FTE reduction when activated"),
    lever("medicare_participation_multiplier",
          "neutral (1.0); scales Medicare-participating supply when activated"),
    lever("medicaid_participation_multiplier",
          "neutral (1.0); scales Medicaid-participating supply when activated")
  )

  rbind(progression, departures, participation, levers)
}

# Reconstruct RETIREMENT_HAZARD_PHYSICIAN (R/16) from the registry departure
# rows. The registry is the SSOT; this is the read-back the byte-identity guard
# checks. Any drift between the table and the constant is a test failure.
.career_retirement_schedule <- function() {
  reg <- career_transition_registry()
  r <- reg[reg$role == "departure" & reg$param == "retirement_hazard", ]
  parts <- Map(function(lo, hi, v) stats::setNames(rep(v, hi - lo + 1L), lo:hi),
               r$age_lo, r$age_hi, r$value)
  sched <- unlist(parts)
  sched[order(as.integer(names(sched)))]
}

#' Departure hazard surfaced through the career-state API
#'
#' Reconstructs [departure_hazard()] (R/16) exactly, organised under the state
#' machine. It exists so the transition matrix and the engine read the SAME
#' hazard; it is guarded to remain identical to `departure_hazard()`.
#'
#' @param age Numeric age(s).
#' @param sex Character sex.
#' @param ... Passed to [departure_hazard()].
#' @return Numeric hazard(s).
#' @family provider state machine
#' @concept supply
#' @export
state_departure_hazard <- function(age, sex = "female", ...) {
  departure_hazard(age, sex = sex, ...)
}

#' Summarise a cohort's departure hazard by career state
#'
#' Reporting helper: groups the (age-continuous) departure hazard by the clinical
#' career state, so a table can show "mean annual exit hazard by career stage"
#' without implying a single per-state constant drives the engine.
#'
#' @param age Numeric age(s) of the active cohort.
#' @param sex Character sex(es), recycled.
#' @param ... Passed to [departure_hazard()].
#' @return A tibble of `career_state`, `n`, `mean_hazard`.
#' @family provider state machine
#' @concept supply
#' @export
career_departure_by_state <- function(age, sex = "female", ...) {
  age <- as.numeric(age)
  sex <- rep_len(as.character(sex), length(age))
  hz <- departure_hazard(age, sex = sex, ...)
  st <- career_state_of(age, entered = TRUE, retired = FALSE)
  keep <- !is.na(st)
  tibble::tibble(career_state = st[keep], hazard = hz[keep]) |>
    dplyr::group_by(.data$career_state, .drop = FALSE) |>
    dplyr::summarise(n = dplyr::n(), mean_hazard = mean(.data$hazard), .groups = "drop")
}

# ---- Publication gate ------------------------------------------------------

#' Assert the career-transition coefficients are publishable
#'
#' The supply analogue of [assert_publishable_demand_coefficients()]. Reduces the
#' registry to its LEAST-calibrated tier and runs the package's accept / opt-in /
#' refuse contract via [assert_publishable_workload()]: `calibrated`/`solved`
#' pass, `derived_by_analogy` needs `allow_analogy = TRUE`, and
#' `uncalibrated_illustrative` is refused (strict errors, relaxed warns).
#'
#' The core career machine (progression + departure + participation) is
#' `derived_by_analogy` at worst -- publishable under `allow_analogy = TRUE`. The
#' scenario levers are `uncalibrated_illustrative`; including them forces a
#' refusal, which is the point: activating burnout / parental-leave /
#' participation knobs makes a run uncalibrated.
#'
#' @param include_scenario_levers Assess the neutral scenario-lever rows too.
#'   Default `FALSE` (they are off by the neutral identity and drive no output).
#' @param allow_analogy Permit `derived_by_analogy` transitions.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) `TRUE` when publishable, `FALSE` otherwise.
#' @family provider state machine
#' @concept supply
#' @export
assert_publishable_supply_transitions <- function(include_scenario_levers = FALSE,
                                                  allow_analogy = FALSE,
                                                  mode = resolve_reproducibility_mode()) {
  reg <- career_transition_registry()
  if (!include_scenario_levers) reg <- reg[reg$role != "scenario_lever", ]
  tiers <- reg$calibration_tier
  worst <- CALIBRATION_TIERS[max(match(tiers, CALIBRATION_TIERS))]
  what <- sprintf("career-transition coefficients (%s)",
                  if (include_scenario_levers) "with scenario levers" else "core machine")
  assert_publishable_workload(status = worst, allow_analogy = allow_analogy,
                              what = what, mode = mode)
}
