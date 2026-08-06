# URPS Roster: Production Cohort and Workload Evidence ----
#
# The roster (ABOG + ABU board-certified URPS subspecialists, matched to NPI and
# enriched with Medicare CY2024 activity) is the first PRODUCTION cohort this
# model has had. Supplying it flips `example_only` to FALSE and clears the
# strict-mode gate that `agents_from_certification_cohorts()` cannot.
#
# WHY IT DOES *NOT* SOLVE THE CAPACITY ANCHOR, WHICH IS THE THING IT LOOKS LIKE
# IT SHOULD SOLVE.
#
# Base-year required FTE is currently pinned by `capacity_survey_adequacy()`,
# whose evidence is a PHYSICAL-THERAPY capacity distribution (Zarek 2025) used
# as a stand-in. That single number sets the level of required FTE and therefore
# the sign of the projected gap: adequacy of 0.708 instead of 0.948 erases the
# 2050 surplus entirely.
#
# It is tempting to replace it with observed workload from this roster. That
# would be circular. Observed workload is what providers DO deliver, not what
# they COULD deliver; treating it as capacity asserts that the base year is
# exactly adequate, which is the question being asked. An anchor derived that
# way would return "adequacy = 1.0" by construction, for any workforce, in any
# year. THE PHYSICAL-THERAPY STAND-IN STILL NEEDS A REAL URPS CAPACITY SURVEY.
#
# WHAT THE ROSTER DOES ESTABLISH IS A SUPPLY-SIDE FACT, and it is not small:
# board certification is not the same thing as delivering urogynaecologic care.
# See `roster_workload_concentration()`. That evidence bears on the supply
# estimand, where it is not circular at all.

#' Load the URPS provider roster
#'
#' Board-certified URPS subspecialists (ABOG and ABU pathways) matched to NPI,
#' with Medicare CY2024 activity. Physician NAMES are deliberately absent: the
#' model needs age, sex, state, certification year and workload, and nothing in
#' the pipeline needs to identify an individual.
#'
#' @param path CSV path.
#' @param in_baseline_only Keep only providers in the model baseline cohort.
#' @return Tibble of roster records.
#' @export
load_urps_roster <- function(path = "data-raw/urps_roster/urps_roster_2026-07-22.csv",
                             in_baseline_only = TRUE) {
  if (!file.exists(path)) {
    stop("URPS roster not found at '", path, "'. It is derived in mufflyt/cliff ",
         "(abog_all_urps_ENRICHED / abu_all_urps_ENRICHED).", call. = FALSE)
  }
  r <- utils::read.csv(path, colClasses = c(npi = "character"), stringsAsFactors = FALSE)
  if (isTRUE(in_baseline_only)) {
    r <- r[r$in_model_baseline %in% c(TRUE, "TRUE", "True"), , drop = FALSE]
  }
  tibble::as_tibble(r)
}

#' Convert the URPS roster into a validated provider roster
#'
#' `last_confirmed_active_year` is derived from Medicare CY2024 billing: a
#' provider who billed in 2024 was demonstrably practising that year. Providers
#' with no Medicare activity get NA rather than a guess -- being unconfirmed is
#' information, and [validate_provider_roster()] requires the field to exist,
#' not to be populated.
#'
#' @param roster Tibble from [load_urps_roster()].
#' @param unknown_sex Sex assigned when the source records none. Declared rather
#'   than silently defaulted: a missing sex yields NA clinical FTE, which
#'   propagates into an unreadable engine error.
#' @return Tibble in the [validate_provider_roster()] contract.
#' @export
urps_provider_roster <- function(roster = load_urps_roster(), unknown_sex = "female") {
  sex <- ifelse(roster$gender == "F", "female",
                ifelse(roster$gender == "M", "male", NA_character_))
  n_unknown <- sum(is.na(sex))
  if (n_unknown > 0) {
    .msg_warn(sprintf(paste("%d roster record(s) carry no sex; assigning '%s' as a",
                            "declared assumption. Left as NA they produce NA clinical",
                            "FTE and the engine fails on a missing value."),
                      n_unknown, unknown_sex))
    sex[is.na(sex)] <- unknown_sex
  }

  out <- tibble::tibble(
      provider_id = as.character(roster$npi),
      pathway = "FPMRS",
      age = as.numeric(roster$age_proxy_from_cert),
      sex = sex,
      state = as.character(roster$state),
      certification_year = as.numeric(roster$cert_year),
    last_confirmed_active_year = if ("last_confirmed_active_year" %in% names(roster)) {
      as.integer(roster$last_confirmed_active_year)
    } else {
      ifelse(roster$has_medicare_2024 %in% c(TRUE, "TRUE", "True"), 2024L, NA_integer_)
    }
  )
  validate_provider_roster(out)
  out
}

# CMS suppresses any provider-HCPCS cell with fewer than 11 services. A provider
# doing a handful of URPS services therefore reports ZERO, so the zero share and
# the concentration below are both UPPER bounds on true inactivity.
MEDICARE_SUPPRESSION_THRESHOLD <- 11L

#' Concentration of URPS workload across the certified roster
#'
#' @param roster Tibble from [load_urps_roster()].
#' @param volume_col Workload column.
#' @return List of distribution statistics, carrying a `caveats` attribute.
#' @export
roster_workload_concentration <- function(roster = load_urps_roster(),
                                          volume_col = "urogyn_services_2024") {
  assertthat::assert_that(volume_col %in% names(roster))
  v <- as.numeric(roster[[volume_col]])
  v <- v[!is.na(v)]
  n <- length(v)
  if (n == 0) stop("roster_workload_concentration: no workload values", call. = FALSE)

  o <- sort(v, decreasing = TRUE)
  cs <- cumsum(o) / sum(o)
  share_at <- function(p) if (sum(o) == 0) NA_real_ else cs[max(1L, floor(p * n))]
  n_for <- function(target) if (sum(o) == 0) NA_integer_ else which(cs >= target)[1]

  structure(
    list(
      n_providers = n,
      n_zero = sum(v == 0),
      share_zero = mean(v == 0),
      median_volume = stats::median(v),
      mean_volume = mean(v),
      total_volume = sum(v),
      share_from_top_decile = share_at(0.10),
      share_from_top_quartile = share_at(0.25),
      n_for_90pct = n_for(0.90),
      share_of_roster_for_90pct = n_for(0.90) / n
    ),
    caveats = c(
      sprintf(paste("CMS suppresses provider-code cells below %d services, so a",
                    "low-volume provider reports zero. The zero share and the",
                    "concentration are UPPER bounds on true inactivity."),
              MEDICARE_SUPPRESSION_THRESHOLD),
      paste("Medicare fee-for-service only. A subspecialist practising",
            "predominantly in women under 65 shows little or no volume here",
            "without being any less active."),
      paste("Workload is not capacity. These figures describe what the certified",
            "workforce DOES deliver and say nothing about what it COULD -- they",
            "cannot anchor base-year adequacy without assuming the answer.")
    ),
    volume_col = volume_col
  )
}

#' @export
print.default_roster_concentration <- function(x, ...) invisible(x)

# ---- The unresolved capacity anchor ----------------------------------------

#' Variables a URPS capacity survey must collect
#'
#' A VIEW onto [urps_practice_survey_requirements()], not a second list. The
#' capacity anchor and the hours-to-FTE curve are both unresolved, both borrowed
#' from other specialties, and both answerable by one questionnaire -- so they
#' are registered as one instrument. Fielding two surveys to collect one
#' questionnaire's worth of questions is how neither gets fielded.
#'
#' @return Tibble of the instrument's capacity-anchor items.
#' @export
urps_capacity_survey_requirements <- function() {
  urps_practice_survey_requirements("capacity_anchor")
}

#' Status of the base-year capacity anchor
#'
#' @return List with `resolved`, the current source, and what would resolve it.
#' @export
capacity_status <- function() {
  list(
    resolved = FALSE,
    current_source = paste("Zarek 2025 physical-therapy capacity distribution,",
                           "used as a stand-in via capacity_survey_adequacy()."),
    why_unresolved = paste(
      "Observed workload cannot substitute: it measures what the workforce does",
      "deliver, not what it could. An adequacy ratio derived from it returns 1.0",
      "for any workforce in any year, because it assumes the question."),
    leverage = paste(
      "Sole remaining input that can change the SIGN of the projected gap:",
      "required FTE = anchor x wRVU(t)/wRVU(base), so the anchor sets the level",
      "while delegation, demand calibration and workload levels all cancel."),
    resolved_by = urps_capacity_survey_requirements()$variable,
    # The same instrument also resolves the hours-to-FTE curve; see
    # fte_curve_status() and unresolved_calibration_items().
    same_instrument_also_resolves = "fte_curve"
  )
}
