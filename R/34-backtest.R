# Historical Back-Test ----
#
# Fit on information available through a cutoff year, project forward, and score
# against observed counts the model never saw. None of the five source papers
# does this; the Dutch study cited by Hooker 2022 is the only reference point,
# and it found 10-year projections materially less reliable than short ones.
#
# The design constraint that matters is LEAKAGE. Every contract read is routed
# through `.series_through()`, which filters to the cutoff and records the
# maximum year touched in a run-scoped audit log. `assert_no_leakage()` then
# asserts that no read exceeded the cutoff. That is a mechanical guarantee, not
# a promise: a future edit that reaches into the validation window fails a test.
#
# THE 2023 TARGET
#
# Several 2023 counts exist in this project and they are NOT interchangeable:
#
#   1,306  national, ABOG_PLUS_ABU, board_certified_active, contract v3.0.0
#          (CURRENT), basis = URPS subspecialty certification year
#   1,303  as above but CONUS geography
#   1,332  contract v2.1.0 (RETIRED), basis = PRIMARY board certification year
#   1,329  as above but CONUS
#   1,027  contract v3.0.0 national with urology EXCLUDED (ABOG only)
#   1,339  roster_snapshot headcount, and for 2025 rather than 2023
#
# 1,306 is the back-test target because the simulated cohort is built from the
# same contract on the same basis: national geography, ABOG_PLUS_ABU pathway,
# `board_certified_active` measure, contract v3.0.0, keyed on the URPS
# SUBSPECIALTY certification year. 1,332/1,329 come from the retired v2.1.0
# contract keyed on the PRIMARY board certification year -- a different
# certification-year treatment, confirmed by `urps_retired_values()` returning
# exactly c(1332, 1329). Scoring a subspecialty-cert-year cohort against a
# primary-cert-year target would inflate apparent error by ~2% for no modelling
# reason. `validate_backtest_target()` enforces every one of these dimensions
# and STOPS on a mismatch.
#
# A DEFINITION MISMATCH THAT CHANGES INTERPRETATION
#
# `n_retired` is 0 in every row of the contract series and `n_active` equals
# `n_ever_certified` in every row. The bundled artifact therefore applies NO
# ATTRITION: the observed series is cumulative certifications, not an active
# count net of departures. The simulation does apply retirement hazards, so the
# two quantities are not the same and the model will structurally under-predict.
#
# This is not a detail to bury in a limitations paragraph. `validate_backtest_target()`
# fails closed on it, and a caller must pass `acknowledge_no_attrition = TRUE` to
# proceed. Both comparisons are then reported: the model's actual active-workforce
# prediction, and a definition-matched no-attrition variant that isolates the
# entrant model, which is the part the observed series can genuinely test.

BACKTEST_CUTOFF_YEAR <- 2020L
BACKTEST_TARGET_YEAR <- 2023L

# ---- Leakage control -------------------------------------------------------

.backtest_audit <- new.env(parent = emptyenv())

#' Reset the leakage audit log
#' @return (Invisibly) NULL.
#' @export
reset_leakage_audit <- function() {
  .backtest_audit$max_year <- -Inf
  .backtest_audit$reads <- character(0)
  invisible(NULL)
}

#' Read the contract series, filtered to a cutoff, recording the years touched
#'
#' The single gate through which back-test code may read the contract. Anything
#' that bypasses it will not be audited, so the leakage test also asserts that
#' the back-test functions call nothing else.
#'
#' @param through_year Latest year the caller is permitted to see.
#' @param geography,board_pathway Contract dimensions.
#' @param what Label recorded in the audit log.
#' @return Tibble of `year`, `n_active`, filtered to `<= through_year`.
#' @export
.series_through <- function(through_year, geography = "national",
                            board_pathway = "ABOG_PLUS_ABU", what = "series") {
  .require_mufflyaccess("The URPS certification series")
  x <- mufflyaccess::urps_counts_long()
  a <- x[x$measure == "board_certified_active" &
           x$geography == geography &
           x$board_pathway == board_pathway, c("year", "n_active")]
  a <- a[order(a$year), ]
  a <- a[a$year <= through_year, , drop = FALSE]

  if (is.null(.backtest_audit$max_year)) reset_leakage_audit()
  .backtest_audit$max_year <- max(.backtest_audit$max_year, max(a$year))
  .backtest_audit$reads <- c(.backtest_audit$reads,
                             sprintf("%s: <= %d", what, max(a$year)))
  tibble::as_tibble(a)
}

#' Assert that no contract read exceeded the cutoff
#'
#' @param through_year The cutoff the run declared.
#' @return (Invisibly) the audit log.
#' @export
assert_no_leakage <- function(through_year = BACKTEST_CUTOFF_YEAR) {
  m <- .backtest_audit$max_year
  if (is.null(m) || !is.finite(m)) {
    stop("assert_no_leakage: no audited contract reads recorded. Either the ",
         "back-test read the contract outside .series_through(), or it did not ",
         "read it at all -- both are failures.", call. = FALSE)
  }
  if (m > through_year) {
    stop(sprintf(paste(
      "LEAKAGE: a contract read reached year %d, beyond the declared cutoff of",
      "%d. The validation period must not inform any model parameter. Reads: %s"),
      m, through_year, paste(.backtest_audit$reads, collapse = "; ")), call. = FALSE)
  }
  invisible(.backtest_audit$reads)
}

# ---- Target reconciliation -------------------------------------------------

#' Every 2023 count in the project, with the dimensions that distinguish them
#' @return Tibble of candidate targets.
#' @export
backtest_target_candidates <- function() {
  tibble::tribble(
    ~value, ~geography,  ~pathway,        ~measure,                 ~contract, ~status,   ~basis,
     1306L, "national",  "ABOG_PLUS_ABU", "board_certified_active", "3.0.0",   "current", "URPS subspecialty cert year",
     1303L, "conus",     "ABOG_PLUS_ABU", "board_certified_active", "3.0.0",   "current", "URPS subspecialty cert year",
     1332L, "national",  "ABOG_PLUS_ABU", "board_certified_active", "2.1.0",   "retired", "primary board cert year",
     1329L, "conus",     "ABOG_PLUS_ABU", "board_certified_active", "2.1.0",   "retired", "primary board cert year",
     1027L, "national",  "ABOG",          "board_certified_active", "3.0.0",   "current", "URPS subspecialty cert year",
     1339L, "national",  "ABOG_PLUS_ABU", "roster_snapshot",        "3.0.0",   "current", "2025 headcount, not 2023 active"
  )
}

#' Validate the back-test target against the simulated cohort's contract
#'
#' Fails closed on every dimension that could make the comparison invalid. The
#' attrition mismatch is separated out because it does not invalidate the target
#' choice -- it changes what the comparison means.
#'
#' @param target_year Year being scored.
#' @param geography,board_pathway,measure Dimensions the cohort was built on.
#' @param acknowledge_no_attrition Proceed despite the observed series applying
#'   no attrition. Required, because the model does apply it.
#' @param expected_value The count the caller believes it is scoring against.
#'   Supplying it turns a silently-different-but-internally-consistent target
#'   into an error: asking for the ABOG-only pathway returns 1,027, which is a
#'   perfectly valid count and the wrong one for an ABOG+ABU cohort.
#' @return List with the validated `value` and the reconciliation record.
#' @export
validate_backtest_target <- function(target_year = BACKTEST_TARGET_YEAR,
                                     geography = "national",
                                     board_pathway = "ABOG_PLUS_ABU",
                                     measure = "board_certified_active",
                                     acknowledge_no_attrition = FALSE,
                                     expected_value = NULL) {
  .require_mufflyaccess("The back-test target")

  lineage <- mufflyaccess::urps_lineage()
  current <- lineage[lineage$status == "current", ]
  if (nrow(current) != 1L) {
    stop("validate_backtest_target: expected exactly one current contract in ",
         "urps_lineage(); got ", nrow(current), call. = FALSE)
  }
  retired <- mufflyaccess::urps_retired_values()

  det <- mufflyaccess::urps_count(
    year = target_year, measure = measure, geography = geography,
    include_urology = identical(board_pathway, "ABOG_PLUS_ABU"), details = TRUE
  )

  fail <- function(...) stop("BACK-TEST CONTRACT MISMATCH: ", ..., call. = FALSE)

  if (!identical(det$board_pathway, board_pathway)) {
    fail(sprintf("pathway is '%s', cohort was built on '%s'.",
                 det$board_pathway, board_pathway))
  }
  if (!identical(det$measure, measure)) {
    fail(sprintf("measure is '%s', expected '%s'. roster_snapshot is a headcount, not an active count.",
                 det$measure, measure))
  }
  if (!identical(det$contract_version, current$contract_version)) {
    fail(sprintf("target is on contract v%s but the current contract is v%s.",
                 det$contract_version, current$contract_version))
  }
  if (det$count %in% retired) {
    fail(sprintf(paste("target %d is a RETIRED contract value (v2.1.0, basis = primary",
                       "board cert year). The cohort is keyed on the URPS SUBSPECIALTY",
                       "cert year, so these are different certification-year treatments.",
                       "Retired values: %s."),
                 det$count, paste(retired, collapse = ", ")))
  }
  if (!is.null(expected_value) && !identical(as.integer(det$count),
                                             as.integer(expected_value))) {
    cand <- backtest_target_candidates()
    hit <- cand[cand$value == det$count, ]
    fail(sprintf(paste(
      "retrieved target is %d but %d was expected. The retrieved value is %s.",
      "These dimensions are not interchangeable: state the target explicitly and",
      "make the cohort match it."),
      det$count, as.integer(expected_value),
      if (nrow(hit)) sprintf("%s geography / %s pathway / %s measure / contract %s (%s)",
                             hit$geography[1], hit$pathway[1], hit$measure[1],
                             hit$contract[1], hit$status[1]) else "not a recognised project value"))
  }
  if (!grepl("subspecialty", current$basis, ignore.case = TRUE)) {
    fail(sprintf("current contract basis is '%s'; the cohort assumes a subspecialty cert-year basis.",
                 current$basis))
  }

  # Attrition: a definition mismatch, not a wrong target.
  series <- mufflyaccess::urps_counts_long()
  no_attrition <- all(series$n_retired == 0) && all(series$n_active == series$n_ever_certified)
  if (no_attrition && !isTRUE(acknowledge_no_attrition)) {
    fail(paste(
      "the observed series applies NO ATTRITION -- n_retired is 0 in every row and",
      "n_active equals n_ever_certified in every row, so it is a cumulative",
      "certification series. The simulation DOES apply retirement hazards, so the",
      "two are not the same quantity and the model will structurally under-predict.",
      "Pass acknowledge_no_attrition = TRUE to proceed with this recorded caveat."))
  }

  list(
    value = det$count,
    target_year = target_year,
    geography = geography,
    board_pathway = board_pathway,
    measure = measure,
    contract_version = det$contract_version,
    basis = current$basis,
    observed_series_applies_attrition = !no_attrition,
    retired_values_rejected = retired,
    candidates = backtest_target_candidates(),
    rationale = sprintf(paste(
      "%d is the target: %s geography, %s pathway, %s measure, contract v%s (current),",
      "basis '%s' -- identical to the basis the simulated cohort is constructed on.",
      "The retired v2.1.0 values (%s) use the primary board cert year and are rejected."),
      det$count, geography, board_pathway, measure, det$contract_version,
      current$basis, paste(retired, collapse = "/"))
  )
}

# ---- Pre-cutoff parameter estimation ---------------------------------------

#' Certification cohorts using information through a cutoff only
#'
#' @param through_year Cutoff year.
#' @param geography,board_pathway Contract dimensions.
#' @return Tibble of `cert_year`, `n_certified`, `basis`.
#' @export
backtest_cohorts_through <- function(through_year = BACKTEST_CUTOFF_YEAR,
                                     geography = "national",
                                     board_pathway = "ABOG_PLUS_ABU") {
  a <- .series_through(through_year, geography, board_pathway, "cohorts")
  tibble::tibble(
    cert_year = a$year,
    n_certified = c(a$n_active[1], diff(a$n_active)),
    basis = c("initial backlog", rep("fellowship graduate cohort", nrow(a) - 1L))
  )
}

#' Entrant rate estimated from pre-cutoff information only
#'
#' The main model estimates this from 2018-2023, which would leak the entire
#' validation window. Here the window ends at the cutoff.
#'
#' The steady-state window still starts at 2018: net growth averaged 86.5/yr
#' over 2014-2017 while the initial certification backlog cleared, which is not a
#' rate that could persist. That judgement uses only pre-cutoff information.
#'
#' @param through_year Cutoff year.
#' @param steady_from First year of the steady-state window.
#' @param agents Cohort supplying the age structure for the departure estimate.
#' @return List with `gross_entrants`, `net_growth`, `departures`, `window`.
#' @export
backtest_entrant_estimate <- function(through_year = BACKTEST_CUTOFF_YEAR,
                                      steady_from = 2018L,
                                      agents) {
  coh <- backtest_cohorts_through(through_year)
  steady <- coh[coh$cert_year >= steady_from & coh$cert_year <= through_year, ]
  if (!nrow(steady)) {
    stop("backtest_entrant_estimate: no years between ", steady_from, " and ",
         through_year, call. = FALSE)
  }
  net <- mean(steady$n_certified)
  rate <- implied_annual_departure_rate(
    agents$age, if ("sex" %in% names(agents)) agents$sex else "female")
  departures <- nrow(agents) * rate

  list(
    gross_entrants = net + departures,
    net_growth = net,
    departures = departures,
    departure_rate = rate,
    window = c(steady_from, through_year),
    n_years = nrow(steady),
    yearly = stats::setNames(steady$n_certified, steady$cert_year)
  )
}

#' Base-year cohort as it stood at the cutoff
#'
#' @param through_year Cutoff year.
#' @param geography,board_pathway Contract dimensions.
#' @param female_share Share drawn female.
#' @param subspecialty Subspecialty label.
#' @return Agent tibble.
#' @export
backtest_cohort_at <- function(through_year = BACKTEST_CUTOFF_YEAR,
                               geography = "national",
                               board_pathway = "ABOG_PLUS_ABU",
                               female_share = 0.55,
                               subspecialty = "URPS") {
  coh <- backtest_cohorts_through(through_year, geography, board_pathway)
  coh <- coh[coh$n_certified > 0, ]

  parts <- lapply(seq_len(nrow(coh)), function(i) {
    yr <- coh$cert_year[i]; n <- coh$n_certified[i]
    backlog <- yr <= URPS_FIRST_CERTIFICATION_YEAR
    age_at_cert <- if (backlog) {
      stats::rnorm(n, BACKLOG_COHORT_AGE_MEAN_AT_CERT, BACKLOG_COHORT_AGE_SD_AT_CERT)
    } else {
      stats::rnorm(n, MICROSIM_ENTRY_AGE, 2.5)
    }
    tibble::tibble(
      provider_id = sprintf("B%d_%05d", yr, seq_len(n)),
      subspecialty = subspecialty,
      sex = ifelse(stats::runif(n) < female_share, "female", "male"),
      age = pmin(pmax(round(age_at_cert + (through_year - yr)), MICROSIM_ENTRY_AGE),
                 MICROSIM_TERMINAL_AGE - 1L),
      entry_year = yr,
      retirement_year = NA_real_,
      origin_cohort = if (backlog) "backlog_2013" else "fellowship_cohort",
      cert_year = yr,
      cohort_source = if (backlog) "assumed" else "observed"
    )
  })
  dplyr::bind_rows(parts)
}
