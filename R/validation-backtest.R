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
#' Target year the back-test scores against
#'
#' Default `target_year` for [validate_backtest_target()] and [run_backtest()].
#' @family backtest
#' @concept validation
#' @export
BACKTEST_TARGET_YEAR <- 2023L

# ---- Leakage control -------------------------------------------------------

.backtest_audit <- new.env(parent = emptyenv())

#' Reset the leakage audit log
#' @return (Invisibly) NULL.
#' @family backtest
#' @concept validation
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
#' @family backtest
#' @concept validation
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
#' @family backtest
#' @concept validation
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
#' @family backtest
#' @concept validation
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

#' Which retirement regime the contract series is in
#'
#' VERSION-TOLERANT BY DESIGN. Two mufflyaccess generations describe the same
#' fact differently: 0.10.0 serves `n_retired` as integer zeros with no
#' `urps_retirement_status()`, while later contracts serve NA and expose that
#' accessor. Both mean "departures were never ascertained", and a guard keyed on
#' either representation alone breaks on the other -- which is exactly what
#' happened: tests asserting the NA form failed against 0.10.0.
#'
#' The contract's own accessor is authoritative when present; otherwise the
#' regime is inferred from the data. Callers branch on the RETURNED VALUE rather
#' than on message text, so neither generation needs a different test.
#'
#' @param series Contract series; read from the contract when NULL.
#' @return One of "not_ascertained", "zero", "ascertained", with a `source`
#'   attribute recording whether it came from the accessor or from inference.
#' @family backtest
#' @concept validation
#' @export
backtest_retirement_regime <- function(series = NULL) {
  declared <- tryCatch({
    if ("urps_retirement_status" %in% getNamespaceExports("mufflyaccess")) {
      as.character(mufflyaccess::urps_retirement_status())[1]
    } else NA_character_
  }, error = function(e) NA_character_)

  if (!is.na(declared) && nzchar(declared)) {
    regime <- if (grepl("not_ascertain|unascertain", declared)) "not_ascertained" else "ascertained"
    return(structure(regime, source = "contract accessor", declared = declared))
  }

  if (is.null(series)) series <- mufflyaccess::urps_counts_long()
  if (!"n_retired" %in% names(series)) {
    return(structure("not_ascertained", source = "absent n_retired column"))
  }
  if (any(is.na(series$n_retired))) {
    return(structure("not_ascertained", source = "n_retired served as NA"))
  }
  if (all(series$n_retired == 0)) {
    return(structure("zero", source = "n_retired is 0 in every row"))
  }
  structure("ascertained", source = "n_retired is non-zero")
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
#' @family backtest
#' @concept validation
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
  #
  # THE na.rm TRAP. This test used to be
  #   all(series$n_retired == 0, na.rm = TRUE) && all(n_active == n_ever_certified)
  # which passes VACUOUSLY when `n_retired` is all NA: na.rm empties the vector
  # and all(logical(0)) is TRUE. Under the installed contract (mufflyaccess
  # 0.10.0) n_retired is integer with no NAs and every value 0, so the guard is
  # correct today -- but a future contract that serves retirement as
  # unascertained NA would flip the `&&` and silently drop the acknowledgement
  # requirement. Retirement being UNKNOWN is not evidence that it is ZERO, and
  # the two must not collapse into the same branch.
  series <- mufflyaccess::urps_counts_long()
  if (!"n_retired" %in% names(series)) {
    fail(paste("the contract series carries no n_retired column, so whether the",
               "observed series applies attrition cannot be established. Refusing",
               "to assume it does not."))
  }
  regime <- backtest_retirement_regime(series)
  retired_unascertained <- identical(as.character(regime), "not_ascertained")
  retired_all_zero <- identical(as.character(regime), "zero")
  stock_is_cumulative <- all(series$n_active == series$n_ever_certified, na.rm = TRUE)

  # Both messages carry the tag "[retirement regime: <x>]", so a caller or test
  # can match either generation's failure without knowing which contract is
  # installed. Only the explanation differs; the requirement does not.
  tag <- sprintf("[retirement regime: %s, via %s]", as.character(regime),
                 attr(regime, "source"))

  if (retired_unascertained && !isTRUE(acknowledge_no_attrition)) {
    fail(paste(
      tag, "attrition is UNASCERTAINED rather than known to be zero -- the",
      "contract does not report departures. An unascertained departure count",
      "cannot be compared with a simulation that applies retirement hazards.",
      "Pass acknowledge_no_attrition = TRUE to proceed with this recorded",
      "caveat."))
  }
  no_attrition <- retired_all_zero && stock_is_cumulative
  if (no_attrition && !isTRUE(acknowledge_no_attrition)) {
    fail(paste(
      tag, "the observed series applies NO ATTRITION -- n_retired is 0 in every",
      "row (verified non-NA) and n_active equals n_ever_certified in every row,",
      "so it is a cumulative certification series. The simulation DOES apply",
      "retirement hazards, so the two are not the same quantity and the model",
      "will structurally under-predict. Pass acknowledge_no_attrition = TRUE to",
      "proceed with this recorded caveat."))
  }

  list(
    value = det$count,
    target_year = target_year,
    geography = geography,
    board_pathway = board_pathway,
    measure = measure,
    contract_version = det$contract_version,
    basis = current$basis,
    # `!no_attrition` collapsed two different states into one. With retirement
    # UNASCERTAINED, retired_all_zero is FALSE, so no_attrition is FALSE, and the
    # negation asserted that the observed series DOES net out departures -- the
    # strongest of the three claims, from the weakest evidence. Unknown is not
    # evidence of attrition any more than it is evidence of none: the series may
    # be said to apply attrition only when retirement was actually ascertained
    # and is non-zero.
    observed_series_applies_attrition =
      !retired_unascertained && !retired_all_zero && !stock_is_cumulative,
    # ONLY a non-zero measured count counts as ascertained. Under mufflyaccess
    # 0.10.0 n_retired is 0 in every row of a series where n_active always
    # equals n_ever_certified -- that is a placeholder for "not tracked", not a
    # measurement that nobody retired. Treating it as ascertained would let the
    # weakest evidence support the strongest claim, which is the same error the
    # `!no_attrition` collapse made.
    retirement_ascertained = identical(as.character(regime), "ascertained"),
    # The regime itself, so callers branch on a value rather than on prose.
    retirement_regime = as.character(regime),
    retirement_regime_source = attr(regime, "source"),
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

# ---- Attrition ascertainment ------------------------------------------------

#' What the contract must supply before the primary comparison is like-for-like
#'
#' The primary back-test arm scores an active headcount, net of retirement,
#' against a series that retires nobody. The gap is not a modelling error and no
#' change to the engine can close it: the observed side is a cumulative
#' certification count, and the contract says so itself --
#' `mufflyaccess::urps_retirement_status()` returns `"not_ascertained"` and
#' `n_retired` is unpopulated in every row.
#'
#' This reports that state as data rather than prose, so a caller can branch on
#' it and so the day the upstream artifact starts ascertaining retirement, the
#' back-test stops apologising automatically. Until then the primary comparison
#' is biased AGAINST the model by roughly the departure rate, and the
#' definition-matched arm is the one that isolates what the observed series can
#' actually test.
#'
#' @return An object of class `urps_attrition_requirement`.
#' @family backtest
#' @concept validation
#' @export
backtest_attrition_requirement <- function() {
  .require_mufflyaccess("The attrition ascertainment status")

  # VERSION-TOLERANT. Calling the accessor directly returns NA on contracts that
  # do not export it (mufflyaccess 0.10.0), which reported the status as
  # UNKNOWN when the series plainly shows it is not ascertained. Route through
  # backtest_retirement_regime(), which prefers the accessor when present and
  # infers from the data otherwise, so both generations report the same state.
  series <- mufflyaccess::urps_counts_long()
  regime <- backtest_retirement_regime(series)
  status <- if (identical(as.character(regime), "ascertained")) "ascertained" else "not_ascertained"
  status_source <- attr(regime, "source")
  n_retired_populated <- "n_retired" %in% names(series) &&
    any(is.finite(series$n_retired) & series$n_retired > 0)
  active_equals_ever <- all(series$n_active == series$n_ever_certified, na.rm = TRUE)

  ascertained <- identical(status, "ascertained") && n_retired_populated

  structure(
    list(
      ascertained = ascertained,
      retirement_status = status,
      retirement_status_source = status_source,
      n_retired_populated = n_retired_populated,
      active_equals_ever_certified = active_equals_ever,
      required = c(
        "n_retired populated per year, not NA or zero throughout",
        "n_active reported net of departures, so it differs from n_ever_certified",
        "the contract reporting retirement as ascertained (via urps_retirement_status() where exposed, or a populated n_retired otherwise)"
      ),
      consequence = paste(
        "Until then the primary arm compares an active count against a",
        "cumulative certification count. The model is biased low against that",
        "series by approximately the annual departure rate, and the",
        "definition-matched (no-attrition) arm is the comparison the observed",
        "series can support."
      )
    ),
    class = "urps_attrition_requirement"
  )
}

#' @export
print.urps_attrition_requirement <- function(x, ...) {
  cat(sprintf("Attrition ascertainment: %s\n",
              if (isTRUE(x$ascertained)) "AVAILABLE" else "NOT AVAILABLE"))
  cat(sprintf("  contract status            %s\n", x$retirement_status))
  cat(sprintf("  n_retired populated        %s\n", x$n_retired_populated))
  cat(sprintf("  n_active net of departures %s\n", !x$active_equals_ever_certified))
  if (!isTRUE(x$ascertained)) {
    cat("  still required:\n")
    for (r in x$required) cat(sprintf("    - %s\n", r))
    cat(sprintf("  %s\n", x$consequence))
  }
  invisible(x)
}

# ---- Pre-cutoff parameter estimation ---------------------------------------

#' Certification cohorts using information through a cutoff only
#'
#' @param through_year Cutoff year.
#' @param geography,board_pathway Contract dimensions.
#' @return Tibble of `cert_year`, `n_certified`, `basis`.
#' @family backtest
#' @concept validation
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
#' NO DEPARTURE ADD-BACK. `n_certified` is a flow of new certifications that
#' nothing is subtracted from, so its mean is already the gross entrant rate;
#' the previous `net + departures` added modelled departures to a series that
#' had never removed them. Correcting this LOWERS the pre-cutoff estimate from
#' 60.7 to 32.7/yr, because the 2018-2020 window contains the COVID-collapsed
#' 2020 cohort (n = 10). The old double-count happened to mask a genuinely
#' unrepresentative estimation window; the fix exposes it, which is the point of
#' a back-test. Neither quantity is re-tuned after seeing the 2023 value.
#'
#' @param through_year Cutoff year.
#' @param steady_from First year of the steady-state window.
#' @param agents Cohort supplying the age structure for the departure estimate.
#' @return List with `gross_entrants`, `departures`, `window`, `sd_entrants`.
#' @family backtest
#' @concept validation
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
  flow <- mean(steady$n_certified)
  rate <- implied_annual_departure_rate(
    agents$age, if ("sex" %in% names(agents)) agents$sex else "female")
  departures <- nrow(agents) * rate

  list(
    gross_entrants = flow,
    observed_flow = flow,
    # Deprecated alias: never a net quantity. Retained so existing readers of
    # the field do not silently get NULL.
    net_growth = flow,
    sd_entrants = if (nrow(steady) > 1) stats::sd(steady$n_certified) else NA_real_,
    departures = departures,
    departure_rate = rate,
    departures_added = FALSE,
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
#' @family backtest
#' @concept validation
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

# ---- Multi-window validation ------------------------------------------------

#' Validate the stock-flow projection across several cutoff/target windows
#'
#' A single endpoint cannot support a calibration claim. This scores the same
#' 3-year stock-flow projection at every cutoff the certification series allows,
#' under two competing entrant predictors, so that direction and magnitude of
#' error can be read across windows rather than from one number.
#'
#' WHAT IT SHOWED. The 2020->2023 back-test has every arm under-predicting, which
#' reads as a structural downward bias. It is not: across cutoffs 2016-2020 the
#' certification-flow predictor errs +17.6%, +6.6%, -2.5%, -1.9%, -8.4%. The
#' direction FLIPS. The apparent bias is a property of the 2020 window, whose
#' pre-cutoff years contain the COVID-collapsed 2020 examination cohort, not of
#' the engine.
#'
#' @param cutoffs Cutoff years to score.
#' @param horizon Years projected from each cutoff.
#' @param predictor "certification" (trailing mean of the certification flow) or
#'   "nrmp" (mean of NRMP filled positions published by the cutoff).
#' @param lookback Trailing years used by the certification predictor.
#' @return Tibble with one row per window.
#' @family backtest
#' @concept validation
#' @export
backtest_multi_window <- function(cutoffs = 2017:2020, horizon = 3L,
                                  predictor = c("certification", "nrmp"),
                                  lookback = 3L) {
  predictor <- match.arg(predictor)
  coh <- urps_certification_cohorts()
  nr <- nrmp_entrant_series()

  rows <- lapply(cutoffs, function(cut) {
    target <- cut + horizon
    observed <- tryCatch(
      mufflyaccess::urps_count(target, geography = "national", include_urology = TRUE),
      error = function(e) NA_real_)
    if (!is.finite(observed)) return(NULL)
    n0 <- sum(coh$n_certified[coh$cert_year <= cut])

    rate <- if (identical(predictor, "certification")) {
      mean(coh$n_certified[coh$cert_year > cut - lookback & coh$cert_year <= cut])
    } else {
      # Filter on PUBLICATION year: a report issued after the cutoff cannot be
      # used, however much better the resulting prediction would be.
      avail <- nr$positions_filled[nr$available_by_year <= cut]
      if (length(avail) == 0) return(NULL)
      mean(avail)
    }
    pred <- n0 + horizon * rate
    tibble::tibble(cutoff_year = cut, target_year = target, predictor = predictor,
                   baseline_stock = n0, entrant_rate = rate, predicted = pred,
                   observed = observed, absolute_error = pred - observed,
                   percent_error = 100 * (pred - observed) / observed)
  })
  out <- dplyr::bind_rows(Filter(Negate(is.null), rows))
  if (nrow(out) == 0) stop("backtest_multi_window: no scorable windows", call. = FALSE)
  out
}

#' Out-of-sample predictive interval from other validation windows
#'
#' Arm 5's interval contains the sampling error of a four-observation mean and
#' nothing else -- 8 providers wide on a count near 1,300. The uncertainty it
#' omits is the PREDICTOR'S OWN out-of-sample error, which is estimable only by
#' scoring other windows.
#'
#' The scored window is EXCLUDED from the error estimate, so the interval is not
#' fitted to the endpoint it is judged against.
#'
#' @param target_cutoff Cutoff whose prediction is being bounded.
#' @param cutoffs Candidate windows; `target_cutoff` is dropped from training.
#' @param predictor Entrant predictor, as in [backtest_multi_window()].
#' @param conf Interval level.
#' @return List with the raw and bias-corrected prediction, interval, and the
#'   training errors it rests on.
#' @family backtest
#' @concept validation
#' @export
backtest_oos_interval <- function(target_cutoff = 2020L, cutoffs = 2017:2020,
                                  predictor = "nrmp", conf = 0.95) {
  w <- backtest_multi_window(cutoffs = cutoffs, predictor = predictor)
  test <- w[w$cutoff_year == target_cutoff, , drop = FALSE]
  train <- w[w$cutoff_year != target_cutoff, , drop = FALSE]
  if (nrow(test) != 1L) stop("backtest_oos_interval: target window not scored", call. = FALSE)
  if (nrow(train) < 2L) {
    stop("backtest_oos_interval: fewer than two training windows; the error ",
         "spread is not estimable", call. = FALSE)
  }

  e <- train$percent_error / 100
  mu <- mean(e); s <- stats::sd(e)
  # t rather than normal: the spread rests on a handful of windows, and with
  # n = 3 the t(2) critical value is 4.30 against the normal's 1.96. That is not
  # conservatism, it is the honest cost of having almost no validation history.
  tq <- stats::qt(1 - (1 - conf) / 2, df = nrow(train) - 1L)

  list(
    target_cutoff = target_cutoff,
    raw_prediction = test$predicted,
    bias_corrected = test$predicted * (1 + mu),
    lower = test$predicted * (1 + mu - tq * s),
    upper = test$predicted * (1 + mu + tq * s),
    observed = test$observed,
    covered = test$observed >= test$predicted * (1 + mu - tq * s) &&
      test$observed <= test$predicted * (1 + mu + tq * s),
    train_errors_pct = train$percent_error,
    n_train = nrow(train),
    mean_error = mu, sd_error = s, t_quantile = tq
  )
}

#' Rolling-origin validation: train only on outcomes known at the origin
#'
#' The distinction from leave-one-out is not pedantry, and it is the reason this
#' function exists separately.
#'
#' A naive LOO estimates the error distribution for window `c` from every OTHER
#' window, including later ones. But a window with cutoff `c'` has target
#' `c' + horizon`, and its error is not OBSERVABLE until that target year. Using
#' it to bound a forecast made at origin `c` requires knowing an outcome that
#' had not happened yet. That is future leakage even though each individual
#' prediction respected its own cutoff.
#'
#' Rolling origin admits a training window only when `target_year <= origin`, so
#' every error in the training set was measurable at the moment of forecast.
#'
#' @param cutoffs Candidate windows.
#' @param horizon Projection length.
#' @param predictor Entrant predictor, as in [backtest_multi_window()].
#' @param min_train Minimum training windows; origins with fewer are excluded
#'   rather than scored on a spread that is not estimable.
#' @param conf Interval level.
#' @return Tibble, one row per eligible origin.
#' @family backtest
#' @concept validation
#' @export
backtest_rolling_origin <- function(cutoffs = 2013:2020, horizon = 3L,
                                    predictor = "nrmp", min_train = 2L,
                                    conf = 0.95) {
  w <- backtest_multi_window(cutoffs = cutoffs, horizon = horizon,
                             predictor = predictor)
  w <- w[order(w$cutoff_year), , drop = FALSE]

  rows <- lapply(seq_len(nrow(w)), function(i) {
    origin <- w$cutoff_year[i]
    # THE HONESTY CONSTRAINT: the training window's own outcome must already
    # have happened by the origin.
    train <- w[w$target_year <= origin, , drop = FALSE]
    if (nrow(train) < min_train) return(NULL)

    e <- train$percent_error / 100
    mu <- mean(e); s <- stats::sd(e)
    tq <- stats::qt(1 - (1 - conf) / 2, df = nrow(train) - 1L)
    pred <- w$predicted[i]
    lo <- pred * (1 + mu - tq * s); hi <- pred * (1 + mu + tq * s)
    obs <- w$observed[i]

    tibble::tibble(
      origin = origin, target_year = w$target_year[i], n_train = nrow(train),
      train_targets = paste(train$target_year, collapse = ","),
      observed = obs, raw_prediction = pred,
      median_prediction = pred * (1 + mu),
      lower = lo, upper = hi, width = hi - lo,
      signed_error = pred * (1 + mu) - obs,
      abs_pct_error = abs(100 * (pred * (1 + mu) - obs) / obs),
      covered = obs >= lo && obs <= hi
    )
  })
  out <- dplyr::bind_rows(Filter(Negate(is.null), rows))
  if (nrow(out) == 0) {
    stop("backtest_rolling_origin: no origin has ", min_train,
         " training windows whose outcomes precede it", call. = FALSE)
  }
  out
}

#' Leave-one-out validation across windows (leaky by construction)
#'
#' Retained ONLY as the comparator that shows what rolling origin corrects.
#' Training uses every other window regardless of whether its outcome had
#' occurred by the origin, so the interval for an early window can be informed
#' by outcomes from years after it. Do not report this as predictive
#' calibration.
#'
#' @inheritParams backtest_rolling_origin
#' @return Tibble, one row per window.
#' @family backtest
#' @concept validation
#' @export
backtest_loo_validation <- function(cutoffs = 2013:2020, horizon = 3L,
                                    predictor = "nrmp", min_train = 2L,
                                    conf = 0.95) {
  w <- backtest_multi_window(cutoffs = cutoffs, horizon = horizon,
                             predictor = predictor)
  w <- w[order(w$cutoff_year), , drop = FALSE]

  rows <- lapply(seq_len(nrow(w)), function(i) {
    train <- w[-i, , drop = FALSE]
    if (nrow(train) < min_train) return(NULL)
    e <- train$percent_error / 100
    mu <- mean(e); s <- stats::sd(e)
    tq <- stats::qt(1 - (1 - conf) / 2, df = nrow(train) - 1L)
    pred <- w$predicted[i]
    lo <- pred * (1 + mu - tq * s); hi <- pred * (1 + mu + tq * s)
    obs <- w$observed[i]
    tibble::tibble(
      origin = w$cutoff_year[i], target_year = w$target_year[i],
      n_train = nrow(train),
      # Training targets AFTER this origin are the leak, counted explicitly.
      n_train_future = sum(train$target_year > w$cutoff_year[i]),
      observed = obs, raw_prediction = pred,
      median_prediction = pred * (1 + mu),
      lower = lo, upper = hi, width = hi - lo,
      signed_error = pred * (1 + mu) - obs,
      abs_pct_error = abs(100 * (pred * (1 + mu) - obs) / obs),
      covered = obs >= lo && obs <= hi
    )
  })
  dplyr::bind_rows(Filter(Negate(is.null), rows))
}
