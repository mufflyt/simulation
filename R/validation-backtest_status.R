# Back-Test Status Stamping ----
#
# The 2020->2023 back-test is the only external validation this engine has, and
# it STILL FAILS coverage: the observed 2023 count falls inside the 95% interval
# in 2 of 10 arms (20%, against a required 80%). That result is written up
# honestly in docs/BACKTEST_2020_TO_2023.md and scored in
# artifacts/backtest_2020_to_2023_summary.csv.
#
# The 2/10 replaces an earlier 0/8, and the improvement is NOT a modelling win --
# it is the removal of two defects that made the earlier score meaningless, plus
# one added arm:
#
#   * The arms carried no parameter uncertainty at all. `run_backtest_arm()`
#     accepted a `param_spec` and `run_backtest()` never passed one, so the
#     intervals were Monte Carlo noise around a pinned entrant rate: PI95 widths
#     of 0-40 providers on a count near 1,300, two arms with LITERALLY zero
#     width. Failing coverage against an interval like that says nothing about
#     the forecast. Widths are now 34-148.
#   * The pre-cutoff entrant estimate added modelled departures to a series that
#     never removed them, inflating it from 32.7 to 60.7/yr. Correcting the
#     double-count moved arms 2 and 4 FURTHER from the observation, because the
#     2018-2020 estimation window contains the COVID-collapsed 2020 cohort
#     (n = 10). The old error was masking an unrepresentative window.
#
#   * Arm 5 was ADDED: entrants estimated from NRMP fellowship matches published
#     before the cutoff (appointment years 2010-2020, mean 49.73/yr). Fellowship
#     is three years, so those cohorts are exactly the people certifying across
#     the validation window, and every report was in print by 2020. See the note
#     on the frozen record below -- extending that series made the arm WORSE.
#
# What survives all three is the finding that matters: all ten arms still
# under-predict, by 3.1% to 17.6%. A one-sided miss across every arm is not
# noise, and no interval widening will fix it.
#
# Neither of those travels with a projection object. A reader handed a table of
# medians and 95% bands has no way to know the bands are unvalidated, and the
# natural reading of "95% interval" is a forecast interval -- which is precisely
# the claim the back-test refuted. This module attaches the validation status to
# the outputs themselves, so the caveat moves with the number instead of living
# in a document beside it.
#
# The status is DERIVED from scored arms, never asserted: `backtest_status()`
# runs the same computation over the frozen record that
# `backtest_status_from_summary()` runs over a live `run_backtest()` result, so a
# future run that achieves coverage flips the verdict automatically.

# Frozen record of the prespecified back-test, transcribed from
# artifacts/backtest_2020_to_2023_summary.csv (manifest: cutoff 2020, target
# 2023, target value 1306, 1000 iterations per arm, seed 20260802). `artifacts/`
# is in .Rbuildignore and does not ship, so the scored numbers are carried here
# rather than read at run time. Re-derive from a live run with
# `backtest_status_from_summary(run_backtest()$summary)`.
#' Frozen record of the prespecified 2020->2023 back-test
#'
#' The default `record` for [backtest_status()] and [verify_backtest_record()].
#' Exported because it is a default argument of exported functions: a caller who
#' cannot name the default cannot reproduce or audit it.
#'
#' @format Tibble of scored arms with `arm`, `percent_error`, `within_80`,
#'   `within_95`.
#' @export
BACKTEST_RECORD_2020_2023 <- tibble::tribble(
  ~arm,                                          ~percent_error,  ~within_80,  ~within_95,
  "1. Derived cohort, assumed entrants",              -9.724349,       FALSE,       FALSE,
  "1. Derived cohort [no-attrition]",                 -3.139357,        TRUE,        TRUE,
  "2. Derived cohort, pre-cutoff entrants",          -14.969372,       FALSE,       FALSE,
  "2. Derived cohort [no-attrition]",                 -8.269525,       FALSE,       FALSE,
  "3. Synthetic cohort, assumed entrants",           -12.633997,       FALSE,       FALSE,
  "3. Synthetic cohort [no-attrition]",               -3.177642,        TRUE,        TRUE,
  "4. Synthetic cohort, pre-cutoff entrants",        -17.611026,       FALSE,       FALSE,
  "4. Synthetic cohort [no-attrition]",               -8.269525,       FALSE,       FALSE,
  "5. Derived cohort, pre-cutoff NRMP entrants",     -11.026034,       FALSE,       FALSE,
  "5. Derived cohort [no-attrition]",                 -4.364472,       FALSE,       FALSE
)

# WHAT EXTENDING THE NRMP SERIES DID, AND IT WAS NOT FLATTERING.
#
# Arm 5 was scored first on NRMP appointment years 2017-2020 (mean 58.0/yr) and
# was then the most accurate arm in the design at -2.53%. Fetching the full
# pre-cutoff series back to 2010 dropped its rate to 49.73/yr, because the mean
# now spans the establishment ramp -- FPMRS filled only 30 positions in 2010,
# against a 57.0 plateau from 2015. Arm 5 moved to -4.36% and its interval
# widened from 8 to 34, since eleven years of a growing series carry far more
# spread than four years of a flat one.
#
# So MORE pre-cutoff evidence made the NRMP arm worse on both axes. That is the
# honest cost of refusing to select an estimation window after seeing the
# outcome: restricting to the 2015+ plateau would score better, and would be a
# choice made with the answer in hand.
#
# The most accurate arm is now arm 1 [no-attrition] at -3.14% -- the SHIPPED
# ASSUMPTION of 55/yr, which matched no source. It lands closest to a rate of
# ~69/yr implied by the observed certifications for no better reason than that
# 55 happened to sit between the ramp-diluted NRMP mean and the truth.
#
# Coverage still tracks interval WIDTH more than accuracy. The two arms that
# cover (1 and 3, widths 145 and 138) inherit the certification series' large
# spread; arm 5 is four times sharper at width 34 and fails. A criterion that
# rewards the blunter estimator is not measuring calibration.
#
# The finding that survives every re-scoring: all ten arms under-predict, by
# 3.1% to 17.6%. A one-sided miss across every arm is bias, not noise, and no
# interval widening addresses it.

BACKTEST_RECORD_SOURCE <- paste(
  "artifacts/backtest_2020_to_2023_summary.csv; cutoff 2020, target 2023",
  "(observed 1306, national/ABOG_PLUS_ABU/board_certified_active, contract",
  "v3.0.0), 1000 iterations per arm, seed 20260802, entrant rate drawn per",
  "iteration from the pre-cutoff series (PI95 widths 34-148 across 10 arms)"
)

# Share of arms whose 95% interval must contain the observed value before the
# engine's intervals may be described as validated forecast intervals. Not all
# eight: the arms deliberately differ in cohort construction and attrition
# definition, so a single discordant arm should not veto the claim. A majority
# threshold is the weakest defensible bar, and the engine currently scores zero.
BACKTEST_COVERAGE_REQUIRED <- 0.8

#' Derive validation status from scored back-test arms
#'
#' @param summary Tibble of scored arms with `within_80`, `within_95` and
#'   `percent_error` (the shape [score_backtest_arm()] returns).
#' @param source Provenance string recorded in the status.
#' @param required Share of arms whose 95% interval must cover the observation.
#' @return An object of class `urps_backtest_status`.
#' @export
backtest_status_from_summary <- function(summary,
                                         source = "live run_backtest() result",
                                         required = BACKTEST_COVERAGE_REQUIRED) {
  need <- c("within_95", "percent_error")
  missing <- setdiff(need, names(summary))
  if (length(missing) > 0) {
    stop(sprintf("backtest_status_from_summary: missing column(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  n <- nrow(summary)
  if (n == 0) stop("backtest_status_from_summary: no scored arms", call. = FALSE)

  cov95 <- mean(as.logical(summary$within_95), na.rm = TRUE)
  cov80 <- if ("within_80" %in% names(summary)) {
    mean(as.logical(summary$within_80), na.rm = TRUE)
  } else NA_real_
  pe <- summary$percent_error[is.finite(summary$percent_error)]

  # DEFINITION-MATCHED SUBSET, reported separately as a diagnostic.
  #
  # Half the arms apply attrition to project an ACTIVE workforce, then score it
  # against a cumulative certification series that removes nobody
  # (`observed_series_applies_attrition = FALSE`). Those arms are expected to
  # under-predict for a reason that has nothing to do with interval quality, and
  # that was knowable before the target was read -- the code already labels the
  # others "definition-matched".
  #
  # This does NOT loosen the bar. `validated` is still computed over ALL arms,
  # and the subset coverage is worse than the headline in any case, so nothing
  # here can turn a failure into a pass. It exists so a reader can see WHICH
  # comparison failed rather than averaging two different estimands together.
  # Neither column is required: `backtest_status_from_summary()` accepts any
  # table carrying within_95/percent_error, so a caller that supplies neither
  # `apply_attrition` nor `arm` gets no subset rather than a warning about a
  # column that was never promised.
  matched <- if ("apply_attrition" %in% names(summary)) {
    !as.logical(summary$apply_attrition)
  } else if ("arm" %in% names(summary)) {
    grepl("no-attrition", summary$arm, fixed = TRUE)
  } else {
    rep(FALSE, n)
  }
  cov95_matched <- if (any(matched)) {
    mean(as.logical(summary$within_95[matched]), na.rm = TRUE)
  } else NA_real_

  structure(
    list(
      validated = isTRUE(cov95 >= required),
      n_arms = n,
      coverage_95 = cov95,
      coverage_80 = cov80,
      coverage_required = required,
      n_definition_matched = sum(matched),
      coverage_95_definition_matched = cov95_matched,
      # A SINGLE observation (the target year) is scored by every arm, so the
      # arms are not independent trials and k/n does not estimate coverage.
      # Recorded so the number is never read as a calibration statistic.
      coverage_is_estimable = FALSE,
      coverage_caveat = paste(
        "All arms score the SAME single observation (one target year), so they",
        "are not independent trials and k/n does not estimate interval",
        "coverage. Read it as: which configurations were consistent with the",
        "one value we can check."
      ),
      worst_percent_error = if (length(pe)) pe[which.max(abs(pe))] else NA_real_,
      median_percent_error = if (length(pe)) stats::median(pe) else NA_real_,
      # Every arm under-predicted. A one-sided miss is a different problem
      # from scatter around the truth: it points at the entrant rate, not noise.
      all_same_direction = length(pe) > 0 && length(unique(sign(pe))) == 1L,
      source = source
    ),
    class = "urps_backtest_status"
  )
}

#' Validation status of this engine's projected intervals
#'
#' The single place any output may ask "has this engine been validated?". Built
#' from the prespecified 2020->2023 back-test.
#'
#' @param record Scored arms; defaults to the frozen published record.
#' @param source Provenance string.
#' @return An object of class `urps_backtest_status`.
#' @export
backtest_status <- function(record = BACKTEST_RECORD_2020_2023,
                            source = BACKTEST_RECORD_SOURCE) {
  backtest_status_from_summary(record, source = source)
}

#' How an interval from this engine may be described
#'
#' Returns the phrase that belongs in a table caption or figure legend. While
#' coverage fails, "forecast interval" is not available: the back-test refuted
#' exactly that claim, and the honest description is a Monte Carlo range.
#'
#' @param status A [backtest_status()] object.
#' @param ci Nominal interval width.
#' @return A character label.
#' @export
interval_label <- function(status = backtest_status(), ci = 0.95) {
  pct <- format(round(100 * ci))
  if (isTRUE(status$validated)) {
    return(sprintf("%s%% forecast interval (back-test coverage %.0f%% of %d arms)",
                   pct, 100 * status$coverage_95, status$n_arms))
  }
  # Coverage can be NaN when every arm's flag is NA -- an unscorable back-test.
  # sprintf("%d", NaN) is an ERROR, not a fallback, so this crashed rather than
  # degrading: the label that exists to keep an unvalidated interval from being
  # called a forecast interval was the thing that failed.
  n_missed <- (1 - status$coverage_95) * status$n_arms
  if (!is.finite(n_missed)) {
    return(sprintf(paste("%s%% Monte Carlo range -- NOT a validated forecast",
                         "interval: coverage could not be scored across the %d",
                         "back-test arm(s)"), pct, status$n_arms))
  }
  sprintf(paste("%s%% Monte Carlo range -- NOT a validated forecast interval:",
                "the observed value fell outside it in %d of %d back-test arms"),
          pct, round(n_missed), status$n_arms)
}

#' Refuse forecast-interval language while coverage fails
#'
#' Call before publishing anything that describes this engine's bands as
#' forecast or prediction intervals.
#'
#' @param status A [backtest_status()] object.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when the language is permitted.
#' @export
assert_forecast_intervals_validated <- function(status = backtest_status(),
                                                mode = resolve_reproducibility_mode()) {
  if (isTRUE(status$validated)) return(invisible(TRUE))
  msg <- sprintf(paste(
    "This engine's intervals are not validated: the observed value fell outside",
    "the 95%% interval in %d of %d back-test arms (coverage %.0f%%, required",
    "%.0f%%), and every arm under-predicted by up to %.1f%%. Report the band as",
    "a Monte Carlo range -- see interval_label() -- not as a forecast or",
    "prediction interval. Source: %s."),
    round((1 - status$coverage_95) * status$n_arms), status$n_arms,
    100 * status$coverage_95, 100 * status$coverage_required,
    abs(status$worst_percent_error), status$source)
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' Attach the back-test status to an exported object
#'
#' The URPS projection contract is a fixed 13-column schema, so the status rides
#' as an attribute rather than a column: adding one would fail
#' `mufflyaccess::validate_urps_projection()`.
#'
#' @param x Object to stamp (typically a projection data frame).
#' @param status A [backtest_status()] object.
#' @return `x`, with a `backtest_status` attribute.
#' @export
stamp_backtest_status <- function(x, status = backtest_status()) {
  attr(x, "backtest_status") <- status
  x
}

#' Read the back-test status stamped on an object
#'
#' @param x A stamped object.
#' @return The [backtest_status()] object, or NULL if the object is unstamped.
#' @export
stamped_backtest_status <- function(x) attr(x, "backtest_status", exact = TRUE)

#' @export
print.urps_backtest_status <- function(x, ...) {
  cat(sprintf("External validation: %s\n",
              if (isTRUE(x$validated)) "PASSED" else "FAILED (coverage)"))
  cat(sprintf("  arms                %d\n", x$n_arms))
  cat(sprintf("  95%% coverage        %.0f%% (required %.0f%%)\n",
              100 * x$coverage_95, 100 * x$coverage_required))
  if (is.finite(x$coverage_80)) {
    cat(sprintf("  80%% coverage        %.0f%%\n", 100 * x$coverage_80))
  }
  if (!is.null(x$coverage_95_definition_matched) &&
      is.finite(x$coverage_95_definition_matched)) {
    cat(sprintf("  of which definition-matched: %.0f%% of %d arms\n",
                100 * x$coverage_95_definition_matched, x$n_definition_matched))
  }
  if (isFALSE(x$coverage_is_estimable)) {
    cat("  NOTE: one target year, so k/n is not a coverage estimate\n")
  }
  cat(sprintf("  worst error         %.1f%%\n", x$worst_percent_error))
  cat(sprintf("  median error        %.1f%%\n", x$median_percent_error))
  if (isTRUE(x$all_same_direction)) {
    cat("  every arm missed in the SAME direction: a level problem, not noise\n")
  }
  cat(sprintf("  intervals           %s\n", interval_label(x)))
  cat(sprintf("  source              %s\n", x$source))
  invisible(x)
}

# ---- Keeping the record and the artifact in step ----------------------------
#
# `BACKTEST_RECORD_2020_2023` is a hand-transcribed copy of the scored artifact,
# carried in the package because artifacts/ is .Rbuildignore'd and the status
# stamp must travel with an installed build. That makes DRIFT the central risk:
# re-running run_backtest() rewrites the artifact and leaves the constant
# unchanged, so every projection then carries a status no artifact supports.
#
# It has already happened once. Extending the NRMP series moved arm 5 from
# -2.53% to -4.36%, and for one commit the record and the artifact disagreed.
#
# The prior test compared three AGGREGATE fields -- coverage, worst error, arm
# count. Two different records can agree on all three and differ arm by arm, and
# the check skipped silently wherever artifacts/ was absent, which is every
# installed build. What follows verifies row by row and records the checksum of
# the artifact the record was taken from, so drift is detectable rather than
# merely improbable.

#' SHA-256 of the scored artifact `BACKTEST_RECORD_2020_2023` was transcribed from
#'
#' Regenerating the artifact changes this. If it no longer matches, the record
#' is stale until proven otherwise -- update both together, in one commit.
#' @export
BACKTEST_RECORD_SHA256 <- "d9a895446d24d6debdb8ff6f634f652123003de0918e428f9c99cf1346b01d8d"

# Tolerance on the transcribed percentage errors. The record carries six decimal
# places, so anything looser would let a genuine re-score pass as a rounding
# difference.
BACKTEST_RECORD_TOLERANCE <- 1e-6

#' Locate the scored back-test artifact, if it is present
#'
#' Returns NULL rather than erroring: artifacts/ does not ship, so absence is
#' the normal case in an installed package and is not itself a fault.
#'
#' @param start Directory to search upward from.
#' @return Path, or NULL.
#' @export
backtest_artifact_path <- function(start = ".") {
  candidates <- c(start, file.path(start, ".."), file.path(start, "..", ".."))
  for (p in candidates) {
    f <- file.path(p, "artifacts", "backtest_2020_to_2023_summary.csv")
    if (file.exists(f)) return(normalizePath(f))
  }
  NULL
}

#' Verify the frozen record against the scored artifact, row by row
#'
#' @param path Artifact path; located automatically when NULL.
#' @param record Frozen record to check.
#' @param tolerance Absolute tolerance on `percent_error`.
#' @return List with `checked`, `current`, `checksum_matches`, and `mismatches`
#'   (a tibble, empty when the record is current).
#' @export
verify_backtest_record <- function(path = NULL,
                                   record = BACKTEST_RECORD_2020_2023,
                                   tolerance = BACKTEST_RECORD_TOLERANCE) {
  if (is.null(path)) path <- backtest_artifact_path()
  if (is.null(path) || !file.exists(path)) {
    return(list(checked = FALSE, current = NA, checksum_matches = NA,
                path = NULL, mismatches = NULL,
                detail = "artifact not present; nothing to verify against"))
  }
  live <- utils::read.csv(path, stringsAsFactors = FALSE)

  # openssl returns a classed object, and as.character() KEEPS the class. The
  # result prints as a plain hex string and compares TRUE under ==, but FALSE
  # under identical() -- so an attribute, not a digest, decided whether the
  # record looked stale. Strip to a bare character before comparing.
  # digest is already a declared Import, so this needs no new dependency, and
  # digest(file = ) returns a BARE character -- unlike openssl::sha256(), whose
  # classed return survived as.character() and compared TRUE under `==` but
  # FALSE under identical(), letting an attribute rather than a digest decide
  # whether the record looked stale.
  sha <- tryCatch(digest::digest(file = path, algo = "sha256"),
                  error = function(e) NA_character_)
  if (is.na(sha)) {
    sha <- tryCatch(unname(tools::md5sum(path)), error = function(e) NA_character_)
    checksum_matches <- NA   # md5 is not comparable with the stored sha256
  } else {
    checksum_matches <- identical(sha, BACKTEST_RECORD_SHA256)
  }

  problems <- list()
  if (nrow(live) != nrow(record)) {
    problems[[length(problems) + 1L]] <- tibble::tibble(
      field = "n_arms", record = as.character(nrow(record)),
      artifact = as.character(nrow(live)), arm = NA_character_)
  } else {
    # Row ORDER is the join key: the record is a transcription of this file in
    # this order, and the emitter preserves it. A reordered artifact is itself a
    # mismatch worth surfacing rather than silently matching on labels.
    for (i in seq_len(nrow(live))) {
      # Pull the label OUT before building the row. Inside tibble(), `record`
      # names the column being defined, so `record$arm[i]` resolves against a
      # character scalar and errors -- the same data-mask shadowing that
      # score_backtest_arm() documents for `arm = label`.
      arm_i <- record$arm[i]
      if (abs(record$percent_error[i] - live$percent_error[i]) > tolerance) {
        problems[[length(problems) + 1L]] <- tibble::tibble(
          field = "percent_error",
          record = sprintf("%.6f", record$percent_error[i]),
          artifact = sprintf("%.6f", live$percent_error[i]),
          arm = arm_i)
      }
      for (f in c("within_80", "within_95")) {
        rec_f <- as.logical(record[[f]][i])
        if (!identical(rec_f, as.logical(live[[f]][i]))) {
          problems[[length(problems) + 1L]] <- tibble::tibble(
            field = f, record = as.character(rec_f),
            artifact = as.character(live[[f]][i]), arm = arm_i)
        }
      }
    }
  }

  mism <- if (length(problems)) dplyr::bind_rows(problems) else
    tibble::tibble(field = character(), record = character(),
                   artifact = character(), arm = character())

  list(checked = TRUE, current = nrow(mism) == 0L,
       checksum_matches = checksum_matches, path = path,
       observed_sha256 = sha, expected_sha256 = BACKTEST_RECORD_SHA256,
       mismatches = mism, n_arms = nrow(live))
}

#' Refuse to ship a status stamp the artifact does not support
#'
#' Call after regenerating the back-test, and before publishing anything that
#' carries [backtest_status()].
#'
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @param path Artifact path; located automatically when NULL.
#' @return (Invisibly) TRUE when the record is current or unverifiable.
#' @export
assert_backtest_record_current <- function(mode = resolve_reproducibility_mode(),
                                           path = NULL) {
  v <- verify_backtest_record(path)
  if (!isTRUE(v$checked)) return(invisible(TRUE))
  if (isTRUE(v$current) && !isFALSE(v$checksum_matches)) return(invisible(TRUE))

  lines <- character()
  if (isFALSE(v$checksum_matches)) {
    lines <- c(lines, sprintf(
      "The artifact has been regenerated: sha256 %s, record transcribed from %s.",
      substr(v$observed_sha256, 1, 16), substr(v$expected_sha256, 1, 16)))
  }
  if (nrow(v$mismatches)) {
    lines <- c(lines, sprintf("%d field(s) differ, including:", nrow(v$mismatches)))
    show <- utils::head(v$mismatches, 4)
    lines <- c(lines, sprintf("  %s [%s]: record %s, artifact %s",
                              show$arm, show$field, show$record, show$artifact))
  }
  msg <- paste(c(
    "BACKTEST_RECORD_2020_2023 does not match the scored artifact, so",
    "backtest_status() is reporting a validation result no artifact supports.",
    lines,
    "Regenerate the constant with scripts/diagnostics/emit_backtest_record.R and",
    "update BACKTEST_RECORD_SHA256, in the SAME commit as the artifact."),
    collapse = "\n  ")

  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}
