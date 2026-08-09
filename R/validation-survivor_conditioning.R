# Survivor conditioning in the retrospective workforce series ----
#
# THE FINDING. The 2013-2023 `board_certified_active` series is a 2025 roster
# back-projected by certification year (docs/DENOMINATOR_AUDIT.md). Physicians
# the 2025 adjudication drops are therefore absent from EVERY earlier year --
# including years in which they were demonstrably delivering care.
#
# This is no longer an inference from a departure hazard. It is directly
# falsified: physicians excluded from the later roster were observed billing
# Medicare Part B in EVERY YEAR of the 2016-2021 validation window. See
# [survivor_falsification_statement()] for the counts, which are read from data
# rather than written here.
#
# EVIDENCE TIERS, which must not be merged into one binary "active" flag:
#
#   Tier 1  Medicare Part B billing        care actually delivered and billed
#   Tier 2  Medicare clinician directory   enrolment / practice listing only --
#                                          NOT proof of patient-care volume
#   Tier 3  no federal observation         in the sources examined
#
# The Medicare clinician directory covers 2018-2025 ONLY, so it is silent on
# 2016-2017 and cannot rescue the early window.
#
# WHERE THE NUMBERS LIVE. In exactly one place: the artifact
# inst/extdata/survivor_falsification.json, built by
# scripts/data_acquisition/09_build_survivor_falsification.R from the cliff
# enriched ABOG/ABU rosters and a local Medicare Part B panel and clinician
# directory. This file deliberately contains NO count literals -- if it did,
# there would be two sources of truth and eventually two answers. The frozen
# values are asserted once, in tests/testthat/test-validation-survivor.R.

#' Path to the survivor-conditioning validation artifact
#'
#' @return Absolute path to the JSON artifact.
#' @keywords internal
#' @noRd
.survivor_artifact_path <- function() {
  p <- system.file("extdata", "survivor_falsification.json", package = "urpssim")
  if (!nzchar(p)) {
    stop("The survivor-conditioning artifact is not installed. Rebuild it with ",
         "scripts/data_acquisition/09_build_survivor_falsification.R, which ",
         "requires URPS_CLIFF_ABOG, URPS_CLIFF_ABU and URPS_DUCKDB.",
         call. = FALSE)
  }
  p
}

#' The derived survivor-conditioning artifact
#'
#' Reads the aggregate artifact produced from the enriched ABOG/ABU rosters and
#' the Medicare sources. Holds counts and provenance only: no physician
#' identifiers, because the underlying linkage is identifiable.
#'
#' @return A list with `denominators`, `partb`, `directory`, `annual`,
#'   `windows` and `provenance`.
#' @family survivor conditioning
#' @concept validation
#' @export
survivor_falsification_artifact <- function() {
  a <- jsonlite::read_json(.survivor_artifact_path(), simplifyVector = TRUE)
  need <- c("schema_version", "windows", "denominators", "partb", "directory",
            "annual", "provenance")
  miss <- setdiff(need, names(a))
  if (length(miss)) {
    stop("The survivor-conditioning artifact is missing section(s): ",
         paste(miss, collapse = ", "), call. = FALSE)
  }
  if (!identical(as.integer(a$schema_version), 1L)) {
    stop("Unsupported survivor-conditioning artifact schema version: ",
         a$schema_version, call. = FALSE)
  }
  a
}

#' Falsification evidence that the retrospective series is survivor-conditioned
#'
#' One row per evidence definition, each with its own denominator, window, and an
#' explicit strength label. Directory evidence is weaker than billing evidence
#' and is labelled so; the two are never summed into a single active flag. All
#' counts come from [survivor_falsification_artifact()].
#'
#' @param a A [survivor_falsification_artifact()].
#' @return Tibble of `evidence`, `window`, `n`, `denominator`, `pct`,
#'   `tier`, `strength`.
#' @family survivor conditioning
#' @concept validation
#' @export
survivor_falsification_table <- function(a = survivor_falsification_artifact()) {
  w <- function(x) paste(a$windows[[x]], collapse = "-")
  d <- a$denominators
  p <- a$partb

  tibble::tibble(
    evidence = c(
      "Excluded from later active contract",
      "  of which lack a usable NPI",
      "NPI-linked, eligible for federal linkage",
      "Any Part B billing",
      "Any Part B billing",
      "Part B billing in ALL SIX validation years",
      "No Part B billing anywhere",
      "  sustained clinician-directory listing",
      "  isolated clinician-directory listing",
      "  neither Part B nor directory"),
    window = c("2025 roster", "2025 roster", "2025 roster",
               w("frame"), w("validation"), w("validation"), w("frame"),
               w("directory"), w("directory"), "no source"),
    n = as.integer(c(d$excluded_total, d$excluded_without_npi,
                     d$linkage_denominator, p$any_frame, p$any_validation,
                     p$persistent_validation, p$none_frame,
                     a$directory$sustained, a$directory$isolated,
                     a$directory$neither)),
    denominator = as.integer(c(rep(d$excluded_total, 2), d$excluded_total,
                               rep(d$linkage_denominator, 4),
                               rep(p$none_frame, 3))),
    tier = c(NA_integer_, NA_integer_, NA_integer_, 1L, 1L, 1L, NA_integer_,
             2L, 2L, 3L),
    strength = c(
      "denominator (pre-linkage)",
      "cannot be linked to any source",
      "the linkage denominator",
      "direct: care billed",
      "direct: care billed",
      "direct, repeated: strongest",
      "residual group",
      "WEAKER: enrolment/listing, not billing",
      "weak: listing only",
      "no federal observation")
  ) |>
    dplyr::mutate(pct = round(100 * .data$n / .data$denominator, 1)) |>
    dplyr::select("evidence", "window", "n", "denominator", "pct", "tier",
                  "strength")
}

#' The frozen primary falsification result
#'
#' Three facts, no model: URPS-identified, excluded from the later roster, and
#' observed billing Medicare Part B in every year of the validation window.
#'
#' @param a A [survivor_falsification_artifact()].
#' @return List with the counts and their provenance.
#' @family survivor conditioning
#' @concept validation
#' @export
survivor_falsification_record <- function(a = survivor_falsification_artifact()) {
  v <- a$windows$validation
  list(
    n_persistent_billers = as.integer(a$partb$persistent_validation),
    validation_years = seq.int(v[1], v[2]),
    provider_years_erased = as.integer(a$partb$provider_years_persistent),
    linkage_denominator = as.integer(a$denominators$linkage_denominator),
    identity_universe = as.integer(a$denominators$identity_universe),
    excluded_total = as.integer(a$denominators$excluded_total),
    excluded_without_npi = as.integer(a$denominators$excluded_without_npi),
    any_partb_window = as.integer(a$partb$any_validation),
    any_partb_frame = as.integer(a$partb$any_frame),
    no_partb_frame = as.integer(a$partb$none_frame),
    directory_only_sustained = as.integer(a$directory$sustained),
    # The roster carries no per-provider exclusion narrative: every one of the
    # excluded is dropped by the same flag, so no finer reason can be reported.
    exclusion_reason = a$exclusion_reason,
    directory_coverage = paste0(paste(a$windows$directory, collapse = "-"),
                                " only -- silent on the early validation years"),
    source = a$provenance
  )
}

#' The falsification result as a sentence
#'
#' Generated from the artifact so that prose, table and figure cannot disagree
#' about a number. Use this rather than retyping counts into documentation.
#'
#' @param a A [survivor_falsification_artifact()].
#' @return A length-one character string.
#' @family survivor conditioning
#' @concept validation
#' @export
survivor_falsification_statement <- function(a = survivor_falsification_artifact()) {
  r <- survivor_falsification_record(a)
  v <- paste(range(r$validation_years), collapse = "-")
  sprintf(paste0(
    "Of %d excluded, NPI-linked urogynecologists, %d (%.1f%%) were observed ",
    "billing Medicare Part B during %s, and %d (%.1f%%) were observed billing ",
    "in every year of that window -- %d directly observed provider-years the ",
    "retrospective series erases."),
    r$linkage_denominator, r$any_partb_window,
    100 * r$any_partb_window / r$linkage_denominator, v,
    r$n_persistent_billers,
    100 * r$n_persistent_billers / r$linkage_denominator,
    r$provider_years_erased)
}

#' The falsification table as markdown
#'
#' Rendered here rather than in the plotting script so that a committed copy of
#' the supplemental table can be tested against its source. A table checked into
#' the repository is the easiest place for a number to go stale.
#'
#' @param a A [survivor_falsification_artifact()].
#' @return A character vector of markdown lines.
#' @family survivor conditioning
#' @concept validation
#' @export
survivor_falsification_markdown <- function(a = survivor_falsification_artifact()) {
  tbl <- survivor_falsification_table(a)
  c("| Evidence | Window | n | Denominator | % | Tier | Strength |",
    "|---|---|---:|---:|---:|:---:|---|",
    sprintf("| %s | %s | %d | %d | %.1f | %s | %s |",
            sub("^  ", "&nbsp;&nbsp;", tbl$evidence), tbl$window, tbl$n,
            tbl$denominator, tbl$pct,
            ifelse(is.na(tbl$tier), "", as.character(tbl$tier)), tbl$strength))
}

#' Refuse to let the falsification counts drift silently
#'
#' The counts are a published claim, so this checks every identity that has to
#' hold for the claim to mean what it says: denominators close, windows nest,
#' the residual partitions, and tier-2 directory evidence is never described in
#' the language reserved for billed care. A future edit that breaks one fails
#' here rather than in a manuscript.
#'
#' These are relationships, not expected values. The expected values are frozen
#' separately in the package tests, so that this function stays honest about a
#' rebuilt artifact instead of rejecting it for being new.
#'
#' @param tbl A [survivor_falsification_table()].
#' @param rec A [survivor_falsification_record()].
#' @param artifact A [survivor_falsification_artifact()].
#' @return (Invisibly) TRUE.
#' @family survivor conditioning
#' @concept validation
#' @export
assert_survivor_falsification <- function(artifact = survivor_falsification_artifact(),
                                          tbl = survivor_falsification_table(artifact),
                                          rec = survivor_falsification_record(artifact)) {
  g <- function(ev, w) tbl$n[tbl$evidence == ev & tbl$window == w][1]
  d <- artifact$denominators

  # The identity universe must split into retained and excluded, and the
  # excluded into linkable and unlinkable. A denominator that does not close is
  # the error this whole module exists to prevent.
  if (d$identity_universe != d$retained + d$excluded_total) {
    stop("assert_survivor_falsification(): the identity universe does not ",
         "close: ", d$identity_universe, " != ", d$retained, " + ",
         d$excluded_total, call. = FALSE)
  }
  if (rec$excluded_total != rec$linkage_denominator + rec$excluded_without_npi) {
    stop("assert_survivor_falsification(): the excluded denominator does not ",
         "close: ", rec$excluded_total, " != ", rec$linkage_denominator, " + ",
         rec$excluded_without_npi, call. = FALSE)
  }

  # Cross-check the table against the record. Without this the guard would only
  # validate the record's internal arithmetic, and an edit to the table alone
  # would pass silently -- which a tampering test caught on the first draft.
  fw <- paste(artifact$windows$frame, collapse = "-")
  vw <- paste(artifact$windows$validation, collapse = "-")
  dw <- paste(artifact$windows$directory, collapse = "-")
  pairs <- list(
    list("Any Part B billing", fw, rec$any_partb_frame),
    list("Any Part B billing", vw, rec$any_partb_window),
    list("Part B billing in ALL SIX validation years", vw, rec$n_persistent_billers),
    list("No Part B billing anywhere", fw, rec$no_partb_frame),
    list("NPI-linked, eligible for federal linkage", "2025 roster", rec$linkage_denominator),
    list("Excluded from later active contract", "2025 roster", rec$excluded_total),
    list("  of which lack a usable NPI", "2025 roster", rec$excluded_without_npi),
    list("  sustained clinician-directory listing", dw, rec$directory_only_sustained))
  for (pr in pairs) {
    got <- g(pr[[1]], pr[[2]])
    if (is.na(got) || got != pr[[3]]) {
      stop(sprintf(paste("assert_survivor_falsification(): table and record",
                         "disagree for '%s' (%s): table %s, record %d."),
                   pr[[1]], pr[[2]], format(got), pr[[3]]), call. = FALSE)
    }
  }

  # The residual group must partition into directory-sustained, directory-
  # isolated, and neither.
  parts <- g("  sustained clinician-directory listing", dw) +
    g("  isolated clinician-directory listing", dw) +
    g("  neither Part B nor directory", "no source")
  if (parts != rec$no_partb_frame) {
    stop("assert_survivor_falsification(): the no-Part-B residual does not ",
         "partition: ", parts, " != ", rec$no_partb_frame, call. = FALSE)
  }

  # Window counts must nest: all-six <= window <= frame <= denominator.
  if (!(rec$n_persistent_billers <= rec$any_partb_window &&
        rec$any_partb_window <= rec$any_partb_frame &&
        rec$any_partb_frame <= rec$linkage_denominator)) {
    stop("assert_survivor_falsification(): window counts are not nested ",
         "(all-six <= validation <= frame <= linkage denominator).",
         call. = FALSE)
  }
  if (rec$any_partb_frame + rec$no_partb_frame != rec$linkage_denominator) {
    stop("assert_survivor_falsification(): Part B present + absent != the ",
         "linkage denominator.", call. = FALSE)
  }
  if (rec$provider_years_erased !=
      rec$n_persistent_billers * length(rec$validation_years)) {
    stop("assert_survivor_falsification(): provider-years erased is not ",
         "n_persistent_billers x validation years.", call. = FALSE)
  }

  # The annual panel must partition into the same two groups as the table.
  an <- artifact$annual
  if (!all(an$retained_observed + an$excluded_observed == an$total_observed)) {
    stop("assert_survivor_falsification(): the annual panel does not partition ",
         "into retained + excluded.", call. = FALSE)
  }
  if (any(an$persistent_observed > an$excluded_observed)) {
    stop("assert_survivor_falsification(): more persistent billers than ",
         "excluded observed physicians in a year; the persistent subgroup must ",
         "be a subset of the excluded group.", call. = FALSE)
  }

  # Tier 2 must never be described with the language reserved for billing.
  # Test on "direct", which is how tier-1 rows are labelled, rather than on the
  # word "billing" -- a first draft of this check rejected the correct string
  # "enrolment/listing, NOT billing" because it contained "billing".
  t2 <- tbl$strength[!is.na(tbl$tier) & tbl$tier == 2L]
  t1 <- tbl$strength[!is.na(tbl$tier) & tbl$tier == 1L]
  if (!all(grepl("listing|enrol", t2, ignore.case = TRUE)) ||
      any(grepl("direct", t2, ignore.case = TRUE))) {
    stop("assert_survivor_falsification(): a tier-2 (clinician-directory) row ",
         "is described with tier-1 language. Directory presence is enrolment/",
         "listing and must not be merged with Part B into one active flag.",
         call. = FALSE)
  }
  if (!all(grepl("direct", t1, ignore.case = TRUE))) {
    stop("assert_survivor_falsification(): a tier-1 (Part B) row has lost its ",
         "'direct' evidence label.", call. = FALSE)
  }
  # Tier-1 rows must all be Part B, and no directory row may claim a billing
  # window: absence from Part B is not inactivity, and a listing is not a bill.
  if (any(grepl("directory", tbl$evidence[!is.na(tbl$tier) & tbl$tier == 1L],
                ignore.case = TRUE))) {
    stop("assert_survivor_falsification(): a clinician-directory row is ",
         "labelled tier 1. Directory evidence can never be reported as Part B ",
         "billing.", call. = FALSE)
  }
  invisible(TRUE)
}
