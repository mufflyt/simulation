# Manuscript-eligibility gate for evidence artifacts ----
#
# Sourced by scripts/manuscript/build_tables.R and exercised directly by
# tests/testthat/test-manuscript-eligibility.R. It lives in its own file for
# that second reason: a gate whose only caller is a script that writes files as
# a side effect cannot be tested without writing those files, and an untested
# gate is a gate that will be wrong in one direction or the other.
#
# THE RULE. A manuscript table may not be built from a source that is
# exploratory, a fallback, failed, incomplete, or otherwise non-citable.
#
# Requires scripts/validation/_provenance.R to have been sourced, for
# read_validation_run().

# Matched case-insensitively as substrings against every character cell of a
# source table, against its manifest VALUES, and against its own path.
#
# NOTE ON "failed" vs "FAIL". The Monte-Carlo convergence table reports a
# verdict of FAIL at n = 250 and n = 500. That is the finding -- the whole
# point of the convergence analysis -- not a statement that the source failed.
# The token below is `failed`, which does not match `FAIL`, and the asymmetry
# is intentional rather than lucky: a status tag describes the artifact, a
# verdict describes the world. Do not "fix" this by broadening the token; the
# test suite pins the distinction.
DISQUALIFYING <- c(
  "exploratory", "fallback", "failed", "incomplete", "placeholder",
  "illustrative", "uncalibrated", "provisional",
  "not citable", "non-citable", "noncitable", "not_citable")

scan_disqualifying <- function(text, where) {
  hits <- character()
  for (tok in DISQUALIFYING) {
    bad <- grep(tok, text, ignore.case = TRUE, value = TRUE)
    if (length(bad))
      hits <- c(hits, sprintf("%s: matched %s in %s", where, sQuote(tok),
                              sQuote(trimws(substr(unique(bad)[1], 1, 90)))))
  }
  hits
}

# A MANIFEST IS KEY/VALUE, NOT PROSE, and scanning it as prose was wrong in a
# way worth recording. The first version of this gate free-text scanned
# MANIFEST.txt and refused all five run directories, because every manifest
# carries the line `exploratory   FALSE` -- it matched its own field NAME and
# read a declaration of eligibility as evidence of ineligibility. The fix is
# not a narrower regex. A structured field must be read as structure: parse the
# manifest, assert `exploratory` is present and FALSE, and scan only the values.
parse_manifest <- function(lines) {
  kv <- regmatches(lines, regexec("^([A-Za-z_0-9]+)\\s{2,}(.*)$", lines))
  kv <- Filter(function(m) length(m) == 3L, kv)
  stats::setNames(vapply(kv, `[`, character(1), 3L), vapply(kv, `[`, character(1), 2L))
}

manuscript_sha256 <- function(f) tryCatch(digest::digest(file = f, algo = "sha256"),
                                          error = function(e) NA_character_)

# ---- Two kinds of source, two gates ----------------------------------------
#
# A validation RUN DIRECTORY is already gated by read_validation_run(), which
# refuses a missing COMPLETED marker, a FAILED marker, an EXPLORATORY marker,
# and a manifest recording a dirty model tree. That function is reused rather
# than reimplemented: a second copy of an eligibility rule is a second place
# for it to drift.
gate_run <- function(run_dir) {
  problems <- character()
  tabs <- tryCatch(read_validation_run(run_dir, allow_exploratory = FALSE),
                   error = function(e) { problems <<- conditionMessage(e); NULL })
  if (is.null(tabs)) return(list(ok = FALSE, problems = problems))

  problems <- c(problems, scan_disqualifying(basename(run_dir), "path"))

  man <- parse_manifest(attr(tabs, "manifest"))
  if (!"exploratory" %in% names(man)) {
    problems <- c(problems, "manifest declares no `exploratory` field")
  } else if (!identical(toupper(trimws(man[["exploratory"]])), "FALSE")) {
    problems <- c(problems, sprintf("manifest declares exploratory = %s",
                                    man[["exploratory"]]))
  }
  problems <- c(problems, scan_disqualifying(unname(man), "manifest value"))

  # COMPLETED carries the run's FINAL status; MANIFEST.txt is written before
  # computation and always says `started`, so checking only the manifest would
  # read the intention rather than the outcome.
  completed <- file.path(run_dir, "COMPLETED")
  if (file.exists(completed))
    problems <- c(problems, scan_disqualifying(readLines(completed, warn = FALSE), "COMPLETED"))

  for (nm in names(tabs)) {
    ch <- unlist(lapply(tabs[[nm]], function(col) if (is.character(col)) col else character()))
    problems <- c(problems, scan_disqualifying(ch, sprintf("%s.csv", nm)))
  }
  files <- list.files(run_dir, "[.]csv$", full.names = TRUE)
  list(ok = length(problems) == 0, problems = problems, tables = tabs,
       run_id = attr(tabs, "run_id"),
       hashes = stats::setNames(vapply(files, manuscript_sha256, character(1)),
                                basename(files)))
}

# A PINNED FLAT ARTIFACT is a CSV outside the run-directory scheme that carries
# its own sidecar manifest. artifacts/backtest_2020_to_2023_summary.csv is the
# case: it predates begin_validation_run() and is the principal specification
# grid, so it cannot simply be excluded. The gate demands the sidecar exist and
# declare the fields that make it a pinned run rather than a scratch file --
# a CSV with no manifest is not evidence, whatever it contains.
PINNED_REQUIRED_KEYS <- c("generated_by", "cutoff_year", "target_year",
                          "n_iterations", "seed", "target_value")

gate_pinned <- function(csv, manifest_json) {
  problems <- character()
  if (!file.exists(csv)) return(list(ok = FALSE, problems = paste("missing:", csv)))
  if (!file.exists(manifest_json))
    return(list(ok = FALSE, problems = paste(
      "no sidecar manifest for", csv,
      "-- a flat CSV without one is not citable evidence, whatever it contains")))

  man <- tryCatch(jsonlite::fromJSON(manifest_json), error = function(e) NULL)
  if (is.null(man)) return(list(ok = FALSE, problems = paste("unparseable manifest:", manifest_json)))
  missing_keys <- setdiff(PINNED_REQUIRED_KEYS, names(man))
  if (length(missing_keys))
    problems <- c(problems, sprintf("manifest %s omits required field(s): %s",
                                    basename(manifest_json), paste(missing_keys, collapse = ", ")))

  d <- utils::read.csv(csv, check.names = FALSE)
  ch <- unlist(lapply(d, function(col) if (is.character(col)) col else character()))
  problems <- c(problems, scan_disqualifying(ch, basename(csv)))
  problems <- c(problems, scan_disqualifying(
    readLines(manifest_json, warn = FALSE), basename(manifest_json)))

  list(ok = length(problems) == 0, problems = problems,
       tables = list(summary = d),
       run_id = sprintf("pinned:%s seed=%s n=%s", man$generated_by, man$seed, man$n_iterations),
       hashes = stats::setNames(c(manuscript_sha256(csv), manuscript_sha256(manifest_json)),
                                c(basename(csv), basename(manifest_json))))
}
