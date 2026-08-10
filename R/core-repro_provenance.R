# Reproducibility & Provenance Scaffolding ----
#
# Ported into the urogynecology workforce microsimulation from the `isochrones`
# pipeline (Non-Negotiables #8, #18, #19). Two ideas travel across:
#
#   1. Reproducibility mode (strict / relaxed) with a single deterministic seed
#      so every stochastic microsimulation run is bit-reproducible in `strict`
#      mode and freely explorable in `relaxed` mode.
#   2. A cache/artifact *provenance contract*: every derived artifact is written
#      atomically alongside a provenance sidecar (input fingerprint, code
#      fingerprint, content SHA-256, tool versions, run metadata) and is
#      *fail-closed on load* — a cache that is merely present is never trusted.
#
# These utilities have no hard dependency on the rest of the model; they are the
# substrate the microsimulation, demand, and access modules build on.
#
# Dependencies: digest, jsonlite (both light-weight).

# ---- Logging --------------------------------------------------------------
#
# Progress and diagnostics go through base::message() (stderr), not a logging
# package. Messages are suppressible with suppressMessages(), capturable, and add
# no dependency. `.msg_debug()` is silent unless MICROSIM_VERBOSE is truthy.

.msg_info <- function(...) message(...)

.msg_warn <- function(...) message("WARNING: ", ...)

.msg_debug <- function(...) {
  v <- tolower(trimws(Sys.getenv("MICROSIM_VERBOSE", unset = "")))
  if (v %in% c("1", "true", "yes", "on")) message("DEBUG: ", ...)
  invisible(NULL)
}

# ---- Reproducibility mode -------------------------------------------------

#' Resolve the active reproducibility mode
#'
#' Mirrors the isochrones `REPRODUCIBILITY_MODE` contract: `strict` for
#' manuscript / production runs (fully seeded, fail-closed), `relaxed` for
#' interactive development.
#'
#' @details
#' Reads the `REPRODUCIBILITY_MODE` environment variable, falling back to
#' `default`. An unrecognised value warns and falls back rather than failing, so
#' a typo degrades to the permissive mode rather than stopping a run.
#'
#' The two modes differ in what they do about an input that cannot support the
#' claim being made of it. In `relaxed` an uncalibrated workload basket, an
#' analogy-derived baseline gap, or a Monte Carlo run in which no parameter
#' varies each emit a warning and proceed. In `strict` each is an error. Use
#' `strict` for anything whose numbers will be published.
#' @param default Character fallback when the environment variable is unset.
#' @return Character scalar, one of "strict" or "relaxed".
#' @family repro provenance
#' @concept core
#' @examples
#' # 'strict' makes uncalibrated inputs and fixed-parameter intervals refuse to
#' # run rather than warn. Set REPRODUCIBILITY_MODE=strict for a publication run.
#' resolve_reproducibility_mode()
#' @export
resolve_reproducibility_mode <- function(default = "relaxed") {
  mode <- Sys.getenv("REPRODUCIBILITY_MODE", unset = default)
  mode <- tolower(trimws(mode))

  if (!mode %in% c("strict", "relaxed")) {
    .msg_warn(sprintf(
      "Unknown REPRODUCIBILITY_MODE '%s'; falling back to '%s'", mode, default
    ))
    mode <- default
  }
  mode
}

#' Seed the RNG for a reproducible microsimulation run
#'
#' In `strict` mode we always seed (default 20260801, the study reference date)
#' so Monte-Carlo trajectories are bit-identical across machines. In `relaxed`
#' mode we honour an explicit seed if given but otherwise leave the RNG alone.
#'
#' @details
#' In `strict` mode the RNG is always seeded, so Monte Carlo trajectories are
#' bit-identical across machines and a reported figure can be reproduced exactly.
#' In `relaxed` mode an explicit seed is honoured but none is imposed, which
#' keeps repeated exploratory runs from looking more stable than they are.
#'
#' Returns the seed actually applied, so a run can record it rather than leaving
#' reproducibility to a claim in a method section.
#' @param seed Integer seed. If NULL, uses `MICROSIM_SEED` env var or 20260801.
#' @param mode Reproducibility mode (see [resolve_reproducibility_mode()]).
#' @return (Invisibly) the integer seed actually applied, or NA if none.
#' @family repro provenance
#' @concept core
#' @export
seed_microsimulation <- function(seed = NULL, mode = resolve_reproducibility_mode()) {
  if (is.null(seed)) {
    env_seed <- Sys.getenv("MICROSIM_SEED", unset = "")
    if (nzchar(env_seed)) {
      # A malformed MICROSIM_SEED used to coerce to NA with the warning
      # suppressed, and the two modes then diverged in opposite unhelpful
      # directions: strict silently substituted the DEFAULT seed (so the run was
      # reproducible, but not the run that was asked for), and relaxed left the
      # RNG entirely UNSEEDED (so the run was not reproducible at all). Either
      # way the caller set a seed and did not get it, with no diagnostic.
      # Parsed as a DOUBLE first and checked for integrality, because
      # as.integer() truncates: as.integer("3.7") is 3, silently, which is the
      # same substitution this guard exists to stop. (as.integer also accepts
      # "0x1F" and "1e3", which are unambiguous integers and are fine.)
      num <- suppressWarnings(as.numeric(env_seed))
      if (is.na(num) || !is.finite(num) || num != trunc(num) ||
          abs(num) > .Machine$integer.max)
        stop(sprintf(paste("seed_microsimulation: MICROSIM_SEED is set to '%s',",
                           "which is not a whole number in integer range. Refusing",
                           "to substitute a different seed: the run would be",
                           "labelled reproducible under a seed nobody chose.",
                           "Unset it or give an integer."),
                     env_seed), call. = FALSE)
      seed <- as.integer(num)
    } else {
      seed <- 20260801L
    }
  }

  if (mode == "relaxed" && is.na(seed)) {
    .msg_info("Reproducibility mode 'relaxed' with no seed: RNG left unseeded")
    return(invisible(NA_integer_))
  }

  if (is.na(seed)) seed <- 20260801L
  # RNGkind pinned so the stream is reproducible across R versions >= 3.6.
  suppressWarnings(RNGkind("Mersenne-Twister", "Inversion", "Rejection"))
  set.seed(seed)
  .msg_debug(sprintf("Seeded microsimulation RNG with %d (mode = %s)", seed, mode))
  invisible(seed)
}

#' Generate a stable run identifier
#'
#' Deterministic in `strict` mode (derived from the seed + a supplied tag) so
#' the same inputs produce the same `run_id`; time-stamped in `relaxed` mode.
#'
#' @param tag Short human-readable label for the run.
#' @param seed Seed used for the run (see [seed_microsimulation()]).
#' @param mode Reproducibility mode.
#' @return Character run identifier.
#' @keywords internal
make_run_id <- function(tag = "microsim", seed = 20260801L,
                        mode = resolve_reproducibility_mode()) {
  if (mode == "strict") {
    fingerprint <- substr(digest::digest(list(tag, seed), algo = "sha256"), 1, 12)
    sprintf("%s_strict_%s", tag, fingerprint)
  } else {
    stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    sprintf("%s_%s", tag, stamp)
  }
}

# ---- Fingerprinting -------------------------------------------------------

#' SHA-256 fingerprint of an in-memory R object
#'
#' @param x Any R object.
#' @return 64-character hex digest.
#' @keywords internal
fingerprint_object <- function(x) {
  digest::digest(x, algo = "sha256")
}

#' SHA-256 fingerprint of the producing source file(s)
#'
#' Lets an artifact record which code produced it (the isochrones
#' `code_fingerprint` field), so a cache is rejected when the generating logic
#' changes even if the inputs did not.
#'
#' @param paths Character vector of file paths.
#' @return 64-character hex digest over the concatenated file contents, or NA if
#'   no path exists.
#' @keywords internal
fingerprint_files <- function(paths) {
  existing <- paths[file.exists(paths)]
  if (length(existing) == 0) {
    return(NA_character_)
  }
  digests <- vapply(
    sort(existing),
    function(p) digest::digest(file = p, algo = "sha256"),
    character(1)
  )
  digest::digest(paste(digests, collapse = ""), algo = "sha256")
}

# ---- Provenance-guarded artifact I/O --------------------------------------

.provenance_sidecar_path <- function(artifact_path) {
  paste0(artifact_path, ".provenance.json")
}

#' Write a derived artifact with a provenance sidecar (atomic, fail-closed)
#'
#' Implements the isochrones Cache Provenance Contract (Non-Negotiable #18):
#'   * tmp-write then atomic rename of the payload,
#'   * a JSON sidecar written *last* recording the input fingerprint, code
#'     fingerprint, content SHA-256 of the payload, tool versions, a composite
#'     key, and run provenance (run_id, run_mode, created_at, source, count).
#'
#' The sidecar is written after (and separately from) the payload so a crash
#' mid-write leaves no sidecar -> the artifact is treated as absent on load.
#'
#' @param object R object to persist (saved with [saveRDS()]).
#' @param artifact_path Destination `.rds` path.
#' @param inputs Object(s) whose fingerprint gates staleness (list or object).
#' @param code_paths Source file(s) that produced the artifact.
#' @param run_id Run identifier ([make_run_id()]).
#' @param mode Reproducibility mode recorded in provenance.
#' @param source Free-text description of the data source.
#' @param extra Named list of additional provenance fields (e.g. tool versions).
#' @return (Invisibly) the provenance list that was written.
#' @family repro provenance
#' @concept core
#' @export
write_artifact_with_provenance <- function(object,
                                           artifact_path,
                                           inputs = NULL,
                                           code_paths = character(0),
                                           run_id = make_run_id(),
                                           mode = resolve_reproducibility_mode(),
                                           source = NA_character_,
                                           extra = list()) {
  dir.create(dirname(artifact_path), recursive = TRUE, showWarnings = FALSE)

  # 1. Atomic payload write: tmp -> rename.
  tmp_path <- paste0(artifact_path, ".tmp-", Sys.getpid())
  saveRDS(object, tmp_path)
  file.rename(tmp_path, artifact_path)

  # 2. Fingerprints computed AFTER the payload is in place.
  content_sha <- digest::digest(file = artifact_path, algo = "sha256")
  input_fp <- if (is.null(inputs)) NA_character_ else fingerprint_object(inputs)
  code_fp <- fingerprint_files(code_paths)

  n_rows <- tryCatch(
    if (is.data.frame(object)) nrow(object) else length(object),
    error = function(e) NA_integer_
  )

  tool_versions <- utils::modifyList(
    list(
      R = as.character(getRversion()),
      digest = as.character(utils::packageVersion("digest"))
    ),
    extra
  )

  provenance <- list(
    schema_version = 1L,
    artifact = basename(artifact_path),
    input_fingerprint = input_fp,
    code_fingerprint = code_fp,
    content_sha256 = content_sha,
    composite_key = digest::digest(
      list(input_fp, code_fp, content_sha), algo = "sha256"
    ),
    tool_versions = tool_versions,
    provenance = list(
      run_id = run_id,
      run_mode = mode,
      created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
      source = source,
      count = n_rows
    )
  )

  # 3. Sidecar written LAST (also atomically).
  sidecar <- .provenance_sidecar_path(artifact_path)
  tmp_sidecar <- paste0(sidecar, ".tmp-", Sys.getpid())
  jsonlite::write_json(provenance, tmp_sidecar, auto_unbox = TRUE, pretty = TRUE)
  file.rename(tmp_sidecar, sidecar)

  .msg_info(sprintf("Wrote artifact %s (%s records) + provenance sidecar",
                    basename(artifact_path), format(n_rows)))
  invisible(provenance)
}

#' Read a provenance-guarded artifact, fail-closed
#'
#' Recomputes the content SHA-256 and compares it to the recorded value *before*
#' deserialising — the only guard that catches a swapped-but-valid payload. On
#' any mismatch/missing/corrupt condition the artifact is rejected (returns NULL
#' in relaxed mode, `stop()`s in strict mode) so the caller recomputes rather
#' than trusting stale or tampered data.
#'
#' @details
#' The hash is recomputed and compared BEFORE deserialising, which is the only
#' ordering that catches a payload that is valid R but not the payload the
#' sidecar describes. Checking after loading would already have trusted it.
#'
#' Rejection is not an error condition to be worked around -- it means recompute.
#' In `relaxed` mode the function returns NULL so the caller regenerates the
#' artifact; in `strict` it stops. `expected_inputs` extends the same idea
#' upstream: an artifact whose recorded input fingerprint does not match the
#' inputs you hold is stale, even if its own content hash is intact.
#' @param artifact_path Path to the `.rds` artifact.
#' @param expected_inputs If supplied, the loaded artifact is rejected unless its
#'   recorded `input_fingerprint` matches `fingerprint_object(expected_inputs)`.
#' @param mode Reproducibility mode; governs reject behaviour (stop vs NULL).
#' @return The deserialised object, or NULL when rejected in relaxed mode.
#' @family repro provenance
#' @concept core
#' @export
read_artifact_with_provenance <- function(artifact_path,
                                          expected_inputs = NULL,
                                          mode = resolve_reproducibility_mode()) {
  reject <- function(reason) {
    msg <- sprintf("Provenance check failed for %s: %s", basename(artifact_path), reason)
    if (identical(mode, "strict")) {
      stop(msg, call. = FALSE)
    }
    .msg_warn(msg, " -> recompute")
    return(NULL)
  }

  sidecar <- .provenance_sidecar_path(artifact_path)
  if (!file.exists(artifact_path)) return(reject("payload absent"))
  if (!file.exists(sidecar)) return(reject("provenance sidecar absent"))

  provenance <- tryCatch(
    jsonlite::read_json(sidecar, simplifyVector = TRUE),
    error = function(e) NULL
  )
  if (is.null(provenance)) return(reject("provenance sidecar corrupt"))

  # Content integrity BEFORE deserialisation.
  actual_sha <- digest::digest(file = artifact_path, algo = "sha256")
  if (!identical(actual_sha, provenance$content_sha256)) {
    return(reject("content SHA-256 mismatch (payload changed under the sidecar)"))
  }

  # Optional input-staleness check.
  if (!is.null(expected_inputs)) {
    expected_fp <- fingerprint_object(expected_inputs)
    if (!identical(expected_fp, provenance$input_fingerprint)) {
      return(reject("input fingerprint drift (cohort/inputs changed)"))
    }
  }

  .msg_debug(sprintf("Loaded artifact %s (run_id = %s)",
                     basename(artifact_path), provenance$provenance$run_id))
  readRDS(artifact_path)
}
