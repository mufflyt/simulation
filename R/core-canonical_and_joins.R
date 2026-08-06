# Canonical Source Resolver & Join-Safety ----
#
# Ported from `isochrones` Non-Negotiables #6 and #19. Two correctness tools:
#
#   1. resolve_canonical(name): every model input has ONE named, checksummed
#      source in config/canonical_sources.yml, reached through ONE resolver.
#      A wrong-source / wrong-grain error is SILENT (a confident wrong number),
#      so the resolver is fail-closed: unknown name or missing file stop(); a
#      recorded-but-mismatched SHA-256 stops in strict mode, warns in relaxed.
#   2. safe_left_join() / safe_inner_join(): joins that refuse to silently drop
#      rows or fan out. A fan-out on a rate/denominator join inflates every
#      downstream workforce statistic; these wrappers make that fail loudly.
#
# Dependencies: yaml, digest, dplyr, logger.

# ---- Canonical source resolver --------------------------------------------

.canonical_config_path <- function(start = getwd()) {
  # Walk up from `start` looking for config/canonical_sources.yml, so the
  # resolver works from any working directory (repo root, scripts/, or the
  # tests/testthat/ dir that testthat sets during checks).
  dir <- normalizePath(start, mustWork = FALSE)
  repeat {
    candidate <- file.path(dir, "config", "canonical_sources.yml")
    if (file.exists(candidate)) return(candidate)
    parent <- dirname(dir)
    if (identical(parent, dir)) break   # reached filesystem root
    dir <- parent
  }
  # Fallback: the conventional repo-root location (may not exist yet).
  file.path(normalizePath(start, mustWork = FALSE), "config", "canonical_sources.yml")
}

#' Resolve a canonical input path by logical name (fail-closed)
#'
#' Looks `name` up in `config/canonical_sources.yml`. The entry may be a bare
#' path string or a mapping with `path` and optional `sha256`. Unknown names and
#' missing files always `stop()`. A recorded SHA-256 that does not match the
#' file's actual digest `stop()`s in strict mode and warns in relaxed mode.
#'
#' @param name Logical name of the source (a key under `sources:` in the YAML).
#' @param config_path Path to the canonical-sources YAML.
#' @param mode Reproducibility mode ([resolve_reproducibility_mode()]).
#' @return Absolute path to the verified canonical file.
#' @export
resolve_canonical <- function(name,
                              config_path = .canonical_config_path(),
                              mode = resolve_reproducibility_mode()) {
  if (!file.exists(config_path)) {
    stop(sprintf("Canonical source registry not found: %s", config_path), call. = FALSE)
  }
  registry <- yaml::read_yaml(config_path)
  sources <- registry$sources %||% registry

  if (is.null(sources[[name]])) {
    stop(sprintf(
      "Unknown canonical source '%s'. Known: %s",
      name, paste(names(sources), collapse = ", ")
    ), call. = FALSE)
  }

  entry <- sources[[name]]
  path <- if (is.list(entry)) entry$path else entry
  recorded_sha <- if (is.list(entry)) entry$sha256 else NULL

  # Resolve relative paths against the registry's directory.
  if (!isTRUE(startsWith(path, "/"))) {
    path <- file.path(dirname(config_path), "..", path)
    path <- normalizePath(path, mustWork = FALSE)
  }

  if (!file.exists(path)) {
    stop(sprintf("Canonical source '%s' resolves to a missing file: %s", name, path),
         call. = FALSE)
  }

  if (!is.null(recorded_sha) && nzchar(recorded_sha)) {
    actual_sha <- digest::digest(file = path, algo = "sha256")
    if (!identical(actual_sha, recorded_sha)) {
      msg <- sprintf(
        "Canonical source '%s' SHA-256 drift: recorded %s, actual %s",
        name, substr(recorded_sha, 1, 12), substr(actual_sha, 1, 12)
      )
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }

  .msg_debug(sprintf("Resolved canonical source '%s' -> %s", name, path))
  path
}

# `%||%` is exported by rlang and, since R 4.4.0, by base. Defining our own would
# shadow whichever is attached and quietly change behaviour depending on load
# order, so we re-use the existing one and only define a fallback for older R.
if (!exists("%||%", envir = baseenv())) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
}

# ---- Join safety ----------------------------------------------------------

.assert_join_keys_present <- function(x, y, by) {
  keys <- if (is.null(names(by))) by else names(by)
  y_keys <- if (is.null(names(by))) by else unname(by)
  missing_x <- setdiff(keys, names(x))
  missing_y <- setdiff(y_keys, names(y))
  if (length(missing_x) > 0) {
    stop(sprintf("safe join: left table missing key(s): %s",
                 paste(missing_x, collapse = ", ")), call. = FALSE)
  }
  if (length(missing_y) > 0) {
    stop(sprintf("safe join: right table missing key(s): %s",
                 paste(missing_y, collapse = ", ")), call. = FALSE)
  }
}

#' Left join that refuses silent row loss / fan-out
#'
#' A left join should preserve the left table's row count exactly. This wrapper
#' asserts keys are present, checks the right table has unique keys unless
#' `allow_fanout = TRUE`, performs the join, and fails if the result row count
#' differs from the left input (which can only happen via fan-out).
#'
#' @param x,y Data frames.
#' @param by Join specification (as in [dplyr::left_join()]).
#' @param allow_fanout If FALSE (default), a non-unique key in `y` is an error.
#' @param min_match_rate Minimum share of left rows that must find a match; a
#'   lower coverage warns (strict: errors). NULL disables the check.
#' @param mode Reproducibility mode governing warn-vs-stop on low coverage.
#' @return The joined data frame.
#' @export
safe_left_join <- function(x, y, by, allow_fanout = FALSE,
                           min_match_rate = NULL,
                           mode = resolve_reproducibility_mode()) {
  .assert_join_keys_present(x, y, by)
  n_left <- nrow(x)

  if (!allow_fanout) {
    y_keys <- if (is.null(names(by))) by else unname(by)
    n_dup <- sum(duplicated(as.data.frame(y)[, y_keys, drop = FALSE]))
    if (n_dup > 0) {
      stop(sprintf(
        "safe_left_join: right table has %d duplicated key row(s); would fan out. Set allow_fanout=TRUE if intended.",
        n_dup
      ), call. = FALSE)
    }
  }

  result <- dplyr::left_join(x, y, by = by)

  if (!allow_fanout && nrow(result) != n_left) {
    stop(sprintf("safe_left_join: row count changed %d -> %d (unexpected fan-out)",
                 n_left, nrow(result)), call. = FALSE)
  }

  if (!is.null(min_match_rate)) {
    # Measure whether each left row found a MATCHING KEY in `y`, not whether one
    # joined column happens to be non-NA. Probing `new_cols[1]` (the previous
    # behaviour) passed a 100% gate whenever that first column was populated even
    # if every other joined column was entirely NA, and reported a false miss
    # whenever the right table legitimately carried NA there. Keying directly is
    # both stricter and correct, and it works when `y` carries only key columns.
    x_keys <- if (is.null(names(by))) by else names(by)
    y_keys <- if (is.null(names(by))) by else unname(by)
    key_of <- function(d, cols) {
      do.call(paste, c(lapply(as.data.frame(d)[, cols, drop = FALSE], as.character),
                       sep = "\r"))
    }
    match_rate <- if (n_left > 0) {
      mean(key_of(x, x_keys) %in% key_of(y, y_keys))
    } else 1
    if (match_rate < min_match_rate) {
      msg <- sprintf("safe_left_join: match rate %.1f%% below required %.1f%%",
                     100 * match_rate, 100 * min_match_rate)
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    }
  }

  result
}

#' Inner join with an explicit key-cardinality guard
#'
#' @inheritParams safe_left_join
#' @return The joined data frame.
#' @export
safe_inner_join <- function(x, y, by, allow_fanout = FALSE) {
  .assert_join_keys_present(x, y, by)
  if (!allow_fanout) {
    y_keys <- if (is.null(names(by))) by else unname(by)
    n_dup <- sum(duplicated(as.data.frame(y)[, y_keys, drop = FALSE]))
    if (n_dup > 0) {
      stop(sprintf("safe_inner_join: right table has %d duplicated key row(s); would fan out.",
                   n_dup), call. = FALSE)
    }
  }
  dplyr::inner_join(x, y, by = by)
}
