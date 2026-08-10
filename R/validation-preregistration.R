# Preregistered rolling-origin evaluation -------------------------------------
#
# WHY THIS EXISTS
# The headline forecast's temporal back-test is contaminated wherever a model
# component was specified AFTER inspecting the years it is scored against (the
# entrant-regime model and the 2021-2023 miss) -- model selection on the test
# set. No re-run on the same data undoes that. The cure is procedural: freeze the
# specification, hash it, and record the freeze BEFORE the next targets are
# observed; then, at every future origin, refit only the parameters on data up to
# that origin and refuse to evaluate unless the live spec still matches the
# preregistered hash. Any post-hoc change to the model form flips the hash and the
# guard fails, so "designed after the miss" cannot recur silently.
#
# This module is the governance layer, not another model:
#   preregister_spec()            freeze + hash a spec into an immutable record
#   assert_spec_matches_prereg()  the anti-contamination guard
#   rolling_origin_evaluation()   leakage-free rolling-origin scoring, gated on
#                                 the preregistration
#
# The spec hash uses digest::sha256 (an Import) over a canonical, order-
# independent serialization, so re-ordering a spec's fields does not change its
# identity but changing any value does. The record is written as plain key:value
# text (base R, no Suggests) so it is human-readable and diffs cleanly in git --
# a preregistration a reviewer can inspect.

# Canonical, order-independent string for a spec (named lists sorted by name).
# Deterministic: same spec -> same string regardless of field order; any value
# change -> a different string. Base R, so it is unit-testable without digest.
.canonicalize_spec <- function(x) {
  if (is.list(x)) {
    if (!is.null(names(x))) x <- x[order(names(x))]
    keys <- names(x); if (is.null(keys)) keys <- as.character(seq_along(x))
    parts <- vapply(seq_along(x), function(i)
      paste0(keys[i], "=", .canonicalize_spec(x[[i]])), character(1))
    paste0("{", paste(parts, collapse = ";"), "}")
  } else {
    paste(format(unlist(x), digits = 15, trim = TRUE), collapse = ",")
  }
}

# v1 (above) collapses TYPE. Measured in adversarial cycle 08, all four of these
# hash identically under it:
#
#   list(a = list(b = 1))  and  list(a = "{b=1}")     nested vs its own rendering
#   list(a = c(1, 2))      and  list(a = "1,2")       vector vs its own rendering
#   list(a = TRUE)         and  list(a = "TRUE")
#   list(a = 1/3)          and  list(a = 0.333333333333333)
#
# That matters here more than it would elsewhere. The module's whole claim is
# that assert_spec_matches_prereg() cannot be satisfied by a spec other than the
# one frozen -- "changing the specification after preregistration is model
# selection on the held-out data". A collision is precisely a way to satisfy it
# with a different spec.
#
# v2 tags each leaf with its type and each list with its length, so a rendering
# can no longer impersonate the thing it renders.
.canonicalize_spec_v2 <- function(x) {
  if (is.list(x)) {
    if (!is.null(names(x))) x <- x[order(names(x))]
    keys <- names(x); if (is.null(keys)) keys <- as.character(seq_along(x))
    parts <- vapply(seq_along(x), function(i)
      paste0(keys[i], "=", .canonicalize_spec_v2(x[[i]])), character(1))
    return(sprintf("<list:%d>{%s}", length(x), paste(parts, collapse = ";")))
  }
  ty <- if (is.character(x)) "chr" else if (is.logical(x)) "lgl" else
        if (is.integer(x)) "int" else if (is.numeric(x)) "dbl" else class(x)[1L]
  body <- if (is.double(x)) {
    # 17 significant digits round-trips a double exactly, so two specs that
    # differ at all differ in the hash. v1 used 15 and collapsed 1/3 onto its
    # own 15-digit rendering.
    paste(vapply(x, function(e) format(e, digits = 17, trim = TRUE), character(1)),
          collapse = ",")
  } else {
    paste(format(unlist(x), digits = 17, trim = TRUE), collapse = ",")
  }
  sprintf("<%s:%d>%s", ty, length(x), body)
}

# The record declares which canonicalisation produced its hash, so the FROZEN
# v1 record in inst/extdata stays verifiable forever. A hash function that
# silently changed under a preregistration would be the same offence the module
# exists to prevent, committed by the guard itself.
PREREG_CURRENT_VERSION <- "2"

.prereg_canonicalize <- function(spec, version = PREREG_CURRENT_VERSION) {
  switch(as.character(version),
         "1" = .canonicalize_spec(spec),
         "2" = .canonicalize_spec_v2(spec),
         stop("unknown prereg_version: ", version, call. = FALSE))
}

.prereg_spec_hash <- function(spec, version = PREREG_CURRENT_VERSION) {
  digest::digest(.prereg_canonicalize(spec, version), algo = "sha256")
}

# Accept a preregistration as a path (read it) or an already-read record list.
.as_prereg <- function(prereg) {
  if (is.list(prereg)) return(prereg)
  if (is.character(prereg) && length(prereg) == 1L) return(.read_preregistration(prereg))
  stop("preregistration must be a record list or a path to one.", call. = FALSE)
}

.read_preregistration <- function(path) {
  if (!file.exists(path))
    stop("preregistration not found: ", path, call. = FALSE)
  ln <- readLines(path, warn = FALSE)
  kv <- ln[grepl("^[a-z_]+: ", ln)]
  key <- sub(": .*$", "", kv)
  val <- sub("^[a-z_]+: ", "", kv)
  stats::setNames(as.list(val), key)
}

#' Preregister (freeze + hash) a model specification
#'
#' Writes an immutable preregistration record: the spec's canonical hash, the
#' freeze date, and the canonical serialization, as plain key:value text that
#' diffs cleanly in git. Re-running with the SAME spec is idempotent; attempting
#' to register a DIFFERENT spec at an existing path errors unless `force = TRUE`,
#' because a change after the freeze is exactly the contamination this guards
#' against -- register a new protocol version instead.
#'
#' @details
#' Freezes the model specification and evaluation protocol BEFORE the held-out
#' period is scored, and records a hash of it. `frozen_at` is required and is
#' metadata only -- it does not enter `spec_hash` -- so the record states when
#' the freeze happened relative to the data vintage, which is the claim a reader
#' actually needs to evaluate.
#'
#' Re-registering a DIFFERENT spec at the same path is an error, because
#' changing the specification after seeing the test period is model selection on
#' the test set no matter how it is described afterwards. Register a new
#' protocol version at a new path instead; `force = TRUE` exists only to correct
#' a mistake made before any data was seen.
#' @param spec A named list fully describing the frozen model: form, fixed
#'   hyperparameters, predictor set, and the evaluation protocol (origins,
#'   horizon, metric). Field order does not matter; values do.
#' @param path File to write the record to (e.g. under
#'   `inst/extdata/preregistration/`).
#' @param frozen_at Freeze date/time as a string (required -- state when the spec
#'   was frozen relative to the data vintage). It is metadata and does NOT enter
#'   the spec hash.
#' @param notes Optional free-text rationale.
#' @param force Overwrite a differing prior registration. Default `FALSE`.
#' @return (invisibly) the record list, including `spec_hash`.
#' @seealso [assert_spec_matches_prereg()], [rolling_origin_evaluation()]
#' @family preregistration
#' @concept validation
#' @export
preregister_spec <- function(spec, path, frozen_at, notes = "", force = FALSE) {
  stopifnot(is.list(spec), is.character(path), length(path) == 1L)
  if (missing(frozen_at) || !nzchar(as.character(frozen_at)))
    stop("preregister_spec(): `frozen_at` is required -- state when the spec was ",
         "frozen relative to the data vintage.", call. = FALSE)
  hash <- .prereg_spec_hash(spec)
  if (file.exists(path) && !isTRUE(force)) {
    prev <- .read_preregistration(path)
    # Compare under the EXISTING record's version, not the current one, or
    # re-registering an unchanged v1 spec would look like a spec change.
    hash <- .prereg_spec_hash(spec, prev$prereg_version %||% "1")
    if (!identical(prev$spec_hash, hash))
      stop("preregister_spec(): a DIFFERENT spec is already registered at ", path,
           " (", substr(prev$spec_hash, 1, 12), " != ", substr(hash, 1, 12), "). ",
           "Changing the spec after the freeze is model selection on the test set; ",
           "register a new protocol version (new path) instead, or pass force = TRUE ",
           "only to correct a pre-data mistake.", call. = FALSE)
  }
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  rec <- list(prereg_version = PREREG_CURRENT_VERSION, spec_hash = hash,
              frozen_at = as.character(frozen_at),
              notes = gsub("[\r\n]+", " ", as.character(notes)),
              canonical_spec = .prereg_canonicalize(spec))
  writeLines(c("# URPS preregistration record (immutable once data are observed)",
               paste0(names(rec), ": ", unlist(rec))), path)
  invisible(rec)
}

#' Assert a live spec matches its preregistration (anti-contamination guard)
#'
#' Recomputes the hash of `spec` and compares it to the preregistered hash. Errors
#' if they differ -- the point being that a model form re-tuned after seeing the
#' held-out targets no longer matches what was frozen, and that is model selection
#' on the test set.
#'
#' @param spec The live spec (named list) about to be evaluated.
#' @param prereg A preregistration record list (from [preregister_spec()]) or a
#'   path to one.
#' @return (invisibly) `TRUE` on a match.
#' @family preregistration
#' @concept validation
#' @export
assert_spec_matches_prereg <- function(spec, prereg) {
  pr <- .as_prereg(prereg)
  ver <- pr$prereg_version %||% "1"
  if (identical(as.character(ver), "1")) {
    .msg_info(paste("Preregistration is canonicalisation v1, which collapses type:",
                    "list(a = 1) and list(a = \"1\") hash the same. Verified under v1",
                    "because that is what was frozen; new records use v2."))
  }
  h <- .prereg_spec_hash(spec, ver)
  if (!identical(h, pr$spec_hash))
    stop("spec does not match the preregistration (", substr(h, 1, 12), " != ",
         substr(pr$spec_hash, 1, 12), "). Changing the specification after ",
         "preregistration is model selection on the held-out data; do not re-tune ",
         "to the target years.", call. = FALSE)
  invisible(TRUE)
}

#' Leakage-free rolling-origin evaluation, gated on a preregistration
#'
#' For each origin, refits with `fit_predict` on data up to and including that
#' origin and predicts `horizon` steps ahead, then scores the prediction against
#' the observed target. Leakage-free by construction: an origin's fit sees only
#' rows at or before it, and the target is strictly in its future. When a
#' preregistration is supplied, the run is refused unless the live `spec` matches
#' it -- so a clean rolling-origin number cannot be produced from a spec that was
#' altered after the targets were seen.
#'
#' @param data Data frame with a time column and a target column (e.g. an annual
#'   national workforce series).
#' @param time_col,target_col Column names for the time index and the observed
#'   target.
#' @param origins Integer vector of origin times to evaluate.
#' @param horizon Steps ahead to forecast. Default 1.
#' @param fit_predict `function(train, target_time)` returning a prediction for
#'   the target time's row(s); must fit only on `train` and never see the target.
#' @param prereg Optional preregistration record or path; when given, `spec` is
#'   required and verified before any evaluation.
#' @param spec The live spec, required when `prereg` is given.
#' @return A list: `by_origin` (`origin`, `target_time`, `observed`, `predicted`,
#'   `signed_error`, `abs_pct_error`) and `summary` (`n`, `mape`, `rmse`,
#'   `horizon`, `all_targets_future`, `preregistered`, `spec_hash`).
#' @seealso [preregister_spec()], [assert_spec_matches_prereg()]
#' @family preregistration
#' @concept validation
#' @export
rolling_origin_evaluation <- function(data, time_col, target_col, origins,
                                      horizon = 1L, fit_predict,
                                      prereg = NULL, spec = NULL) {
  stopifnot(is.data.frame(data), time_col %in% names(data), target_col %in% names(data),
            is.function(fit_predict), length(origins) >= 1L, horizon >= 1L)
  if (!is.null(prereg)) {
    if (is.null(spec))
      stop("rolling_origin_evaluation(): `spec` is required when `prereg` is given, ",
           "so it can be verified against the preregistration.", call. = FALSE)
    assert_spec_matches_prereg(spec, prereg)
  }
  tvec <- data[[time_col]]
  rows <- list()
  for (o in origins) {
    tt <- o + horizon
    train <- data[tvec <= o, , drop = FALSE]
    test  <- data[tvec == tt, , drop = FALSE]
    if (!nrow(train) || !nrow(test)) next
    yhat <- fit_predict(train, tt)
    if (length(yhat) != nrow(test))
      stop("rolling_origin_evaluation(): fit_predict returned ", length(yhat),
           " values for a ", nrow(test), "-row target at time ", tt, ".", call. = FALSE)
    rows[[length(rows) + 1L]] <- data.frame(
      origin = o, target_time = tt, observed = as.numeric(test[[target_col]]),
      predicted = as.numeric(yhat), stringsAsFactors = FALSE)
  }
  if (!length(rows))
    stop("rolling_origin_evaluation(): no (origin, target) pair had both training ",
         "and observed data.", call. = FALSE)
  by_origin <- do.call(rbind, rows)
  by_origin$signed_error <- by_origin$predicted - by_origin$observed
  by_origin$abs_pct_error <- ifelse(by_origin$observed != 0,
    100 * abs(by_origin$signed_error) / abs(by_origin$observed), NA_real_)
  pr_hash <- if (!is.null(prereg)) .as_prereg(prereg)$spec_hash else NA_character_
  summary <- data.frame(
    n = nrow(by_origin),
    mape = mean(by_origin$abs_pct_error, na.rm = TRUE),
    rmse = sqrt(mean(by_origin$signed_error^2)),
    horizon = horizon,
    all_targets_future = all(by_origin$target_time > by_origin$origin),   # leakage guard
    preregistered = !is.null(prereg),
    spec_hash = pr_hash, stringsAsFactors = FALSE)
  list(by_origin = by_origin, summary = summary)
}
