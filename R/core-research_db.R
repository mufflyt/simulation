# Fail-closed access to a removable research database ----
#
# ABSENCE MUST NEVER SILENTLY BECOME ZERO.
#
# ENTRY_PANEL_DB_DEFAULT once held "/Volumes/MufflySamsung 1 1/DuckDB/..." -- a
# path that did not exist. DuckDB CREATES a database when asked to connect to a
# missing one, so the failure mode was not an error: it was an empty database
# and zero counts everywhere downstream. "Missing input" became "zero observed
# events", which is how a plausible but false scientific result gets made.
#
# Resolving the path with researchpaths fixed the mount problem. It did NOT fix
# the second half: returning NA and letting a canonical analysis skip its data
# source is still absence-as-absence-of-evidence. A canonical run must STOP.
#
# THE ORDER BELOW IS LOAD-BEARING:
#
#   resolve -> EXISTS? -> (optional: skip | canonical: STOP)
#           -> open READ-ONLY -> verify schema -> verify row counts -> use
#
# No connection is opened until existence has been verified, so DuckDB's
# create-if-missing behaviour can never represent scientific absence as an
# empty database.

#' Resolve a removable research database, failing closed
#'
#' @param relative_path Path beneath the volume, e.g. `"DuckDB/x.duckdb"`.
#' @param volume_pattern Glob for the volume name, e.g. `"MufflySamsung*"`.
#' @param env_var Environment variable holding an explicit override.
#' @param required `TRUE` for canonical scientific runs: absence is an ERROR.
#'   `FALSE` only for explicitly optional or exploratory paths, where absence
#'   returns `NA_character_` with a stated reason.
#' @param what Human-readable name used in messages.
#' @return Resolved path, or `NA_character_` when `required = FALSE` and absent.
#' @family research database
#' @concept core
#' @export
resolve_research_db <- function(relative_path,
                                volume_pattern,
                                env_var,
                                required = TRUE,
                                what = relative_path) {
  reason <- NULL
  path <- if (!requireNamespace("researchpaths", quietly = TRUE)) {
    reason <- "the researchpaths package is not installed"
    NA_character_
  } else {
    tryCatch(
      researchpaths::resolve_duckdb(
        relative_path  = relative_path,
        volume_pattern = volume_pattern,
        env_var        = env_var,
        quiet          = TRUE
      ),
      error = function(e) {
        reason <<- conditionMessage(e)
        NA_character_
      }
    )
  }

  # EXISTENCE IS CHECKED HERE, BEFORE ANY CONNECTION IS ATTEMPTED.
  if (!is.na(path) && !file.exists(path)) {
    reason <- sprintf("resolved to %s, which does not exist", path)
    path <- NA_character_
  }

  if (!is.na(path)) return(path)

  msg <- sprintf(
    paste0("%s is unavailable: %s. Mount the drive, or set %s to an explicit ",
           "path. NOTE: this is deliberately NOT downgraded to an empty result ",
           "-- DuckDB would create an empty database from a missing path and ",
           "every downstream count would silently become zero."),
    what, reason %||% "not found on any matching volume", env_var)

  if (isTRUE(required)) stop(msg, call. = FALSE)
  .msg_warn(paste0("SKIPPING (optional): ", msg))
  NA_character_
}

#' Open a research database read-only, with schema and row-count verification
#'
#' @details
#' Verification is ordered deliberately. Existence is confirmed before any
#' connection is opened; the schema is confirmed before any count is trusted;
#' and a required table that is EMPTY is treated as a failure rather than as a
#' zero result, because an empty table is exactly what a mis-resolved path
#' produces.
#'
#' @param path Path from [resolve_research_db()].
#' @param required_tables Character vector of tables that must be present.
#' @param min_rows Named integer vector of minimum row counts, or `NULL`.
#' @param what Human-readable name used in messages.
#' @return A list with the open `con` (caller must disconnect) and a
#'   `provenance` tibble recording what was verified.
#' @family research database
#' @concept core
#' @export
open_research_db <- function(path,
                             required_tables = character(0),
                             min_rows = NULL,
                             what = basename(path %||% "database")) {
  if (is.null(path) || is.na(path)) {
    stop(what, ": no path. resolve_research_db(required = TRUE) first.",
         call. = FALSE)
  }
  if (!file.exists(path)) {
    stop(what, ": ", path, " does not exist. Refusing to connect -- DuckDB ",
         "would CREATE an empty database here.", call. = FALSE)
  }
  info <- file.info(path)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  ok <- FALSE
  on.exit(if (!ok) try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE),
          add = TRUE)

  present <- DBI::dbListTables(con)
  missing <- setdiff(required_tables, present)
  if (length(missing) > 0L) {
    stop(what, ": required table(s) absent: ", paste(missing, collapse = ", "),
         ". Present: ", paste(utils::head(present, 20), collapse = ", "),
         call. = FALSE)
  }

  counts <- vapply(required_tables, function(tb) {
    as.numeric(DBI::dbGetQuery(
      con, sprintf("SELECT COUNT(*) AS n FROM %s", DBI::dbQuoteIdentifier(con, tb)))$n[[1]])
  }, numeric(1))

  # AN EMPTY REQUIRED TABLE IS A FAILURE, NOT A ZERO RESULT.
  empty <- names(counts)[counts == 0]
  if (length(empty) > 0L) {
    stop(what, ": required table(s) are EMPTY: ", paste(empty, collapse = ", "),
         ". This is the signature of a mis-resolved path or a truncated ",
         "extract, and must not be reported as zero observed events.",
         call. = FALSE)
  }
  if (!is.null(min_rows)) {
    short <- names(min_rows)[counts[names(min_rows)] < min_rows]
    if (length(short) > 0L) {
      stop(what, ": table(s) below the required minimum row count: ",
           paste(short, collapse = ", "), call. = FALSE)
    }
  }

  ok <- TRUE
  list(
    con = con,
    provenance = tibble::tibble(
      what                 = what,
      resolved_path        = path,
      file_size_bytes      = as.numeric(info$size),
      file_modified        = as.character(info$mtime),
      required_tables      = paste(required_tables, collapse = ","),
      row_counts           = paste(sprintf("%s=%s", names(counts), format(counts, scientific = FALSE)),
                                   collapse = ","),
      researchpaths_version = if (requireNamespace("researchpaths", quietly = TRUE)) {
        as.character(utils::packageVersion("researchpaths"))
      } else NA_character_
    )
  )
}
