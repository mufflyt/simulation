# Retirement artifacts: immutable identity and a four-dimensional confidence -
#
# TWO DEFECTS, ONE CAUSE: a scientific result that cannot say which inputs and
# which code produced it.
#
# 1. `CREATE OR REPLACE TABLE` on a consensus artifact. Rebuilding silently
#    overwrites the numbers a manuscript already cites, so "the retirement
#    panel" names whatever was built last rather than a specific object. Two
#    people reading the same table name are not necessarily reading the same
#    data, and neither can tell.
#
# 2. A single `final_confidence`. Collapsing four independent questions --
#    is this the right person, did the event happen, is the date right, was
#    the provider practising -- into one number makes them unrecoverable.
#    Downstream code then reads whichever meaning it happens to need, and the
#    identity gate in R/supply-retirement_contract.R cannot be enforced at
#    all, because the identity component no longer exists separately.
#
# The migration hazard is worse than the original defect: copying one legacy
# value into all four new columns satisfies a schema check while asserting
# something false four times. validate_retirement_evidence_v2() refuses that
# explicitly rather than trusting the column names.

#' The four independent confidence dimensions a retirement signal must carry
#'
#' @description
#' These answer different questions and cannot substitute for one another:
#'
#' \describe{
#'   \item{`identity_confidence`}{Is this record about this physician? Gates
#'     everything else -- see [adjudicate_terminal_events()].}
#'   \item{`event_confidence`}{Did the event occur at all?}
#'   \item{`timing_confidence`}{Is the event's year right? A signal can be
#'     certainly true and badly dated.}
#'   \item{`activity_confidence`}{Was the provider practising in the year
#'     observed?}
#' }
#'
#' @return Character vector of required confidence column names.
#' @family retirement contract
#' @concept supply
#' @export
retirement_confidence_dimensions <- function() {
  base::c(
    "identity_confidence", "event_confidence",
    "timing_confidence", "activity_confidence"
  )
}

#' Validate the v2 retirement evidence schema
#'
#' @description
#' Requires all four dimensions from [retirement_confidence_dimensions()] and
#' refuses a legacy single-confidence value broadcast across them.
#'
#' @param evidence_tbl Evidence table with `npi`, `signal_source`,
#'   `evidence_family`, `retirement_year` and the four confidence columns.
#' @param legacy_column Name of the deprecated single-confidence column, used
#'   only to detect a broadcast migration. Set to `NULL` to skip that check.
#' @param broadcast_tolerance Fraction of rows whose four dimensions may be
#'   exactly equal before the table is treated as a broadcast legacy value.
#'   Genuine ties happen; near-universal ties do not.
#'
#' @return Invisibly `TRUE`. Errors on a violated contract.
#' @family retirement contract
#' @concept supply
#' @export
validate_retirement_evidence_v2 <- function(evidence_tbl,
                                            legacy_column = "final_confidence",
                                            broadcast_tolerance = 0.99) {
  base::message("[retirement] Validating evidence confidence schema v2.")

  confidence_columns <- retirement_confidence_dimensions()
  required_columns <- base::c(
    "npi", "signal_source", "evidence_family", "retirement_year",
    confidence_columns
  )
  missing_columns <- base::setdiff(required_columns, base::names(evidence_tbl))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Evidence schema v2 is missing: ",
      base::paste(missing_columns, collapse = ", "),
      ". Do not substitute a legacy confidence value for a missing dimension.",
      call. = FALSE
    )
  }

  out_of_range <- base::vapply(
    confidence_columns,
    function(column) {
      values <- evidence_tbl[[column]]
      base::sum(!base::is.na(values) & (values < 0 | values > 1))
    },
    integer(1)
  )
  if (base::sum(out_of_range) > 0L) {
    base::stop(
      base::sum(out_of_range), " confidence value(s) fall outside [0, 1]: ",
      base::paste(
        base::names(out_of_range)[out_of_range > 0L], collapse = ", "
      ),
      call. = FALSE
    )
  }

  # THE MIGRATION HAZARD. Four columns holding one number is not a v2 schema;
  # it is a v1 schema wearing v2 column names, and it passes every check that
  # only looks at names. Detected by content, not by provenance, because a
  # broadcast is equally wrong whether or not the legacy column survived.
  row_count <- base::nrow(evidence_tbl)
  if (row_count > 0L) {
    dimension_matrix <- base::as.matrix(
      base::as.data.frame(evidence_tbl[, confidence_columns, drop = FALSE])
    )
    identical_rows <- base::apply(dimension_matrix, 1L, function(row_values) {
      observed <- row_values[!base::is.na(row_values)]
      base::length(observed) > 1L &&
        base::length(base::unique(observed)) == 1L
    })
    identical_share <- base::sum(identical_rows) / row_count

    if (identical_share >= broadcast_tolerance) {
      legacy_note <- if (!base::is.null(legacy_column) &&
                         legacy_column %in% base::names(evidence_tbl)) {
        base::paste0(
          " The deprecated '", legacy_column,
          "' column is still present, which is where the value came from."
        )
      } else {
        ""
      }
      base::stop(
        base::sprintf(
          base::paste0(
            "%.1f%% of rows carry an identical value in all four confidence ",
            "dimensions. That is a single legacy confidence broadcast across ",
            "the v2 schema, which asserts the same unverified claim four ",
            "times and makes the identity gate unenforceable.%s"
          ),
          100 * identical_share, legacy_note
        ),
        call. = FALSE
      )
    }
  }

  dated_without_timing <- base::sum(
    !base::is.na(evidence_tbl$retirement_year) &
      base::is.na(evidence_tbl$timing_confidence)
  )
  if (dated_without_timing > 0L) {
    # base::warning(), not this package's .msg_warn() (which emits a message).
    # A schema-contract violation must be capturable by tryCatch, visible to
    # R CMD check, and escalatable with options(warn = 2). Progress chatter can
    # be a message; a signal that cannot establish the timing it claims to
    # establish should not be something a caller can scroll past.
    base::warning(
      base::sprintf(
        base::paste(
          "%s dated retirement signal(s) carry no timing_confidence; a date",
          "without a confidence in that date cannot establish terminal timing."
        ),
        base::format(dated_without_timing, big.mark = ",")
      ),
      call. = FALSE
    )
  }

  base::message("[retirement] Evidence confidence schema validated.")
  base::invisible(TRUE)
}

# Canonicalize before hashing. Column order and row order are presentation,
# not content: without this a reordered SELECT produces a different artifact_id
# for identical science, and the immutability check then fires on a false
# difference.
.retirement_canonicalize <- function(input_tbl, key_columns) {
  canonical_tbl <- base::as.data.frame(
    input_tbl, stringsAsFactors = FALSE
  )
  canonical_tbl <- canonical_tbl[, base::sort(base::names(canonical_tbl)),
                                 drop = FALSE]
  available_keys <- base::intersect(key_columns, base::names(canonical_tbl))
  if (base::length(available_keys) > 0L && base::nrow(canonical_tbl) > 0L) {
    ordering <- base::do.call(
      base::order, base::unname(base::as.list(canonical_tbl[available_keys]))
    )
    canonical_tbl <- canonical_tbl[ordering, , drop = FALSE]
  }
  base::rownames(canonical_tbl) <- NULL
  canonical_tbl
}

#' Build an immutable manifest identifying a retirement consensus artifact
#'
#' @description
#' Derives an `artifact_id` from content and provenance -- source hashes,
#' consensus hash, schema version, config version and code SHA -- so the id
#' changes if and only if something that could change the science changed.
#' Timestamps are recorded but deliberately excluded from the hash: a rebuild
#' of identical inputs must reproduce the same id, or immutability degrades
#' into "every build is new".
#'
#' @param consensus_tbl Consensus table, must contain `npi`.
#' @param source_manifest Table with `source_name`, `source_table`,
#'   `row_count`, `content_hash`.
#' @param schema_version Integer schema version.
#' @param code_sha Commit SHA that produced the artifact.
#' @param config_version Configuration version string.
#'
#' @return One-row tibble: `artifact_id`, `schema_version`, `code_sha`,
#'   `config_version`, `source_hash`, `consensus_hash`, `row_count`,
#'   `created_at`.
#' @family retirement contract
#' @concept supply
#' @export
build_retirement_artifact_manifest <- function(
    consensus_tbl,
    source_manifest,
    schema_version = 2L,
    code_sha = base::Sys.getenv("GITHUB_SHA", unset = "local"),
    config_version = "unknown") {
  base::message("[retirement] Building consensus artifact manifest.")

  required_source_columns <- base::c(
    "source_name", "source_table", "row_count", "content_hash"
  )
  missing_source_columns <- base::setdiff(
    required_source_columns, base::names(source_manifest)
  )
  if (base::length(missing_source_columns) > 0L) {
    base::stop(
      "Source manifest is missing: ",
      base::paste(missing_source_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (!"npi" %in% base::names(consensus_tbl)) {
    base::stop("Consensus table must contain npi.", call. = FALSE)
  }

  source_hash <- digest::digest(
    .retirement_canonicalize(source_manifest, base::c("source_name", "source_table")),
    algo = "sha256", serialize = TRUE
  )
  consensus_hash <- digest::digest(
    .retirement_canonicalize(consensus_tbl, "npi"),
    algo = "sha256", serialize = TRUE
  )
  artifact_hash <- digest::digest(
    base::list(
      schema_version = base::as.integer(schema_version),
      code_sha = base::as.character(code_sha),
      config_version = base::as.character(config_version),
      source_hash = source_hash,
      consensus_hash = consensus_hash
    ),
    algo = "sha256", serialize = TRUE
  )
  artifact_id <- base::paste0(
    "retirement-v", schema_version, "-", base::substr(artifact_hash, 1L, 20L)
  )

  base::message("[retirement] Artifact ID: ", artifact_id)

  tibble::tibble(
    artifact_id = artifact_id,
    schema_version = base::as.integer(schema_version),
    code_sha = base::as.character(code_sha),
    config_version = base::as.character(config_version),
    source_hash = source_hash,
    consensus_hash = consensus_hash,
    row_count = base::as.integer(base::nrow(consensus_tbl)),
    created_at = base::Sys.time()
  )
}

#' Persist a retirement consensus artifact append-only
#'
#' @description
#' Appends rows tagged with `artifact_id` and records the manifest. Never
#' replaces. Re-persisting identical content is a no-op; re-persisting
#' DIFFERENT content under the same `artifact_id` is an error, because that
#' would silently change numbers a manuscript may already cite.
#'
#' @param con Open DBI connection.
#' @param consensus_tbl Consensus rows to persist.
#' @param artifact_manifest One-row manifest from
#'   [build_retirement_artifact_manifest()].
#' @param schema Target schema name.
#'
#' @return Invisibly the `artifact_id`.
#' @family retirement contract
#' @concept supply
#' @export
persist_retirement_consensus_artifact <- function(con,
                                                  consensus_tbl,
                                                  artifact_manifest,
                                                  schema = "credentials") {
  base::message("[retirement] Persisting retirement artifact (append-only).")

  if (base::nrow(artifact_manifest) != 1L) {
    base::stop(
      "artifact_manifest must contain exactly one row.", call. = FALSE
    )
  }
  artifact_id <- base::as.character(artifact_manifest$artifact_id[[1L]])

  artifact_ref <- DBI::Id(schema = schema,
                          table = "retirement_consensus_artifacts")
  manifest_ref <- DBI::Id(schema = schema,
                          table = "retirement_consensus_manifest")

  if (DBI::dbExistsTable(con, manifest_ref)) {
    existing <- DBI::dbGetQuery(
      con,
      base::paste0(
        "SELECT artifact_id, consensus_hash, row_count FROM ",
        base::as.character(DBI::dbQuoteIdentifier(con, manifest_ref)),
        " WHERE artifact_id = ?"
      ),
      params = base::list(artifact_id)
    )
    if (base::nrow(existing) > 0L) {
      same_hash <- base::identical(
        base::as.character(existing$consensus_hash[[1L]]),
        base::as.character(artifact_manifest$consensus_hash[[1L]])
      )
      same_rows <- base::identical(
        base::as.numeric(existing$row_count[[1L]]),
        base::as.numeric(artifact_manifest$row_count[[1L]])
      )
      if (!same_hash || !same_rows) {
        base::stop(
          "Artifact ", artifact_id, " already exists with different content. ",
          "Refusing to mutate a published consensus artifact; build a new ",
          "one, which will receive a different artifact_id.",
          call. = FALSE
        )
      }
      base::message("[retirement] Artifact already present: ", artifact_id)
      return(base::invisible(artifact_id))
    }
  }

  artifact_rows <- consensus_tbl |>
    dplyr::mutate(
      artifact_id = artifact_id,
      artifact_schema_version = artifact_manifest$schema_version[[1L]]
    )

  DBI::dbBegin(con)
  base::tryCatch(
    {
      append_table <- function(reference, value_tbl) {
        if (DBI::dbExistsTable(con, reference)) {
          DBI::dbWriteTable(con, reference, value_tbl, append = TRUE)
        } else {
          DBI::dbWriteTable(con, reference, value_tbl)
        }
      }
      append_table(artifact_ref, artifact_rows)
      append_table(manifest_ref, artifact_manifest)
      DBI::dbCommit(con)
    },
    error = function(condition) {
      base::try(DBI::dbRollback(con), silent = TRUE)
      base::stop(
        "Failed to persist artifact: ", base::conditionMessage(condition),
        call. = FALSE
      )
    }
  )

  base::message("[retirement] Persisted artifact: ", artifact_id)
  base::invisible(artifact_id)
}
