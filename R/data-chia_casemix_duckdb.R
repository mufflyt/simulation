################################################################################
# R/data-chia_casemix_duckdb.R
# Connector for the CHIA Case Mix DuckDB (chia_cadr.duckdb), built by
# scripts/chia/pull_dropbox_link.py (download) + load_chia_mdb.py (raw .mdb
# tables) + finalize_db.py (year-union views).
#
# PATH RESOLUTION REUSES .chia_duckdb_default() (R/calibration-transport_to_national.R)
# rather than a second resolver: that one already exists, is already used by
# four call sites, and is more robust than a naive config-key lookup -- it
# globs the volume name (this physical drive mounts as "MufflySamsung",
# "MufflySamsung 1", "MufflySamsung 1 1" depending on remount history) and
# refuses a suspiciously small file rather than silently opening/creating an
# empty stub DuckDB. A second, weaker resolver here would be exactly the kind
# of drifting duplicate this repository's own conventions warn against.
################################################################################

#' Open a read-only connection to the CHIA Case Mix DuckDB
#'
#' Thin, fail-closed connector: verifies the file exists and the
#' `chia_casemix` schema is present before returning. The caller owns the
#' connection and must `DBI::dbDisconnect(con, shutdown = TRUE)` when done --
#' this is meant to be passed as `con` into the chia_casemix consumer
#' functions (e.g. [build_chia_inpatient_urps_series()]), which may issue
#' more than one query per call.
#'
#' NOTE: as of this writing the database holds only the RAW per-year tables
#' (`chia_casemix.hdd_discharge_2015`, etc.) and finalize_db.py's
#' `v_*_all_years` unions. The `v_hdd_discharge_canonical` /
#' `v_ood_observation_canonical` views that [build_chia_inpatient_urps_series()]
#' and related functions query are a separate, more involved layer (physician
#' attribution, era-column normalisation across the 2004-2018 schema drift,
#' procedure classification) that is not yet built -- see
#' `scripts/chia/test_chia.py` for the intended shape. Querying those views
#' before that layer exists will fail with "view does not exist", not
#' silently return nothing.
#'
#' @param duckdb_path Path to the DuckDB. Default resolves via
#'   [.chia_duckdb_default()] (env var `URPS_CHIA_DUCKDB`, else a glob for the
#'   `MufflySamsung*` volume).
#' @return An open `DBIConnection`.
#' @family chia inpatient surgery
#' @concept demand
#' @export
chia_casemix_con <- function(duckdb_path = .chia_duckdb_default()) {
  if (is.na(duckdb_path) || !file.exists(duckdb_path)) {
    stop(sprintf(
      "chia_casemix_con(): DuckDB not found (resolved to '%s'). Set URPS_CHIA_DUCKDB, mount the drive, or run scripts/chia/run_remaining.sh to build it.",
      duckdb_path), call. = FALSE)
  }
  con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
  ok <- FALSE
  on.exit(if (!ok) try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE),
          add = TRUE)

  schemas <- DBI::dbGetQuery(
    con, "SELECT DISTINCT schema_name FROM information_schema.schemata")$schema_name
  if (!"chia_casemix" %in% schemas) {
    stop(sprintf(
      "chia_casemix_con(): '%s' has no chia_casemix schema. Present: %s",
      duckdb_path, paste(schemas, collapse = ", ")), call. = FALSE)
  }
  ok <- TRUE
  con
}
