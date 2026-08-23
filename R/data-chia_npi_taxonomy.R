################################################################################
# R/data-chia_npi_taxonomy.R
# Resolves rendering_npi -> NUCC taxonomy_code for CHIA-attributed physicians,
# reusing the NPPES source R/supply-entry_panel.R already relies on
# (credentials.temporal_nppes_<year>_fixed in the NBER/credentials DuckDB)
# instead of hand-curating CHIA's free-text BORIM specialty_1 strings. This is
# the shape classify_chia_service_share_events() expects for its
# npi_taxonomy argument (R/data-chia_urogynecology_service_shares.R).
################################################################################

#' Default location of the credentials/claims DuckDB (NPPES taxonomy source)
#'
#' Thin re-export of the resolution already done once in
#' `R/supply-entry_panel.R`'s `ENTRY_PANEL_DB_DEFAULT` -- same database, same
#' `researchpaths::resolve_duckdb()` discovery, so this does not become a
#' second, independently-drifting resolver.
#'
#' @return Character scalar path, or `NA_character_` if unresolved.
#' @family chia physician attribution
#' @concept supply
#' @export
chia_npi_taxonomy_credentials_db_default <- function() {
  ENTRY_PANEL_DB_DEFAULT
}

#' Build an NPI -> NUCC taxonomy_code crosswalk for CHIA-attributed physicians
#'
#' Resolves each CHIA discharge's operating-physician BORIM license to an NPI
#' (via `chia_provider.borim_stdrel_npi_straight_from_cd`, the same join
#' [build_chia_surgeon_year_volume_views()] uses), then looks up that NPI's
#' primary taxonomy code in the year-matched `credentials.temporal_nppes_<year>_fixed`
#' table -- CHIA's own BORIM `specialty_1` is free text ("Obstetrics and
#' Gynecology") and not a NUCC code, so it is not used here.
#'
#' Requires [build_chia_hdd_discharge_physician_view()] to have been run
#' first. `con` must be a connection to the CHIA database with the credentials
#' database `ATTACH`ed read-only as `chia_npi_taxonomy_credentials` (this
#' function issues the `ATTACH`/`DETACH` itself).
#'
#' @param con Open, writable DuckDB connection to the CHIA database.
#' @param credentials_db Path to the credentials/claims DuckDB. Defaults to
#'   [chia_npi_taxonomy_credentials_db_default()].
#' @param years Integer vector of `_data_year` values to resolve. Defaults to
#'   2015:2018, the years CHIA physician attribution covers.
#' @return A tibble with columns `rendering_npi`, `taxonomy_code`,
#'   `is_primary` (always `TRUE` -- only `taxonomy_1` is resolved), one row
#'   per distinct NPI actually attributed to a CHIA discharge in `years`.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_npi_taxonomy_crosswalk <- function(
    con,
    credentials_db = chia_npi_taxonomy_credentials_db_default(),
    years = 2015:2018) {
  if (is.na(credentials_db) || !file.exists(credentials_db)) {
    stop(
      "build_chia_npi_taxonomy_crosswalk(): credentials DuckDB not found ",
      "(resolved to '", credentials_db, "'). Set URPS_NBER_DUCKDB or mount the drive.",
      call. = FALSE
    )
  }

  DBI::dbExecute(con, sprintf(
    "ATTACH '%s' AS chia_npi_taxonomy_credentials (READ_ONLY)", credentials_db
  ))
  on.exit(
    DBI::dbExecute(con, "DETACH chia_npi_taxonomy_credentials"),
    add = TRUE
  )

  year_union <- paste(
    sprintf(
      "SELECT %d AS _data_year, npi, taxonomy_1 AS taxonomy_code
       FROM chia_npi_taxonomy_credentials.credentials.temporal_nppes_%d_fixed
       WHERE taxonomy_1 IS NOT NULL",
      years, years
    ),
    collapse = "\nUNION ALL\n"
  )

  out <- DBI::dbGetQuery(con, sprintf("
    SELECT DISTINCT b.NPI AS rendering_npi, t.taxonomy_code
    FROM chia_casemix.v_hdd_discharge_physician d
    JOIN chia_provider.borim_stdrel_npi_straight_from_cd b
      ON TRY_CAST(b.license AS BIGINT) = d.borim_license
    JOIN (%s) t ON t.npi = b.NPI AND t._data_year = d._data_year
    WHERE b.NPI IS NOT NULL AND trim(b.NPI) <> ''
      AND d._data_year IN (%s)
  ", year_union, paste(years, collapse = ",")))

  tibble::tibble(
    rendering_npi = out$rendering_npi,
    taxonomy_code = out$taxonomy_code,
    is_primary = TRUE
  )
}
