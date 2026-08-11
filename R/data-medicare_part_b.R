# Medicare Part B reader (Physician & Other Practitioners, by Provider and
# Service) -------------------------------------------------------------------
#
# DATA INFRASTRUCTURE, not a model change. This module is intentionally THIN: it
# only pulls the CMS "by Provider and Service" PUF rows for a requested HCPCS set
# out of DuckDB into an in-memory claims tibble. All grouping, service mapping,
# estimand labeling and the three-measure accounting are done by the CANONICAL
# functions, not re-implemented here:
#   codes  -> urps_medicare_service_crosswalk()  (from URPS_CPT_BASKET)
#   aggregate -> aggregate_medicare_realized_care()  (services + benes + bene-day)
#   compare   -> compare_medicare_realized_care()
# See scripts/data_acquisition/07_extract_medicare_part_b.R for the wiring.
#
# ESTIMAND / SCOPE. CMS Original Medicare Fee-For-Service Part B only (traditional
# carrier claims); age/payer-specific (skews 65+, excludes MA / Medicaid /
# commercial). A Medicare-FFS validation series, NEVER a national all-payer anchor.

#' Default DuckDB path for the CMS Part B PUF
#'
#' Resolves through the canonical path config (`R/core-paths.R`) rather than a
#' hardcoded literal: the `MEDICARE_PARTB_DUCKDB` env var, else the
#' `medicare_part_b_duckdb` key from `config/paths.local.yml`/`paths.yml`. Fails
#' loudly if neither is set (never a silent hardcoded fallback).
#' @return Character scalar path (not required to exist; callers `file.exists()`).
#' @keywords internal
default_part_b_duckdb <- function() {
  env <- Sys.getenv("MEDICARE_PARTB_DUCKDB", "")
  if (nzchar(env)) return(env)
  cfg <- .paths_config()$medicare_part_b_duckdb
  if (!is.null(cfg) && nzchar(cfg)) return(normalizePath(path.expand(cfg), mustWork = FALSE))
  stop("default_part_b_duckdb(): set MEDICARE_PARTB_DUCKDB or add ",
       "`medicare_part_b_duckdb:` to config/paths.local.yml or config/paths.yml.",
       call. = FALSE)
}

# Case-insensitive column resolver: the actual column name in `have` matching any
# candidate, else NA.
.pb_col <- function(candidates, have) {
  hit <- have[tolower(have) %in% tolower(candidates)]
  if (length(hit)) hit[1] else NA_character_
}

#' Read CMS Part B by-Provider-and-Service claim rows for a HCPCS set
#'
#' Thin, read-only DuckDB reader. Returns provider-level (NPI-grain) claim rows
#' for the requested codes, with columns renamed to the CMS-canonical names that
#' [aggregate_medicare_realized_care()] expects, so the reader plugs straight
#' into the canonical aggregator. Does NOT aggregate, map services, or interpret;
#' that is the aggregator's job.
#'
#' @param hcpcs Character vector of HCPCS/CPT codes to pull (e.g.
#'   `urps_medicare_service_crosswalk()$hcpcs`).
#' @param duckdb_path Path to the DuckDB holding the PUF. Default
#'   [default_part_b_duckdb()].
#' @param years Integer vector of calendar years, or NULL for all present.
#' @param table DuckDB table name. Default "medicare_part_b_by_service_all_years".
#' @param year_col Year column in `table`. Default "data_year".
#' @return Tibble of claim rows with (where present) `year`, `HCPCS_Cd`,
#'   `Tot_Srvcs`, `Tot_Benes`, `Tot_Bene_Day_Srvcs`, `Rndrng_Prvdr_Type`,
#'   `Place_Of_Srvc`, `Rndrng_NPI`, `Rndrng_Prvdr_State_Abrvtn`. A `source_md5`
#'   attribute records the DuckDB file hash.
#' @keywords internal
read_part_b_claims <- function(hcpcs,
                               duckdb_path = default_part_b_duckdb(),
                               years = NULL,
                               table = "medicare_part_b_by_service_all_years",
                               year_col = "data_year") {
  assertthat::assert_that(is.character(hcpcs), length(hcpcs) > 0)
  if (!file.exists(duckdb_path)) {
    stop(sprintf("read_part_b_claims(): DuckDB not found at '%s'. Set MEDICARE_PARTB_DUCKDB or mount the drive.", duckdb_path), call. = FALSE)
  }
  conn <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  have_tbl <- DBI::dbGetQuery(conn,
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")$table_name
  if (!table %in% have_tbl) {
    stop(sprintf("read_part_b_claims(): table '%s' absent. Present: %s",
                 table, paste(utils::head(have_tbl, 20), collapse = ", ")), call. = FALSE)
  }
  cols <- DBI::dbGetQuery(conn, sprintf(
    "SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table))$column_name

  # (db column -> canonical output name expected by aggregate_medicare_realized_care)
  want <- list(
    year  = list(cands = c(year_col, "data_year", "year"),                         out = "year",                       required = TRUE),
    hcpcs = list(cands = c("HCPCS_Cd", "hcpcs_cd", "hcpcs"),                        out = "HCPCS_Cd",                   required = TRUE),
    srv   = list(cands = c("Tot_Srvcs", "tot_srvcs"),                              out = "Tot_Srvcs",                  required = TRUE),
    ben   = list(cands = c("Tot_Benes", "tot_benes"),                              out = "Tot_Benes",                  required = FALSE),
    bds   = list(cands = c("Tot_Bene_Day_Srvcs", "tot_bene_day_srvcs"),           out = "Tot_Bene_Day_Srvcs",         required = FALSE),
    ptype = list(cands = c("Rndrng_Prvdr_Type", "rndrng_prvdr_type", "provider_type"), out = "Rndrng_Prvdr_Type",     required = FALSE),
    pos   = list(cands = c("Plc_Of_Srvc", "Place_Of_Srvc", "place_of_service"),   out = "Place_Of_Srvc",              required = FALSE),
    npi   = list(cands = c("Rndrng_NPI", "rndrng_npi", "npi"),                    out = "Rndrng_NPI",                 required = FALSE),
    state = list(cands = c("Rndrng_Prvdr_State_Abrvtn", "state", "prvdr_state"),  out = "Rndrng_Prvdr_State_Abrvtn",  required = FALSE)
  )
  sel <- character(0)
  for (w in want) {
    hit <- .pb_col(w$cands, cols)
    if (is.na(hit)) {
      if (isTRUE(w$required)) stop(sprintf("read_part_b_claims(): required column (%s) not found in '%s'.", w$out, table), call. = FALSE)
      next
    }
    sel <- c(sel, sprintf("%s AS %s", hit, w$out))
  }

  c_hcpcs <- .pb_col(want$hcpcs$cands, cols)
  c_year  <- .pb_col(want$year$cands, cols)
  qv <- paste(sprintf("'%s'", gsub("'", "''", unique(as.character(hcpcs)))), collapse = ", ")
  where <- sprintf("%s IN (%s)", c_hcpcs, qv)
  if (!is.null(years)) where <- paste(where, sprintf("AND %s IN (%s)", c_year, paste(as.integer(years), collapse = ", ")))

  out <- DBI::dbGetQuery(conn, sprintf("SELECT %s FROM %s WHERE %s",
                                       paste(sel, collapse = ", "), table, where))
  out$HCPCS_Cd <- as.character(out$HCPCS_Cd)
  attr(out, "source_md5") <- tryCatch(unname(tools::md5sum(duckdb_path)), error = function(e) NA_character_)
  tibble::as_tibble(out)
}
