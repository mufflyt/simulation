# Medicare Part B utilization (Physician & Other Practitioners, by Provider and
# Service) -------------------------------------------------------------------
#
# DATA INFRASTRUCTURE, not a model change. This module produces reproducible
# HCPCS-level annual utilization series from the CMS "Medicare Physician & Other
# Practitioners - by Provider and Service" Public Use File, loaded into DuckDB
# (tables medicare_part_b_by_service_YYYY and a unioned *_all_years with a
# `data_year` column). It generalises the ad-hoc cliff tabulation across
# arbitrary years and code sets.
#
# ESTIMAND / SCOPE. The CMS product is Original Medicare Fee-For-Service Part B
# only (traditional carrier claims), organised by provider x HCPCS x place of
# service. It is therefore an age/payer-SPECIFIC series (skewed to 65+, excludes
# Medicare Advantage, Medicaid, and commercial payers). Treat the output as a
# Medicare-FFS VALIDATION series, NEVER as a national all-payer anchor, and do
# NOT divide it by an assumed Medicare share to manufacture a national count.
#
# THREE DISTINCT CMS UTILIZATION MEASURES, KEPT SEPARATE (never collapsed to a
# generic "procedures"):
#   Tot_Srvcs          - number of services/lines (the service count)
#   Tot_Benes          - distinct beneficiaries PER provider-code row; SUMMING it
#                        across providers double-counts a patient seen by several
#                        providers, so the summed value is NOT a de-duplicated
#                        national unique-patient count
#   Tot_Bene_Day_Srvcs - beneficiary-day services (distinct patient-days)
# The output preserves each under its own column and documents it in
# part_b_field_dictionary().

#' Version tag for the URPS Part B code-group definitions
#' @keywords internal
PART_B_CODE_GROUP_VERSION <- "2026-08-08"

#' Default DuckDB path for the CMS Part B PUF
#'
#' Resolution order: the `MEDICARE_PARTB_DUCKDB` environment variable, else the
#' known external-drive location. Returned even if absent so callers can test
#' `file.exists()`.
#' @return Character scalar path.
#' @keywords internal
default_part_b_duckdb <- function() {
  env <- Sys.getenv("MEDICARE_PARTB_DUCKDB", "")
  if (nzchar(env)) return(env)
  "/Volumes/MufflySamsung/DuckDB/nber_my_duckdb.duckdb"
}

#' URPS Medicare Part B code groups
#'
#' The urogynecology-relevant HCPCS/CPT code set, grouped. Burch/colposuspension
#' codes are near-vanishing in Medicare FFS and are retained so their scarcity is
#' visible rather than silently dropped.
#'
#' @return Tibble with columns `code_group`, `hcpcs`.
#' @family medicare part b
#' @keywords internal
urps_part_b_code_groups <- function() {
  tibble::tribble(
    ~code_group,       ~hcpcs,
    "sling",           "57288",
    "prolapse_repair", "57240",
    "prolapse_repair", "57250",
    "prolapse_repair", "57260",
    "prolapse_repair", "57265",
    "prolapse_repair", "57282",
    "prolapse_repair", "57283",
    "prolapse_repair", "57284",
    "prolapse_repair", "57285",
    "prolapse_repair", "45560",
    "pessary",         "57160",
    "urodynamics",     "51728",
    "urodynamics",     "51729",
    "urodynamics",     "51741",
    "urodynamics",     "51784",
    "urodynamics",     "51797",
    "cystoscopy",      "52000",
    "burch",           "51840",
    "burch",           "51841",
    "burch",           "58152",
    "burch",           "58267"
  )
}

#' Output field dictionary for the Part B extraction
#'
#' Maps each output column to the exact CMS source field and its definition, so
#' downstream users never conflate the three utilization measures.
#'
#' @return Named list.
#' @family medicare part b
#' @keywords internal
part_b_field_dictionary <- function() {
  list(
    year               = "CMS `data_year` (calendar year of the by-Provider-and-Service PUF).",
    code_group         = "Analyst grouping (urps_part_b_code_groups()); versioned by PART_B_CODE_GROUP_VERSION.",
    hcpcs              = "CMS `HCPCS_Cd` (HCPCS/CPT code).",
    rndrng_prvdr_type  = "CMS `Rndrng_Prvdr_Type` (rendering provider specialty); NA when not grouped by provider type.",
    place_of_service   = "CMS `Plc_Of_Srvc` (F=facility, O=office); NA when column absent or not grouped.",
    tot_srvcs          = "CMS `Tot_Srvcs`: number of SERVICES. Not patients.",
    tot_benes_sum      = "SUM of CMS `Tot_Benes` (distinct benes per provider-code row). Double-counts patients seen by multiple providers; NOT a de-duplicated unique-patient count.",
    tot_bene_day_srvcs = "CMS `Tot_Bene_Day_Srvcs`: beneficiary-day services (distinct patient-days).",
    n_provider_rows    = "Count of provider-code(-POS) rows contributing (proxy for provider count when a distinct-NPI count is unavailable).",
    n_distinct_npi     = "COUNT(DISTINCT rendering NPI) when an NPI column exists; else NA.",
    rows_with_na_benes = "Number of contributing rows where Tot_Benes was NULL/NA (CMS small-cell suppression); makes suppression explicit."
  )
}

# Case-insensitive column resolver: return the actual column name in `have` that
# matches any candidate, else NA.
.pb_col <- function(candidates, have) {
  hit <- have[tolower(have) %in% tolower(candidates)]
  if (length(hit)) hit[1] else NA_character_
}

#' Extract Medicare Part B utilization for a code set
#'
#' Reproducible replacement for ad-hoc SQL. Aggregates the CMS by-Provider-and-
#' Service PUF (in DuckDB) to annual, code-level utilization, keeping the three
#' CMS measures distinct and attaching provenance. Read-only; never writes to the
#' database.
#'
#' @param duckdb_path Path to the DuckDB holding the PUF tables. Default
#'   [default_part_b_duckdb()].
#' @param years Integer vector of calendar years, or NULL for all present.
#' @param code_groups Tibble `code_group`, `hcpcs`. Default
#'   [urps_part_b_code_groups()].
#' @param provider_type Optional character vector; if supplied, restrict to and
#'   group by these `Rndrng_Prvdr_Type` values.
#' @param place_of_service Optional character vector (e.g. c("F","O")); if the
#'   POS column exists, restrict to and group by these values.
#' @param table DuckDB table name. Default "medicare_part_b_by_service_all_years".
#' @param year_col Year column in `table`. Default "data_year".
#' @param extraction_date Date stamp for provenance. Default `Sys.Date()`.
#' @return Tibble of aggregated utilization with a `provenance` attribute (list:
#'   source, cms_dataset, duckdb_path, source_sha256, years, extraction_date,
#'   code_group_version, field_dictionary, caveat). Grouping columns
#'   `rndrng_prvdr_type` / `place_of_service` are present only when requested.
#' @family medicare part b
#' @keywords internal
extract_part_b_utilization <- function(duckdb_path = default_part_b_duckdb(),
                                       years = NULL,
                                       code_groups = urps_part_b_code_groups(),
                                       provider_type = NULL,
                                       place_of_service = NULL,
                                       table = "medicare_part_b_by_service_all_years",
                                       year_col = "data_year",
                                       extraction_date = Sys.Date()) {
  assertthat::assert_that(is.data.frame(code_groups),
                          all(c("code_group", "hcpcs") %in% names(code_groups)))
  if (!file.exists(duckdb_path)) {
    stop(sprintf("extract_part_b_utilization(): DuckDB not found at '%s'. Set MEDICARE_PARTB_DUCKDB or mount the drive.", duckdb_path))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  have_tbl <- DBI::dbGetQuery(conn,
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")$table_name
  if (!table %in% have_tbl) {
    stop(sprintf("extract_part_b_utilization(): table '%s' absent. Present: %s",
                 table, paste(utils::head(have_tbl, 20), collapse = ", ")))
  }

  cols <- DBI::dbGetQuery(conn, sprintf(
    "SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table))$column_name

  c_year  <- .pb_col(c(year_col, "data_year", "year"), cols)
  c_hcpcs <- .pb_col(c("HCPCS_Cd", "hcpcs_cd", "hcpcs"), cols)
  c_srv   <- .pb_col(c("Tot_Srvcs", "tot_srvcs"), cols)
  c_ben   <- .pb_col(c("Tot_Benes", "tot_benes"), cols)
  c_bds   <- .pb_col(c("Tot_Bene_Day_Srvcs", "tot_bene_day_srvcs"), cols)
  c_ptype <- .pb_col(c("Rndrng_Prvdr_Type", "rndrng_prvdr_type", "provider_type"), cols)
  c_pos   <- .pb_col(c("Plc_Of_Srvc", "plc_of_srvc", "place_of_service"), cols)
  c_npi   <- .pb_col(c("Rndrng_NPI", "rndrng_npi", "npi"), cols)
  for (nm in c("year", "hcpcs", "srv")) {
    if (is.na(get(paste0("c_", nm)))) stop(sprintf("extract_part_b_utilization(): required column for '%s' not found in '%s'.", nm, table))
  }

  quote_vals <- function(v) paste(sprintf("'%s'", gsub("'", "''", v)), collapse = ", ")
  codes <- unique(as.character(code_groups$hcpcs))

  group_by_ptype <- !is.null(provider_type) && !is.na(c_ptype)
  group_by_pos   <- !is.null(place_of_service) && !is.na(c_pos)

  sel <- c(sprintf("%s AS year", c_year), sprintf("%s AS hcpcs", c_hcpcs))
  grp <- c("year", "hcpcs")
  if (group_by_ptype) { sel <- c(sel, sprintf("%s AS rndrng_prvdr_type", c_ptype)); grp <- c(grp, "rndrng_prvdr_type") }
  if (group_by_pos)   { sel <- c(sel, sprintf("%s AS place_of_service", c_pos));    grp <- c(grp, "place_of_service") }

  aggs <- c(
    sprintf("SUM(%s) AS tot_srvcs", c_srv),
    if (!is.na(c_ben)) sprintf("SUM(%s) AS tot_benes_sum", c_ben) else "CAST(NULL AS DOUBLE) AS tot_benes_sum",
    if (!is.na(c_bds)) sprintf("SUM(%s) AS tot_bene_day_srvcs", c_bds) else "CAST(NULL AS DOUBLE) AS tot_bene_day_srvcs",
    "COUNT(*) AS n_provider_rows",
    if (!is.na(c_npi)) sprintf("COUNT(DISTINCT %s) AS n_distinct_npi", c_npi) else "CAST(NULL AS BIGINT) AS n_distinct_npi",
    if (!is.na(c_ben)) sprintf("SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) AS rows_with_na_benes", c_ben) else "CAST(NULL AS BIGINT) AS rows_with_na_benes"
  )

  where <- c(sprintf("%s IN (%s)", c_hcpcs, quote_vals(codes)))
  if (!is.null(years))       where <- c(where, sprintf("%s IN (%s)", c_year, paste(as.integer(years), collapse = ", ")))
  if (group_by_ptype)        where <- c(where, sprintf("%s IN (%s)", c_ptype, quote_vals(provider_type)))
  if (group_by_pos)          where <- c(where, sprintf("%s IN (%s)", c_pos, quote_vals(place_of_service)))

  sql <- sprintf("SELECT %s, %s FROM %s WHERE %s GROUP BY %s ORDER BY %s",
                 paste(sel, collapse = ", "), paste(aggs, collapse = ", "),
                 table, paste(where, collapse = " AND "),
                 paste(grp, collapse = ", "), paste(grp, collapse = ", "))
  raw <- DBI::dbGetQuery(conn, sql)
  raw$hcpcs <- as.character(raw$hcpcs)

  out <- dplyr::left_join(raw,
                          dplyr::distinct(code_groups[, c("hcpcs", "code_group")]),
                          by = "hcpcs")
  front <- intersect(c("year", "code_group", "hcpcs", "rndrng_prvdr_type", "place_of_service"), names(out))
  out <- out[, c(front, setdiff(names(out), front))]
  out <- tibble::as_tibble(out[do.call(order, out[front]), , drop = FALSE])

  src_hash <- tryCatch(unname(tools::md5sum(duckdb_path)), error = function(e) NA_character_)
  attr(out, "provenance") <- list(
    source             = "Medicare Physician & Other Practitioners - by Provider and Service (Public Use File)",
    cms_dataset        = "CMS Original Medicare FFS Part B; provider x HCPCS x place of service",
    cms_url            = "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service",
    duckdb_path        = duckdb_path,
    duckdb_table       = table,
    source_md5         = src_hash,
    years              = if (is.null(years)) sort(unique(out$year)) else as.integer(years),
    extraction_date    = as.character(extraction_date),
    code_group_version = PART_B_CODE_GROUP_VERSION,
    field_dictionary   = part_b_field_dictionary(),
    caveat             = "Medicare FFS only (excludes MA/Medicaid/commercial; skews 65+). Not a national all-payer count. tot_benes_sum double-counts patients across providers. Do NOT divide by an assumed Medicare share to derive a national total."
  )
  out
}
