# Provider career evidence warehouse ---------------------------------------
#
# Normalizes public evidence used to distinguish clinical activity,
# Medicare opt-out, administrative exclusion, research, leadership,
# inactivity, retirement, and death. Missing sources remain missing.

#' Provider career evidence source registry
#'
#' @return A tibble describing each public source and its interpretation.
#' @export
provider_career_source_registry <- function() {
  tibble::tribble(
    ~source_id, ~source_name, ~identity_key, ~evidence_role,
    "cms_opt_out", "CMS Medicare Opt-Out Affidavits", "npi",
    "Private-contracting signal; not inactivity",
    "cms_pecos", "CMS FFS Public Provider Enrollment", "npi",
    "Enrollment, group, specialty, and practice-setting signal",
    "cms_part_d", "CMS Medicare Part D Prescribers", "npi",
    "Observed prescribing activity",
    "cms_revoked", "CMS Revoked Providers and Suppliers", "npi",
    "Administrative Medicare inactivity; not retirement",
    "irs_form_990", "IRS Form 990 bulk filings", "verified_person_org",
    "Executive and key-employee evidence",
    "clinical_trials", "ClinicalTrials.gov API", "verified_person_org",
    "Research and principal-investigator evidence",
    "orcid", "ORCID public record", "verified_orcid",
    "Employment, education, and research-affiliation evidence"
  ) |>
    dplyr::mutate(
      source_url = base::c(
        base::paste0(
          "https://data.cms.gov/provider-characteristics/",
          "medicare-provider-supplier-enrollment/opt-out-affidavits"
        ),
        base::paste0(
          "https://data.cms.gov/provider-characteristics/",
          "medicare-provider-supplier-enrollment/",
          "medicare-fee-for-service-public-provider-enrollment"
        ),
        base::paste0(
          "https://data.cms.gov/provider-summary-by-type-of-service/",
          "medicare-part-d-prescribers"
        ),
        base::paste0(
          "https://data.cms.gov/provider-characteristics/",
          "medicare-provider-supplier-enrollment/",
          "revoked-medicare-providers-and-suppliers"
        ),
        base::paste0(
          "https://www.irs.gov/charities-non-profits/",
          "tax-exempt-organization-search"
        ),
        "https://clinicaltrials.gov/data-api/api",
        "https://info.orcid.org/documentation/api-tutorials/"
      ),
      absence_means_zero = FALSE,
      downloaded_at = base::as.POSIXct(NA)
    )
}

#' Initialize a provider-career DuckDB
#'
#' @param connection Open writable DuckDB connection.
#' @return `TRUE` invisibly.
#' @export
initialize_provider_career_duckdb <- function(connection) {
  base::message("initialize_provider_career_duckdb(): creating schemas")
  for (schema_name in base::c("career_raw", "career", "career_meta")) {
    DBI::dbExecute(
      connection,
      base::sprintf(
        "CREATE SCHEMA IF NOT EXISTS %s",
        DBI::dbQuoteIdentifier(connection, schema_name)
      )
    )
  }

  table_statements <- base::c(
    "CREATE TABLE IF NOT EXISTS career_meta.source_manifest (
       source_id VARCHAR,
       source_url VARCHAR,
       local_path VARCHAR,
       content_sha256 VARCHAR,
       downloaded_at TIMESTAMP,
       ingested_at TIMESTAMP,
       row_count BIGINT,
       status VARCHAR,
       notes VARCHAR
     )",
    "CREATE TABLE IF NOT EXISTS career.provider_identity (
       provider_id VARCHAR NOT NULL,
       npi VARCHAR,
       orcid VARCHAR,
       normalized_name VARCHAR,
       organization_name VARCHAR,
       identity_tier INTEGER NOT NULL,
       identity_verified BOOLEAN NOT NULL,
       PRIMARY KEY(provider_id)
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.cms_opt_out (
       npi VARCHAR,
       effective_date DATE,
       end_date DATE,
       specialty VARCHAR,
       source_year INTEGER
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.cms_pecos (
       npi VARCHAR,
       enrollment_id VARCHAR,
       enrollment_type VARCHAR,
       specialty VARCHAR,
       organization_name VARCHAR,
       state VARCHAR,
       enrollment_date DATE,
       source_year INTEGER
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.cms_part_d (
       npi VARCHAR,
       source_year INTEGER,
       total_claim_count DOUBLE,
       total_30_day_fills DOUBLE,
       total_drug_cost DOUBLE
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.cms_revoked (
       npi VARCHAR,
       revocation_date DATE,
       reinstatement_date DATE,
       revocation_reason VARCHAR,
       state VARCHAR
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.irs_form_990 (
       normalized_name VARCHAR,
       organization_name VARCHAR,
       organization_ein VARCHAR,
       tax_year INTEGER,
       role_title VARCHAR,
       compensation DOUBLE
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.clinical_trials (
       normalized_name VARCHAR,
       organization_name VARCHAR,
       nct_id VARCHAR,
       source_year INTEGER,
       investigator_role VARCHAR,
       overall_status VARCHAR
     )",
    "CREATE TABLE IF NOT EXISTS career_raw.orcid_affiliation (
       orcid VARCHAR,
       organization_name VARCHAR,
       start_year INTEGER,
       end_year INTEGER,
       role_title VARCHAR,
       affiliation_type VARCHAR
     )"
  )
  base::invisible(base::lapply(
    table_statements,
    function(statement) DBI::dbExecute(connection, statement)
  ))
  base::message("initialize_provider_career_duckdb(): schemas ready")
  base::invisible(TRUE)
}

#' Register verified provider identities
#'
#' @param connection Open writable DuckDB connection.
#' @param identities Provider identity tibble.
#' @param overwrite Replace existing identities when `TRUE`.
#' @return Number of identities written.
#' @export
register_provider_career_identities <- function(
    connection,
    identities,
    overwrite = FALSE) {
  required_columns <- base::c(
    "provider_id", "npi", "orcid", "normalized_name",
    "organization_name", "identity_tier", "identity_verified"
  )
  missing_columns <- base::setdiff(required_columns, base::names(identities))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Missing identity columns: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::anyDuplicated(identities$provider_id) > 0L) {
    base::stop("provider_id must be unique.", call. = FALSE)
  }
  if (base::any(
    identities$identity_verified & identities$identity_tier > 2L
  )) {
    base::stop(
      "Verified identities must have identity_tier 1 or 2.",
      call. = FALSE
    )
  }
  identities <- identities |>
    dplyr::mutate(
      npi = stringr::str_trim(base::as.character(.data$npi)),
      orcid = stringr::str_trim(base::as.character(.data$orcid)),
      normalized_name = stringr::str_squish(base::tolower(
        base::as.character(.data$normalized_name)
      )),
      organization_name = stringr::str_squish(base::tolower(
        base::as.character(.data$organization_name)
      ))
    )

  initialize_provider_career_duckdb(connection)
  base::message(
    "register_provider_career_identities(): rows=",
    scales::comma(base::nrow(identities)),
    ", overwrite=", overwrite
  )
  if (base::isTRUE(overwrite)) {
    DBI::dbExecute(connection, "DELETE FROM career.provider_identity")
  }
  temporary_name <- "career_identity_stage"
  DBI::dbWriteTable(
    connection,
    temporary_name,
    identities,
    temporary = TRUE,
    overwrite = TRUE
  )
  DBI::dbExecute(
    connection,
    "INSERT OR REPLACE INTO career.provider_identity
     SELECT provider_id, npi, orcid, normalized_name,
            organization_name, identity_tier, identity_verified
     FROM career_identity_stage"
  )
  base::message(
    "register_provider_career_identities(): wrote ",
    scales::comma(base::nrow(identities)), " identities"
  )
  base::nrow(identities)
}

#' Ingest a normalized public source into DuckDB
#'
#' @param connection Open writable DuckDB connection.
#' @param source_id Registry source identifier.
#' @param source_rows Normalized tibble matching the destination schema.
#' @param local_path Optional downloaded-file path.
#' @param source_url Optional source URL.
#' @param replace_source Replace the source table when `TRUE`.
#' @return Source-manifest row invisibly.
#' @export
ingest_provider_career_source <- function(
    connection,
    source_id,
    source_rows,
    local_path = NA_character_,
    source_url = NA_character_,
    replace_source = TRUE) {
  destination_tables <- base::c(
    cms_opt_out = "career_raw.cms_opt_out",
    cms_pecos = "career_raw.cms_pecos",
    cms_part_d = "career_raw.cms_part_d",
    cms_revoked = "career_raw.cms_revoked",
    irs_form_990 = "career_raw.irs_form_990",
    clinical_trials = "career_raw.clinical_trials",
    orcid = "career_raw.orcid_affiliation"
  )
  if (!source_id %in% base::names(destination_tables)) {
    base::stop("Unknown source_id: ", source_id, call. = FALSE)
  }
  initialize_provider_career_duckdb(connection)
  destination_table <- destination_tables[[source_id]]
  expected_columns <- DBI::dbGetQuery(
    connection,
    base::sprintf(
      "SELECT column_name FROM information_schema.columns
       WHERE table_schema = 'career_raw' AND table_name = '%s'
       ORDER BY ordinal_position",
      base::sub("^career_raw\\.", "", destination_table)
    )
  )$column_name
  missing_columns <- base::setdiff(
    expected_columns,
    base::names(source_rows)
  )
  if (base::length(missing_columns) > 0L) {
    base::stop(
      source_id, " lacks normalized columns: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  normalized_rows <- source_rows |>
    dplyr::select(dplyr::all_of(expected_columns))
  if (base::isTRUE(replace_source)) {
    DBI::dbExecute(
      connection,
      base::paste("DELETE FROM", destination_table)
    )
  }
  stage_name <- base::paste0("stage_", source_id)
  DBI::dbWriteTable(
    connection,
    stage_name,
    normalized_rows,
    temporary = TRUE,
    overwrite = TRUE
  )
  DBI::dbExecute(
    connection,
    base::sprintf(
      "INSERT INTO %s SELECT * FROM %s",
      destination_table,
      stage_name
    )
  )
  source_hash <- if (
    !base::is.na(local_path) && base::file.exists(local_path)
  ) {
    digest::digest(file = local_path, algo = "sha256")
  } else {
    digest::digest(normalized_rows, algo = "sha256")
  }
  manifest_row <- tibble::tibble(
    source_id = source_id,
    source_url = source_url,
    local_path = local_path,
    content_sha256 = source_hash,
    downloaded_at = base::as.POSIXct(
      if (!base::is.na(local_path) && base::file.exists(local_path)) {
        base::file.info(local_path)$mtime
      } else {
        NA
      },
      origin = "1970-01-01"
    ),
    ingested_at = base::Sys.time(),
    row_count = base::nrow(normalized_rows),
    status = "ingested",
    notes = "Normalized source; absence is not zero"
  )
  DBI::dbWriteTable(
    connection,
    "source_manifest_stage",
    manifest_row,
    temporary = TRUE,
    overwrite = TRUE
  )
  DBI::dbExecute(
    connection,
    "INSERT INTO career_meta.source_manifest
     SELECT * FROM source_manifest_stage"
  )
  base::message(
    "ingest_provider_career_source(): source=", source_id,
    ", rows=", scales::comma(base::nrow(normalized_rows)),
    ", sha256=", base::substr(source_hash, 1L, 12L)
  )
  base::invisible(manifest_row)
}

#' Build provider-year career evidence from normalized DuckDB tables
#'
#' @param connection Open DuckDB connection.
#' @param years Integer years to construct.
#' @return A lazy DuckDB relation when `collect = FALSE`, otherwise a tibble.
#' @param collect Collect the evidence panel into memory.
#' @export
build_provider_career_evidence_panel <- function(
    connection,
    years,
    collect = FALSE) {
  initialize_provider_career_duckdb(connection)
  years <- base::sort(base::unique(base::as.integer(years)))
  if (base::length(years) == 0L || base::any(!base::is.finite(years))) {
    base::stop("years must contain finite integers.", call. = FALSE)
  }
  base::message(
    "build_provider_career_evidence_panel(): years=",
    base::min(years), "-", base::max(years),
    ", collect=", collect
  )
  year_rows <- tibble::tibble(year = years)
  DBI::dbWriteTable(
    connection,
    "career_year_stage",
    year_rows,
    temporary = TRUE,
    overwrite = TRUE
  )
  DBI::dbExecute(
    connection,
    "CREATE OR REPLACE TEMP VIEW career_evidence_panel AS
     WITH provider_year AS (
       SELECT i.*, y.year
       FROM career.provider_identity i
       CROSS JOIN career_year_stage y
     ),
     opt_out AS (
       SELECT npi, YEAR(effective_date) AS start_year,
              YEAR(COALESCE(end_date, DATE '2999-12-31')) AS end_year
       FROM career_raw.cms_opt_out
       GROUP BY npi, effective_date, end_date
     ),
     pecos AS (
       SELECT npi, source_year,
              COUNT(DISTINCT enrollment_id) AS enrollment_count
       FROM career_raw.cms_pecos
       GROUP BY npi, source_year
     ),
     part_d AS (
       SELECT npi, source_year AS year,
              SUM(total_claim_count) AS part_d_claims,
              SUM(total_30_day_fills) AS part_d_fills,
              SUM(total_drug_cost) AS part_d_cost
       FROM career_raw.cms_part_d
       GROUP BY npi, source_year
     ),
     revoked AS (
       SELECT npi, YEAR(revocation_date) AS start_year,
              YEAR(COALESCE(
                reinstatement_date, DATE '2999-12-31'
              )) AS end_year
       FROM career_raw.cms_revoked
       GROUP BY npi, revocation_date, reinstatement_date
     ),
     trial_signal AS (
       SELECT normalized_name, organization_name, source_year AS year,
              COUNT(DISTINCT nct_id) AS active_trials,
              MAX(CASE WHEN LOWER(investigator_role) LIKE '%principal%'
                       THEN 1 ELSE 0 END) AS principal_investigator
       FROM career_raw.clinical_trials
       GROUP BY normalized_name, organization_name, source_year
     ),
     form_990_signal AS (
       SELECT normalized_name, organization_name, tax_year AS year,
              MAX(CASE WHEN REGEXP_MATCHES(
                LOWER(COALESCE(role_title, '')),
                'chief|executive|president|vice president|officer|dean'
              ) THEN 1 ELSE 0 END) AS executive_role,
              MAX(compensation) AS executive_compensation
       FROM career_raw.irs_form_990
       GROUP BY normalized_name, organization_name, tax_year
     ),
     orcid_signal AS (
       SELECT orcid, y.year,
              MAX(CASE WHEN LOWER(COALESCE(role_title, '')) LIKE '%professor%'
                       THEN 1 ELSE 0 END) AS academic_role,
              MAX(CASE WHEN REGEXP_MATCHES(
                LOWER(COALESCE(role_title, '')),
                'chief|executive|chair|dean|director'
              ) THEN 1 ELSE 0 END) AS orcid_leadership_role
       FROM career_raw.orcid_affiliation o
       CROSS JOIN career_year_stage y
       WHERE y.year >= COALESCE(o.start_year, y.year)
         AND y.year <= COALESCE(o.end_year, y.year)
       GROUP BY orcid, y.year
     )
     SELECT p.provider_id, p.npi, p.orcid, p.year,
            p.identity_tier, p.identity_verified,
            CASE WHEN o.npi IS NULL THEN 0 ELSE 1 END AS medicare_opt_out,
            CASE WHEN o.npi IS NULL THEN 0 ELSE 1 END
              AS private_contracting_signal,
            CASE WHEN e.npi IS NULL THEN 0 ELSE 1 END AS pecos_enrolled,
            CASE WHEN d.npi IS NULL THEN 0 ELSE 1 END AS part_d_observed,
            d.part_d_claims, d.part_d_fills, d.part_d_cost,
            CASE WHEN r.npi IS NULL THEN 0 ELSE 1 END AS medicare_revoked,
            CASE WHEN t.normalized_name IS NULL THEN 0
                 ELSE t.active_trials END AS active_trials,
            CASE WHEN t.normalized_name IS NULL THEN 0
                 ELSE t.principal_investigator END AS principal_investigator,
            CASE WHEN f.normalized_name IS NULL THEN 0
                 ELSE f.executive_role END AS form_990_executive,
            f.executive_compensation,
            CASE WHEN a.orcid IS NULL THEN 0
                 ELSE a.academic_role END AS orcid_academic,
            CASE WHEN a.orcid IS NULL THEN 0
                 ELSE a.orcid_leadership_role END AS orcid_leadership,
            CASE WHEN d.npi IS NOT NULL THEN 3
                 WHEN e.npi IS NOT NULL THEN 1
                 ELSE 0 END AS clinical_activity_score,
            CASE WHEN f.executive_role = 1
                   OR a.orcid_leadership_role = 1 THEN 1 ELSE 0 END
              AS leadership_signal,
            CASE WHEN t.active_trials > 0 OR a.academic_role = 1
                 THEN 1 ELSE 0 END AS academic_signal,
            CASE WHEN r.npi IS NOT NULL THEN 1 ELSE 0 END
              AS administrative_inactivity_signal
     FROM provider_year p
     LEFT JOIN opt_out o
       ON p.npi = o.npi
      AND p.year BETWEEN o.start_year AND o.end_year
     LEFT JOIN pecos e
       ON p.npi = e.npi AND p.year = e.source_year
     LEFT JOIN part_d d
       ON p.npi = d.npi AND p.year = d.year
     LEFT JOIN revoked r
       ON p.npi = r.npi
      AND p.year BETWEEN r.start_year AND r.end_year
     LEFT JOIN trial_signal t
       ON p.identity_verified
      AND p.normalized_name = t.normalized_name
      AND p.organization_name = t.organization_name
      AND p.year = t.year
     LEFT JOIN form_990_signal f
       ON p.identity_verified
      AND p.normalized_name = f.normalized_name
      AND p.organization_name = f.organization_name
      AND p.year = f.year
     LEFT JOIN orcid_signal a
       ON p.identity_verified
      AND p.orcid = a.orcid
      AND p.year = a.year"
  )
  panel_relation <- dplyr::tbl(connection, "career_evidence_panel")
  if (base::isTRUE(collect)) {
    panel_rows <- dplyr::collect(panel_relation)
    base::message(
      "build_provider_career_evidence_panel(): collected ",
      scales::comma(base::nrow(panel_rows)), " provider-years"
    )
    return(panel_rows)
  }
  base::message(
    "build_provider_career_evidence_panel(): returning lazy relation"
  )
  panel_relation
}

#' Classify career evidence without turning missingness into retirement
#'
#' @param evidence_panel Provider-year evidence tibble.
#' @param part_d_full_time_claims Calibrated Part D threshold, or `Inf` to avoid
#'   assigning full-time status from Part D alone.
#' @return Provider-year evidence with provisional state and confidence.
#' @export
classify_provider_career_evidence <- function(
    evidence_panel,
    part_d_full_time_claims = Inf) {
  base::message(
    "classify_provider_career_evidence(): rows=",
    scales::comma(base::nrow(evidence_panel)),
    ", part_d_full_time_claims=", part_d_full_time_claims
  )
  classified_panel <- evidence_panel |>
    dplyr::mutate(
      provisional_state = dplyr::case_when(
        .data$leadership_signal == 1L ~ "admin_executive",
        .data$academic_signal == 1L ~ "academic_leadership",
        .data$administrative_inactivity_signal == 1L ~ "inactive",
        .data$part_d_observed == 1L &
          base::is.finite(part_d_full_time_claims) &
          .data$part_d_claims >= part_d_full_time_claims ~
          "full_time_clinical",
        .data$part_d_observed == 1L &
          base::is.finite(part_d_full_time_claims) ~
          "part_time_clinical",
        .data$pecos_enrolled == 1L | .data$medicare_opt_out == 1L ~
          NA_character_,
        TRUE ~ NA_character_
      ),
      state_confidence = dplyr::case_when(
        .data$form_990_executive == 1L & .data$identity_verified ~ 0.90,
        .data$orcid_leadership == 1L & .data$identity_verified ~ 0.80,
        .data$principal_investigator == 1L & .data$identity_verified ~ 0.75,
        .data$administrative_inactivity_signal == 1L ~ 0.80,
        .data$part_d_observed == 1L ~ 0.65,
        TRUE ~ 0.00
      ),
      retirement_ascertained = FALSE,
      death_ascertained = FALSE,
      clinical_activity_ascertained = .data$part_d_observed == 1L,
      unresolved = base::is.na(.data$provisional_state)
    )
  base::message(
    "classify_provider_career_evidence(): classified=",
    scales::comma(base::sum(!classified_panel$unresolved)),
    ", unresolved=", scales::comma(base::sum(classified_panel$unresolved))
  )
  classified_panel
}

#' Merge public evidence into an observed provider-year career panel
#'
#' @param provider_year_panel Provider-year records containing provider_id,
#'   year, clinical_fte, and optional verified_state.
#' @param evidence_panel Output of [build_provider_career_evidence_panel()].
#' @param full_time_fte Clinical FTE threshold for full-time classification.
#' @return Enriched provider-year transition-training panel.
#' @export
merge_provider_career_evidence <- function(
    provider_year_panel,
    evidence_panel,
    full_time_fte = 0.80) {
  required_provider_columns <- base::c(
    "provider_id", "year", "clinical_fte"
  )
  missing_provider_columns <- base::setdiff(
    required_provider_columns,
    base::names(provider_year_panel)
  )
  if (base::length(missing_provider_columns) > 0L) {
    base::stop(
      "Provider-year panel lacks: ",
      base::paste(missing_provider_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (!"verified_state" %in% base::names(provider_year_panel)) {
    provider_year_panel$verified_state <- NA_character_
  }
  base::message(
    "merge_provider_career_evidence(): provider-years=",
    scales::comma(base::nrow(provider_year_panel)),
    ", evidence rows=", scales::comma(base::nrow(evidence_panel)),
    ", full_time_fte=", full_time_fte
  )
  enriched_panel <- provider_year_panel |>
    dplyr::left_join(
      evidence_panel,
      by = base::c("provider_id", "year")
    ) |>
    dplyr::mutate(
      career_state = dplyr::case_when(
        .data$verified_state == "deceased" ~ "deceased",
        .data$verified_state == "retired" ~ "retired",
        .data$verified_state == "inactive" ~ "inactive",
        .data$leadership_signal == 1L ~ "admin_executive",
        .data$academic_signal == 1L ~ "academic_leadership",
        .data$administrative_inactivity_signal == 1L ~ "inactive",
        .data$part_d_observed == 1L &
          !base::is.na(.data$clinical_fte) &
          .data$clinical_fte >= full_time_fte ~ "full_time_clinical",
        .data$part_d_observed == 1L &
          !base::is.na(.data$clinical_fte) &
          .data$clinical_fte > 0 ~ "part_time_clinical",
        .data$verified_state %in% base::c(
          "full_time_clinical", "part_time_clinical",
          "academic_leadership", "admin_executive"
        ) ~ .data$verified_state,
        TRUE ~ NA_character_
      ),
      career_state_source = dplyr::case_when(
        .data$verified_state %in% base::c(
          "deceased", "retired", "inactive"
        ) ~ "direct_verification",
        .data$form_990_executive == 1L ~ "verified_irs_form_990",
        .data$orcid_leadership == 1L ~ "verified_orcid",
        .data$principal_investigator == 1L ~
          "verified_clinicaltrials_gov",
        .data$administrative_inactivity_signal == 1L ~
          "cms_revocation",
        .data$part_d_observed == 1L &
          !base::is.na(.data$clinical_fte) ~ "cms_part_d_plus_fte",
        !base::is.na(.data$verified_state) ~ "direct_verification",
        TRUE ~ "unresolved"
      ),
      public_evidence_count = base::rowSums(
        base::cbind(
          .data$medicare_opt_out == 1L,
          .data$pecos_enrolled == 1L,
          .data$part_d_observed == 1L,
          .data$medicare_revoked == 1L,
          .data$form_990_executive == 1L,
          .data$principal_investigator == 1L,
          .data$orcid_academic == 1L,
          .data$orcid_leadership == 1L
        ),
        na.rm = TRUE
      ),
      career_state_resolved = !base::is.na(.data$career_state),
      retirement_ascertained = .data$verified_state == "retired",
      death_ascertained = .data$verified_state == "deceased"
    )
  base::message(
    "merge_provider_career_evidence(): resolved=",
    scales::comma(base::sum(enriched_panel$career_state_resolved)),
    ", unresolved=",
    scales::comma(base::sum(!enriched_panel$career_state_resolved))
  )
  enriched_panel
}

#' Audit coverage of every career evidence source
#'
#' @param connection Open DuckDB connection.
#' @return Source-level row counts and availability status.
#' @export
audit_provider_career_sources <- function(connection) {
  initialize_provider_career_duckdb(connection)
  source_tables <- tibble::tribble(
    ~source_id, ~table_name,
    "cms_opt_out", "cms_opt_out",
    "cms_pecos", "cms_pecos",
    "cms_part_d", "cms_part_d",
    "cms_revoked", "cms_revoked",
    "irs_form_990", "irs_form_990",
    "clinical_trials", "clinical_trials",
    "orcid", "orcid_affiliation"
  )
  audit_rows <- source_tables |>
    dplyr::rowwise() |>
    dplyr::mutate(
      row_count = DBI::dbGetQuery(
        connection,
        base::paste0(
          "SELECT COUNT(*) AS n FROM career_raw.", .data$table_name
        )
      )$n[[1]],
      available = .data$row_count > 0,
      interpretation = dplyr::if_else(
        .data$available,
        "available",
        "unavailable; must not be interpreted as zero"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      "source_id",
      "table_name",
      "row_count",
      "available",
      "interpretation"
    )
  base::message(
    "audit_provider_career_sources(): available=",
    base::sum(audit_rows$available), "/", base::nrow(audit_rows)
  )
  audit_rows
}
