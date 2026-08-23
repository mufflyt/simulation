################################################################################
# R/data-chia_physician_attribution.R
# Builds chia_casemix.surgeon_year_volume and its supporting views on an open
# DuckDB connection. Extracted from scripts/chia/build_physician_attribution.R
# so the classification logic is testable against a small synthetic fixture
# (tests/testthat/test-chia-physician-attribution.R) instead of only the real
# 10GB external CHIA database. See that script's header for the empirical
# findings behind these choices (raw-vs-encrypted physician IDs, the
# ICD-9/ICD-10-PCS operative definition, the specialty-share refinement, the
# newborn-exclusion-from-signal fix, and the DuckDB UNPIVOT persistence bug
# that is why physician melting uses UNION ALL instead).
#
# Every function here takes an open, WRITABLE `con` and returns it invisibly
# so calls can be chained; none of them open or close a connection themselves
# -- that is the caller's job (chia_casemix_con() is read-only and not
# suitable here; scripts/chia/build_physician_attribution.R opens its own
# writable connection).
################################################################################

#' Build the long-format diagnosis-code view
#'
#' Thin rename over `v_hdd_diagnoscode_all_years` (already one row per
#' `(RecordType20ID, DiagnosisCode)`, so this is a rename, not a melt).
#' Several already-committed call sites (`R/calibration-transport_to_national.R`,
#' `scripts/calibration/build_empirical_calibration_targets.R`,
#' `scripts/chia/run_chia_revenue_setting.R`'s `dx_sql()`) reference
#' `chia_casemix.hdd_diagnosis_long` without anything in the repo creating it;
#' this closes that gap.
#'
#' @param con Open, writable DuckDB connection.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_hdd_diagnosis_long_view <- function(con) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.hdd_diagnosis_long AS
    SELECT RecordType20ID, _data_year, DiagnosisCode AS code
    FROM chia_casemix.v_hdd_diagnosiscode_all_years
  ")
  invisible(con)
}

#' Build the physician-attribution view (2015-2018 raw BORIM numbers)
#'
#' Melts the 15 `OperatingPhysician*` columns on
#' `chia_casemix.v_hdd_discharge_all_years` into one row per (discharge,
#' physician) pair via `UNION ALL` (not `UNPIVOT` -- DuckDB v1.5.2 loses an
#' UNPIVOT-based view's column list on catalog reload; confirmed empirically).
#' Values are read as raw BORIM license numbers, not decrypted via a FIPA
#' crosswalk: verified empirically that the 2015-2018 files (unlike the
#' 2004-2014 era) already carry plain license numbers.
#'
#' @param con Open, writable DuckDB connection.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_hdd_discharge_physician_view <- function(con) {
  physician_cols <- c("OperatingPhysicianPrincipal",
                      paste0("OperatingPhysicianSignificant", 1:14))
  union_sql <- paste(
    sprintf("SELECT RecordType20ID, _data_year, TRY_CAST(%s AS BIGINT) AS borim_license FROM chia_casemix.v_hdd_discharge_all_years",
            physician_cols),
    collapse = "\nUNION ALL\n"
  )
  DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_physician AS
    SELECT DISTINCT RecordType20ID, _data_year, borim_license
    FROM (%s)
    WHERE borim_license IS NOT NULL
  ", union_sql))
  invisible(con)
}

#' Build the procedure-classification views (raw + specialty-share refined)
#'
#' Two passes, in dependency order:
#' 1. `v_hdd_discharge_procedure_raw`: a coding-system-level operative rule
#'    (ICD-9-CM category 01-86 for FY<=2015; ICD-10-PCS section 0 or 1 for
#'    FY2016+). Deliberately coarse -- both systems file bedside and
#'    endoscopic procedures under "surgical" categories despite not being OR
#'    surgery.
#' 2. `v_procedure_specialty_share` + the final `v_hdd_discharge_procedure`:
#'    empirically downgrades a raw-operative code to non_operative when fewer
#'    than half of its physician-attributed occurrences (among non-newborn
#'    discharges, `n >= 20`) are performed by a surgical-keyword specialty.
#'    Newborn discharges are excluded from the SHARE COMPUTATION itself (not
#'    just the final case count) because the delivering physician is recorded
#'    as operating physician on newborn stays, which would otherwise swamp a
#'    code's true performer mix -- confirmed empirically to wrongly downgrade
#'    genuinely-surgical newborn-adjacent codes (e.g. circumcision) if left in.
#'
#' Requires [build_chia_hdd_discharge_physician_view()] to have been run
#' first (joins against `chia_casemix.v_hdd_discharge_physician`) and a
#' `chia_provider.borim_stdrel_npi_straight_from_cd` table to exist.
#'
#' @param con Open, writable DuckDB connection.
#' @param min_n Minimum physician-attributed occurrences before a code's
#'   specialty share is trusted to override the raw classification.
#' @param surgical_share_threshold Below this `pct_surgical`, a raw-operative
#'   code is downgraded to non_operative.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_hdd_discharge_procedure_views <- function(con, min_n = 20,
                                                      surgical_share_threshold = 50.0) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_procedure_raw AS
    SELECT
      RecordType20ID,
      _data_year,
      NULLIF(NULLIF(trim(ProcedureCode), '-'), '') AS principal_procedure,
      CASE
        WHEN ProcedureCode IS NULL OR trim(ProcedureCode) IN ('-', '') THEN NULL
        WHEN _data_year <= 2015 THEN
          CASE WHEN TRY_CAST(LEFT(ProcedureCode, 2) AS INTEGER) BETWEEN 1 AND 86
               THEN 'operative' ELSE 'non_operative' END
        ELSE
          CASE WHEN LEFT(ProcedureCode, 1) IN ('0', '1')
               THEN 'operative' ELSE 'non_operative' END
      END AS procedure_class
    FROM chia_casemix.v_hdd_procedurecode_all_years
    WHERE AssociatedIndicator = 1
  ")

  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.v_procedure_specialty_share AS
    WITH attrib AS (
      SELECT p.principal_procedure AS code, b.specialty_1 AS specialty
      FROM chia_casemix.v_hdd_discharge_procedure_raw p
      JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
      JOIN chia_provider.borim_stdrel_npi_straight_from_cd b ON TRY_CAST(b.license AS BIGINT) = d.borim_license
      JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID, _data_year)
      WHERE p.procedure_class = 'operative' AND p.principal_procedure IS NOT NULL
        AND b.specialty_1 IS NOT NULL AND a.AdmissionType <> '4'
    )
    SELECT code, count(*) AS n,
           100.0 * sum(CASE WHEN regexp_matches(lower(specialty),
             '(surg|orthop|urolog|neuro|cardio|obstet|gyneco|transplant|otolar)')
             THEN 1 ELSE 0 END) / count(*) AS pct_surgical
    FROM attrib
    GROUP BY 1
  ")

  DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_procedure AS
    SELECT
      r.RecordType20ID, r._data_year, r.principal_procedure,
      CASE
        WHEN r.procedure_class = 'operative' AND s.n >= %d AND s.pct_surgical < %f
          THEN 'non_operative'
        ELSE r.procedure_class
      END AS procedure_class
    FROM chia_casemix.v_hdd_discharge_procedure_raw r
    LEFT JOIN chia_casemix.v_procedure_specialty_share s ON s.code = r.principal_procedure
  ", min_n, surgical_share_threshold))
  invisible(con)
}

#' Build the canonical HDD discharge view (demographics + is_surgical)
#'
#' Requires [build_chia_hdd_discharge_procedure_views()] and
#' [build_chia_hdd_procedure_family_view()] to have been run first.
#'
#' `procedure_family` is a `LEFT JOIN`, deliberately optional: most discharges
#' are not urogynecologic and correctly get `NULL`, matching how
#' `build_chia_inpatient_urps_series()` already filters on
#' `procedure_family IS NOT NULL`.
#'
#' @param con Open, writable DuckDB connection.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_hdd_discharge_canonical_view <- function(con) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_canonical AS
    SELECT
      c.RecordType20ID,
      c._data_year,
      TRY_CAST(c.AgeLDS AS INTEGER) AS age,
      c.SexLDS AS sex,
      c.Race1 AS race_primary,
      c.IdOrgSite AS org_site,
      c.IdOrgFiler AS org_filer,
      TRY_CAST(c.TotalChargesSpecial AS DOUBLE) AS charges_special,
      c.AdmissionType,
      (p.procedure_class = 'operative') AS is_surgical,
      f.procedure_family
    FROM chia_casemix.v_hdd_discharge_all_years c
    LEFT JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
    LEFT JOIN chia_casemix.v_hdd_discharge_procedure_family f USING (RecordType20ID, _data_year)
  ")
  invisible(con)
}

#' Build surgeon_year_volume and its completeness view
#'
#' Requires [build_chia_hdd_discharge_physician_view()],
#' [build_chia_hdd_discharge_procedure_views()], and
#' [build_chia_hdd_discharge_canonical_view()] to have been run first.
#'
#' @param con Open, writable DuckDB connection.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_surgeon_year_volume_views <- function(con) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.surgeon_year_volume AS
    SELECT
      b.NPI AS npi,
      c._data_year AS fy,
      MAX(b.specialty_1) AS borim_specialty,
      count(*) FILTER (WHERE p.procedure_class = 'operative' AND c.AdmissionType <> '4') AS operative_cases,
      count(*) FILTER (WHERE p.procedure_class = 'operative' AND c.AdmissionType = '4')  AS newborn_cases
    FROM chia_casemix.v_hdd_discharge_canonical c
    JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
    JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
    JOIN chia_provider.borim_stdrel_npi_straight_from_cd b
      ON TRY_CAST(b.license AS BIGINT) = d.borim_license
    WHERE b.NPI IS NOT NULL AND trim(b.NPI) <> ''
    GROUP BY 1, 2
  ")

  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.v_surgeon_year_completeness AS
    WITH op AS (
      SELECT c._data_year AS fy, c.RecordType20ID,
             bool_or(d.borim_license IS NOT NULL) AS attributed
      FROM chia_casemix.v_hdd_discharge_canonical c
      LEFT JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
      WHERE c.is_surgical
      GROUP BY 1, 2
    )
    SELECT fy,
           round(100.0 * sum(CASE WHEN attributed THEN 1 ELSE 0 END) / count(*), 1) AS pct_attributed,
           NULL AS dark_sites
    FROM op
    GROUP BY 1
    ORDER BY 1
  ")
  invisible(con)
}

#' Build the full CHIA physician-attribution layer
#'
#' Orchestrates [build_chia_hdd_discharge_physician_view()],
#' [build_chia_hdd_discharge_procedure_views()],
#' [build_chia_hdd_diagnosis_long_view()],
#' [build_chia_hdd_procedure_family_view()],
#' [build_chia_hdd_discharge_canonical_view()], and
#' [build_chia_surgeon_year_volume_views()] in dependency order. Requires
#' `chia_casemix.v_hdd_discharge_all_years` / `v_hdd_procedurecode_all_years`
#' / `v_hdd_diagnosiscode_all_years` (built by `scripts/chia/finalize_db.py`)
#' and `chia_provider.borim_stdrel_npi_straight_from_cd` to already exist on
#' `con`.
#'
#' @param con Open, writable DuckDB connection.
#' @param procedure_family_config Passed through to
#'   [build_chia_hdd_procedure_family_view()] as `config`. Exposed so tests
#'   can inject a small fixture config instead of reading
#'   `config/chia_urps_inpatient_codes.yml` (a path relative to the package
#'   root, which does not resolve under testthat's working directory).
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_physician_attribution <- function(
    con,
    procedure_family_config = yaml::read_yaml("config/chia_urps_inpatient_codes.yml")) {
  build_chia_hdd_discharge_physician_view(con)
  build_chia_hdd_discharge_procedure_views(con)
  build_chia_hdd_diagnosis_long_view(con)
  build_chia_hdd_procedure_family_view(con, config = procedure_family_config)
  build_chia_hdd_discharge_canonical_view(con)
  build_chia_surgeon_year_volume_views(con)
  invisible(con)
}
